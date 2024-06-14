#include "raft_server.h"

namespace snail {
namespace stream {

seastar::future<> RaftServer::HandlePropose() {
    std::vector<seastar::promise<Status<>>> promise_vec;
    auto msg = make_raft_message();
    msg->type = MessageType::MsgProp;
    while (!propose_pending_.empty()) {
        ProposeEntry& entry = propose_pending_.front();
        EntryPtr ent = make_entry();
        ent->set_data(std::move(entry.b));
        promise_vec.emplace_back(std::move(entry.pr));
        propose_pending_.pop();
        msg->entries.emplace_back(std::move(ent));
    }
    auto s = co_await raft_node_->Step(msg);
    for (int i = 0; i < promise_vec.size(); ++i) {
        auto st = s;
        promise_vec[i].set_value(std::move(st));
    }
    co_return;
}

seastar::future<> RaftServer::handleConfChange(EntryPtr ent) {
    ConfChange cc;
    RaftNode rn;
    Status<> s;
    if (!cc.Unmarshal(ent->data().share())) {
        LOG_FATAL("unmarshal confchange error");
    }
    if (!cc.context().empty()) {
        if (!rn.ParseFromArray(cc.context().get(), cc.context().size())) {
            LOG_FATAL("parse conf change context error");
        }
    }
    auto type = cc.type();
    s = co_await sm_->ApplyConfChange(type, cc.node_id(), rn.raft_host,
                                      rn.raft_port, rn.host, rn.port,
                                      ent->index());
    if (!s) {
        LOG_FATAL("apply conf change error: {}", s);
    }
    conf_change_.push(ent);
    cv_.signal();
    co_return;
}

void RaftServer::ReportSnapshot(raft::SnapshotStatus status) {
    raft::MessagePtr msg = raft::make_raft_message();
    msg->type = raft::MessageType::MsgSnapStatus;
    msg->from = node_id_;
    msg->Reject = status == raft::SnapshotStatus::SnapshotFailure;
    recv_pending_.push_back(msg);
    cv_.signal();
}

seastar::future<> RaftServer::ApplyLoop() {
    while (!gate_.is_closed()) {
        while (!apply_pending_.empty()) {
            std::vector<seastar::future<Status<>>> fu_vec;
            auto rd = apply_pending_.front();
            apply_pending_.pop();
            auto entries = rd->GetEntries();
            for (int i = 0; i < entries.size(); i++) {
                // if entry index is smaller than applied, application should
                // ignore this apply
                if (store_->Applied() >= entries[i].index()) {
                    continue;
                }
                auto type = entries[i]->type();
                if (type == EntryType::EntryNormal) {
                    auto fu = sm_->Apply(entries[i]->data().share(),
                                         entries[i]->index());
                    fu_vec.emplace_back(std::move(fu));
                } else if (type == EntryType::EntryConfChange) {
                    if (!fu_vec.empty()) {
                        auto results = co_await seastar::when_all_succeed(
                            fu_vec.begin(), fu_vec.end());
                        for (int j = 0; j < results.size(); ++j) {
                            if (!results[j]) {
                                LOG_FATAL("apply raft entry error: {}",
                                          results[j]);
                            }
                        }
                        fu_vec.clear();
                    }
                    auto fu = handleConfChange(entries[i]);
                    fu_vec.emplace_back(std::move(fu));
                } else if (type == EntryType::EntryConfChangeV2) {
                    // ignore this type
                } else {
                    LOG_FATAL("not support EntryType={}",
                              static_cast<int>(type));
                }
            }
            co_await seastar::when_all_succeed(fu_vec.begin(), fu_vec.end());
            apply_completed_.push(rd);
            cv_.signal();
            apply_sem_.signal();
        }
        apply_cv_.wait();
    }
    co_return;
}

seastar::future<> RaftServer::handleSnapshotMsg(raft::MessagePtr msg) {
    auto snap = msg->snapshot;
    const Buffer& data = snap->metadata().data();
    SnapshotMetaPayload payload;
    if (!payload.ParseFromArray(data.get(), data.size())) {
        LOG_ERROR("parse snapshot meta payload error");
        ReportSnapshot(raft::SnapshotStatus::SnapshotFailure);
        co_return;
    }

    auto sm_snap = store_->GetSnapshot(payload.name());
    if (!sm_snap) {
        LOG_ERROR("not get snapshot {}", payload.name());
        ReportSnapshot(raft::SnapshotStatus::SnapshotFailure);
        store_->ReleaseSnapshot(payload.name());
        co_return;
    }

    auto s = co_await sender_->SendSnapshot(msg->to, snap, sm_snap);
    if (!s) {
        LOG_ERROR("not get snapshot {} to raft node {} error: {}",
                  payload.name(), msg->to, s);
        ReportSnapshot(raft::SnapshotStatus::SnapshotFailure);
        store_->ReleaseSnapshot(payload.name());
        co_return;
    }

    ReportSnapshot(raft::SnapshotStatus::SnapshotFinish);
    store_->ReleaseSnapshot(payload.name());
    co_return;
}

seastar::future<> RaftServer::HandleReady(raft::ReadyPtr rd) {
    auto msgs = rd->GetSendMsgs();
    bool sent_app_resp = false;
    for (int i = msgs.size() - 1; i >= 0; --i) {
        if (msgs[i]->type == MessageType::MsgAppResp) {
            if (sent_app_resp) {
                msgs[i]->to = 0;
            } else {
                sent_app_resp = true;
            }
        }

        if (msgs[i]->type == MessageType::MsgSnap) {
            (void)handleSnapshotMsg(std::move(msgs[i]));
            msgs[i] = nullptr;
        }
    }
    auto fu1 = sender_->Send(std::move(msgs));
    auto fu2 = store_->Save(std::move(rd->GetHardState()),
                            std::move(rd->GetEntries()));
    co_await ready_sem_.wait();
    ready_pending_.push(rd);
    co_await seastar::when_all_succeed(std::move(fu1), std::move(fu2));
    co_return;
}

seastar::future<> RaftServer::Run() {
    seastar::timer<seastar::steady_clock_type> tick_timer([this] {
        tick_ = true;
        cv_.signal();
    });

    tick_timer.rearm_periodic(
        std::chrono::seconds(opt_.heartbeat_tick * opt_.tick_interval));
    while (!gate_.is_closed()) {
        if (!raft_node_->HasReady() && propose_pending_.empty() &&
            apply_completed_.empty() && recv_pending_.empty() &&
            conf_change_.empty() && !pending_release_index_ && !tick_) {
            co_await cv_.wait();
            continue;
        }
        if (tick_) {
            tick_.reset();
            auto s = co_await raft_node_->Tick();
            if (!s) {
                LOG_FATAL("raft tick error: {}", s);
            }
        }
        if (!propose_pending_.empty()) {
            co_await HandlePropose();
        }

        if (raft_node_->HasReady()) {
            auto rd = co_await raft_node_->GetReady();
            co_await HandleReady(rd);
        }

        while (!conf_change_.empty()) {
            auto ent = conf_change_.first();
            conf_change_.pop();
            ConfChange cc;
            RaftNodePtr rn = seastar::make_lw_shared<RaftNode>();
            if (!cc.Unmarshal(ent->data().share())) {
                LOG_FATAL("unmarshal confchange error");
            }
            if (!cc.context().empty()) {
                if (!rn->ParseFromArray(cc.context().get(),
                                        cc.context().size())) {
                    LOG_FATAL("parse conf change context error");
                }
            }
            auto type = cc.type();
            switch (type) {
                case ConfChangeType::ConfChangeAddNode:
                case ConfChangeType::ConfChangeAddLearnerNode:
                    sender_->AddRaftNode(cc.node_id,
                                         rn.raft_host().rn.raft_port());
                    store_->AddRaftNode(rn);
                    break;
                case ConfChangeType::ConfChangeRemoveNode:
                    store_->RemoveRaftNode(cc.node_id);
                    co_await sender_->RemoveRaftNode(cc.node_id);
                    break;
            }
            auto st = co_await raft_node_->ApplyConfChange(ConfChangeI(cc));
            if (!st) {
                LOG_FATAL("raft node apply conf change error: {}", st);
            }
            store_->SetConfState(st.Value());
        }

        while (!recv_pending_.empty()) {
            auto msg = recv_pending_.front();
            recv_pending_.pop();
            auto s = co_await raft_node_->Step(msg);
            if (s.Code() == ErrCode::ErrRaftAbort) {
                LOG_FATAL("recv step error: raft node has already abort");
            }
        }

        while (!apply_completed_.empty()) {
            auto rd = apply_completed_.front();
            apply_completed_.pop();
            auto entries = rd->GetEntries();
            if (!entries.empty()) {
                store_->SetApplied(entries.back()->index());
            }

            auto snap = rd->GetSnapshot();
            if (snap) {
                auto s = co_await store_->ApplySnapshot(
                    snap->metadata().index(), snap->metadata().term());
                if (!s) {
                    LOG_FATAL("apply raft snapshot error: {}", s);
                }
            }
            auto s = co_await raft_node_->Advance(rd);
            if (!s) {
                LOG_FATAL("raft node advance error: {}", s);
            }
        }

        if (pending_release_index_) {
            uint64_t index = pending_release_index_.value();
            pending_release_index_.reset();
            auto s = co_await store_->Release(index);
            if (!s) {
                LOG_FATAL("release raft wal error: {}, index={}", s, index);
            }
        }
    }
}

seastar::future<Status<RaftServerPtr>> RaftServer::Create(
    const RaftServerOption opt, uint64_t applied, std::vector<RaftNode> nodes,
    raft::StatemachinePtr sm) {
    Status<RaftServerPtr> s;
    RaftServerPtr ptr = seastar::make_lw_shared<RaftServer>(RaftServer());

    ptr->opt_ = opt;
    ptr->node_id_ = opt.node_id;

    raft::Raft::Config cfg;
    auto st = co_await raft::RawNode::Create(cfg);
}

seastar::future<Status<>> RaftServer::Propose(Buffer b) {
    Status<> s;
    if (raft_node_->HasAbort()) {
        LOG_FATAL("propose error: raft node has already abort");
    }
    auto s = co_await raft_node_->Propose(std::move(b));
    if (!s) {
        if (s.Code() == ErrCode::ErrRaftAbort) {
            LOG_FATAL("propose error: raft node has already abort");
        }
        co_return s;
    }
    cv_.signal();
    co_return s;
}

}  // namespace stream
}  // namespace snail
