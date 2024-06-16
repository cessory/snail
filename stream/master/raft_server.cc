#include "raft_server.h"

namespace snail {
namespace stream {

static bool VerifyRaftServerOption(const RaftServerOption& opt) {
    if (opt.node_id == 0) {
        LOG_ERROR("invalid node_id={} in RaftServerOption", opt.node_id);
        return false;
    }

    if (opt.raft_port == 0) {
        LOG_ERROR("raft_port={} in RaftServerOption is invalid", opt.raft_port);
        return false;
    }

    if (opt.tick_interval == 0) {
        LOG_ERROR("tick_interval={} in RaftServerOption is invalid",
                  opt.tick_interval);
        return false;
    }

    if (opt.heartbeat_tick == 0) {
        LOG_ERROR("heartbeat_tick={} in RaftServerOption is invalid",
                  opt.heartbeat_tick);
        return false;
    }
    if (opt.election_tick <= opt.heartbeat_tick) {
        LOG_ERROR("election_tick={} in RaftServerOption is invalid",
                  opt.election_tick);
        return false;
    }
    if (opt.wal_dir.empty()) {
        LOG_ERROR("wal_dir in RaftServerOption is empty");
        return false;
    }
    return true;
}

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

seastar::future<> RaftServer::HandleSnapshotMsg(raft::MessagePtr msg) {
    auto snap = msg->snapshot;
    const Buffer& data = snap->data();
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

    auto s = co_await sender_->SendSnapshot(msg, sm_snap);
    if (!s) {
        LOG_ERROR("send snapshot {} to raft node {} error: {}", payload.name(),
                  msg->to, s);
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
            (void)HandleSnapshotMsg(std::move(msgs[i]));
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
                store_->SetConfState(snap->metadata().conf_state());
                SnapshotMetaPayload payload;
                payload.ParseFromArray(snap->data().get(), snap->data().size());
                int n = payload.nodes_size();
                std::vector<RaftNode> nodes;
                for (int i = 0; i < n; ++i) {
                    nodes.push_back(payload.nodes(i));
                }
                store_->UpdateRaftNodes(nodes);
                // TODO update store and sender
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

void RaftServer::HandleRecvMsg(raft::MessagePtr msg) {
    recv_pending_.push_back(msg);
    cv_.signal();
}

seastar::future<Status<>> RaftServer::ApplySnapshot(raft::SnapshotPtr meta,
                                                    raft::SmSnapshotPtr body) {
    return sm_->ApplySnapshot(meta, body);
}

void RaftServer::ApplySnapshotCompleted(raft::MessagePtr msg) {
    recv_pending_.push_back(msg);
    cv_.signal();
}

seastar::future<Status<RaftServerPtr>> RaftServer::Create(
    const RaftServerOption opt, uint64_t applied, std::vector<RaftNode> nodes,
    raft::StatemachinePtr sm) {
    Status<RaftServerPtr> s;
    if (!VerifyRaftServerOption(opt)) {
        s.Set(EINVAL);
        co_return s;
    }
    if (nodes.empty() || !sm) {
        LOG_ERROR("nodes is empty or Statemachine is null");
        s.Set(EINVAL);
        co_return s;
    }

    RaftServerPtr raft_ptr = seastar::make_lw_shared<RaftServer>(RaftServer());

    // open wal log
    auto st = co_await RaftWalFactory::Create(opt.wal_dir);
    if (!st) {
        LOG_ERROR("create raft wal factory error: {}", st);
        s.Set(st.Code(), st.Reason());
        co_return s;
    }
    raft_ptr->wal_factory_ = std::move(st.Value());
    auto st1 = co_await raft_ptr->wal_factory_->OpenRaftWal(0);
    if (!st1) {
        LOG_ERROR("create raft wal error: {}", st1);
        s.Set(st1.Code(), st1.Reason());
        co_return s;
    }
    raft_ptr->sender_ = seastar::make_lw_shared<RaftSender>();

    raft::ConfState cs;
    seastar::shared_ptr<RaftStorage> store = seastar::make_shared<RaftStorage>(
        cs, applied, std::move(st1.Value()), sm);

    std::vector<uint64_t> voters;
    std::vector<uint64_t> learners;
    std::string local_raft_host;
    uint16_t local_raft_port;
    for (int i = 0; i < nodes.size(); ++i) {
        if (nodes[i].learner()) {
            learners.push_back(nodes[i].node_id());
        } else {
            voters.push_back(nodes[i].node_id());
        }
        raft_ptr->sender_->AddRaftNode(nodes[i].node_id(), nodes[i].raft_host(),
                                       nodes[i].raft_port());
        RaftNodePtr raft_node_ptr = seastar::make_lw_shared<RaftNode>();
        raft_node_ptr->set_node_id(nodes[i].node_id());
        raft_node_ptr->set_raft_host(nodes[i].raft_host());
        raft_node_ptr->set_raft_port(nodes[i].raft_port());
        raft_node_ptr->set_host(nodes[i].host());
        raft_node_ptr->set_port(nodes[i].port());
        raft_node_ptr->set_learner(nodes[i].learner());
        store->AddRaftNode(raft_node_ptr);
        if (nodes[i].node_id() == opt.node_id) {
            local_raft_host = nodes[i].raft_host();
            local_raft_port = nodes[i].raft_port();
        }
    }
    // update ConfState
    cs.set_voters(std::move(voters));
    cs.set_learners(std::move(learners));
    store->SetConfState(cs);

    raft_ptr->receiver_ = seastar::make_lw_shared<RaftReceiver>(
        local_raft_host, local_raft_port,
        [ptr = raft_ptr.get()](raft::MessagePtr msg) {
            ptr->HandleRecvMsg(msg);
        },
        [ptr = raft_ptr.get()](raft::SnapshotPtr meta, raft::SmSnapshotPtr body)
            -> seastar::future<Status<>> {
            return ptr->ApplySnapshot(meta, body);
        });

    raft_ptr->opt_ = opt;
    raft_ptr->node_id_ = opt.node_id;

    raft::Raft::Config cfg;
    cfg.id = opt.node_id;
    cfg.election_tick = opt.election_tick;
    cfg.heartbeat_tick = opt.heartbeat_tick;
    cfg.storage = store;
    auto st = co_await raft::RawNode::Create(cfg);
    if (!st) {
        LOG_ERROR("create raft raw node error: {}", st);
        s.Set(st.Code(), st.Reason());
        co_return s;
    }
    raft_ptr->raft_node_ = std::move(st.Value());
    raft_ptr->receiver_->Start();
    s.SetValue(raft_ptr);
    co_return s;
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
