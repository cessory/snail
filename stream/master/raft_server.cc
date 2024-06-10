#include "raft_server.h"

namespace snail {
namespace stream {

seastar::future<> RaftServer::handlePropose() {
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

seastar::future<> RaftServer::ApplyLoop() {
    while (!ready_pending_.empty()) {
        std::vector<seastar::future<Status<>>> fu_vec;
        auto rd = ready_pending_.front();
        ready_pending_.pop();
        auto entries = rd->GetEntries();
        auto snapshot = rd->GetSnapshot();
        for (int i = 0; i < entries.size(); i++) {
            // if entry index is smaller than applied, application should ignore
            // this apply
            auto type = entries[i]->type();
            if (type == EntryType::EntryNormal) {
                auto fu =
                    sm_->Apply(entries[i]->data().share(), entries[i]->index());
                fu_vec.emplace_back(std::move(fu));
            } else if (type == EntryType::EntryConfChange) {
                if (!fu_vec.empty()) {
                    auto results = co_await seastar::when_all_succeed(
                        fu_vec.begin(), fu_vec.end());
                    for (int j = 0; j < results.size(); ++j) {
                        if (!results[j]) {
                            LOG_FATAL("apply raft entry error: {}", results[j]);
                        }
                    }
                    fu_vec.clear();
                }
                auto fu = handleConfChange(entries[i]);
                fu_vec.emplace_back(std::move(fu));
            } else {
                LOG_FATAL("not support EntryType={}", static_cast<int>(type));
            }
        }
        if (snapshot) {
        }
        co_await seastar::when_all_succeed(fu_vec.begin(), fu_vec.end());
        ready_completed_.push(rd);
        cv_.signal();
        ready_sem_.signal();
    }
    co_return;
}

seastar::future<> RaftServer::handleReady(ReadyPtr rd) {
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
    while (!gate_.is_closed()) {
        if (!raft_node_->HasReady() && propose_pending_.empty() &&
            ready_pending_.empty() && ready_completed_.empty() &&
            recv_pending_.empty()) {
            co_await cv_.wait();
            continue;
        }
        if (!propose_pending_.empty()) {
            co_await handlePropose();
        }

        if (raft_node_->HasReady()) {
            auto rd = co_await raft_node_->GetReady();
            co_await handleReady(rd);
        }

        while (!conf_change_.empty()) {
            auto ent = conf_change_.first();
            conf_change_.pop();
            ConfChange cc;
            RaftNode rn;
            if (!cc.Unmarshal(ent->data().share())) {
                LOG_FATAL("unmarshal confchange error");
            }
            if (!cc.context().empty()) {
                if (!rn.ParseFromArray(cc.context().get(),
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
                    break;
                case ConfChangeType::ConfChangeRemoveNode:
                    co_await sender_->RemoveRaftNode(cc.node_id);
            }
            auto st = co_await raft_node_->ApplyConfChange(ConfChangeI(cc));
            if (!st) {
                LOG_FATAL("raft node apply conf change error: {}", st);
            }
        }

        while (!recv_pending_.empty()) {
            auto msg = recv_pending_.front();
            recv_pending_.pop();
            auto s = co_await raft_node_->Step(msg);
            if (s.Code() == ErrCode::ErrRaftAbort) {
                LOG_FATAL("recv step error: raft node has already abort");
            }
        }

        while (!ready_completed_.empty()) {
            auto& rd = ready_completed_.front();
            auto s = co_await raft_node_->Advance(rd);
            if (!s) {
                LOG_FATAL("raft node advance error: {}", s);
            }
            ready_completed_.pop();
        }
    }
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
