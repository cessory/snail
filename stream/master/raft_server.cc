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
    if (!cc.Unmarshal(ent->data().share())) {
        LOG_FATAL("unmarshal confchange error");
    }
    auto type = cc.type();
    switch (type) {
        case ConfChangeType::ConfChangeAddNode:
        case ConfChangeType::ConfChangeAddLearnerNode:
            RaftNode rn;
            co_await sender_->AddRaftNode();  // TODO
    }
    co_return;
}

seastar::future<> RaftServer::handleApply() {
    while (!ready_pending_.empty()) {
        std::vector<seastar::future<Status<>>> fu_vec;
        auto rd = ready_pending_.front();
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
            } else {
                LOG_FATAL("not support EntryType={}", static_cast<int>(type));
            }
        }

        ready_pending_.pop();
    }
    co_return;
}

seastar::future<> RaftServer::handleReady() {
    if (!raft_node_->HasReady()) {
        co_return;
    }

    auto rd = co_await raft_node_->GetReady();
    auto ft1 = sender_->Send(rd->GetSendMsgs());
    auto ft2 = store_->Save(std::move(rd->GetHardState()),
                            std::move(rd->GetEntries()));
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
