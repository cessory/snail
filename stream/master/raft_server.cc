#include "raft_server.h"

#include <seastar/core/when_all.hh>
#include <seastar/util/defer.hh>

#include "net/byteorder.h"
#include "util/logger.h"

namespace snail {
namespace stream {

static bool VerifyRaftServerOption(const RaftServerOption& opt) {
    if (opt.node_id == 0) {
        LOG_ERROR("invalid node_id={} in RaftServerOption", opt.node_id);
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
    std::vector<ProposeEntry*> propose_entries;
    raft::MessagePtr msg = raft::make_raft_message();
    while (!propose_pending_.empty()) {
        ProposeEntry* entry = propose_pending_.front();
        propose_pending_.pop();
        msg->entries.push_back(entry->ent);
        propose_entries.push_back(entry);
    }
    auto s = co_await raft_node_->RawStep(msg);
    for (int i = 0; i < propose_entries.size(); ++i) {
        auto st = s;
        propose_entries[i]->pr.set_value(std::move(st));
    }
    co_return;
}

seastar::future<Status<>> RaftServer::HandleConfChange(raft::EntryPtr ent) {
    raft::ConfChange cc;
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
    s = co_await sm_->ApplyConfChange(type, cc.node_id(), rn.raft_host(),
                                      rn.raft_port(), rn.host(), rn.port(),
                                      ent->index());
    if (!s) {
        LOG_FATAL("apply conf change error: {}", s);
    }
    conf_change_.push(ent);
    cv_.signal();
    co_return s;
}

void RaftServer::ReportSnapshot(raft::SnapshotStatus status) {
    raft::MessagePtr msg = raft::make_raft_message();
    msg->type = raft::MessageType::MsgSnapStatus;
    msg->from = node_id_;
    msg->reject = status == raft::SnapshotStatus::SnapshotFailure;
    recv_pending_.push(msg);
    cv_.signal();
}

seastar::future<> RaftServer::ApplyLoop() {
    seastar::gate::holder holder(gate_);
    while (!gate_.is_closed()) {
        while (!apply_pending_.empty()) {
            auto rd = apply_pending_.front();
            apply_pending_.pop();
            auto entries = rd->GetCommittedEntries();
            std::vector<Buffer> data_vec;
            uint64_t index = 0;

            for (int i = 0; i < entries.size(); i++) {
                // if entry index is smaller than applied, application should
                // ignore this apply
                if (store_->Applied() >= entries[i]->index()) {
                    continue;
                }
                auto type = entries[i]->type();
                if (type == raft::EntryType::EntryNormal) {
                    data_vec.emplace_back(
                        std::move(entries[i]->data().share()));
                    index = entries[i]->index();
                } else if (type == raft::EntryType::EntryConfChange) {
                    if (!data_vec.empty()) {
                        auto s =
                            co_await sm_->Apply(std::move(data_vec), index);
                        if (!s) {
                            LOG_FATAL("apply raft entry error: {}", s);
                        }
                    }
                    index = entries[i]->index();
                    co_await HandleConfChange(entries[i]);
                } else if (type == raft::EntryType::EntryConfChangeV2) {
                    // ignore this type
                    index = entries[i]->index();
                } else {
                    LOG_FATAL("not support EntryType={}",
                              static_cast<int>(type));
                }
            }
            if (!data_vec.empty()) {
                auto s = co_await sm_->Apply(std::move(data_vec), index);
                if (!s) {
                    LOG_FATAL("apply raft entry error: {}", s);
                }
            }
            if (index > 0) {
                store_->SetApplied(index);
                apply_waiter_.broadcast();
            }

            auto snap = rd->GetSnapshot();
            if (snap) {
                auto s = co_await store_->ApplySnapshot(
                    snap->metadata().index(), snap->metadata().term());
                if (!s) {
                    LOG_FATAL("apply raft snapshot error: {}", s);
                }
                apply_waiter_.broadcast();
                store_->SetConfState(snap->metadata().conf_state());
                SnapshotMetaPayload payload;
                payload.ParseFromArray(snap->data().get(), snap->data().size());
                int n = payload.nodes_size();
                std::vector<RaftNode> nodes;
                for (int i = 0; i < n; ++i) {
                    nodes.push_back(payload.nodes(i));
                }
                // update store and sender
                store_->UpdateRaftNodes(nodes);
                co_await sender_->UpdateRaftNodes(nodes);
            }
            apply_sem_.signal();
        }
        co_await apply_cv_.wait();
    }
    co_return;
}

seastar::future<> RaftServer::SendSnapshotMsg(raft::MessagePtr msg) {
    if (gate_.is_closed()) {
        co_return;
    }
    seastar::gate::holder holder(gate_);
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
    const std::vector<raft::ReadState>& read_states = rd->GetReadStates();
    auto soft_state = rd->GetSoftState();
    if (soft_state && (*soft_state).lead != lead_) {
        lead_ = (*soft_state).lead;
        sm_->LeadChange(lead_);
    }
    if (!read_states.empty()) {
        uint64_t id =
            net::BigEndian::Uint64(read_states.back().request_ctx.get());
        if (read_index_ && (*read_index_)->id == id) {
            Status<uint64_t> s;
            s.SetValue(read_states.back().index);
            (*read_index_)->pr.set_value(s);
            (*read_index_)->timer.cancel();
            read_index_.reset();
        }
    }
    auto msgs = rd->GetSendMsgs();
    bool sent_app_resp = false;
    for (int i = msgs.size() - 1; i >= 0; --i) {
        if (msgs[i]->type == raft::MessageType::MsgAppResp) {
            if (sent_app_resp) {
                msgs[i]->to = 0;
            } else {
                sent_app_resp = true;
            }
        }

        if (msgs[i]->type == raft::MessageType::MsgSnap) {
            (void)SendSnapshotMsg(std::move(msgs[i]));
            msgs[i] = nullptr;
        }
    }
    auto fu1 = sender_->Send(std::move(msgs));
    auto entries = rd->GetEntries();
    auto fu2 = store_->Save(std::move(entries), rd->GetHardState());
    co_await apply_sem_.wait();
    apply_pending_.push(rd);
    apply_cv_.signal();
    co_await seastar::when_all_succeed(std::move(fu1), std::move(fu2));
    auto s = co_await raft_node_->Advance(rd);
    if (!s) {
        LOG_FATAL("raft node advance error: {}", s);
    }
    co_return;
}

seastar::future<> RaftServer::Run() {
    seastar::timer<seastar::steady_clock_type> tick_timer([this] {
        tick_ = true;
        cv_.signal();
    });
    seastar::gate::holder holder(gate_);
    tick_timer.rearm_periodic(
        std::chrono::seconds(opt_.heartbeat_tick * opt_.tick_interval));

    auto defer = seastar::defer([this, &tick_timer] {
        tick_timer.cancel();
        raft_node_->Close();
    });

    while (!gate_.is_closed()) {
        if (!raft_node_->HasReady() && propose_pending_.empty() &&
            recv_pending_.empty() && conf_change_.empty() &&
            !pending_release_wal_ && !tick_) {
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
            auto s = co_await raft_node_->GetReady();
            if (!s) {
                break;
            }
            co_await HandleReady(s.Value());
        }

        while (!conf_change_.empty()) {
            auto ent = conf_change_.front();
            conf_change_.pop();
            raft::ConfChange cc;
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
            LOG_INFO("begin conf change......");
            switch (type) {
                case raft::ConfChangeType::ConfChangeAddNode:
                case raft::ConfChangeType::ConfChangeAddLearnerNode:
                    sender_->AddRaftNode(cc.node_id(), rn->raft_host(),
                                         rn->raft_port());
                    store_->AddRaftNode(rn);
                    break;
                case raft::ConfChangeType::ConfChangeRemoveNode:
                    store_->RemoveRaftNode(cc.node_id());
                    co_await sender_->RemoveRaftNode(cc.node_id());
                    break;
            }
            auto st =
                co_await raft_node_->ApplyConfChange(raft::ConfChangeI(cc));
            if (!st) {
                LOG_FATAL("raft node apply conf change error: {}", st);
            }
            store_->SetConfState(st.Value());
        }

        while (!recv_pending_.empty()) {
            auto msg = recv_pending_.front();
            recv_pending_.pop();
            auto s = co_await raft_node_->RawStep(msg);
            if (s.Code() == ErrCode::ErrRaftAbort) {
                LOG_FATAL("recv step error: raft node has already abort");
            }
        }

        if (pending_release_wal_) {
            uint64_t index = pending_release_wal_.value();
            pending_release_wal_.reset();
            auto s = co_await store_->Release(index);
            if (!s) {
                LOG_FATAL("release raft wal error: {}, index={}", s, index);
            }
        }
    }
    co_return;
}

void RaftServer::HandleRecvMsg(raft::MessagePtr msg) {
    recv_pending_.push(msg);
    cv_.signal();
}

seastar::future<Status<>> RaftServer::ApplySnapshot(raft::SnapshotPtr meta,
                                                    SmSnapshotPtr body) {
    return sm_->ApplySnapshot(meta, body);
}

void RaftServer::ApplySnapshotCompleted(raft::MessagePtr msg) {
    recv_pending_.push(msg);
    cv_.signal();
}

seastar::future<Status<RaftServerPtr>> RaftServer::Create(
    const RaftServerOption opt, uint64_t applied, std::vector<RaftNode> nodes,
    StatemachinePtr sm) {
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
    raft_ptr->sm_ = sm;
    struct timeval tv;
    gettimeofday(&tv, NULL);
    raft_ptr->read_index_seq_ = tv.tv_sec * 1000 + tv.tv_usec / 1000;
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
        cs, applied, std::move(st1.Value()), sm.get());

    std::vector<uint64_t> voters;
    std::vector<uint64_t> learners;
    std::string local_raft_host;
    uint16_t local_raft_port;
    for (int i = 0; i < nodes.size(); ++i) {
        if (nodes[i].learner()) {
            learners.push_back(nodes[i].id());
        } else {
            voters.push_back(nodes[i].id());
        }
        raft_ptr->sender_->AddRaftNode(nodes[i].id(), nodes[i].raft_host(),
                                       nodes[i].raft_port());
        RaftNodePtr raft_node_ptr = seastar::make_lw_shared<RaftNode>();
        raft_node_ptr->set_id(nodes[i].id());
        raft_node_ptr->set_raft_host(nodes[i].raft_host());
        raft_node_ptr->set_raft_port(nodes[i].raft_port());
        raft_node_ptr->set_host(nodes[i].host());
        raft_node_ptr->set_port(nodes[i].port());
        raft_node_ptr->set_learner(nodes[i].learner());
        store->AddRaftNode(raft_node_ptr);
        if (nodes[i].id() == opt.node_id) {
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
        [ptr = raft_ptr.get()](raft::SnapshotPtr meta, SmSnapshotPtr body)
            -> seastar::future<Status<>> {
            return ptr->ApplySnapshot(meta, body);
        });

    raft_ptr->opt_ = opt;
    raft_ptr->node_id_ = opt.node_id;

    raft::Raft::Config cfg;
    cfg.id = opt.node_id;
    cfg.election_tick = opt.election_tick;
    cfg.heartbeat_tick = opt.heartbeat_tick;
    cfg.storage =
        seastar::dynamic_pointer_cast<raft::Storage, RaftStorage>(store);
    auto st2 = co_await raft::RawNode::Create(cfg);
    if (!st2) {
        LOG_ERROR("create raft raw node error: {}", st2);
        s.Set(st2.Code(), st2.Reason());
        co_return s;
    }
    raft_ptr->store_ = store;
    raft_ptr->raft_node_ = std::move(st2.Value());
    s.SetValue(raft_ptr);
    co_return s;
}

seastar::future<> RaftServer::Start() {
    auto f1 = Run();
    auto f2 = ApplyLoop();
    auto f3 = receiver_->Start();
    co_await seastar::when_all_succeed(std::move(f1), std::move(f2),
                                       std::move(f3));
    co_return;
}

seastar::future<Status<>> RaftServer::Propose(Buffer b) {
    Status<> s;
    if (raft_node_->HasAbort()) {
        s.Set(ErrCode::ErrRaftAbort);
        co_return s;
    }
    if (gate_.is_closed()) {
        s.Set(EPIPE);
        co_return s;
    }
    raft::EntryPtr ent = raft::make_entry();
    ent->set_type(raft::EntryType::EntryNormal);
    ent->set_data(std::move(b));
    std::unique_ptr<ProposeEntry> pe = std::make_unique<ProposeEntry>();
    pe->ent = ent;
    propose_pending_.push(pe.get());
    cv_.signal();
    s = co_await pe->pr.get_future();
    co_return s;
}

seastar::future<Status<>> RaftServer::AddRaftNode(
    uint64_t id, std::string raft_host, uint16_t raft_port, bool learner,
    std::string host, uint16_t port) {
    Status<> s;
    if (raft_node_->HasAbort()) {
        s.Set(ErrCode::ErrRaftAbort);
        co_return s;
    }
    if (gate_.is_closed()) {
        s.Set(EPIPE);
        co_return s;
    }

    raft::ConfChange cc;
    RaftNode rn;
    rn.set_id(id);
    rn.set_raft_host(raft_host);
    rn.set_raft_port(raft_port);
    rn.set_host(host);
    rn.set_port(port);
    rn.set_learner(learner);

    Buffer b(rn.ByteSizeLong());
    rn.SerializeToArray(b.get_write(), b.size());
    cc.set_node_id(id);
    cc.set_type(learner ? raft::ConfChangeType::ConfChangeAddLearnerNode
                        : raft::ConfChangeType::ConfChangeAddNode);
    cc.set_context(std::move(b));

    raft::EntryPtr ent = raft::make_entry();
    ent->set_type(raft::EntryType::EntryConfChange);
    ent->set_data(cc.Marshal());
    std::unique_ptr<ProposeEntry> pe = std::make_unique<ProposeEntry>();
    pe->ent = ent;
    propose_pending_.push(pe.get());
    cv_.signal();
    s = co_await pe->pr.get_future();
    co_return s;
}

seastar::future<Status<>> RaftServer::RemoveRaftNode(uint64_t node_id) {
    Status<> s;
    if (raft_node_->HasAbort()) {
        s.Set(ErrCode::ErrRaftAbort);
        co_return s;
    }
    if (gate_.is_closed()) {
        s.Set(EPIPE);
        co_return s;
    }

    raft::ConfChange cc;
    cc.set_node_id(node_id);
    cc.set_type(raft::ConfChangeType::ConfChangeRemoveNode);

    raft::EntryPtr ent = raft::make_entry();
    ent->set_type(raft::EntryType::EntryConfChange);
    ent->set_data(cc.Marshal());
    std::unique_ptr<ProposeEntry> pe = std::make_unique<ProposeEntry>();
    pe->ent = ent;
    propose_pending_.push(pe.get());
    cv_.signal();
    s = co_await pe->pr.get_future();
    co_return s;
}

RaftNode RaftServer::GetRaftNode(uint64_t id) {
    return store_->GetRaftNode(id);
}

void RaftServer::TransferLeader(uint64_t transferee) {
    if (raft_node_->HasAbort()) {
        return;
    }
    if (gate_.is_closed()) {
        return;
    }

    raft::MessagePtr msg = raft::make_raft_message();
    msg->type = raft::MessageType::MsgTransferLeader;
    msg->from = transferee;
    recv_pending_.push(msg);
    cv_.signal();
}

seastar::future<Status<uint64_t>> RaftServer::ReadIndex() {
    Status<uint64_t> s;
    if (raft_node_->HasAbort()) {
        s.Set(ErrCode::ErrRaftAbort);
        co_return s;
    }
    if (gate_.is_closed()) {
        s.Set(EPIPE);
        co_return s;
    }
    seastar::gate::holder holder(gate_);

    auto timeout = std::chrono::milliseconds(opt_.heartbeat_tick *
                                             opt_.tick_interval * 1000);
    if (!read_index_) {
        read_index_ = seastar::make_lw_shared<RaftServer::ReadIndexItem>();
        (*read_index_)->id =
            (node_id_ << 48 | ((++read_index_seq_) & (ULONG_MAX >> 16)));
        (*read_index_)->timer.set_callback([this] {
            Status<uint64_t> st;
            st.Set(ETIME);
            (*read_index_)->pr.set_value(std::move(st));
            read_index_.reset();
        });
        Buffer b(8);
        net::BigEndian::PutUint64(b.get_write(), (*read_index_)->id);
        raft::MessagePtr msg = raft::make_raft_message();
        raft::EntryPtr ent = raft::make_entry();
        ent->set_data(std::move(b));
        msg->type = raft::MessageType::MsgReadIndex;
        msg->entries.push_back(ent);
        recv_pending_.push(msg);
        cv_.signal();
        (*read_index_)->timer.arm(timeout);
    }
    const auto start = std::chrono::steady_clock::now();
    auto fu = (*read_index_)->pr.get_shared_future();
    s = co_await std::move(fu);
    if (!s) {
        co_return s;
    }

    while (store_->Applied() < s.Value() && !gate_.is_closed()) {
        const std::chrono::milliseconds diff =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start);
        if (diff >= timeout) {
            s.Set(ETIME);
            break;
        }
        try {
            co_await apply_waiter_.wait(timeout - diff);
        } catch (...) {
            s.Set(ETIME);
            break;
        }
    }
    if (gate_.is_closed()) {
        s.Set(EPIPE);
    }
    co_return s;
}

void RaftServer::ReleaseWal(uint64_t index) {
    pending_release_wal_ = index;
    cv_.signal();
}

seastar::future<> RaftServer::Close() {
    if (!gate_.is_closed()) {
        auto fu = gate_.close();
        apply_waiter_.broadcast();
        co_await receiver_->Close();
        cv_.signal();
        apply_cv_.signal();
        co_await std::move(fu);
        store_ = nullptr;  // release all snapshot in cache
        co_await sender_->Close();
    }
    co_return;
}

}  // namespace stream
}  // namespace snail
