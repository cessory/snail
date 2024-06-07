#include "rawnode.h"

#include <seastar/core/coroutine.hh>

namespace snail {
namespace raft {

seastar::future<Status<RawNodePtr>> RawNode::Create(const Raft::Config cfg) {
    Status<RawNodePtr> s;

    RawNodePtr ptr(new RawNode());
    auto st = co_await Raft::Create(cfg);
    if (!st) {
        s.Set(st.Code(), st.Reason());
        co_return s;
    }
    ptr->raft_ = std::move(st.Value());

    ptr->prev_soft_state_ = ptr->raft_->GetSoftState();
    ptr->prev_hard_state_ = ptr->raft_->GetHardState();
    s.SetValue(std::move(ptr));
    co_return s;
}

seastar::future<Status<>> RawNode::Tick() {
    Status<> s;
    if (abort_) {
        s.Set(ErrCode::ErrRaftAbort);
        co_return s;
    }
    try {
        s = co_await raft_->tick_();
    } catch (...) {
        s.Set(ErrCode::ErrRaftAbort);
        abort_ = true;
    }
    co_return s;
}

seastar::future<Status<>> RawNode::Campaign() {
    Status<> s;
    if (abort_) {
        s.Set(ErrCode::ErrRaftAbort);
        co_return s;
    }
    MessagePtr m = make_raft_message();
    m->type = MessageType::MsgHup;
    try {
        s = co_await raft_->Step(m);
    } catch (...) {
        s.Set(ErrCode::ErrRaftAbort);
        abort_ = true;
    }
    co_return s;
}

seastar::future<Status<>> RawNode::Propose(
    seastar::temporary_buffer<char> data) {
    Status<> s;
    if (abort_) {
        s.Set(ErrCode::ErrRaftAbort);
        co_return s;
    }
    MessagePtr m = make_raft_message();
    m->type = MessageType::MsgProp;
    m->from = raft_->id_;
    EntryPtr ent = make_entry();
    ent->set_data(std::move(data));
    m->entries.push_back(ent);
    try {
        s = co_await raft_->Step(m);
    } catch (...) {
        s.Set(ErrCode::ErrRaftAbort);
        abort_ = true;
    }
    co_return s;
}

seastar::future<Status<>> RawNode::ProposeConfChange(ConfChangeI cc) {
    Status<> s;
    if (abort_) {
        s.Set(ErrCode::ErrRaftAbort);
        co_return s;
    }
    auto data = cc.Marshal();
    EntryType type =
        cc.IsV1() ? EntryType::EntryConfChange : EntryType::EntryConfChangeV2;

    MessagePtr m = make_raft_message();
    m->type = MessageType::MsgProp;
    EntryPtr ent = make_entry();
    ent->set_type(type);
    ent->set_data(std::move(data));
    m->entries.push_back(ent);
    try {
        s = co_await raft_->Step(m);
    } catch (...) {
        s.Set(ErrCode::ErrRaftAbort);
        abort_ = true;
    }
    co_return s;
}

seastar::future<Status<ConfState>> RawNode::ApplyConfChange(ConfChangeI cc) {
    Status<ConfState> s;
    if (abort_) {
        s.Set(ErrCode::ErrRaftAbort);
        co_return s;
    }
    auto c = cc.AsV2();
    try {
        auto cs = co_await raft_->ApplyConfChange(std::move(c));
        s.SetValue(std::move(cs));
    } catch (...) {
        s.Set(ErrCode::ErrRaftAbort);
        abort_ = true;
    }
    co_return std::move(s);
}

seastar::future<Status<>> RawNode::Step(MessagePtr m) {
    Status<> s;
    if (abort_) {
        s.Set(ErrCode::ErrRaftAbort);
        co_return s;
    }
    if (IsLocalMsg(m->type)) {
        s.Set(ErrCode::ErrRaftStepLocalMsg);
        co_return s;
    }

    auto it = raft_->prs_->progress().find(m->from);
    if (it != raft_->prs_->progress().end() || !IsResponseMsg(m->type)) {
        try {
            s = co_await raft_->Step(m);
        } catch (...) {
            s.Set(ErrCode::ErrRaftAbort);
            abort_ = true;
        }
        co_return s;
    }
    s.Set(ErrCode::ErrRaftStepPeerNotFound);
    co_return s;
}

seastar::future<Status<ReadyPtr>> RawNode::GetReady() {
    Status<ReadyPtr> s;
    if (abort_) {
        s.Set(ErrCode::ErrRaftAbort);
        co_return s;
    }
    try {
        ReadyPtr rd = seastar::make_lw_shared<Ready>();
        rd->entries_ = std::move(raft_->raft_log_->UnstableEntries());
        rd->committed_entries_ = co_await raft_->raft_log_->NextEnts();
        rd->msgs_ = std::move(raft_->msgs_);

        auto softSt = raft_->GetSoftState();
        if (!(softSt.lead == prev_soft_state_.lead &&
              softSt.raft_state == prev_soft_state_.raft_state)) {
            rd->st_ = softSt;
        }
        auto hardSt = raft_->GetHardState();
        if (hardSt != prev_hard_state_) {
            rd->hs_ = hardSt;
        }

        rd->snapshot_ = raft_->raft_log_->UnstableSnapshot();
        rd->rss_ = std::move(raft_->read_states_);
        rd->sync_ = !rd->entries_.empty() ||
                    hardSt.vote() != prev_hard_state_.vote() ||
                    hardSt.term() != prev_hard_state_.term();
        if (rd->st_) {
            prev_soft_state_ = rd->st_.value();
        }
        s.SetValue(rd);
    } catch (...) {
        s.Set(ErrCode::ErrRaftAbort);
        abort_ = true;
    }
    co_return s;
}

bool RawNode::HasReady() {
    if (abort_) {
        return false;
    }
    try {
        auto st = raft_->GetSoftState();
        if (!(st.lead == prev_soft_state_.lead &&
              st.raft_state == prev_soft_state_.raft_state)) {
            return true;
        }

        auto hs = raft_->GetHardState();
        if (!hs.Empty() && hs != prev_hard_state_) {
            return true;
        }

        if (raft_->raft_log_->HasPendingSnapshot()) {
            return true;
        }

        if (!raft_->msgs_.empty() ||
            !raft_->raft_log_->UnstableEntries().empty() ||
            raft_->raft_log_->HasNextEnts()) {
            return true;
        }

        if (!raft_->read_states_.empty()) {
            return true;
        }
    } catch (...) {
        abort_ = true;
    }

    return false;
}

seastar::future<Status<>> RawNode::Advance(ReadyPtr rd) {
    Status<> s;
    if (abort_) {
        s.Set(ErrCode::ErrRaftAbort);
        co_return s;
    }
    if (!rd->hs_.Empty()) {
        prev_hard_state_ = rd->hs_;
    }
    try {
        s = co_await raft_->Advance(rd);
    } catch (...) {
        abort_ = true;
        s.Set(ErrCode::ErrRaftAbort);
    }
    co_return s;
}

BasicStatus RawNode::GetBasicStatus() {
    BasicStatus s;

    s.group = raft_->group_;
    s.id = raft_->id_;
    s.hs = raft_->GetHardState();
    s.st = raft_->GetSoftState();
    s.applied = raft_->raft_log_->applied();
    s.lead_transferee = raft_->lead_transferee_;
    return s;
}

RaftStatus RawNode::GetStatus() {
    RaftStatus s;
    auto bs = GetBasicStatus();
    s.group = bs.group;
    s.id = bs.id;
    s.hs = bs.hs;
    s.st = bs.st;
    s.applied = bs.applied;
    s.lead_transferee = bs.lead_transferee;
    if (s.st.raft_state == RaftState::StateLeader) {
        s.prs = GetProgressCopy();
    }
    s.cfg = raft_->prs_->Clone();
    return s;
}

void RawNode::WithProgress(std::function<void(uint64_t id, ProgressType typ,
                                              ProgressPtr pr)> const &visitor) {
    raft_->prs_->Visit([&visitor](uint64_t id, Progress *pr) {
        ProgressType typ = ProgressType::ProgressTypePeer;
        if (pr->is_learner()) {
            typ = ProgressType::ProgressTypeLearner;
        }
        ProgressPtr p = seastar::make_lw_shared<Progress>(*pr);
        visitor(id, typ, p);
    });
}

seastar::future<Status<>> RawNode::ReportUnreachable(uint64_t id) {
    Status<> s;
    if (abort_) {
        s.Set(ErrCode::ErrRaftAbort);
        co_return s;
    }
    MessagePtr m = make_raft_message();
    m->type = MessageType::MsgUnreachable;
    m->from = id;
    try {
        s = co_await raft_->Step(m);
    } catch (...) {
        s.Set(ErrCode::ErrRaftAbort);
        abort_ = true;
    }
    co_return s;
}

seastar::future<Status<>> RawNode::ReportSnapshot(uint64_t id,
                                                  SnapshotStatus status) {
    Status<> s;
    if (abort_) {
        s.Set(ErrCode::ErrRaftAbort);
        co_return s;
    }
    MessagePtr m = make_raft_message();
    m->type = MessageType::MsgSnapStatus;
    m->from = id;
    m->reject = status == SnapshotStatus::SnapshotFailure;
    try {
        s = co_await raft_->Step(m);
    } catch (...) {
        s.Set(ErrCode::ErrRaftAbort);
        abort_ = true;
    }
    co_return s;
}

seastar::future<Status<>> RawNode::TransferLeader(uint64_t transferee) {
    Status<> s;
    if (abort_) {
        s.Set(ErrCode::ErrRaftAbort);
        co_return s;
    }
    MessagePtr m = make_raft_message();
    m->type = MessageType::MsgTransferLeader;
    m->from = transferee;
    try {
        s = co_await raft_->Step(m);
    } catch (...) {
        s.Set(ErrCode::ErrRaftAbort);
        abort_ = true;
    }
    co_return s;
}

seastar::future<Status<>> RawNode::ReadIndex(
    seastar::temporary_buffer<char> rctx) {
    Status<> s;
    if (abort_) {
        s.Set(ErrCode::ErrRaftAbort);
        co_return s;
    }
    MessagePtr m = make_raft_message();
    m->type = MessageType::MsgReadIndex;
    EntryPtr ent = make_entry();
    ent->set_data(std::move(rctx));
    m->entries.push_back(ent);
    try {
        s = co_await raft_->Step(m);
    } catch (...) {
        s.Set(ErrCode::ErrRaftAbort);
        abort_ = true;
    }
    co_return s;
}

ProgressMap RawNode::GetProgressCopy() {
    ProgressMap prs;
    raft_->prs_->Visit([&prs](uint64_t id, Progress *pr) {
        ProgressPtr p = seastar::make_lw_shared<Progress>(*pr);
        prs[id] = p;
    });
    return prs;
}

}  // namespace raft
}  // namespace snail
