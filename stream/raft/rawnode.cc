#include "rawnode.h"

#include <seastar/core/coroutine.hh>

#include "raft_errno.h"

namespace snail {
namespace raft {

seastar::future<int> RawNode::Init(const Raft::Config cfg) {
  int err = co_await raft_->Init(cfg);
  if (err != RAFT_OK) {
    co_return err;
  }

  prev_soft_state_ = raft_->GetSoftState();
  prev_hard_state_ = raft_->GetHardState();
  co_return RAFT_OK;
}

seastar::future<> RawNode::Tick() { return raft_->tick_(); }

seastar::future<int> RawNode::Campaign() {
  MessagePtr m = make_raft_message();
  m->type = MessageType::MsgHup;
  return raft_->Step(m);
}

seastar::future<int> RawNode::Propose(seastar::temporary_buffer<char> data) {
  MessagePtr m = make_raft_message();
  m->type = MessageType::MsgProp;
  m->from = raft_->id_;
  EntryPtr ent = make_entry();
  ent->set_data(std::move(data));
  m->entries.push_back(ent);
  return raft_->Step(m);
}

seastar::future<int> RawNode::ProposeConfChange(ConfChangeI cc) {
  auto data = cc.Marshal();
  EntryType type =
      cc.IsV1() ? EntryType::EntryConfChange : EntryType::EntryConfChangeV2;

  MessagePtr m = make_raft_message();
  m->type = MessageType::MsgProp;
  EntryPtr ent = make_entry();
  ent->set_type(type);
  ent->set_data(std::move(data));
  m->entries.push_back(ent);
  return raft_->Step(m);
}

seastar::future<ConfState> RawNode::ApplyConfChange(ConfChangeI cc) {
  auto c = cc.AsV2();
  auto cs = co_await raft_->ApplyConfChange(std::move(c));
  co_return std::move(cs);
}

seastar::future<int> RawNode::Step(MessagePtr m) {
  if (IsLocalMsg(m->type)) {
    co_return RAFT_ERR_STEP_LOCALMSG;
  }

  auto it = raft_->prs_->progress().find(m->from);
  if (it != raft_->prs_->progress().end() || !IsResponseMsg(m->type)) {
    int err = co_await raft_->Step(m);
    co_return err;
  }
  co_return RAFT_ERR_STEP_PEER_NOTFOUND;
}

seastar::future<ReadyPtr> RawNode::GetReady(const SoftState st,
                                            const HardState hs) {
  ReadyPtr rd = seastar::make_lw_shared<Ready>();
  rd->entries = std::move(raft_->raft_log_->UnstableEntries());
  rd->committed_entries = co_await raft_->raft_log_->NextEnts();
  rd->msgs = std::move(raft_->msgs_);

  auto softSt = raft_->GetSoftState();
  if (!(softSt.lead == st.lead && softSt.raft_state == st.raft_state)) {
    rd->st = softSt;
  }
  auto hardSt = raft_->GetHardState();
  if (hardSt != hs) {
    rd->hs = hardSt;
  }

  rd->snapshot = raft_->raft_log_->UnstableSnapshot();
  rd->rss = std::move(raft_->read_states_);
  rd->sync = !rd->entries.empty() || hardSt.vote() != hs.vote() ||
             hardSt.term() != hs.term();
  if (rd->st) {
    prev_soft_state_ = rd->st.value();
  }
  co_return rd;
}

bool RawNode::HasReady() {
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

  if (!raft_->msgs_.empty() || !raft_->raft_log_->UnstableEntries().empty() ||
      raft_->raft_log_->HasNextEnts()) {
    return true;
  }

  if (!raft_->read_states_.empty()) {
    return true;
  }

  return false;
}

seastar::future<> RawNode::Advance(ReadyPtr rd) {
  if (!rd->hs.Empty()) {
    prev_hard_state_ = rd->hs;
  }
  return raft_->Advance(rd);
}

BasicStatus RawNode::GetBasicStatus() {
  BasicStatus s;

  s.id = raft_->id_;
  s.hs = raft_->GetHardState();
  s.st = raft_->GetSoftState();
  s.applied = raft_->raft_log_->applied();
  s.lead_transferee = raft_->lead_transferee_;
  return s;
}

Status RawNode::GetStatus() {
  Status s;
  auto bs = GetBasicStatus();
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

seastar::future<int> RawNode::ReportUnreachable(uint64_t id) {
  MessagePtr m = make_raft_message();
  m->type = MessageType::MsgUnreachable;
  m->from = id;
  return raft_->Step(m);
}

seastar::future<int> RawNode::ReportSnapshot(uint64_t id,
                                             SnapshotStatus status) {
  MessagePtr m = make_raft_message();
  m->type = MessageType::MsgSnapStatus;
  m->from = id;
  m->reject = status == SnapshotStatus::SnapshotFailure;
  return raft_->Step(m);
}

seastar::future<int> RawNode::TransferLeader(uint64_t transferee) {
  MessagePtr m = make_raft_message();
  m->type = MessageType::MsgTransferLeader;
  m->from = transferee;
  return raft_->Step(m);
}

seastar::future<int> RawNode::ReadIndex(seastar::temporary_buffer<char> rctx) {
  MessagePtr m = make_raft_message();
  m->type = MessageType::MsgReadIndex;
  EntryPtr ent = make_entry();
  ent->set_data(std::move(rctx));
  m->entries.push_back(ent);
  return raft_->Step(m);
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
