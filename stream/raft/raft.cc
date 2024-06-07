#include "raft.h"

#include <seastar/core/coroutine.hh>

#include "util/logger.h"

namespace snail {
namespace raft {

static std::random_device random_dev;

Raft::Raft()
    : group_(0),
      id_(0),
      term_(0),
      vote_(0),
      max_msg_size_(0),
      max_uncommitted_size_(0),
      state_(RaftState::StateFollower),
      is_learner_(false),
      lead_(0),
      lead_transferee_(0),
      pending_conf_index_(0),
      uncommitted_size_(0),
      election_elapsed_(0),
      heartbeat_elapsed_(0),
      check_quorum_(true),
      pre_vote_(true),
      heartbeat_timeout_(0),
      election_timeout_(0),
      randomized_election_timeout_(0),
      disable_proposal_forwarding_(0),
      e_(random_dev()) {}

seastar::future<Status<RaftPtr>> Raft::Create(const Config c) {
    Status<RaftPtr> s;
    RaftPtr raft_ptr = seastar::make_lw_shared<Raft>(Raft());
    if (!raft_ptr->Validate(c)) {
        s.Set(ErrCode::ErrRaftUnvalidata);
        co_return s;
    }
    uint64_t max_committed_size_per_ready =
        c.max_committed_size_per_ready == 0 ? c.max_size_per_msg
                                            : c.max_committed_size_per_ready;
    raft_ptr->raft_log_ = RaftLog::MakeRaftLog(
        c.storage, max_committed_size_per_ready, c.group, c.id);

    HardState hs;
    ConfState cs;

    auto st = co_await c.storage->InitialState();
    if (!st) {
        LOG_ERROR("[{}-{}] storage InitialState error {}", c.group, c.id, st);
        s.Set(st.Code());
        co_return s;
    }
    std::tie(hs, cs) = st.Value();

    raft_ptr->group_ = c.group;
    raft_ptr->id_ = c.id;
    raft_ptr->max_msg_size_ = c.max_size_per_msg;
    raft_ptr->max_uncommitted_size_ = c.max_uncommitted_entries_size == 0
                                          ? std::numeric_limits<uint64_t>::max()
                                          : c.max_uncommitted_entries_size;
    raft_ptr->prs_ =
        seastar::make_lw_shared<ProgressTracker>(c.max_inflight_msgs);
    raft_ptr->election_timeout_ = c.election_tick;
    raft_ptr->heartbeat_timeout_ = c.heartbeat_tick;
    raft_ptr->check_quorum_ = c.check_quorum;
    raft_ptr->pre_vote_ = c.pre_vote;
    raft_ptr->read_only_ =
        seastar::make_lw_shared<ReadOnly>(ReadOnly(c.read_only_option));
    raft_ptr->disable_proposal_forwarding_ = c.disable_proposal_forwarding;
    raft_ptr->dist_ = std::make_optional<std::uniform_int_distribution<int>>(
        0, raft_ptr->election_timeout_ - 1);

    Changer chg(raft_ptr->raft_log_->LastIndex(), raft_ptr->prs_,
                raft_ptr->group_, raft_ptr->id_);
    TrackerConfig cfg;
    ProgressMap prs;

    auto st1 = chg.Restore(cs);
    if (!st1) {
        s.Set(st1.Code());
        co_return s;
    }
    std::tie(cfg, prs) = st1.Value();

    auto cs1 = co_await raft_ptr->SwitchToConfig(cfg, prs);
    if (cs != cs1) {
        s.Set(ErrCode::ErrRaftConfStates);
        co_return s;
    }

    if (!hs.Empty()) {
        raft_ptr->LoadState(hs);
    }

    if (c.applied > 0) {
        raft_ptr->raft_log_->AppliedTo(c.applied);
    }

    raft_ptr->BecomeFollower(raft_ptr->term_, 0);
    auto last_term = co_await raft_ptr->raft_log_->LastTerm();
    LOG_INFO(
        "[{}-{}] init raft [peers: {}, term: {}, commit: {}, applied: "
        "{}, lastindex: {}, lastterm: {}]",
        raft_ptr->group_, raft_ptr->id_,
        fmt::join(raft_ptr->prs_->VoterNodes(), ","), raft_ptr->term_,
        raft_ptr->raft_log_->committed(), raft_ptr->raft_log_->applied(),
        raft_ptr->raft_log_->LastIndex(), last_term);
    s.SetValue(std::move(raft_ptr));
    co_return s;
}

SoftState Raft::GetSoftState() {
    SoftState st = {lead_, state_};
    return st;
}

HardState Raft::GetHardState() {
    HardState hs = {term_, vote_, raft_log_->committed()};
    return hs;
}

void Raft::LoadState(const HardState& hs) {
    if (hs.commit() < raft_log_->committed() ||
        hs.commit() > raft_log_->LastIndex()) {
        LOG_FATAL_THROW("[{}-{}] state.commit {} is out of range [{}, {}]",
                        group_, id_, hs.commit(), raft_log_->committed(),
                        raft_log_->LastIndex());
    }
    raft_log_->set_committed(hs.commit());
    term_ = hs.term();
    vote_ = hs.vote();
}

bool Raft::Validate(const Raft::Config& c) {
    if (c.id == 0) {
        LOG_ERROR("cannot use 0 as id");
        return false;
    }

    if (c.heartbeat_tick <= 0) {
        LOG_ERROR("[{}-{}] heartbeat tick must be greater than 0", c.group,
                  c.id);
        return false;
    }

    if (c.election_tick <= c.heartbeat_tick) {
        LOG_ERROR("[{}-{}] election tick must be greater than heartbeat tick",
                  c.group, c.id);
        return false;
    }

    if (!c.storage) {
        LOG_ERROR("[{}-{}] storage cannot be null", c.group, c.id);
        return false;
    }

    if (c.max_inflight_msgs <= 0) {
        LOG_ERROR("[{}-{}] max inflight messages must be greater than 0",
                  c.group, c.id);
        return false;
    }

    if (c.read_only_option == ReadOnlyOption::ReadOnlyLeaseBased &&
        !c.check_quorum) {
        LOG_ERROR(
            "[{}-{}] CheckQuorum must be enabled when ReadOnlyOption is "
            "ReadOnlyLeaseBased",
            c.group, c.id);
        return false;
    }
    return true;
}

seastar::future<ConfState> Raft::SwitchToConfig(TrackerConfig cfg,
                                                ProgressMap prs) {
    prs_->set_config(cfg);
    prs_->set_progress(prs);

    LOG_INFO("[{}-{}] switched to configuration {}", group_, id_, cfg);
    auto cs = prs_->GetConfState();

    auto iter = prs_->progress().find(id_);
    if (iter != prs_->progress().end()) {
        is_learner_ = iter->second->is_learner();
    }

    if ((iter == prs_->progress().end() || is_learner_) &&
        state_ == RaftState::StateLeader) {
        co_return cs;
    }

    if (state_ != RaftState::StateLeader || cs.voters().size() == 0) {
        co_return cs;
    }

    auto ok = co_await MaybeCommit();
    if (ok) {
        co_await BcastAppend();
    } else {
        co_await prs_->AsyncVisit(
            [this](uint64_t id, ProgressPtr pr) -> seastar::future<Status<>> {
                co_await MaybeSendAppend(id, false);
                co_return Status<>();
            });
    }

    // If the the leadTransferee was removed or demoted, abort the leadership
    // transfer.
    if (lead_transferee_ != 0) {
        auto ids = IDs(prs_->voters());
        auto it = ids.find(lead_transferee_);
        if (it == ids.end()) {
            AbortLeaderTransfer();
        }
    }
    co_return cs;
}

seastar::future<bool> Raft::MaybeCommit() {
    auto mci = prs_->Committed();
    return raft_log_->MaybeCommit(mci, term_);
}

seastar::future<Status<>> Raft::BcastAppend() {
    auto s = co_await prs_->AsyncVisit(
        [this](uint64_t id, ProgressPtr pr) -> seastar::future<Status<>> {
            if (id_ == id) {
                return seastar::make_ready_future<Status<>>();
            }
            return SendAppend(id);
        });
    co_return s;
}

void Raft::BcastHeartbeat() {
    auto last_ctx = read_only_->LastPendingRequestCtx();
    BcastHeartbeatWithCtx(last_ctx);
}

void Raft::BcastHeartbeatWithCtx(const seastar::sstring& ctx) {
    prs_->Visit([this, &ctx](uint64_t id, Progress* pr) {
        if (id_ == id) {
            return;
        }
        SendHeartbeat(id, ctx);
    });
}

void Raft::SendHeartbeat(uint64_t to, const seastar::sstring& ctx) {
    uint64_t commit =
        std::min(prs_->progress()[to]->match(), raft_log_->committed());
    MessagePtr msg = seastar::make_lw_shared<Message>();
    msg->to = to;
    msg->type = MessageType::MsgHeartbeat;
    msg->commit = commit;
    if (!ctx.empty()) {
        msg->context =
            std::move(seastar::temporary_buffer<char>(ctx.c_str(), ctx.size()));
    }
    Send(msg);
}

seastar::future<Status<>> Raft::SendAppend(uint64_t to) {
    co_await MaybeSendAppend(to, true);
    co_return Status<>();
}

seastar::future<bool> Raft::MaybeSendAppend(uint64_t to, bool send_if_empty) {
    auto iter = prs_->progress().find(to);
    if (iter == prs_->progress().end()) {
        LOG_FATAL_THROW("[{}-{}] not found {} in progress map", group_, id_,
                        to);
    }

    auto pr = iter->second;
    if (pr->IsPaused()) {
        co_return false;
    }

    MessagePtr m = seastar::make_lw_shared<Message>();
    m->to = to;
    auto s1 = co_await raft_log_->Term(pr->next() - 1);
    auto s2 = co_await raft_log_->Entries(pr->next(), max_msg_size_);

    if (s2.Value().size() == 0 && !send_if_empty) {
        co_return false;
    }

    if (!s1 || !s2) {
        if (!pr->recent_active()) {
            LOG_DEBUG(
                "[{}-{}] ignore sending snapshot to {} since it is not "
                "recently active",
                group_, id_, to);
            co_return false;
        }
        m->type = MessageType::MsgSnap;
        auto s3 = co_await raft_log_->Snapshot();
        if (!s3) {
            if (s3.Code() == ErrCode::ErrRaftSnapshotTemporarilyUnavailable) {
                LOG_DEBUG(
                    "[{}-{}] failed to send snapshot to {} because snapshot "
                    "is temporarily unavailable",
                    group_, id_, to);
                co_return false;
            }
            LOG_FATAL_THROW("[{}-{}] failed to send snapshot to {} because {}",
                            group_, id_, to, s3);
        }

        SnapshotPtr snapshot = s3.Value();
        if (snapshot->metadata().index() == 0) {
            LOG_FATAL_THROW("[{}-{}] need non-empty snapshot", group_, id_);
        }
        m->snapshot = snapshot;
        auto sindex = snapshot->metadata().index();
        auto sterm = snapshot->metadata().term();
        LOG_DEBUG(
            "[{}-{}] [firstindex: {}, commit: {}] sent snapshot[index: "
            "{}, term: {}] to {} [{}]",
            group_, id_, raft_log_->FirstIndex(), raft_log_->committed(),
            sindex, sterm, to, *pr);
        pr->BecomeSnapshot(sindex);
        LOG_DEBUG("[{}-{}] paused sending replication messages to {} [{}]",
                  group_, id_, to, *pr);
    } else {
        m->type = MessageType::MsgApp;
        m->index = pr->next() - 1;
        m->log_term = s1.Value();
        m->entries = std::move(s2.Value());
        m->commit = raft_log_->committed();
        if (m->entries.size() != 0) {
            auto state = pr->state();
            switch (state) {
                case StateType::StateReplicate: {
                    auto last = m->entries.back()->index();
                    pr->OptimisticUpdate(last);
                    pr->inflights().Add(last);
                    break;
                }
                case StateType::StateProbe:
                    pr->set_probe_sent(true);
                    break;
                default:
                    LOG_FATAL_THROW(
                        "[{}-{}] is sending append in unhandled state {}",
                        group_, id_, StateTypeToString(state));
            }
        }
    }
    Send(m);
    co_return true;
}

void Raft::Send(MessagePtr m) {
    if (m->from == 0) {
        m->from = id_;
    }

    if (m->type == MessageType::MsgVote ||
        m->type == MessageType::MsgVoteResp ||
        m->type == MessageType::MsgPreVote ||
        m->type == MessageType::MsgPreVoteResp) {
        if (m->term == 0) {
            LOG_FATAL_THROW("[{}-{}] term should be set when sending {}",
                            group_, id_, MessageTypeToString(m->type));
        }
    } else {
        if (m->term != 0) {
            LOG_FATAL_THROW(
                "[{}-{}] term should not be set when sending {} (was {})",
                group_, id_, MessageTypeToString(m->type), m->term);
        }
        if (m->type != MessageType::MsgProp &&
            m->type != MessageType::MsgReadIndex) {
            m->term = term_;
        }
    }

    msgs_.push_back(m);
}

void Raft::AbortLeaderTransfer() { lead_transferee_ = 0; }

void Raft::BecomeFollower(uint64_t term, uint64_t lead) {
    step_ = [this](MessagePtr m) -> seastar::future<Status<>> {
        return StepFollower(m);
    };
    Reset(term);
    tick_ = [this]() -> seastar::future<Status<>> { return TickElection(); };
    lead_ = lead;
    state_ = RaftState::StateFollower;
    LOG_INFO("[{}-{}] became follower at term {}", group_, id_, term_);
}

void Raft::BecomePreCandidate() {
    if (state_ == RaftState::StateLeader) {
        LOG_FATAL_THROW("[{}-{}] invalid transition [leader -> pre-candidate]",
                        group_, id_);
    }

    step_ = [this](MessagePtr m) -> seastar::future<Status<>> {
        return StepCandidate(m);
    };
    prs_->ResetVotes();
    tick_ = [this]() -> seastar::future<Status<>> { return TickElection(); };
    lead_ = 0;
    state_ = RaftState::StatePreCandidate;
    LOG_INFO("[{}-{}] became pre-candidate at term {}", group_, id_, term_);
}

void Raft::BecomeCandidate() {
    if (state_ == RaftState::StateLeader) {
        LOG_FATAL_THROW("[{}-{}] invalid transition [leader -> candidate]",
                        group_, id_);
    }

    step_ = [this](MessagePtr m) -> seastar::future<Status<>> {
        return StepCandidate(m);
    };
    Reset(term_ + 1);
    tick_ = [this]() -> seastar::future<Status<>> { return TickElection(); };
    vote_ = id_;
    state_ = RaftState::StateCandidate;
    LOG_INFO("[{}-{}] became candidate at term {}", group_, id_, term_);
}

seastar::future<Status<>> Raft::BecomeLeader() {
    if (state_ == RaftState::StateFollower) {
        LOG_FATAL_THROW("[{}-{}] invalid transition [follower -> leader]",
                        group_, id_);
    }

    step_ = [this](MessagePtr m) -> seastar::future<Status<>> {
        return StepLeader(m);
    };
    Reset(term_);
    tick_ = [this]() -> seastar::future<Status<>> { return TickHeartbeat(); };
    lead_ = id_;
    state_ = RaftState::StateLeader;
    prs_->progress()[id_]->BecomeReplicate();
    pending_conf_index_ = raft_log_->LastIndex();

    EntryPtr empty_ent = make_entry();
    std::vector<EntryPtr> ents;
    ents.push_back(empty_ent);
    auto ok = co_await AppendEntry(std::move(ents));
    if (!ok) {
        LOG_FATAL_THROW("[{}-{}] empty entry was dropped", group_, id_);
    }
    ents.push_back(empty_ent);
    ReduceUncommittedSize(ents);
    LOG_INFO("[{}-{}] became leader at term {}", group_, id_, term_);
    co_return Status<>();
}

seastar::future<Status<>> Raft::TickElection() {
    election_elapsed_++;
    if (Promotable() && PastElectionTimeout()) {
        election_elapsed_ = 0;
        MessagePtr m = seastar::make_lw_shared<Message>();
        m->from = id_;
        m->type = MessageType::MsgHup;
        co_await Step(m);
    }
    co_return Status<>();
}

seastar::future<Status<>> Raft::TickHeartbeat() {
    heartbeat_elapsed_++;
    election_elapsed_++;

    if (election_elapsed_ >= election_timeout_) {
        election_elapsed_ = 0;
        if (check_quorum_) {
            MessagePtr msg = seastar::make_lw_shared<Message>();
            msg->from = id_;
            msg->type = MessageType::MsgCheckQuorum;
            co_await Step(msg);
        }
        if (state_ == RaftState::StateLeader && lead_transferee_ != 0) {
            AbortLeaderTransfer();
        }
    }

    if (state_ != RaftState::StateLeader) {
        co_return Status<>();
    }

    if (heartbeat_elapsed_ >= heartbeat_timeout_) {
        heartbeat_elapsed_ = 0;
        MessagePtr msg = seastar::make_lw_shared<Message>();
        msg->from = id_;
        msg->type = MessageType::MsgBeat;
        co_await Step(msg);
    }
    co_return Status<>();
}

seastar::future<Status<>> Raft::StepLeader(MessagePtr m) {
    Status<> s;
    switch (m->type) {
        case MessageType::MsgBeat:
            BcastHeartbeat();
            co_return s;
        case MessageType::MsgCheckQuorum: {
            auto it = prs_->progress().find(id_);
            if (it != prs_->progress().end()) {
                it->second->set_recent_active(true);
            }

            if (!prs_->QuorumActive()) {
                LOG_WARN(
                    "[{}-{}] stepped down to follower since quorum "
                    "is not active",
                    group_, id_);
                BecomeFollower(term_, 0);
            }
            prs_->Visit([this](uint64_t id, Progress* pr) {
                if (id != id_) {
                    pr->set_recent_active(false);
                }
            });
            co_return s;
        }
        case MessageType::MsgProp: {
            if (m->entries.empty()) {
                LOG_FATAL_THROW("[{}-{}] stepped empty MsgProp", group_, id_);
            }

            auto it = prs_->progress().find(id_);
            if (it == prs_->progress().end()) {
                s.Set(ErrCode::ErrRaftProposalDropped);
                co_return s;
            }
            if (lead_transferee_ != 0) {
                LOG_DEBUG(
                    "[{}-{}] [term {}] transfer leadership to {} is in "
                    "progress; dropping proposal",
                    group_, id_, term_, lead_transferee_);
                s.Set(ErrCode::ErrRaftTransfering);
                co_return s;
            }

            for (int i = 0; i < m->entries.size(); i++) {
                EntryPtr e = m->entries[i];
                std::optional<ConfChangeI> cc;
                if (e->type() == EntryType::EntryConfChange) {
                    ConfChange ccc;
                    if (!ccc.Unmarshal(std::move(e->data().share()))) {
                        LOG_FATAL_THROW("[{}-{}] unmarshal ConfChange error",
                                        group_, id_);
                    }
                    cc = ccc;
                } else if (e->type() == EntryType::EntryConfChangeV2) {
                    ConfChangeV2 ccc;
                    if (!ccc.Unmarshal(std::move(e->data().share()))) {
                        LOG_FATAL_THROW("[{}-{}] unmarshal ConfChangeV2 error",
                                        group_, id_);
                    }
                    cc = ccc;
                }

                if (cc) {
                    bool already_pending =
                        pending_conf_index_ > raft_log_->applied();
                    bool already_joint = prs_->voters()[1].size() > 0;
                    bool wants_leave_joint = (*cc).AsV2().changes().size() == 0;

                    bool refused = false;
                    if (already_pending) {
                        refused = true;
                        LOG_INFO(
                            "[{}-{}] ignoring conf change {} at config {}: "
                            "possible "
                            "unapplied conf change at index {} (applied to {})",
                            group_, id_, *cc, *prs_, pending_conf_index_,
                            raft_log_->applied());
                    } else if (already_joint && !wants_leave_joint) {
                        refused = true;
                        LOG_INFO(
                            "[{}-{}] ignoring conf change {} at "
                            "config {}: must "
                            "transition out of joint config first",
                            group_, id_, *cc, *prs_);
                    } else if (!already_joint && wants_leave_joint) {
                        refused = true;
                        LOG_INFO(
                            "[{}-{}] ignoring conf change {} at "
                            "config {}: not in "
                            "joint state; refusing empty conf change",
                            group_, id_, *cc, *prs_);
                    }

                    if (refused) {
                        EntryPtr ent = make_entry();
                        ent->set_type(EntryType::EntryNormal);
                        m->entries[i] = ent;
                    } else {
                        pending_conf_index_ =
                            raft_log_->LastIndex() + (uint64_t)i + 1;
                    }
                }
            }

            bool ok = co_await AppendEntry(std::move(m->entries));
            if (!ok) {
                s.Set(ErrCode::ErrRaftProposalDropped);
                co_return s;
            }

            co_await BcastAppend();
            co_return s;
        }
        case MessageType::MsgReadIndex:
            if (prs_->IsSingleton()) {
                auto resp = ResponseToReadIndexReq(m, raft_log_->committed());
                if (resp->to != 0) {
                    Send(resp);
                }
                co_return s;
            }

            bool ok = co_await CommittedEntryInCurrentTerm();
            if (!ok) {
                pending_readindex_messages_.push_back(m);
                co_return s;
            }

            SendMsgReadIndexResponse(m);
            co_return s;
    }
    // All other message types require a progress for m.From (pr).
    auto it = prs_->progress().find(m->from);
    if (it == prs_->progress().end()) {
        LOG_DEBUG("[{}-{}] no progress available for {}", group_, id_, m->from);
        co_return s;
    }
    ProgressPtr pr = it->second;

    switch (m->type) {
        case MessageType::MsgAppResp:
            pr->set_recent_active(true);
            if (m->reject) {
                LOG_DEBUG(
                    "[{}-{}] received MsgAppResp(rejected, hint: (index {}, "
                    "term {})) from {} for index {}",
                    group_, id_, m->reject_hint, m->log_term, m->from,
                    m->index);
                uint64_t next_probe_idx = m->reject_hint;
                if (m->log_term > 0) {
                    next_probe_idx = co_await raft_log_->FindConflictByTerm(
                        m->reject_hint, m->log_term);
                }
                if (pr->MaybeDecrTo(m->index, next_probe_idx)) {
                    LOG_DEBUG("[{}-{}] decreased progress of {} to [{}]",
                              group_, id_, m->from, *pr);
                    if (pr->state() == StateType::StateReplicate) {
                        pr->BecomeProbe();
                    }
                    co_await SendAppend(m->from);
                }
            } else {
                bool old_paused = pr->IsPaused();
                if (pr->MaybeUpdate(m->index)) {
                    switch (pr->state()) {
                        case StateType::StateProbe:
                            pr->BecomeReplicate();
                            break;
                        case StateType::StateSnapshot:
                            if (pr->match() >= pr->pending_snapshot()) {
                                LOG_DEBUG(
                                    "[{}-{}] recovered from needing snapshot, "
                                    "resumed "
                                    "sending replication messages to {} [{}]",
                                    group_, id_, m->from, *pr);
                                pr->BecomeProbe();
                                pr->BecomeReplicate();
                            }
                            break;
                        case StateType::StateReplicate:
                            pr->inflights().FreeLE(m->index);
                            break;
                    }

                    auto ok = co_await MaybeCommit();
                    if (ok) {
                        co_await ReleasePendingReadIndexMessages();
                        co_await BcastAppend();
                    } else if (old_paused) {
                        co_await SendAppend(m->from);
                    }

                    while (1) {
                        ok = co_await MaybeSendAppend(m->from, false);
                        if (!ok) {
                            break;
                        }
                    }

                    if (m->from == lead_transferee_ &&
                        pr->match() == raft_log_->LastIndex()) {
                        LOG_INFO(
                            "[{}-{}] sent MsgTimeoutNow to {} after "
                            "received MsgAppResp",
                            group_, id_, m->from);
                        SendTimeoutNow(m->from);
                    }
                }
            }
            break;
        case MessageType::MsgHeartbeatResp: {
            pr->set_recent_active(true);
            pr->set_probe_sent(false);
            if (pr->state() == StateType::StateReplicate &&
                pr->inflights().Full()) {
                pr->inflights().FreeFirstOne();
            }
            if (pr->match() < raft_log_->LastIndex()) {
                co_await SendAppend(m->from);
            }
            if (read_only_->option() != ReadOnlyOption::ReadOnlySafe ||
                m->context.empty()) {
                co_return s;
            }

            if (VoteResult::VoteWon !=
                GetVoteResult(
                    prs_->voters(),
                    read_only_->RecvAck(m->from,
                                        seastar::sstring(m->context.get(),
                                                         m->context.size())))) {
                co_return s;
            }

            auto rss = read_only_->Advance(m);
            for (auto rs : rss) {
                auto resp = ResponseToReadIndexReq(rs->req, rs->index);
                if (resp->to != 0) {
                    Send(resp);
                }
            }
            break;
        }
        case MessageType::MsgSnapStatus:
            if (pr->state() != StateType::StateSnapshot) {
                co_return s;
            }
            if (!m->reject) {
                pr->BecomeProbe();
                LOG_DEBUG(
                    "[{}-{}] snapshot succeeded, resumed sending replication "
                    "messages to {} [{}]",
                    group_, id_, m->from, *pr);
            } else {
                pr->set_pending_snapshot(0);
                pr->BecomeProbe();
                LOG_DEBUG(
                    "[{}-{}] snapshot failed, resumed sending replication "
                    "messages to {} [{}]",
                    group_, id_, m->from, *pr);
            }
            pr->set_probe_sent(true);
            break;
        case MessageType::MsgUnreachable:
            if (pr->state() == StateType::StateReplicate) {
                pr->BecomeProbe();
            }
            LOG_DEBUG(
                "[{}-{}] failed to send message to {} because it is "
                "unreachable [{}]",
                group_, id_, m->from, *pr);
            break;
        case MessageType::MsgTransferLeader: {
            if (pr->is_learner()) {
                LOG_DEBUG("[{}-{}] is learner. Ignored transferring leadership",
                          group_, id_);

                s.Set(ErrCode::ErrRaftIslearner);
                co_return s;
            }
            uint64_t lead_transferee = m->from;
            uint64_t last_lead_transferee = lead_transferee_;
            if (last_lead_transferee != 0) {
                if (last_lead_transferee == lead_transferee) {
                    LOG_INFO(
                        "[{}-{}] [term {}] transfer leadership to {} is in "
                        "progress, ignores request to same node {}",
                        group_, id_, term_, lead_transferee, lead_transferee);
                    s.Set(ErrCode::ErrRaftLeadtransferProgressing);
                    co_return s;
                }
                AbortLeaderTransfer();
                LOG_INFO(
                    "[{}-{}] [term {}] abort previous transferring "
                    "leadership to {}",
                    group_, id_, term_, last_lead_transferee);
            }
            if (lead_transferee == id_) {
                LOG_DEBUG(
                    "[{}-{}] is already leader. Ignored transferring "
                    "leadership to self",
                    group_, id_);
                s.Set(ErrCode::ErrRaftLeadtransferSelf);
                co_return s;
            }

            LOG_INFO("[{}-{}] [term {}] starts to transfer leadership to {}",
                     group_, id_, term_, lead_transferee);
            election_elapsed_ = 0;
            lead_transferee_ = lead_transferee;
            if (pr->match() == raft_log_->LastIndex()) {
                SendTimeoutNow(lead_transferee);
                LOG_INFO(
                    "[{}-{}] sends MsgTimeoutNow to {} immediately as {} "
                    "already has up-to-date log",
                    group_, id_, lead_transferee, lead_transferee);
            } else {
                co_await SendAppend(lead_transferee);
            }
            break;
        }
    }
    co_return s;
}

seastar::future<Status<>> Raft::StepFollower(MessagePtr m) {
    Status<> s;
    switch (m->type) {
        case MessageType::MsgProp:
            if (lead_ == 0) {
                LOG_INFO("[{}-{}] no leader at term {}; dropping proposal",
                         group_, id_, term_);
                s.Set(ErrCode::ErrRaftNoLeader);
                co_return s;
            } else if (disable_proposal_forwarding_) {
                LOG_INFO(
                    "[{}-{}] not forwarding to leader {} at term {}; "
                    "dropping proposal",
                    group_, id_, lead_, term_);
                s.Set(ErrCode::ErrRaftProposalDropped);
                co_return s;
            }
            m->to = lead_;
            Send(m);
            break;
        case MessageType::MsgApp:
            election_elapsed_ = 0;
            lead_ = m->from;
            co_await HandleAppendEntries(m);
            break;
        case MessageType::MsgHeartbeat:
            election_elapsed_ = 0;
            lead_ = m->from;
            HandleHeartbeat(m);
            break;
        case MessageType::MsgSnap:
            election_elapsed_ = 0;
            lead_ = m->from;
            co_await HandleSnapshot(m);
            break;
        case MessageType::MsgTransferLeader:
            if (lead_ == 0) {
                LOG_INFO(
                    "[{}-{}] no leader at term {}; dropping leader "
                    "transfer msg",
                    group_, id_, term_);
                s.Set(ErrCode::ErrRaftNoLeader);
                co_return s;
            }
            m->to = lead_;
            Send(m);
            break;
        case MessageType::MsgTimeoutNow:
            LOG_INFO(
                "[{}-{}] [term {}] received MsgTimeoutNow from {} and starts "
                "an election to get leadership.",
                group_, id_, term_, m->from);
            co_await Hup(CampaignType::CampaignTransfer);
            break;
        case MessageType::MsgReadIndex:
            if (lead_ == 0) {
                LOG_INFO(
                    "[{}-{}] no leader at term {}; dropping index reading msg",
                    group_, id_, term_);
                s.Set(ErrCode::ErrRaftNoLeader);
                co_return s;
            }
            m->to = lead_;
            Send(m);
            break;
        case MessageType::MsgReadIndexResp: {
            if (m->entries.size() != 1) {
                LOG_ERROR(
                    "[{}-{}] invalid format of MsgReadIndexResp from {}, "
                    "entries count: {}",
                    group_, id_, m->from, m->entries.size());
                co_return s;
            }

            ReadState rs;
            rs.index = m->index;
            rs.request_ctx = std::move(m->entries[0]->data());
            read_states_.push_back(std::move(rs));
            break;
        }
    }
    co_return s;
}

seastar::future<Status<>> Raft::StepCandidate(MessagePtr m) {
    Status<> s;
    MessageType my_vote_resp_type = state_ == RaftState::StatePreCandidate
                                        ? MessageType::MsgPreVoteResp
                                        : MessageType::MsgVoteResp;
    switch (m->type) {
        case MessageType::MsgProp:
            LOG_INFO("[{}-{}] no leader at term {}; dropping proposal", group_,
                     id_, term_);
            s.Set(ErrCode::ErrRaftNoLeader);
            co_return s;
        case MessageType::MsgApp:
            BecomeFollower(m->term, m->from);
            co_await HandleAppendEntries(m);
            break;
        case MessageType::MsgHeartbeat:
            BecomeFollower(m->term, m->from);
            HandleHeartbeat(m);
            break;
        case MessageType::MsgSnap:
            BecomeFollower(m->term, m->term);
            co_await HandleSnapshot(m);
            break;
        case MessageType::MsgPreVoteResp:
        case MessageType::MsgVoteResp: {
            if (m->type != my_vote_resp_type) {
                co_return s;
            }
            auto r = Poll(m->from, m->type, !m->reject);
            int gr, rj;
            VoteResult res;
            std::tie(gr, rj, res) = r;
            LOG_INFO("[{}-{}] has received {} {} votes and {} vote rejections",
                     group_, id_, gr, MessageTypeToString(m->type), rj);
            switch (res) {
                case VoteResult::VoteWon:
                    if (state_ == RaftState::StatePreCandidate) {
                        co_await Campaign(CampaignType::CampaignElection);
                    } else {
                        co_await BecomeLeader();
                        co_await BcastAppend();
                    }
                    break;
                case VoteResult::VoteLost:
                    BecomeFollower(term_, 0);
                    break;
            };
            break;
        }
        case MessageType::MsgTimeoutNow:
            LOG_DEBUG(
                "[{}-{}] [term {} state {}] ignored MsgTimeoutNow from {}",
                group_, id_, term_, RaftStateToString(state_), m->from);
            break;
    }
    co_return s;
}

seastar::future<Status<>> Raft::Hup(CampaignType t) {
    Status<> s;
    if (state_ == RaftState::StateLeader) {
        LOG_DEBUG("[{}-{}] ignoring MsgHup because already leader", group_,
                  id_);
        co_return s;
    }

    if (!Promotable()) {
        LOG_WARN("[{}-{}] is unpromotable and can not campaign", group_, id_);
        co_return s;
    }

    auto st = co_await raft_log_->Slice(raft_log_->applied() + 1,
                                        raft_log_->committed() + 1,
                                        std::numeric_limits<uint64_t>::max());
    if (!st) {
        LOG_FATAL_THROW(
            "[{}-{}] unexpected error getting unapplied entries ({})", group_,
            id_, st);
    }

    std::vector<EntryPtr> ents = std::move(st.Value());
    int n = NumOfPendingConf(ents);
    if (n != 0 && raft_log_->committed() > raft_log_->applied()) {
        LOG_WARN(
            "[{}-{}] cannot campaign at term {} since there are still {} "
            "pending configuration changes to apply",
            group_, id_, term_, n);
        co_return s;
    }

    LOG_INFO("[{}-{}] is starting a new election at term {}", group_, id_,
             term_);
    co_await Campaign(t);
    co_return s;
}

int Raft::NumOfPendingConf(const std::vector<EntryPtr>& ents) {
    int n = 0;
    for (auto e : ents) {
        if (e->type() == EntryType::EntryConfChange ||
            e->type() == EntryType::EntryConfChangeV2) {
            n++;
        }
    }
    return n;
}

seastar::future<Status<>> Raft::Campaign(CampaignType t) {
    Status<> s;
    if (!Promotable()) {
        LOG_WARN("[{}-{}] is unpromotable; campaign() should have been called",
                 group_, id_);
    }
    uint64_t term;
    MessageType vote_msg;

    if (t == CampaignType::CampaignPreElection) {
        BecomePreCandidate();
        vote_msg = MessageType::MsgPreVote;
        term = term_ + 1;
    } else {
        BecomeCandidate();
        vote_msg = MessageType::MsgVote;
        term = term_;
    }
    auto r =
        Poll(id_,
             (vote_msg == MessageType::MsgPreVote ? MessageType::MsgPreVoteResp
                                                  : MessageType::MsgVoteResp),
             true);
    VoteResult res;
    std::tie(std::ignore, std::ignore, res) = r;
    if (res == VoteResult::VoteWon) {
        if (t == CampaignType::CampaignPreElection) {
            co_await Campaign(CampaignType::CampaignElection);
        } else {
            co_await BecomeLeader();
        }
        co_return s;
    }

    auto last_term = co_await raft_log_->LastTerm();
    auto last_index = raft_log_->LastIndex();

    auto voters = prs_->voters();
    for (int i = 0; i < voters.size(); i++) {
        for (auto id : voters[i]) {
            if (id_ == id) {
                continue;
            }
            LOG_INFO(
                "[{}-{}] [logterm: {}, index: {}] sent {} request to "
                "{} at term {}",
                group_, id_, last_term, last_index,
                MessageTypeToString(vote_msg), id, term_);
            MessagePtr msg = seastar::make_lw_shared<Message>();
            if (t == CampaignType::CampaignTransfer) {
                msg->context = std::move(
                    seastar::temporary_buffer<char>((const char*)(&t), 1));
            }
            msg->term = term;
            msg->to = id;
            msg->type = vote_msg;
            msg->index = last_index;
            msg->log_term = last_term;
            Send(msg);
        }
    }
    co_return s;
}

std::tuple<int, int, VoteResult> Raft::Poll(uint64_t id, MessageType t,
                                            bool v) {
    if (v) {
        LOG_INFO("[{}-{}] received {} from {} at term {}", group_, id_,
                 MessageTypeToString(t), id, term_);
    } else {
        LOG_INFO("[{}-{}] received {} rejection from {} at term {}", group_,
                 id_, MessageTypeToString(t), id, term_);
    }

    prs_->RecordVote(id, v);
    return prs_->TallyVotes();
}

seastar::future<Status<>> Raft::HandleAppendEntries(MessagePtr m) {
    Status<> s;
    if (m->index < raft_log_->committed()) {
        MessagePtr msg = seastar::make_lw_shared<Message>();
        msg->to = m->from;
        msg->type = MessageType::MsgAppResp;
        msg->index = raft_log_->committed();
        Send(msg);
        co_return s;
    }

    uint64_t last_index;
    bool ok;
    auto r = co_await raft_log_->MaybeAppend(m->index, m->log_term, m->commit,
                                             std::move(m->entries));
    std::tie(last_index, ok) = r;
    if (ok) {
        MessagePtr msg = seastar::make_lw_shared<Message>();
        msg->to = m->from;
        msg->type = MessageType::MsgAppResp;
        msg->index = last_index;
        Send(msg);
    } else {
        LOG_DEBUG("[{}-{}] rejected MsgApp [logterm: {}, index: {}] from {}",
                  group_, id_, m->log_term, m->index, m->from);

        auto hint_index = std::min(m->index, raft_log_->LastIndex());
        hint_index =
            co_await raft_log_->FindConflictByTerm(hint_index, m->log_term);
        auto st = co_await raft_log_->Term(hint_index);
        if (!st) {
            LOG_FATAL_THROW("[{}-{}] term({}) must be valid, but got {}",
                            group_, id_, hint_index, st);
        }
        MessagePtr msg = seastar::make_lw_shared<Message>();
        msg->to = m->from;
        msg->type = MessageType::MsgAppResp;
        msg->index = m->index;
        msg->reject = true;
        msg->reject_hint = hint_index;
        msg->log_term = st.Value();
        Send(msg);
    }
    co_return s;
}

void Raft::HandleHeartbeat(MessagePtr m) {
    raft_log_->CommitTo(m->commit);
    MessagePtr msg = seastar::make_lw_shared<Message>();
    msg->to = m->from;
    msg->type = MessageType::MsgHeartbeatResp;
    msg->context = std::move(m->context.share());
    Send(msg);
}

seastar::future<Status<>> Raft::HandleSnapshot(MessagePtr m) {
    Status<> s;
    uint64_t sindex = m->snapshot->metadata().index();
    uint64_t sterm = m->snapshot->metadata().term();

    bool ok = co_await Restore(m->snapshot);
    MessagePtr msg = seastar::make_lw_shared<Message>();
    msg->to = m->from;
    msg->type = MessageType::MsgAppResp;
    if (ok) {
        msg->index = raft_log_->LastIndex();
        LOG_INFO("[{}-{}] [commit: {}] restored snapshot [index: {}, term: {}]",
                 group_, id_, raft_log_->committed(), sindex, sterm);
        Send(msg);
    } else {
        msg->index = raft_log_->committed();
        LOG_INFO("[{}-{}] [commit: {}] ignored snapshot [index: {}, term: {}]",
                 group_, id_, raft_log_->committed(), sindex, sterm);
        Send(msg);
    }
    co_return s;
}

seastar::future<bool> Raft::Restore(SnapshotPtr s) {
    if (s->metadata().index() <= raft_log_->committed()) {
        co_return false;
    }

    if (state_ != RaftState::StateFollower) {
        LOG_WARN(
            "[{}-{}] attempted to restore snapshot as leader; should "
            "never happen",
            group_, id_);
        BecomeFollower(term_ + 1, 0);
        co_return false;
    }

    bool found = false;
    auto cs = s->metadata().conf_state();

    auto f = [](const ConfState& cs, uint64_t id) -> bool {
        for (auto e : cs.voters()) {
            if (e == id) {
                return true;
            }
        }
        for (auto e : cs.learners()) {
            if (e == id) {
                return true;
            }
        }
        for (auto e : cs.voters_outgoing()) {
            if (e == id) {
                return true;
            }
        }
        return false;
    };

    found = f(cs, id_);
    if (!found) {
        LOG_WARN(
            "[{}-{}] attempted to restore snapshot but it is not in the "
            "ConfState; should never happen",
            group_, id_);
        co_return false;
    }

    // Now go ahead and actually restore.

    auto match = co_await raft_log_->MatchTerm(s->metadata().index(),
                                               s->metadata().term());
    if (match) {
        auto last_term = co_await raft_log_->LastTerm();
        LOG_INFO(
            "[{}-{}] [commit: {}, lastindex: {}, lastterm: {}] "
            "fast-forwarded commit to snapshot [index: {}, term: {}]",
            group_, id_, raft_log_->committed(), raft_log_->LastIndex(),
            last_term, s->metadata().index(), s->metadata().term());
        raft_log_->CommitTo(s->metadata().index());
        co_return false;
    }

    raft_log_->Restore(s);
    prs_ = seastar::make_lw_shared<ProgressTracker>(prs_->max_inflight());
    Changer chg(raft_log_->LastIndex(), prs_);
    auto st = chg.Restore(cs);
    if (!st) {
        LOG_FATAL_THROW("[{}-{}] unable to restore config: {}", group_, id_,
                        st);
    }
    TrackerConfig cfg;
    ProgressMap prs;
    std::tie(cfg, prs) = st.Value();

    auto cs1 = co_await SwitchToConfig(std::move(cfg), std::move(prs));
    if (cs != cs1) {
        LOG_FATAL_THROW("[{}-{}] confstate is not equivalent", group_, id_);
    }
    auto pr = prs_->progress()[id_];
    pr->MaybeUpdate(pr->next() - 1);

    auto last_term = co_await raft_log_->LastTerm();
    LOG_INFO(
        "[{}-{}] [commit: {}, lastindex: {}, lastterm: {}] restored "
        "snapshot [index: {}, term: {}]",
        group_, id_, raft_log_->committed(), raft_log_->LastIndex(), last_term,
        s->metadata().index(), s->metadata().term());
    co_return true;
}

bool Raft::Promotable() {
    auto it = prs_->progress().find(id_);

    ProgressPtr pr;
    if (it == prs_->progress().end()) {
        return false;
    }
    pr = it->second;
    return !pr->is_learner() && !raft_log_->HasPendingSnapshot();
}

bool Raft::PastElectionTimeout() {
    return election_elapsed_ >= randomized_election_timeout_;
}

void Raft::Reset(uint64_t term) {
    if (term_ != term) {
        term_ = term;
        vote_ = 0;
    }
    lead_ = 0;
    election_elapsed_ = 0;
    heartbeat_elapsed_ = 0;
    ResetRandomizedElectionTimeout();

    AbortLeaderTransfer();

    prs_->ResetVotes();

    prs_->Visit([this](uint64_t id, Progress* pr) {
        pr->set_match(0);
        pr->set_next(raft_log_->LastIndex() + 1);
        pr->ResetState(StateType::StateProbe);
        pr->set_recent_active(false);
        if (id == id_) {
            pr->set_match(raft_log_->LastIndex());
        }
    });

    pending_conf_index_ = 0;
    uncommitted_size_ = 0;
    read_only_->Reset();
}

void Raft::ResetRandomizedElectionTimeout() {
    randomized_election_timeout_ = election_timeout_ + (*dist_)(e_);
}

seastar::future<bool> Raft::AppendEntry(std::vector<EntryPtr> es) {
    uint64_t li = raft_log_->LastIndex();

    uint64_t s = 0;
    for (size_t i = 0; i < es.size(); i++) {
        es[i]->set_term(term_);
        es[i]->set_index(li + 1 + i);
        s += es[i]->data().size();
    }

    if (uncommitted_size_ > 0 && s > 0 &&
        uncommitted_size_ + s > max_uncommitted_size_) {
        LOG_INFO(
            "[{}-{}] appending new entries to log would exceed uncommitted "
            "entry size limit; dropping proposal",
            group_, id_);
        co_return false;
    }
    uncommitted_size_ += s;
    li = raft_log_->Append(es);
    prs_->progress()[id_]->MaybeUpdate(li);
    co_await MaybeCommit();
    co_return true;
}

void Raft::ReduceUncommittedSize(const std::vector<EntryPtr>& es) {
    if (uncommitted_size_ == 0) {
        return;
    }

    uint64_t s = 0;
    for (auto e : es) {
        s += e->data().size();
    }
    if (s > uncommitted_size_) {
        uncommitted_size_ = 0;
    } else {
        uncommitted_size_ -= s;
    }
}

MessagePtr Raft::ResponseToReadIndexReq(MessagePtr req, uint64_t read_index) {
    MessagePtr resp = make_raft_message();

    if (req->from == 0 || req->from == id_) {
        ReadState rs;
        rs.index = read_index;
        rs.request_ctx = std::move(req->entries[0]->data().share());
        read_states_.push_back(std::move(rs));
        return resp;
    }

    resp->type = MessageType::MsgReadIndexResp;
    resp->to = req->from;
    resp->index = read_index;
    resp->entries = std::move(req->entries);
    return resp;
}

seastar::future<bool> Raft::CommittedEntryInCurrentTerm() {
    auto s = co_await raft_log_->Term(raft_log_->committed());
    auto index = raft_log_->ZeroTermOnErrCompacted(s);
    co_return index == term_;
}

void Raft::SendMsgReadIndexResponse(MessagePtr m) {
    switch (read_only_->option()) {
        case ReadOnlyOption::ReadOnlySafe: {
            read_only_->AddRequest(raft_log_->committed(), m);
            seastar::sstring s(m->entries[0]->data().get(),
                               m->entries[0]->data().size());
            read_only_->RecvAck(id_, s);
            BcastHeartbeatWithCtx(s);
            break;
        }
        case ReadOnlyOption::ReadOnlyLeaseBased: {
            auto resp = ResponseToReadIndexReq(m, raft_log_->committed());
            if (resp->to != 0) {
                Send(resp);
            }
            break;
        }
    }
}

seastar::future<Status<>> Raft::ReleasePendingReadIndexMessages() {
    Status<> s;
    auto ok = co_await CommittedEntryInCurrentTerm();
    if (!ok) {
        LOG_ERROR(
            "[{}-{}] pending MsgReadIndex should be released only after "
            "first commit in current term",
            group_, id_);
        co_return s;
    }

    for (auto e : pending_readindex_messages_) {
        SendMsgReadIndexResponse(e);
    }
    pending_readindex_messages_.clear();
    co_return s;
}

void Raft::SendTimeoutNow(uint64_t to) {
    MessagePtr msg = make_raft_message();
    msg->to = to;
    msg->type = MessageType::MsgTimeoutNow;
    Send(msg);
}

seastar::future<Status<>> Raft::Step(MessagePtr m) {
    Status<> s;
    if (m->term == 0) {
    } else if (m->term > term_) {
        if (m->type == MessageType::MsgVote ||
            m->type == MessageType::MsgPreVote) {
            bool force =
                (m->context.size() == 1 &&
                 *m->context.get() == (char)CampaignType::CampaignTransfer);
            bool in_lease = check_quorum_ && lead_ != 0 &&
                            election_elapsed_ < election_timeout_;
            if (!force && in_lease) {
                auto last_term = co_await raft_log_->LastTerm();
                LOG_INFO(
                    "[{}-{}] [logterm: {}, index: {}, vote: {}] ignored {} "
                    "from {} [logterm: {}, index: {}] at term {}: lease is "
                    "not expired (remaining ticks: {})",
                    group_, id_, last_term, raft_log_->LastIndex(), vote_,
                    MessageTypeToString(m->type), m->from, m->log_term,
                    m->index, term_, election_timeout_ - election_elapsed_);
                co_return s;
            }
        }
        if (m->type != MessageType::MsgPreVote &&
            !(m->type == MessageType::MsgPreVoteResp && !m->reject)) {
            LOG_INFO(
                "[{}-{}] [term: {}] received a {} message with higher term "
                "from {} [term: {}]",
                group_, id_, term_, MessageTypeToString(m->type), m->from,
                m->term);
            if (m->type == MessageType::MsgApp ||
                m->type == MessageType::MsgHeartbeat ||
                m->type == MessageType::MsgSnap) {
                BecomeFollower(m->term, m->from);
            } else {
                BecomeFollower(m->term, 0);
            }
        }
    } else if (m->term < term_) {
        if ((check_quorum_ || pre_vote_) &&
            (m->type == MessageType::MsgHeartbeat ||
             m->type == MessageType::MsgApp)) {
            MessagePtr msg = make_raft_message();
            msg->to = m->from;
            msg->type = MessageType::MsgAppResp;
            Send(msg);
        } else if (m->type == MessageType::MsgPreVote) {
            auto last_term = co_await raft_log_->LastTerm();
            LOG_INFO(
                "[{}-{}] [logterm: {}, index: {}, vote: {}] rejected {} from "
                "{} [logterm: {}, index: {}] at term {}",
                group_, id_, last_term, raft_log_->LastIndex(), vote_,
                MessageTypeToString(m->type), m->from, m->log_term, m->index,
                term_);
            MessagePtr msg = make_raft_message();
            msg->to = m->from;
            msg->term = term_;
            msg->type = MessageType::MsgPreVoteResp;
            msg->reject = true;
            Send(msg);
        } else {
            LOG_INFO(
                "[{}-{}] [term: {}] ignored a {} message with lower term "
                "from {} [term: {}]",
                group_, id_, term_, MessageTypeToString(m->type), m->from,
                m->term);
        }
        co_return s;
    }

    switch (m->type) {
        case MessageType::MsgHup:
            if (pre_vote_) {
                co_await Hup(CampaignType::CampaignPreElection);
            } else {
                co_await Hup(CampaignType::CampaignElection);
            }
            break;
        case MessageType::MsgVote:
        case MessageType::MsgPreVote: {
            bool can_vote =
                vote_ == m->from || (vote_ == 0 && lead_ == 0) ||
                (m->type == MessageType::MsgPreVote && m->term > term_);
            bool is_up_to_date =
                co_await raft_log_->IsUpToDate(m->index, m->log_term);
            uint64_t last_term = co_await raft_log_->LastTerm();
            if (can_vote && is_up_to_date) {
                LOG_INFO(
                    "[{}-{}] [logterm: {}, index: {}, vote: {}] cast {} for {} "
                    "[logterm: {}, index: {}] at term {}",
                    group_, id_, last_term, raft_log_->LastIndex(), vote_,
                    MessageTypeToString(m->type), m->from, m->log_term,
                    m->index, term_);
                MessagePtr msg = make_raft_message();
                msg->to = m->from;
                msg->term = m->term;
                msg->type = m->type == MessageType::MsgVote
                                ? MessageType::MsgVoteResp
                                : MessageType::MsgPreVoteResp;
                Send(msg);
                if (m->type == MessageType::MsgVote) {
                    election_elapsed_ = 0;
                    vote_ = m->from;
                }
            } else {
                LOG_INFO(
                    "[{}-{}] [logterm: {}, index: {}, vote: {}] "
                    "rejected {} from {} "
                    "[logterm: {}, index: {}] at term {}",
                    group_, id_, last_term, raft_log_->LastIndex(), vote_,
                    MessageTypeToString(m->type), m->from, m->log_term,
                    m->index, term_);
                MessagePtr msg = make_raft_message();
                msg->to = m->from;
                msg->term = term_;
                msg->type = m->type == MessageType::MsgVote
                                ? MessageType::MsgVoteResp
                                : MessageType::MsgPreVoteResp;
                msg->reject = true;
                Send(msg);
            }
        }
        default: {
            s = co_await step_(m);
            if (!s) {
                co_return s;
            }
        }
    }
    co_return s;
}

seastar::future<ConfState> Raft::ApplyConfChange(ConfChangeV2 cc) {
    auto f = [this, &cc]() -> Status<std::tuple<TrackerConfig, ProgressMap>> {
        Changer changer(raft_log_->LastIndex(), prs_);
        if (cc.LeaveJoint()) {
            return changer.LeaveJoint();
        }

        bool auto_leave, ok;
        auto r = cc.EnterJoint();
        std::tie(auto_leave, ok) = r;
        if (ok) {
            return changer.EnterJoint(auto_leave, cc.changes());
        }

        return changer.Simple(cc.changes());
    };

    auto s = f();

    if (!s) {
        LOG_FATAL_THROW("[{}-{}] ApplyConfChange error: {}", group_, id_, s);
    }
    TrackerConfig cfg;
    ProgressMap prs;

    std::tie(cfg, prs) = s.Value();
    auto cs = co_await SwitchToConfig(std::move(cfg), std::move(prs));
    co_return std::move(cs);
}

seastar::future<Status<>> Raft::Advance(ReadyPtr rd) {
    Status<> s;
    uint64_t new_applied = 0;

    ReduceUncommittedSize(rd->committed_entries_);
    if (rd->committed_entries_.size() > 0) {
        new_applied = rd->committed_entries_.back()->index();
    } else if (rd->snapshot_ && rd->snapshot_->metadata().index() > 0) {
        new_applied = rd->snapshot_->metadata().index();
    }

    if (new_applied > 0) {
        auto old_applied = raft_log_->applied();
        raft_log_->AppliedTo(new_applied);
        if (prs_->auto_leave() && old_applied <= pending_conf_index_ &&
            new_applied >= pending_conf_index_ &&
            state_ == RaftState::StateLeader) {
            EntryPtr ent = make_entry();
            ent->set_type(EntryType::EntryConfChangeV2);
            std::vector<EntryPtr> ents;
            ents.push_back(ent);
            auto ok = co_await AppendEntry(std::move(ents));
            if (!ok) {
                LOG_FATAL_THROW(
                    "[{}-{}] refused un-refusable auto-leaving ConfChangeV2",
                    group_, id_);
            }
            pending_conf_index_ = raft_log_->LastIndex();
            LOG_INFO(
                "[{}-{}] initiating automatic transition out of joint "
                "configuration {}",
                group_, id_, *prs_);
        }
    }

    if (rd->entries_.size() > 0) {
        auto e = rd->entries_.back();
        raft_log_->StableTo(e->index(), e->term());
    }

    if (rd->snapshot_ && rd->snapshot_->metadata().index() != 0) {
        raft_log_->StableSnapTo(rd->snapshot_->metadata().index());
    }
    co_return s;
}

}  // namespace raft
}  // namespace snail
