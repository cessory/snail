#pragma once

#include <optional>
#include <random>

#include "confchange.h"
#include "progress.h"
#include "raft_log.h"
#include "raft_proto.h"
#include "read_only.h"
#include "tracker.h"

namespace snail {
namespace raft {

struct SoftState {
    uint64_t lead = 0;
    RaftState raft_state = RaftState::StateFollower;
};

class Raft;
class RawNode;

class Ready {
    std::optional<SoftState> st_;
    HardState hs_;
    std::vector<ReadState> rss_;
    std::vector<EntryPtr> entries_;  // can't move, but entry data can moveing
    SnapshotPtr snapshot_;
    std::vector<EntryPtr>
        committed_entries_;         // can't move include entry and entry data
    std::vector<MessagePtr> msgs_;  // can move
    bool sync_ = false;

   public:
    friend class Raft;
    friend class RawNode;
    Ready() = default;

    std::optional<SoftState> GetSoftState() { return st_; }

    HardState GetHardState() { return hs_; }

    const std::vector<ReadState>& GetReadStates() { return rss_; }

    std::vector<EntryPtr> GetEntries() { return entries_; }

    SnapshotPtr GetSnapshot() { return snapshot_; }

    std::vector<EntryPtr> GetCommittedEntries() { return committed_entries_; }

    const std::vector<MessagePtr>& GetSendMsgs() { return msgs_; }

    bool IsSync() { return sync_; }
};

using ReadyPtr = seastar::lw_shared_ptr<Ready>;

class Raft;
using RaftPtr = seastar::lw_shared_ptr<Raft>;

class Raft {
#ifdef RAFT_UT_TEST
   public:
#endif
    uint64_t group_;
    uint64_t id_;
    uint64_t term_;
    uint64_t vote_;
    std::vector<ReadState> read_states_;
    RaftLogPtr raft_log_;
    uint64_t max_msg_size_;
    uint64_t max_uncommitted_size_;
    ProgressTrackerPtr prs_;
    RaftState state_;
    bool is_learner_;
    std::vector<MessagePtr> msgs_;
    uint64_t lead_;
    uint64_t lead_transferee_;
    uint64_t pending_conf_index_;
    uint64_t uncommitted_size_;
    seastar::lw_shared_ptr<ReadOnly> read_only_;
    int election_elapsed_;
    int heartbeat_elapsed_;
    bool check_quorum_;
    bool pre_vote_;
    int heartbeat_timeout_;
    int election_timeout_;
    int randomized_election_timeout_;
    bool disable_proposal_forwarding_;
    std::function<seastar::future<Status<>>()> tick_;
    std::function<seastar::future<Status<>>(MessagePtr)> step_;
    std::vector<MessagePtr> pending_readindex_messages_;
    std::default_random_engine e_;
    std::optional<std::uniform_int_distribution<int>> dist_;

    Raft();

   public:
    friend class RawNode;
    struct Config {
        uint64_t group = 0;
        uint64_t id = 0;
        int election_tick = 0;
        int heartbeat_tick = 0;
        seastar::shared_ptr<Storage> storage;
        uint64_t applied = 0;
        uint64_t max_size_per_msg = std::numeric_limits<uint64_t>::max();
        uint64_t max_committed_size_per_ready =
            std::numeric_limits<uint64_t>::max();
        uint64_t max_uncommitted_entries_size =
            std::numeric_limits<uint64_t>::max();
        int max_inflight_msgs = 1024;
        bool check_quorum = true;
        bool pre_vote = true;
        ReadOnlyOption read_only_option = ReadOnlyOption::ReadOnlySafe;
        bool disable_proposal_forwarding = false;
    };

    static seastar::future<Status<RaftPtr>> Create(const Raft::Config c);

    seastar::future<Status<>> Step(MessagePtr m);

    seastar::future<ConfState> ApplyConfChange(ConfChangeV2 cc);

    SoftState GetSoftState();
    HardState GetHardState();

    bool HasLeader() { return lead_ != 0; }

#ifdef RAFT_UT_TEST
   public:
#else
   private:
#endif
    bool Validate(const Config& c);

    seastar::future<ConfState> SwitchToConfig(TrackerConfig cfg,
                                              ProgressMap prs);

    seastar::future<bool> MaybeCommit();

    seastar::future<Status<>> BcastAppend();

    void BcastHeartbeat();

    void BcastHeartbeatWithCtx(const seastar::sstring& ctx);

    void SendHeartbeat(uint64_t to, const seastar::sstring& ctx);

    seastar::future<Status<>> SendAppend(uint64_t to);

    seastar::future<bool> MaybeSendAppend(uint64_t to, bool send_if_empty);

    void Send(MessagePtr m);

    void AbortLeaderTransfer();

    void LoadState(const HardState& hs);

    void BecomeFollower(uint64_t term, uint64_t lead);

    void BecomePreCandidate();

    void BecomeCandidate();

    seastar::future<Status<>> BecomeLeader();

    void Reset(uint64_t term);

    void ResetRandomizedElectionTimeout();

    seastar::future<Status<>> TickElection();

    seastar::future<Status<>> TickHeartbeat();

    seastar::future<Status<>> StepLeader(MessagePtr m);

    seastar::future<Status<>> StepFollower(MessagePtr m);

    seastar::future<Status<>> StepCandidate(MessagePtr m);

    bool Promotable();

    bool PastElectionTimeout();

    seastar::future<Status<>> HandleAppendEntries(MessagePtr m);

    void HandleHeartbeat(MessagePtr m);

    seastar::future<Status<>> HandleSnapshot(MessagePtr m);

    seastar::future<bool> Restore(SnapshotPtr s);

    seastar::future<Status<>> Hup(CampaignType t);

    int NumOfPendingConf(const std::vector<EntryPtr>& ents);

    seastar::future<Status<>> Campaign(CampaignType t);

    std::tuple<int, int, VoteResult> Poll(uint64_t id, MessageType t, bool v);

    seastar::future<bool> AppendEntry(std::vector<EntryPtr> es);

    void ReduceUncommittedSize(const std::vector<EntryPtr>& es);

    MessagePtr ResponseToReadIndexReq(MessagePtr req, uint64_t read_index);

    seastar::future<bool> CommittedEntryInCurrentTerm();

    void SendMsgReadIndexResponse(MessagePtr m);

    seastar::future<Status<>> ReleasePendingReadIndexMessages();

    void SendTimeoutNow(uint64_t to);

    seastar::future<Status<>> Advance(ReadyPtr rd);
};

}  // namespace raft
}  // namespace snail
