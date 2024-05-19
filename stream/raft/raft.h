#pragma once

#include <optional>
#include <random>

#include "common/macro.h"
#include "confchange.h"
#include "progress.h"
#include "raft_log.h"
#include "raft_proto.h"
#include "read_only.h"
#include "tracker.h"

namespace snail {
namespace raft {

struct SoftState {
    uint64_t lead;
    RaftState raft_state;
};

struct Ready {
    std::optional<SoftState> st;
    HardState hs;
    std::vector<ReadState> rss;
    std::vector<EntryPtr> entries;  // can't move, but entry data can moveing
    SnapshotPtr snapshot;
    std::vector<EntryPtr>
        committed_entries;         // can't move include entry and entry data
    std::vector<MessagePtr> msgs;  // can move
    bool sync = false;

    Ready() = default;
};

using ReadyPtr = seastar::lw_shared_ptr<Ready>;

class RawNode;
class Node;

class Raft {
    SNAIL_PRIVATE
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
    std::function<seastar::future<>()> tick_;
    std::function<seastar::future<int>(MessagePtr)> step_;
    std::vector<MessagePtr> pending_readindex_messages_;
    std::default_random_engine e_;
    std::optional<std::uniform_int_distribution<int>> dist_;

   public:
    friend class RawNode;
    friend class Node;
    struct Config {
        uint64_t group = 0;
        uint64_t id = 0;
        int election_tick = 0;
        int heartbeat_tick = 0;
        seastar::shared_ptr<Storage> storage;
        uint64_t applied = 0;
        uint64_t max_size_per_msg = 0;
        uint64_t max_committed_size_per_ready = 0;
        uint64_t max_uncommitted_entries_size = 0;
        int max_inflight_msgs = 0;
        bool check_quorum = false;
        bool pre_vote = false;
        ReadOnlyOption read_only_option = ReadOnlyOption::ReadOnlySafe;
        bool disable_proposal_forwarding = false;
    };

    Raft();

    seastar::future<Status<>> Init(const Config c);

    seastar::future<int> Step(MessagePtr m);

    seastar::future<ConfState> ApplyConfChange(ConfChangeV2 cc);

    SoftState GetSoftState();
    HardState GetHardState();

    bool HasLeader() { return lead_ != 0; }

    SNAIL_PRIVATE

    bool Validate(const Config& c);

    seastar::future<ConfState> SwitchToConfig(TrackerConfig cfg,
                                              ProgressMap prs);

    seastar::future<bool> MaybeCommit();

    seastar::future<> BcastAppend();

    void BcastHeartbeat();

    void BcastHeartbeatWithCtx(const seastar::sstring& ctx);

    void SendHeartbeat(uint64_t to, const seastar::sstring& ctx);

    seastar::future<> SendAppend(uint64_t to);

    seastar::future<bool> MaybeSendAppend(uint64_t to, bool send_if_empty);

    void Send(MessagePtr m);

    void AbortLeaderTransfer();

    void LoadState(const HardState& hs);

    void BecomeFollower(uint64_t term, uint64_t lead);

    void BecomePreCandidate();

    void BecomeCandidate();

    seastar::future<> BecomeLeader();

    void Reset(uint64_t term);

    void ResetRandomizedElectionTimeout();

    seastar::future<> TickElection();

    seastar::future<> TickHeartbeat();

    seastar::future<int> StepLeader(MessagePtr m);

    seastar::future<int> StepFollower(MessagePtr m);

    seastar::future<int> StepCandidate(MessagePtr m);

    bool Promotable();

    bool PastElectionTimeout();

    seastar::future<> HandleAppendEntries(MessagePtr m);

    void HandleHeartbeat(MessagePtr m);

    seastar::future<> HandleSnapshot(MessagePtr m);

    seastar::future<bool> Restore(SnapshotPtr s);

    seastar::future<> Hup(CampaignType t);

    int NumOfPendingConf(const std::vector<EntryPtr>& ents);

    seastar::future<> Campaign(CampaignType t);

    std::tuple<int, int, VoteResult> Poll(uint64_t id, MessageType t, bool v);

    seastar::future<bool> AppendEntry(std::vector<EntryPtr> es);

    void ReduceUncommittedSize(const std::vector<EntryPtr>& es);

    MessagePtr ResponseToReadIndexReq(MessagePtr req, uint64_t read_index);

    seastar::future<bool> CommittedEntryInCurrentTerm();

    void SendMsgReadIndexResponse(MessagePtr m);

    seastar::future<> ReleasePendingReadIndexMessages();

    void SendTimeoutNow(uint64_t to);

    seastar::future<> Advance(ReadyPtr rd);
};

using RaftPtr = seastar::lw_shared_ptr<Raft>;

}  // namespace raft
}  // namespace snail
