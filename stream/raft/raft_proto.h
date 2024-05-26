#pragma once

#include <fmt/format.h>
#include <fmt/ostream.h>

#include <ostream>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>

namespace snail {
namespace raft {

enum class StateType {
    StateProbe = 0,
    StateReplicate = 1,
    StateSnapshot = 2,
};

const char* StateTypeToString(StateType type);

enum class RaftState {
    StateFollower = 0,
    StateCandidate = 1,
    StateLeader = 2,
    StatePreCandidate = 3,
};

const char* RaftStateToString(RaftState state);

enum class CampaignType {
    CampaignPreElection = 0,
    CampaignElection = 1,
    CampaignTransfer = 2,
};

enum class EntryType {
    EntryNormal = 0,
    EntryConfChange = 1,    // corresponds to ConfChange
    EntryConfChangeV2 = 2,  // corresponds to ConfChangeV2
};

std::ostream& operator<<(std::ostream& os, const StateType& type);
std::ostream& operator<<(std::ostream& os, const RaftState& rs);
std::ostream& operator<<(std::ostream& os, const CampaignType& type);
std::ostream& operator<<(std::ostream& os, const EntryType& type);

class Entry {
    EntryType type_;
    uint64_t term_;
    uint64_t index_;
    seastar::temporary_buffer<char> data_;

   public:
    Entry() noexcept : type_(EntryType::EntryNormal), term_(0), index_(0) {}
    Entry(Entry&& x)
        : type_(x.type_),
          term_(x.term_),
          index_(x.index_),
          data_(std::move(x.data_)) {}
    Entry(const Entry&) = delete;
    Entry& operator=(const Entry&) = delete;
    Entry& operator=(Entry&& x) {
        if (this != &x) {
            type_ = x.type_;
            term_ = x.term_;
            index_ = x.index_;
            data_ = std::move(x.data_);
        }
        return *this;
    }

    bool operator==(const Entry& x) const noexcept {
        return type_ == x.type_ && term_ == x.term_ && index_ == x.index_ &&
               data_ == x.data_;
    }

    bool operator!=(const Entry& x) const noexcept { return !(*this == x); }

    void Reset();

    size_t ByteSize() const;

    void MarshalTo(char* b);

    bool Unmarshal(seastar::temporary_buffer<char> data);

    EntryType type() const { return type_; }
    void set_type(EntryType type) { type_ = type; }
    uint64_t term() const { return term_; }
    void set_term(uint64_t term) { term_ = term; }
    uint64_t index() const { return index_; }
    void set_index(uint64_t index) { index_ = index; }
    const seastar::temporary_buffer<char>& data() const { return data_; }
    seastar::temporary_buffer<char>& data() { return data_; }
    void set_data(seastar::temporary_buffer<char>&& data) {
        data_ = std::move(data);
    }
};

using EntryPtr = seastar::lw_shared_ptr<Entry>;

EntryPtr make_entry();

class ConfState {
    // The voters in the incoming config. (If the configuration is not joint,
    // then the outgoing config is empty).
    std::vector<uint64_t> voters_;
    // The learners in the incoming config.
    std::vector<uint64_t> learners_;
    // The voters in the outgoing config.
    std::vector<uint64_t> voters_outgoing_;
    // The nodes that will become learners when the outgoing config is removed.
    // These nodes are necessarily currently in nodes_joint (or they would have
    // been added to the incoming config right away).
    std::vector<uint64_t> learners_next_;
    // If set, the config is joint and Raft will automatically transition into
    // the final config (i.e. remove the outgoing config) when this is safe.
    bool auto_leave_;

   public:
    ConfState() noexcept : auto_leave_(false) {}
    ConfState(ConfState&& x) noexcept
        : voters_(std::move(x.voters_)),
          learners_(std::move(x.learners_)),
          voters_outgoing_(std::move(x.voters_outgoing_)),
          learners_next_(std::move(x.learners_next_)),
          auto_leave_(x.auto_leave_) {}

    ConfState(const ConfState& x) noexcept
        : voters_(x.voters_),
          learners_(x.learners_),
          voters_outgoing_(x.voters_outgoing_),
          learners_next_(x.learners_next_),
          auto_leave_(x.auto_leave_) {}

    ConfState& operator=(const ConfState& x) {
        if (this != &x) {
            voters_ = x.voters_;
            learners_ = x.learners_;
            voters_outgoing_ = x.voters_outgoing_;
            learners_next_ = x.learners_next_;
            auto_leave_ = x.auto_leave_;
        }
        return *this;
    }

    ConfState& operator=(ConfState&& x) {
        if (this != &x) {
            voters_ = std::move(x.voters_);
            learners_ = std::move(x.learners_);
            voters_outgoing_ = std::move(x.voters_outgoing_);
            learners_next_ = std::move(x.learners_next_);
            auto_leave_ = x.auto_leave_;
        }
        return *this;
    }

    bool operator==(const ConfState& x) const noexcept {
        std::vector<uint64_t> voters1(voters_);
        std::vector<uint64_t> voters2(x.voters_);
        std::sort(voters1.begin(), voters1.end());
        std::sort(voters2.begin(), voters2.end());

        std::vector<uint64_t> learners1(learners_);
        std::vector<uint64_t> learners2(x.learners_);
        std::sort(learners1.begin(), learners1.end());
        std::sort(learners2.begin(), learners2.end());

        std::vector<uint64_t> voters_outgoing1(voters_outgoing_);
        std::vector<uint64_t> voters_outgoing2(x.voters_outgoing_);
        std::sort(voters_outgoing1.begin(), voters_outgoing1.end());
        std::sort(voters_outgoing2.begin(), voters_outgoing2.end());

        std::vector<uint64_t> learners_next1(learners_next_);
        std::vector<uint64_t> learners_next2(x.learners_next_);
        std::sort(learners_next1.begin(), voters_outgoing1.end());
        std::sort(learners_next2.begin(), voters_outgoing2.end());

        return voters1 == voters2 && learners1 == learners2 &&
               voters_outgoing1 == voters_outgoing2 &&
               learners_next1 == learners_next2 && auto_leave_ == x.auto_leave_;
    }

    bool operator!=(const ConfState& x) const noexcept { return !(*this == x); }

    void Reset();

    bool Empty() const;

    size_t ByteSize() const;

    void MarshalTo(char* b);

    bool Unmarshal(seastar::temporary_buffer<char> data);

    std::vector<uint64_t>& voters() { return voters_; }
    const std::vector<uint64_t>& voters() const { return voters_; }

    void set_voters(std::vector<uint64_t> v) { voters_ = std::move(v); }

    std::vector<uint64_t>& learners() { return learners_; }
    const std::vector<uint64_t>& learners() const { return learners_; }

    void set_learners(std::vector<uint64_t> learners) {
        learners_ = std::move(learners);
    }

    std::vector<uint64_t>& voters_outgoing() { return voters_outgoing_; }
    const std::vector<uint64_t>& voters_outgoing() const {
        return voters_outgoing_;
    }

    void set_voters_outgoing(std::vector<uint64_t> v) {
        voters_outgoing_ = std::move(v);
    }
    std::vector<uint64_t>& learners_next() { return learners_next_; }
    const std::vector<uint64_t>& learners_next() const {
        return learners_next_;
    }

    void set_learners_next(std::vector<uint64_t> v) {
        learners_next_ = std::move(v);
    }
    bool auto_leave() const { return auto_leave_; }
    void set_auto_leave(bool v) { auto_leave_ = v; }
};

class SnapshotMetadata {
    ConfState conf_state_;
    uint64_t index_;
    uint64_t term_;

   public:
    SnapshotMetadata() : conf_state_(), index_(0), term_(0) {}
    SnapshotMetadata(SnapshotMetadata&& x) = default;

    SnapshotMetadata(const SnapshotMetadata&) = delete;
    SnapshotMetadata& operator=(const SnapshotMetadata&) = delete;

    SnapshotMetadata& operator=(SnapshotMetadata&& x) {
        if (this != &x) {
            conf_state_ = std::move(x.conf_state_);
            index_ = x.index_;
            term_ = x.term_;
        }
        return *this;
    }

    bool operator==(const SnapshotMetadata& x) const noexcept {
        return conf_state_ == x.conf_state_ && index_ == x.index_ &&
               term_ == x.term_;
    }

    bool operator!=(const SnapshotMetadata& x) const noexcept {
        return !(*this == x);
    }

    bool Empty() const;

    void Reset();

    size_t ByteSize() const;

    void MarshalTo(char* b);

    bool Unmarshal(seastar::temporary_buffer<char> data);

    ConfState& conf_state() { return conf_state_; }
    const ConfState& conf_state() const { return conf_state_; }
    uint64_t index() const { return index_; }
    uint64_t term() const { return term_; }

    void set_index(uint64_t v) { index_ = v; }
    void set_term(uint64_t v) { term_ = v; }
    void set_conf_state(ConfState v) { conf_state_ = std::move(v); }
};

class Snapshot {
    seastar::temporary_buffer<char> data_;
    SnapshotMetadata metadata_;

   public:
    Snapshot() : data_(), metadata_() {}
    Snapshot(Snapshot&& x)
        : data_(std::move(x.data_)), metadata_(std::move(x.metadata_)) {}

    Snapshot(const Snapshot&) = delete;
    Snapshot& operator=(const Snapshot&) = delete;

    Snapshot& operator=(Snapshot&& x) {
        if (this != &x) {
            data_ = std::move(x.data_);
            metadata_ = std::move(x.metadata_);
        }
        return *this;
    }

    bool operator==(const Snapshot& x) const noexcept {
        return data_ == x.data_ && metadata_ == x.metadata_;
    }

    bool operator!=(const Snapshot& x) const noexcept { return !(*this == x); }

    bool Empty() const;

    void Reset();

    size_t ByteSize() const;

    void MarshalTo(char* b);

    bool Unmarshal(seastar::temporary_buffer<char> data);

    seastar::temporary_buffer<char>& data() { return data_; }
    const seastar::temporary_buffer<char>& data() const { return data_; }
    SnapshotMetadata& metadata() { return metadata_; }
    const SnapshotMetadata& metadata() const { return metadata_; }

    void set_data(seastar::temporary_buffer<char> data) {
        data_ = std::move(data);
    }

    void set_metadata(SnapshotMetadata metadata) {
        metadata_ = std::move(metadata);
    }
};

using SnapshotPtr = seastar::lw_shared_ptr<Snapshot>;

SnapshotPtr make_snapshot();

enum class MessageType {
    MsgHup = 0,
    MsgBeat = 1,
    MsgProp = 2,
    MsgApp = 3,
    MsgAppResp = 4,
    MsgVote = 5,
    MsgVoteResp = 6,
    MsgSnap = 7,
    MsgHeartbeat = 8,
    MsgHeartbeatResp = 9,
    MsgUnreachable = 10,
    MsgSnapStatus = 11,
    MsgCheckQuorum = 12,
    MsgTransferLeader = 13,
    MsgTimeoutNow = 14,
    MsgReadIndex = 15,
    MsgReadIndexResp = 16,
    MsgPreVote = 17,
    MsgPreVoteResp = 18,
};

const char* MessageTypeToString(const MessageType type);

std::ostream& operator<<(std::ostream& os, const MessageType& type);

inline bool IsLocalMsg(MessageType t) {
    return (t == MessageType::MsgHup || t == MessageType::MsgBeat ||
            t == MessageType::MsgUnreachable ||
            t == MessageType::MsgSnapStatus ||
            t == MessageType::MsgCheckQuorum);
}

inline bool IsResponseMsg(MessageType t) {
    return (t == MessageType::MsgAppResp || t == MessageType::MsgVoteResp ||
            t == MessageType::MsgHeartbeatResp ||
            t == MessageType::MsgUnreachable ||
            t == MessageType::MsgPreVoteResp);
}

struct Message {
    MessageType type;
    uint64_t to;
    uint64_t from;
    uint64_t term;
    uint64_t log_term;
    uint64_t index;
    std::vector<EntryPtr> entries;
    uint64_t commit;
    SnapshotPtr snapshot;
    bool reject;
    uint64_t reject_hint;
    seastar::temporary_buffer<char> context;

    Message()
        : type(MessageType::MsgHup),
          to(0),
          from(0),
          term(0),
          log_term(0),
          index(0),
          commit(0),
          reject(false),
          reject_hint(0),
          context() {}

    bool operator==(const Message& x) const noexcept {
        if (entries.size() != x.entries.size()) {
            return false;
        }
        int n = entries.size();
        for (int i = 0; i < n; i++) {
            if (*(entries[i]) != *(x.entries[i])) {
                return false;
            }
        }
        if (!snapshot) {
            if (x.snapshot) {
                return false;
            }
        } else {
            if (!x.snapshot) {
                return false;
            } else if (*snapshot != *x.snapshot) {
                return false;
            }
        }
        return type == x.type && to == x.to && from == x.from &&
               term == x.term && log_term == x.log_term && index == x.index &&
               commit == x.commit && reject == x.reject && context == x.context;
    }

    bool operator!=(const Message& x) const noexcept { return !(*this == x); }

    bool Empty() const;

    void Reset();

    size_t ByteSize() const;

    void MarshalTo(char* b);

    bool Unmarshal(seastar::temporary_buffer<char> data);
};

using MessagePtr = seastar::lw_shared_ptr<Message>;

MessagePtr make_raft_message();

class HardState {
    uint64_t term_;
    uint64_t vote_;
    uint64_t commit_;

   public:
    HardState() noexcept : term_(0), vote_(0), commit_(0) {}
    HardState(uint64_t t, uint64_t v, uint64_t c) noexcept
        : term_(t), vote_(v), commit_(c) {}
    HardState(const HardState& x) noexcept
        : term_(x.term_), vote_(x.vote_), commit_(x.commit_) {}

    HardState(HardState&& x) noexcept
        : term_(x.term_), vote_(x.vote_), commit_(x.commit_) {}

    HardState& operator=(const HardState& x) {
        if (this != &x) {
            term_ = x.term_;
            vote_ = x.vote_;
            commit_ = x.commit_;
        }
        return *this;
    }

    bool operator==(const HardState& x) {
        return term_ == x.term_ && vote_ == x.vote_ && commit_ == x.commit_;
    }

    bool operator!=(const HardState& x) { return !(*this == x); }

    bool Empty() const { return term_ == 0 && vote_ == 0 && commit_ == 0; }

    void Reset();

    size_t ByteSize() const;

    void MarshalTo(char* b);

    bool Unmarshal(seastar::temporary_buffer<char> data);

    uint64_t term() const { return term_; }
    uint64_t vote() const { return vote_; }
    uint64_t commit() const { return commit_; }

    void set_term(uint64_t v) { term_ = v; }
    void set_vote(uint64_t v) { vote_ = v; }
    void set_commit(uint64_t v) { commit_ = v; }
};

enum class ConfChangeTransition {
    // Automatically use the simple protocol if possible, otherwise fall back
    // to ConfChangeJointImplicit. Most applications will want to use this.
    ConfChangeTransitionAuto = 0,
    // Use joint consensus unconditionally, and transition out of them
    // automatically (by proposing a zero configuration change).
    //
    // This option is suitable for applications that want to minimize the time
    // spent in the joint configuration and do not store the joint configuration
    // in the state machine (outside of InitialState).
    ConfChangeTransitionJointImplicit = 1,
    // Use joint consensus and remain in the joint configuration until the
    // application proposes a no-op configuration change. This is suitable for
    // applications that want to explicitly control the transitions, for example
    // to use a custom payload (via the Context field).
    ConfChangeTransitionJointExplicit = 2,
};

enum class ConfChangeType {
    ConfChangeAddNode = 0,
    ConfChangeRemoveNode = 1,
    ConfChangeUpdateNode = 2,
    ConfChangeAddLearnerNode = 3,
};

class ConfChange {
    ConfChangeType type_;
    uint64_t node_id_;
    seastar::temporary_buffer<char> context_;

   public:
    ConfChange() : type_(ConfChangeType::ConfChangeAddNode), node_id_(0) {}
    ConfChange(const ConfChange& x)
        : type_(x.type_),
          node_id_(x.node_id_),
          context_(std::move(x.context_.clone())) {}

    ConfChange(ConfChange&& x)
        : type_(x.type_),
          node_id_(x.node_id_),
          context_(std::move(x.context_)) {}

    ConfChange& operator=(const ConfChange& x) noexcept {
        if (this != &x) {
            type_ = x.type_;
            node_id_ = x.node_id_;
            context_ = x.context_.clone();
        }
        return *this;
    }

    ConfChange& operator=(ConfChange&& x) noexcept {
        if (this != &x) {
            type_ = x.type_;
            node_id_ = x.node_id_;
            context_ = std::move(x.context_);
        }
        return *this;
    }

    bool Empty() const;

    void Reset();

    size_t ByteSize() const;

    void MarshalTo(char* b);

    bool Unmarshal(seastar::temporary_buffer<char> data);

    seastar::temporary_buffer<char> Marshal();

    void set_type(ConfChangeType type) { type_ = type; }
    ConfChangeType type() const { return type_; }

    void set_node_id(uint64_t node_id) { node_id_ = node_id; }
    uint64_t node_id() const { return node_id_; }

    void set_context(seastar::temporary_buffer<char> context) {
        context_ = std::move(context);
    }
    seastar::temporary_buffer<char>& context() { return context_; }
    const seastar::temporary_buffer<char>& context() const { return context_; }

    friend std::ostream& operator<<(std::ostream& os, const ConfChange& cc);
};

class ConfChangeSingle {
    ConfChangeType type_;
    uint64_t node_id_;

   public:
    ConfChangeSingle()
        : type_(ConfChangeType::ConfChangeAddNode), node_id_(0) {}

    ConfChangeSingle(ConfChangeType type, uint64_t id)
        : type_(type), node_id_(id) {}

    ConfChangeSingle(const ConfChangeSingle& x)
        : type_(x.type_), node_id_(x.node_id_) {}

    ConfChangeSingle(ConfChangeSingle&&) = default;

    ConfChangeSingle& operator=(const ConfChangeSingle& x) noexcept {
        if (this != &x) {
            type_ = x.type_;
            node_id_ = x.node_id_;
        }
        return *this;
    }

    ConfChangeSingle& operator=(ConfChangeSingle&& x) noexcept {
        if (this != &x) {
            type_ = x.type_;
            node_id_ = x.node_id_;
        }
        return *this;
    }

    bool Empty() const;

    void Reset();

    size_t ByteSize() const;

    void MarshalTo(char* b);

    bool Unmarshal(seastar::temporary_buffer<char> data);

    seastar::temporary_buffer<char> Marshal();

    void set_type(ConfChangeType type) { type_ = type; }
    ConfChangeType type() const { return type_; }

    void set_node_id(uint64_t node_id) { node_id_ = node_id; }
    uint64_t node_id() const { return node_id_; }
};

class ConfChangeV2 {
    ConfChangeTransition transition_;
    std::vector<ConfChangeSingle> changes_;
    seastar::temporary_buffer<char> context_;

   public:
    ConfChangeV2()
        : transition_(ConfChangeTransition::ConfChangeTransitionAuto) {}

    ConfChangeV2(const ConfChangeV2& x)
        : transition_(x.transition_),
          changes_(x.changes_),
          context_(std::move(x.context_.clone())) {}

    ConfChangeV2(ConfChangeV2&& x)
        : transition_(x.transition_),
          changes_(std::move(x.changes_)),
          context_(std::move(x.context_)) {}

    ConfChangeV2& operator=(const ConfChangeV2& x) {
        if (this != &x) {
            transition_ = x.transition_;
            changes_ = x.changes_;
            context_ = x.context_.clone();
        }
        return *this;
    }

    ConfChangeV2& operator=(ConfChangeV2&& x) {
        if (this != &x) {
            transition_ = x.transition_;
            changes_ = std::move(x.changes_);
            context_ = std::move(x.context_);
        }
        return *this;
    }

    bool Empty() const;

    void Reset();

    size_t ByteSize() const;

    void MarshalTo(char* b);

    bool Unmarshal(seastar::temporary_buffer<char> data);

    seastar::temporary_buffer<char> Marshal();

    bool LeaveJoint() {
        return transition_ == ConfChangeTransition::ConfChangeTransitionAuto &&
               changes_.empty();
    }

    std::tuple<bool, bool> EnterJoint();

    void set_transition(ConfChangeTransition t) { transition_ = t; }
    ConfChangeTransition transition() const { return transition_; }
    std::vector<ConfChangeSingle>& changes() { return changes_; }
    const std::vector<ConfChangeSingle>& changes() const { return changes_; }
    seastar::temporary_buffer<char>& context() { return context_; }
    const seastar::temporary_buffer<char>& context() const { return context_; }
    void set_context(seastar::temporary_buffer<char> c) {
        context_ = std::move(c);
    }

    friend std::ostream& operator<<(std::ostream& os, const ConfChangeV2& cc);
};

class ConfChangeI {
    ConfChange v1_;
    ConfChangeV2 v2_;
    bool is_v1_;

   public:
    ConfChangeI(const ConfChange& v1) : v1_(v1), is_v1_(true) {
        ConfChangeSingle cs(v1.type(), v1.node_id());
        v2_.changes().push_back(std::move(cs));
        v2_.set_context(v1_.context().share());
    }
    ConfChangeI(const ConfChangeV2& v2) : v2_(v2), is_v1_(false) {}

    ConfChangeI(const ConfChangeI& x)
        : v1_(x.v1_), v2_(x.v2_), is_v1_(x.is_v1_) {}

    ConfChangeI(ConfChangeI&& x)
        : v1_(std::move(x.v1_)), v2_(std::move(x.v2_)), is_v1_(x.is_v1_) {}

    ConfChangeI& operator=(const ConfChangeI& x) {
        if (this != &x) {
            v1_ = x.v1_;
            v2_ = x.v2_;
            is_v1_ = x.is_v1_;
        }
        return *this;
    }

    ConfChangeI& operator=(ConfChangeI&& x) {
        if (this != &x) {
            v1_ = std::move(x.v1_);
            v2_ = std::move(x.v2_);
            is_v1_ = x.is_v1_;
        }
        return *this;
    }

    ConfChangeV2& AsV2() { return v2_; }

    ConfChange& AsV1() { return v1_; }

    bool IsV1() const { return is_v1_; }

    friend std::ostream& operator<<(std::ostream& os, const ConfChangeI& cc);

    seastar::temporary_buffer<char> Marshal();
};

}  // namespace raft
}  // namespace snail

#if FMT_VERSION >= 90000
template <>
struct fmt::formatter<snail::raft::StateType> : fmt::ostream_formatter {};

template <>
struct fmt::formatter<snail::raft::RaftState> : fmt::ostream_formatter {};

template <>
struct fmt::formatter<snail::raft::CampaignType> : fmt::ostream_formatter {};

template <>
struct fmt::formatter<snail::raft::MessageType> : fmt::ostream_formatter {};

template <>
struct fmt::formatter<snail::raft::EntryType> : fmt::ostream_formatter {};

template <>
struct fmt::formatter<snail::raft::ConfChange> : fmt::ostream_formatter {};

template <>
struct fmt::formatter<snail::raft::ConfChangeV2> : fmt::ostream_formatter {};

template <>
struct fmt::formatter<snail::raft::ConfChangeI> : fmt::ostream_formatter {};
#endif
