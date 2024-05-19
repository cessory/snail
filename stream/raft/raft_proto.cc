#include "raft_proto.h"

#include <fmt/format.h>
#include <string.h>

#include <seastar/net/byteorder.hh>

#include "raft_errno.h"
#include "util/util.h"

namespace snail {
namespace raft {

static const char* prstmap[] = {
    "StateProbe",
    "StateReplicate",
    "StateSnapshot",
};

static const char* raft_states[] = {
    "StateFollower",
    "StateCandidate",
    "StateLeader",
    "StatePreCandidate",
};

static const char* msg_types[] = {
    "MsgHup",           "MsgBeat",           "MsgProp",        "MsgApp",
    "MsgAppResp",       "MsgVote",           "MsgVoteResp",    "MsgSnap",
    "MsgHeartbeat",     "MsgHeartbeatResp",  "MsgUnreachable", "MsgSnapStatus",
    "MsgCheckQuorum",   "MsgTransferLeader", "MsgTimeoutNow",  "MsgReadIndex",
    "MsgReadIndexResp", "MsgPreVote",        "MsgPreVoteResp",
};

const char* StateTypeToString(StateType type) { return prstmap[(int)type]; }

const char* RaftStateToString(RaftState state) {
    return raft_states[(int)state];
}

void Entry::Reset() {
    seastar::temporary_buffer<char> empty;
    type_ = EntryType::EntryNormal;
    term_ = 0;
    index_ = 0;
    data_ = std::move(empty);
}

size_t Entry::ByteSize() const {
    size_t size = 0;
    if (type_ != EntryType::EntryNormal) {
        size += 1 + VarintLength((uint64_t)type_);
    }
    if (term_) {
        size += 1 + VarintLength(term_);
    }

    if (index_) {
        size += 1 + VarintLength(index_);
    }

    if (!data_.empty()) {
        size += 1 + VarintLength(data_.size()) + data_.size();
    }
    return size;
}

void Entry::MarshalTo(char* b) {
    if (type_ != EntryType::EntryNormal) {
        b = PutVarint32(b, 1);
        b = PutVarint32(b, (uint32_t)type_);
    }
    if (term_) {
        b = PutVarint32(b, 2);
        b = PutVarint64(b, term_);
    }
    if (index_) {
        b = PutVarint32(b, 3);
        b = PutVarint64(b, index_);
    }
    if (!data_.empty()) {
        b = PutVarint32(b, 4);
        b = PutVarint64(b, data_.size());
        memcpy(b, data_.get(), data_.size());
    }
    return;
}

bool Entry::Unmarshal(seastar::temporary_buffer<char> data) {
    const char* s = data.begin();
    const char* end = data.end();
    size_t n = data.size();
    uint32_t old_tag = 0;

    Reset();
    if (n == 0) {
        return true;
    }
    while (old_tag < 4 && s < end) {
        uint32_t tag = 0;
        // 1 get tag
        s = GetVarint32(s, n, &tag);
        if (s == nullptr) {
            return false;
        }
        n = end - s;
        if (tag == 0 || tag > 4 || tag <= old_tag) {
            return false;
        }
        old_tag = tag;
        uint32_t type = 0;
        size_t data_len = 0;
        switch (tag) {
            case 1:
                s = GetVarint32(s, n, &type);
                if (s == nullptr || type > 2) {
                    return false;
                }
                n = end - s;
                type_ = static_cast<EntryType>(type);
                break;
            case 2:
                s = GetVarint64(s, n, &term_);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                break;
            case 3:
                s = GetVarint64(s, n, &index_);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                break;
            case 4:
                s = GetVarint64(s, n, &data_len);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                if (n < data_len) {
                    return false;
                }
                data_ = std::move(data.share(s - data.begin(), data_len));
                s += data_len;
                n -= data_len;
                break;
        }
    }
    return s == end ? true : false;
}

EntryPtr make_entry() { return seastar::make_lw_shared<Entry>(); }

void ConfState::Reset() {
    voters_.clear();
    learners_.clear();
    voters_outgoing_.clear();
    learners_next_.clear();
    auto_leave_ = false;
}

bool ConfState::Empty() const {
    return voters_.empty() && learners_.empty() && voters_outgoing_.empty() &&
           learners_next_.empty() && !auto_leave_;
}

size_t ConfState::ByteSize() const {
    size_t size = 0;
    if (!voters_.empty()) {
        size += 1 + VarintLength(voters_.size());
        for (int i = 0; i < voters_.size(); i++) {
            size += VarintLength(voters_[i]);
        }
    }
    if (!learners_.empty()) {
        size += 1 + VarintLength(learners_.size());
        for (int i = 0; i < learners_.size(); i++) {
            size += VarintLength(learners_[i]);
        }
    }
    if (!voters_outgoing_.empty()) {
        size += 1 + VarintLength(voters_outgoing_.size());
        for (int i = 0; i < voters_outgoing_.size(); i++) {
            size += VarintLength(voters_outgoing_[i]);
        }
    }
    if (!learners_next_.empty()) {
        size += 1 + VarintLength(learners_next_.size());
        for (int i = 0; i < learners_next_.size(); i++) {
            size += VarintLength(learners_next_[i]);
        }
    }
    if (auto_leave_) {
        size += 1;
    }
    return size;
}

void ConfState::MarshalTo(char* b) {
    if (!voters_.empty()) {
        b = PutVarint32(b, 1);
        b = PutVarint32(b, voters_.size());
        for (int i = 0; i < voters_.size(); i++) {
            b = PutVarint64(b, voters_[i]);
        }
    }
    if (!learners_.empty()) {
        b = PutVarint32(b, 2);
        b = PutVarint32(b, learners_.size());
        for (int i = 0; i < learners_.size(); i++) {
            b = PutVarint64(b, learners_[i]);
        }
    }
    if (!voters_outgoing_.empty()) {
        b = PutVarint32(b, 3);
        b = PutVarint32(b, voters_outgoing_.size());
        for (int i = 0; i < voters_outgoing_.size(); i++) {
            b = PutVarint64(b, voters_outgoing_[i]);
        }
    }
    if (!learners_next_.empty()) {
        b = PutVarint32(b, 4);
        b = PutVarint32(b, learners_next_.size());
        for (int i = 0; i < learners_next_.size(); i++) {
            b = PutVarint64(b, learners_next_[i]);
        }
    }
    if (auto_leave_) {
        b = PutVarint32(b, 5);
    }
}

bool ConfState::Unmarshal(seastar::temporary_buffer<char> data) {
    const char* s = data.begin();
    const char* end = data.end();
    size_t n = data.size();
    uint32_t old_tag = 0;

    Reset();
    if (n == 0) {
        return true;
    }
    while (old_tag < 5 && s < end) {
        uint32_t tag = 0;
        // 1 get tag
        s = GetVarint32(s, n, &tag);
        if (s == nullptr) {
            return false;
        }
        n = end - s;
        if (tag == 0 || tag > 5 || tag <= old_tag) {
            return false;
        }
        old_tag = tag;
        if (tag < 5) {
            uint32_t num = 0;
            s = GetVarint32(s, n, &num);
            if (s == nullptr) {
                return false;
            }
            n = end - s;
            for (int i = 0; i < num; i++) {
                uint64_t id = 0;
                s = GetVarint64(s, n, &id);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                switch (tag) {
                    case 1:
                        voters_.push_back(id);
                        break;
                    case 2:
                        learners_.push_back(id);
                        break;
                    case 3:
                        voters_outgoing_.push_back(id);
                        break;
                    case 4:
                        learners_next_.push_back(id);
                        break;
                }
            }
        } else {
            auto_leave_ = true;
        }
    }
    return s == end ? true : false;
}

bool SnapshotMetadata::Empty() const {
    return conf_state_.Empty() && index_ == 0 && term_ == 0;
}

void SnapshotMetadata::Reset() {
    conf_state_.Reset();
    index_ = 0;
    term_ = 0;
}

size_t SnapshotMetadata::ByteSize() const {
    size_t size = 0;
    if (!conf_state_.Empty()) {
        auto bsize = conf_state_.ByteSize();
        size += 1 + VarintLength(bsize) + bsize;
    }
    if (index_) {
        size += 1 + VarintLength(index_);
    }
    if (term_) {
        size += 1 + VarintLength(term_);
    }
    return size;
}

void SnapshotMetadata::MarshalTo(char* b) {
    if (!conf_state_.Empty()) {
        auto bsize = conf_state_.ByteSize();
        b = PutVarint32(b, 1);
        b = PutVarint32(b, bsize);
        conf_state_.MarshalTo(b);
        b += bsize;
    }

    if (index_) {
        b = PutVarint32(b, 2);
        b = PutVarint64(b, index_);
    }

    if (term_) {
        b = PutVarint32(b, 3);
        b = PutVarint64(b, term_);
    }
}

bool SnapshotMetadata::Unmarshal(seastar::temporary_buffer<char> data) {
    const char* s = data.begin();
    const char* end = data.end();
    size_t n = data.size();
    uint32_t old_tag = 0;

    Reset();
    if (n == 0) {
        return true;
    }
    while (old_tag < 3 && s < end) {
        uint32_t tag = 0;
        // get tag
        s = GetVarint32(s, n, &tag);
        if (s == nullptr) {
            return false;
        }
        n = end - s;
        if (tag == 0 || tag > 3 || tag <= old_tag) {
            return false;
        }
        old_tag = tag;
        uint32_t conf_state_len = 0;
        switch (tag) {
            case 1:
                s = GetVarint32(s, n, &conf_state_len);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                if (n < conf_state_len) {
                    return false;
                }
                if (!conf_state_.Unmarshal(
                        data.share(s - data.begin(), conf_state_len))) {
                    return false;
                }
                n -= conf_state_len;
                s += conf_state_len;
                break;
            case 2:
                s = GetVarint64(s, n, &index_);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                break;
            case 3:
                s = GetVarint64(s, n, &term_);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                break;
        }
    }
    return s == end ? true : false;
}

SnapshotPtr make_snapshot() { return seastar::make_lw_shared<Snapshot>(); }

const char* MessageTypeToString(const MessageType type) {
    return msg_types[(int)type];
}

bool Snapshot::Empty() const { return data_.empty() && metadata_.Empty(); }

void Snapshot::Reset() {
    seastar::temporary_buffer<char> emtpy;
    data_ = std::move(emtpy);
    metadata_.Reset();
}

size_t Snapshot::ByteSize() const {
    size_t size = 0;
    if (!data_.empty()) {
        size += 1 + VarintLength(data_.size()) + data_.size();
    }
    if (!metadata_.Empty()) {
        auto n = metadata_.ByteSize();
        size += 1 + VarintLength(n) + n;
    }
    return size;
}

void Snapshot::MarshalTo(char* b) {
    if (!data_.empty()) {
        b = PutVarint32(b, 1);
        b = PutVarint32(b, data_.size());
        memcpy(b, data_.begin(), data_.size());
        b += data_.size();
    }
    if (!metadata_.Empty()) {
        auto n = metadata_.ByteSize();
        b = PutVarint32(b, 2);
        b = PutVarint32(b, n);
        metadata_.MarshalTo(b);
        b += n;
    }
}

bool Snapshot::Unmarshal(seastar::temporary_buffer<char> data) {
    const char* s = data.begin();
    const char* end = data.end();
    size_t n = data.size();
    uint32_t old_tag = 0;

    Reset();
    if (n == 0) {
        return true;
    }
    while (old_tag < 2 && s < end) {
        uint32_t tag = 0;
        // get tag
        s = GetVarint32(s, n, &tag);
        if (s == nullptr) {
            return false;
        }
        n = end - s;
        if (tag == 0 || tag > 2 || tag <= old_tag) {
            return false;
        }
        old_tag = tag;
        uint32_t data_len = 0;
        uint32_t meta_len = 0;
        switch (tag) {
            case 1:
                s = GetVarint32(s, n, &data_len);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                if (n < data_len) {
                    return false;
                }
                data_ = data.share(s - data.begin(), data_len);
                n -= data_len;
                s += data_len;
                break;
            case 2:
                s = GetVarint32(s, n, &meta_len);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                if (n < meta_len) {
                    return false;
                }
                if (!metadata_.Unmarshal(
                        data.share(s - data.begin(), meta_len))) {
                    return false;
                }
                n -= meta_len;
                s += meta_len;
                break;
        }
    }
    return s == data.end() ? true : false;
}

MessagePtr make_raft_message() { return seastar::make_lw_shared<Message>(); }

bool Message::Empty() const {
    return type == MessageType::MsgHup && from == 0 && to == 0 && term == 0 &&
           log_term == 0 && index == 0 && entries.empty() && commit == 0 &&
           !snapshot && !reject && reject_hint == 0 && context.empty();
}

void Message::Reset() {
    seastar::temporary_buffer<char> empty;
    type = MessageType::MsgHup;
    to = 0;
    from = 0;
    term = 0;
    log_term = 0;
    index = 0;
    entries.clear();
    commit = 0;
    snapshot = nullptr;
    reject = false;
    reject_hint = 0;
    if (!context.empty()) {
        context = std::move(empty);
    }
}

size_t Message::ByteSize() const {
    size_t size = 0;
    if (type != MessageType::MsgHup) {
        size += 1 + VarintLength((uint64_t)type);
    }
    if (to) {
        size += 1 + VarintLength(to);
    }
    if (from) {
        size += 1 + VarintLength(from);
    }
    if (term) {
        size += 1 + VarintLength(term);
    }
    if (log_term) {
        size += 1 + VarintLength(log_term);
    }
    if (index) {
        size += 1 + VarintLength(index);
    }
    if (!entries.empty()) {
        size += 1 + VarintLength(entries.size());
        for (int i = 0; i < entries.size(); i++) {
            auto n = entries[i]->ByteSize();
            size += VarintLength(n) + n;
        }
    }
    if (commit) {
        size += 1 + VarintLength(commit);
    }
    if (snapshot) {
        auto n = snapshot->ByteSize();
        size += 1 + VarintLength(n) + n;
    }
    if (reject) {
        size += 1;
    }
    if (!context.empty()) {
        size += 1 + VarintLength(context.size()) + context.size();
    }
    return size;
}

void Message::MarshalTo(char* b) {
    if (type != MessageType::MsgHup) {
        b = PutVarint32(b, 1);
        b = PutVarint32(b, uint32_t(type));
    }
    if (to) {
        b = PutVarint32(b, 2);
        b = PutVarint64(b, to);
    }
    if (from) {
        b = PutVarint32(b, 3);
        b = PutVarint64(b, from);
    }
    if (term) {
        b = PutVarint32(b, 4);
        b = PutVarint64(b, term);
    }
    if (log_term) {
        b = PutVarint32(b, 5);
        b = PutVarint64(b, log_term);
    }
    if (index) {
        b = PutVarint32(b, 6);
        b = PutVarint64(b, index);
    }
    if (!entries.empty()) {
        b = PutVarint32(b, 7);
        b = PutVarint32(b, entries.size());
        for (int i = 0; i < entries.size(); i++) {
            auto n = entries[i]->ByteSize();
            b = PutVarint32(b, n);
            if (n > 0) {
                entries[i]->MarshalTo(b);
                b += n;
            }
        }
    }
    if (commit) {
        b = PutVarint32(b, 8);
        b = PutVarint64(b, commit);
    }
    if (snapshot) {
        auto n = snapshot->ByteSize();
        b = PutVarint32(b, 9);
        b = PutVarint64(b, n);
        snapshot->MarshalTo(b);
        b += n;
    }
    if (reject) {
        b = PutVarint32(b, 10);
    }
    if (!context.empty()) {
        b = PutVarint32(b, 11);
        b = PutVarint32(b, context.size());
        memcpy(b, context.get(), context.size());
        b += context.size();
    }
}

bool Message::Unmarshal(seastar::temporary_buffer<char> data) {
    const char* s = data.begin();
    const char* end = data.end();
    size_t n = data.size();
    uint32_t old_tag = 0;

    Reset();
    if (n == 0) {
        return true;
    }
    while (old_tag < 11 && s < end) {
        uint32_t tag = 0;
        // get tag
        s = GetVarint32(s, n, &tag);
        if (s == nullptr) {
            return false;
        }
        n = end - s;
        if (tag == 0 || tag > 11 || tag <= old_tag) {
            return false;
        }
        old_tag = tag;
        uint32_t u32 = 0;
        uint64_t u64 = 0;
        switch (tag) {
            case 1:
                s = GetVarint32(s, n, &u32);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                type = (MessageType)u32;
                break;
            case 2:
                s = GetVarint64(s, n, &to);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                break;
            case 3:
                s = GetVarint64(s, n, &from);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                break;
            case 4:
                s = GetVarint64(s, n, &term);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                break;
            case 5:
                s = GetVarint64(s, n, &log_term);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                break;
            case 6:
                s = GetVarint64(s, n, &index);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                break;
            case 7:
                u32 = 0;
                s = GetVarint32(s, n, &u32);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                for (uint32_t i = 0; i < u32; i++) {
                    uint32_t len = 0;
                    auto ent = make_entry();
                    entries.push_back(ent);
                    s = GetVarint32(s, n, &len);
                    if (s == nullptr) {
                        return false;
                    }
                    n = end - s;
                    if (len == 0) {
                        continue;
                    } else if (len > n) {
                        return false;
                    }
                    if (!ent->Unmarshal(data.share(s - data.begin(), len))) {
                        return false;
                    }
                    s += len;
                    n = end - s;
                }
                break;
            case 8:
                s = GetVarint64(s, n, &commit);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                break;
            case 9:
                s = GetVarint64(s, n, &u64);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                if (u64 > n) {
                    return false;
                }
                snapshot = make_snapshot();
                snapshot->Unmarshal(data.share(s - data.begin(), u64));
                s += u64;
                n -= u64;
                break;
            case 10:
                reject = true;
                break;
            case 11:
                u32 = 0;
                s = GetVarint32(s, n, &u32);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                if (u64 > n) {
                    return false;
                }
                context = data.share(s - data.begin(), u32);
                s += u32;
                n -= u32;
                break;
        }
    }
    return s == end ? true : false;
}

void HardState::Reset() {
    term_ = 0;
    vote_ = 0;
    commit_ = 0;
}

size_t HardState::ByteSize() const {
    size_t size = 0;
    if (term_) {
        size += 1 + VarintLength(term_);
    }
    if (vote_) {
        size += 1 + VarintLength(vote_);
    }
    if (commit_) {
        size += 1 + VarintLength(commit_);
    }
    return size;
}

void HardState::MarshalTo(char* b) {
    if (term_) {
        b = PutVarint32(b, 1);
        b = PutVarint64(b, term_);
    }
    if (vote_) {
        b = PutVarint32(b, 2);
        b = PutVarint64(b, vote_);
    }
    if (commit_) {
        b = PutVarint32(b, 3);
        b = PutVarint64(b, commit_);
    }
}

bool HardState::Unmarshal(seastar::temporary_buffer<char> data) {
    const char* s = data.begin();
    const char* end = data.end();
    size_t n = data.size();
    uint32_t old_tag = 0;

    Reset();
    if (n == 0) {
        return true;
    }
    while (old_tag < 3 && s < end) {
        uint32_t tag = 0;
        // get tag
        s = GetVarint32(s, n, &tag);
        if (s == nullptr) {
            return false;
        }
        n = end - s;
        if (tag == 0 || tag > 3 || tag <= old_tag) {
            return false;
        }
        old_tag = tag;
        switch (tag) {
            case 1:
                s = GetVarint64(s, n, &term_);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                break;
            case 2:
                s = GetVarint64(s, n, &vote_);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                break;
            case 3:
                s = GetVarint64(s, n, &commit_);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                break;
        }
    }
    return s == end ? true : false;
}

bool ConfChange::Empty() const {
    return type_ == ConfChangeType::ConfChangeAddNode && node_id_ == 0 &&
           context_.empty();
}

void ConfChange::Reset() {
    seastar::temporary_buffer<char> empty;
    type_ = ConfChangeType::ConfChangeAddNode;
    node_id_ = 0;
    context_ = std::move(empty);
}

size_t ConfChange::ByteSize() const {
    size_t size = 0;
    if (type_ != ConfChangeType::ConfChangeAddNode) {
        size += 1 + VarintLength(static_cast<uint64_t>(type_));
    }
    if (node_id_) {
        size += 1 + VarintLength(node_id_);
    }
    if (!context_.empty()) {
        size += 1 + VarintLength(context_.size()) + context_.size();
    }
    return size;
}

void ConfChange::MarshalTo(char* b) {
    if (type_ != ConfChangeType::ConfChangeAddNode) {
        b = PutVarint32(b, 1);
        b = PutVarint32(b, static_cast<uint32_t>(type_));
    }
    if (node_id_) {
        b = PutVarint32(b, 2);
        b = PutVarint64(b, node_id_);
    }
    if (!context_.empty()) {
        b = PutVarint32(b, 3);
        b = PutVarint32(b, context_.size());
        memcpy(b, context_.begin(), context_.size());
        b += context_.size();
    }
}

bool ConfChange::Unmarshal(seastar::temporary_buffer<char> data) {
    const char* s = data.begin();
    const char* end = data.end();
    size_t n = data.size();
    uint32_t old_tag = 0;

    Reset();
    if (n == 0) {
        return true;
    }
    while (old_tag < 3 && s < end) {
        uint32_t tag = 0;
        // get tag
        s = GetVarint32(s, n, &tag);
        if (s == nullptr) {
            return false;
        }
        n = end - s;
        if (tag == 0 || tag > 3 || tag <= old_tag) {
            return false;
        }
        old_tag = tag;
        uint32_t u32 = 0;
        switch (tag) {
            case 1:
                s = GetVarint32(s, n, &u32);
                if (s == nullptr) {
                    return false;
                }
                if (u32 > 3) {
                    return false;
                }
                n = end - s;
                type_ = static_cast<ConfChangeType>(u32);
                break;
            case 2:
                s = GetVarint64(s, n, &node_id_);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                break;
            case 3:
                u32 = 0;
                s = GetVarint32(s, n, &u32);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                if (u32 > n) {
                    return false;
                }
                context_ = data.share(s - data.begin(), u32);
                s += u32;
                n -= u32;
                break;
        }
    }
    return s == end ? true : false;
}

seastar::temporary_buffer<char> ConfChange::Marshal() {
    auto n = ByteSize();
    seastar::temporary_buffer<char> buf(n);
    MarshalTo(buf.get_write());
    return buf;
}

std::string ConfChange::String() {
    return fmt::format("{}\"type\":{}, \"nodeid\":{}, \"context\":{}{}", "{",
                       (int)type_, node_id_,
                       std::string(context_.get(), context_.size()), "}");
}

bool ConfChangeSingle::Empty() const {
    return type_ == ConfChangeType::ConfChangeAddNode && node_id_ == 0;
}

void ConfChangeSingle::Reset() {
    type_ = ConfChangeType::ConfChangeAddNode;
    node_id_ = 0;
}

size_t ConfChangeSingle::ByteSize() const {
    size_t size = 0;
    if (type_ != ConfChangeType::ConfChangeAddNode) {
        size += 1 + VarintLength(static_cast<uint64_t>(type_));
    }

    if (node_id_) {
        size += 1 + VarintLength(node_id_);
    }
    return size;
}

void ConfChangeSingle::MarshalTo(char* b) {
    if (type_ != ConfChangeType::ConfChangeAddNode) {
        b = PutVarint32(b, 1);
        b = PutVarint32(b, static_cast<uint32_t>(type_));
    }

    if (node_id_) {
        b = PutVarint32(b, 2);
        b = PutVarint64(b, node_id_);
    }
}

bool ConfChangeSingle::Unmarshal(seastar::temporary_buffer<char> data) {
    const char* s = data.begin();
    const char* end = data.end();
    size_t n = data.size();
    uint32_t old_tag = 0;

    Reset();
    if (n == 0) {
        return true;
    }
    while (old_tag < 2 && s < end) {
        uint32_t tag = 0;
        // get tag
        s = GetVarint32(s, n, &tag);
        if (s == nullptr) {
            return false;
        }
        n = end - s;
        if (tag == 0 || tag > 2 || tag <= old_tag) {
            return false;
        }
        old_tag = tag;
        uint32_t u32 = 0;
        switch (tag) {
            case 1:
                s = GetVarint32(s, n, &u32);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                if (u32 > 3) {
                    return false;
                }
                type_ = static_cast<ConfChangeType>(u32);
                break;
            case 2:
                s = GetVarint64(s, n, &node_id_);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                break;
        }
    }
    return s == end ? true : false;
}

seastar::temporary_buffer<char> ConfChangeSingle::Marshal() {
    auto n = ByteSize();
    seastar::temporary_buffer<char> buf(n);
    MarshalTo(buf.get_write());
    return buf;
}

bool ConfChangeV2::Empty() const {
    return transition_ == ConfChangeTransition::ConfChangeTransitionAuto &&
           changes_.empty() && context_.empty();
}

void ConfChangeV2::Reset() {
    seastar::temporary_buffer<char> empty;
    transition_ = ConfChangeTransition::ConfChangeTransitionAuto;
    changes_.clear();
    context_ = std::move(empty);
}

size_t ConfChangeV2::ByteSize() const {
    size_t size = 0;
    if (transition_ != ConfChangeTransition::ConfChangeTransitionAuto) {
        size += 1 + VarintLength(static_cast<uint64_t>(transition_));
    }

    if (!changes_.empty()) {
        auto n = changes_.size();
        size += 1 + VarintLength(n);
        for (int i = 0; i < n; i++) {
            auto len = changes_[i].ByteSize();
            size += VarintLength(len) + len;
        }
    }

    if (!context_.empty()) {
        size += 1 + VarintLength(context_.size()) + context_.size();
    }
    return size;
}

void ConfChangeV2::MarshalTo(char* b) {
    if (transition_ != ConfChangeTransition::ConfChangeTransitionAuto) {
        b = PutVarint32(b, 1);
        b = PutVarint32(b, static_cast<uint64_t>(transition_));
    }

    if (!changes_.empty()) {
        auto n = changes_.size();
        b = PutVarint32(b, 2);
        b = PutVarint32(b, n);
        for (int i = 0; i < n; i++) {
            auto len = changes_[i].ByteSize();
            b = PutVarint32(b, len);
            changes_[i].MarshalTo(b);
            b += len;
        }
    }

    if (!context_.empty()) {
        b = PutVarint32(b, 3);
        b = PutVarint32(b, context_.size());
        memcpy(b, context_.begin(), context_.size());
        b += context_.size();
    }
}

bool ConfChangeV2::Unmarshal(seastar::temporary_buffer<char> data) {
    const char* s = data.begin();
    const char* end = data.end();
    size_t n = data.size();
    uint32_t old_tag = 0;

    Reset();
    if (n == 0) {
        return true;
    }
    while (old_tag < 3 && s < end) {
        uint32_t tag = 0;
        // get tag
        s = GetVarint32(s, n, &tag);
        if (s == nullptr) {
            return false;
        }
        n = end - s;
        if (tag == 0 || tag > 3 || tag <= old_tag) {
            return false;
        }
        old_tag = tag;
        uint32_t u32 = 0;
        switch (tag) {
            case 1:
                s = GetVarint32(s, n, &u32);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                if (u32 > 2) {
                    return false;
                }
                transition_ = static_cast<ConfChangeTransition>(u32);
                break;
            case 2:
                u32 = 0;
                s = GetVarint32(s, n, &u32);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                for (int i = 0; i < u32; i++) {
                    uint32_t len = 0;
                    ConfChangeSingle ccs;
                    s = GetVarint32(s, n, &len);
                    if (s == nullptr) {
                        return false;
                    }
                    n = end - s;
                    if (len > n) {
                        return false;
                    } else if (len == 0) {
                        changes_.push_back(ccs);
                        continue;
                    }
                    if (!ccs.Unmarshal(data.share(s - data.begin(), len))) {
                        return false;
                    }
                    changes_.push_back(ccs);
                    s += len;
                    n -= len;
                }
                break;
            case 3:
                u32 = 0;
                s = GetVarint32(s, n, &u32);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                if (u32 > n) {
                    return false;
                }
                context_ = std::move(data.share(s - data.begin(), u32));
                s += u32;
                n -= u32;
                break;
        }
    }
    return s == end ? true : false;
}

seastar::temporary_buffer<char> ConfChangeV2::Marshal() {
    auto n = ByteSize();
    seastar::temporary_buffer<char> buf(n);
    MarshalTo(buf.get_write());
    return buf;
}

std::tuple<bool, bool> ConfChangeV2::EnterJoint() {
    bool auto_leave = false;
    bool ok = false;
    if (transition_ != ConfChangeTransition::ConfChangeTransitionAuto ||
        changes_.size() > 1) {
        switch (transition_) {
            case ConfChangeTransition::ConfChangeTransitionAuto:
                auto_leave = true;
                break;
            case ConfChangeTransition::ConfChangeTransitionJointImplicit:
                auto_leave = true;
                break;
            case ConfChangeTransition::ConfChangeTransitionJointExplicit:
                break;
        }
        ok = true;
    }
    return std::tuple<bool, bool>(auto_leave, ok);
}

std::string ConfChangeV2::String() {
    auto out = fmt::memory_buffer();
    fmt::format_to(std::back_inserter(out), "{}\"transition\":{}", "{}",
                   (int)transition_);
    fmt::format_to(std::back_inserter(out), ", \"changes\":[");
    for (size_t i = 0; i < changes_.size(); i++) {
        if (i == 0) {
            fmt::format_to(std::back_inserter(out),
                           "{}\"type\":{}, \"nodeid\": {}{}", "{",
                           (int)changes_[i].type(), changes_[i].node_id(), "}");
        } else {
            fmt::format_to(std::back_inserter(out),
                           ", {}\"type\":{}, \"nodeid\": {}{}", "{",
                           (int)changes_[i].type(), changes_[i].node_id(), "}");
        }
    }
    fmt::format_to(std::back_inserter(out), "]");
    fmt::format_to(std::back_inserter(out), ", \"context\":{}",
                   std::string(context_.get(), context_.size()));
    return fmt::to_string(out);
}

std::string ConfChangeI::String() {
    if (is_v1_) {
        return v1_.String();
    }
    return v2_.String();
}

seastar::temporary_buffer<char> ConfChangeI::Marshal() {
    if (is_v1_) {
        return v1_.Marshal();
    }
    return v2_.Marshal();
}

}  // namespace raft
}  // namespace snail
