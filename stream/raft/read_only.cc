#include "read_only.h"

namespace snail {
namespace raft {

void ReadOnly::AddRequest(uint64_t index, MessagePtr m) {
    seastar::sstring s(m->entries[0]->data().get(),
                       m->entries[0]->data().size());
    auto iter = pending_read_index_.find(s);
    if (iter != pending_read_index_.end()) {
        return;
    }

    ReadIndexStatusPtr ptr =
        seastar::make_lw_shared<ReadIndexStatus>(ReadIndexStatus());
    ptr->req = m;
    ptr->index = index;
    pending_read_index_[s] = ptr;
    read_index_queue_.push_back(std::move(s));
    return;
}

std::unordered_map<uint64_t, bool> ReadOnly::RecvAck(
    uint64_t id, const seastar::sstring& context) {
    std::unordered_map<uint64_t, bool> acks;

    auto it = pending_read_index_.find(context);
    if (it == pending_read_index_.end()) {
        return acks;
    }
    it->second->acks[id] = true;
    acks = it->second->acks;
    return acks;
}

std::vector<ReadOnly::ReadIndexStatusPtr> ReadOnly::Advance(MessagePtr m) {
    std::vector<ReadIndexStatusPtr> rss;
    seastar::sstring ctx(m->context.get(), m->context.size());
    int pos = 0;
    bool found = false;
    for (auto& okctx : read_index_queue_) {
        pos++;
        auto it = pending_read_index_.find(okctx);
        if (it == pending_read_index_.end()) {
            throw std::runtime_error(
                "cannot find corresponding read state from pending map");
        }
        rss.push_back(it->second);
        if (okctx == ctx) {
            found = true;
            break;
        }
    }
    if (found) {
        for (int i = 0; i < pos; i++) {
            read_index_queue_.pop_front();
            pending_read_index_.erase(
                seastar::sstring(rss[i]->req->entries[0]->data().get(),
                                 rss[i]->req->entries[0]->data().size()));
        }
        return rss;
    }
    return std::vector<ReadIndexStatusPtr>();
}

seastar::sstring ReadOnly::LastPendingRequestCtx() {
    if (read_index_queue_.empty()) {
        return "";
    }
    return read_index_queue_[read_index_queue_.size() - 1];
}

void ReadOnly::Reset() {
    pending_read_index_.clear();
    read_index_queue_.clear();
}

}  // namespace raft
}  // namespace snail
