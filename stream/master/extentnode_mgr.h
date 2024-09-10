#pragma once
#include <map>
#include <unordered_map>
#include <unordered_set>

#include "id_generator.h"
#include "storage.h"

namespace snail {
namespace stream {

class ExtentnodeMgr : public ApplyHandler {
    StoragePtr store_;
    IDGeneratorPtr id_gen_;
    ApplyType type_;
    uint32_t next_node_id_;
    Buffer node_id_key_;

    struct apply_item {
        seastar::promise<Status<uint32_t>> pr;
    };
    std::unordered_map<uint64_t, apply_item*> pendings_;

    struct extent_node {
        uint32_t node_id = 0;
        std::string host;
        std::string rack = "default";
        std::string az = "default";
        std::unordered_set<uint16_t> ports;
        std::unordered_map<uint16_t, bool> actives;  // in memory

        extent_node() = default;
        extent_node& operator=(const extent_node& x);
        Buffer Marshal();
        bool Unmarshal(std::string_view b);
    };
    using extent_node_ptr = seastar::lw_shared_ptr<extent_node>;

    enum class OP {
        ADD_NODE = 1,
        REMOVE_NODE = 2,
        UPDATE_NODE = 3,
    };

    std::map<uint32_t, extent_node_ptr> all_node_map_;
    std::unordered_map<std::string, uint32_t>
        host_index_map_;  // host ---> nodeid
    std::unordered_map<std::string, std::unordered_set<uint32_t>>
        rack_index_map_;
    std::unordered_map<std::string, std::unordered_set<uint32_t>> az_index_map_;

    seastar::future<Status<>> ApplyAddNode(uint64_t id, extent_node_ptr ptr);
    seastar::future<Status<>> ApplyRemoveNode(uint64_t id, extent_node_ptr ptr);
    seastar::future<Status<>> ApplyUpdateNode(uint64_t id, extent_node_ptr ptr);

    seastar::future<Status<uint32_t>> Propose(
        RaftServerPtr raft, Buffer reqid, OP op, Buffer data,
        std::chrono::milliseconds timeout);

   public:
    explicit ExtentnodeMgr(StoragePtr store, IDGeneratorPtr id_gen,
                           ApplyType type);
    virtual ~ExtentnodeMgr() {}

    seastar::future<Status<>> Init();
    seastar::future<Status<>> Apply(Buffer reqid, uint64_t id, Buffer ctx,
                                    Buffer data) override;
    void Reset() override;

    void Restore(Buffer key, Buffer val) override;

    seastar::future<Status<uint32_t>> AddNode(
        RaftServerPtr raft, Buffer reqid, std::string_view host, uint16_t port,
        std::string_view rack, std::string_view az,
        std::chrono::milliseconds timeout);

    seastar::future<Status<>> RemoveNode(RaftServerPtr raft, Buffer reqid,
                                         uint32_t node_id,
                                         std::string_view host, uint16_t port,
                                         std::chrono::milliseconds timeout);

    seastar::future<Status<>> UpdateNode(RaftServerPtr raft, Buffer reqid,
                                         uint32_t node_id,
                                         std::string_view host,
                                         std::string_view rack,
                                         std::string_view az,
                                         std::chrono::milliseconds timeout);
};

using ExtentnodeMgrPtr = seastar::shared_ptr<ExtentnodeMgr>;

}  // namespace stream
}  // namespace snail
