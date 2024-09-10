#include "extentnode_mgr.h"

#include "net/byteorder.h"

namespace snail {
namespace stream {

ExtentnodeMgr::extent_node& ExtentnodeMgr::extent_node::operator=(
    const ExtentnodeMgr::extent_node& x) {
    if (this != &x) {
        node_id = x.node_id;
        host = x.host;
        rack = x.rack;
        az = x.az;
        ports = x.ports;
        actives = x.actives;
    }
    return *this;
}

Buffer ExtentnodeMgr::extent_node::Marshal() {
    size_t size = 1 + VarintLength(node_id);
    if (host.size()) {
        size += 1 + VarintLength(host.size()) + host.size();
    }
    if (rack.size()) {
        size += 1 + VarintLength(rack.size()) + rack.size();
    }
    if (az.size()) {
        size += 1 + VarintLength(az.size()) + az.size();
    }
    if (ports.size()) {
        size +=
            1 + VarintLength(ports.size()) + ports.size() * sizeof(uint16_t);
    }

    Buffer b(size);
    char* p = b.get_write();
    p = PutVarint32(p, 1);
    p = PutVarint32(p, node_id);
    if (host.size()) {
        p = PutVarint32(p, 2);
        p = PutVarint32(p, host.size());
        memcpy(p, host.data(), host.size());
        p += host.size();
    }
    if (rack.size()) {
        p = PutVarint32(p, 3);
        p = PutVarint32(p, rack.size());
        memcpy(p, rack.data(), rack.size());
        p += rack.size();
    }
    if (az.size()) {
        p = PutVarint32(p, 4);
        p = PutVarint32(p, az.size());
        memcpy(p, az.data(), az.size());
        p += az.size();
    }
    if (ports.size()) {
        p = PutVarint32(p, 4);
        p = PutVarint32(p, ports.size());
        for (auto port : ports) {
            net::BigEndian::PutUint16(p, port);
            p += sizeof(uint16_t);
        }
    }
    return b;
}

bool ExtentnodeMgr::extent_node::Unmarshal(std::string_view b) {
    const char* s = b.data();
    const char* end = b.data() + b.size();
    size_t n = b.size();
    uint32_t prev_tag = 0;

    if (n == 0) {
        return true;
    }
    while (prev_tag < 5 && s < end) {
        uint32_t tag = 0;
        // get tag
        s = GetVarint32(s, n, &tag);
        if (s == nullptr) {
            return false;
        }
        n = end - s;
        if (tag == 0 || tag <= prev_tag) {
            return false;
        }
        prev_tag = tag;
        uint32_t len = 0;
        uint32_t port_n = 0;
        switch (tag) {
            case 1:
                s = GetVarint32(s, n, &node_id);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                break;
            case 2:
                s = GetVarint32(s, n, &len);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                if (n < len) {
                    return false;
                }
                host = std::string(s, len);
                n -= len;
                break;
            case 3:
                s = GetVarint32(s, n, &len);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                if (n < len) {
                    return false;
                }
                rack = std::string(s, len);
                n -= len;
                break;
            case 4:
                s = GetVarint32(s, n, &len);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                if (n < len) {
                    return false;
                }
                az = std::string(s, len);
                n -= len;
                break;
            case 5:
                s = GetVarint32(s, n, &len);
                if (s == nullptr) {
                    return false;
                }
                n = end - s;
                if (n < len * sizeof(uint16_t)) {
                    return false;
                }
                for (int i = 0; i < len; i++) {
                    uint16_t port = net::BigEndian::Uint16(s);
                    s += sizeof(port);
                    ports.insert(port);
                }
                n -= len * sizeof(uint16_t);
                break;
            default:
                break;
        }
    }
    return true;
}

ExtentnodeMgr::ExtentnodeMgr(StoragePtr store, IDGeneratorPtr id_gen,
                             ApplyType type)
    : store_(store), id_gen_(id_gen), type_(type) {}

seastar::future<Status<>> ExtentnodeMgr::Init() {
    Status<> s;
    auto st1 = co_await store_->Get(node_id_key_.share());
    if (!st1) {
        s.Set(st1.Code(), st1.Reason());
        co_return s;
    }
    Buffer b = std::move(st1.Value());
    if (b.empty()) {
        next_node_id_ = 1;
    } else {
        next_node_id_ = net::BigEndian::Uint32(b.get());
    }

    char cstart[1];
    char cend[1];
    cstart[0] = static_cast<char>(ApplyType::Node);
    cend[0] = static_cast<char>(ApplyType::Node) + 1;
    Buffer start(cstart, sizeof(cstart), seastar::deleter());
    Buffer end(cend, sizeof(cend), seastar::deleter());

    s = co_await store_->Range(
        std::move(start), std::move(end),
        [this](const std::string& key, const std::string& val) {
            extent_node_ptr node = seastar::make_lw_shared<extent_node>();
            node->Unmarshal(val);
            all_node_map_[node->node_id] = node;
            host_index_map_[node->host] = node->node_id;
            rack_index_map_[node->rack].insert(node->node_id);
            az_index_map_[node->az].insert(node->node_id);
        });
    co_return s;
}

seastar::future<Status<>> ExtentnodeMgr::ApplyAddNode(uint64_t id,
                                                      extent_node_ptr ptr) {
    Status<> s;
    Status<uint32_t> st;

    auto iter = host_index_map_.find(ptr->host);
    if (iter == host_index_map_.end()) {  // this node has never registered
        ptr->node_id = next_node_id_++;
        WriteBatch batch;
        char val[sizeof(next_node_id_)];
        net::BigEndian::PutUint32(val, next_node_id_);
        batch.Put(std::string_view(node_id_key_.get(), node_id_key_.size()),
                  std::string_view(val, sizeof(val)));
        char key1[sizeof(ptr->node_id) + 1];
        Buffer val1 = ptr->Marshal();
        key1[0] = static_cast<char>(ApplyType::Node);
        net::BigEndian::PutUint32(key1 + 1, ptr->node_id);
        batch.Put(std::string_view(key1, sizeof(key1)),
                  std::string_view(val1.get(), val1.size()));
        s = co_await store_->Write(std::move(batch));
        if (!s) {
            co_return s;
        }
        all_node_map_[ptr->node_id] = ptr;
        host_index_map_[ptr->host] = ptr->node_id;
        rack_index_map_[ptr->rack].insert(ptr->node_id);
        az_index_map_[ptr->az].insert(ptr->node_id);
        auto it = pendings_.find(id);
        if (it != pendings_.end()) {
            st.SetValue(ptr->node_id);
            it->second->pr.set_value(st);
        }
        co_return s;
    }

    extent_node_ptr node_ptr = all_node_map_[iter->second];
    extent_node tmp_node = *node_ptr;
    if (tmp_node.host != ptr->host || tmp_node.rack != ptr->rack ||
        tmp_node.az != ptr->az) {
        auto it = pendings_.find(id);
        if (it != pendings_.end()) {
            st.Set(EINVAL);
            it->second->pr.set_value(st);
        }
        co_return s;
    }

    ptr->node_id = tmp_node.node_id;
    bool update = false;
    for (auto port : ptr->ports) {
        if (tmp_node.ports.count(port)) {
            continue;
        }
        update = true;
        tmp_node.ports.insert(port);
    }

    if (!update) {
        co_return s;
    }

    char key[sizeof(tmp_node.node_id) + 1];
    Buffer val = tmp_node.Marshal();
    key[0] = static_cast<char>(ApplyType::Node);
    net::BigEndian::PutUint32(key + 1, tmp_node.node_id);
    s = co_await store_->Put(Buffer(key, sizeof(key), seastar::deleter()),
                             val.share());
    if (s) {
        *node_ptr = tmp_node;
        auto it = pendings_.find(id);
        if (it != pendings_.end()) {
            st.SetValue(ptr->node_id);
            it->second->pr.set_value(st);
        }
    }
    co_return s;
}

seastar::future<Status<>> ExtentnodeMgr::ApplyRemoveNode(uint64_t id,
                                                         extent_node_ptr ptr) {
    Status<> s;
    Status<uint32_t> st;

    auto iter = all_node_map_.find(ptr->node_id);
    if (iter == all_node_map_.end()) {
        auto it = pendings_.find(id);
        if (it != pendings_.end()) {
            it->second->pr.set_value(st);
        }
        co_return s;
    }

    extent_node_ptr node_ptr = iter->second;
    extent_node tmp_node = *node_ptr;
    if (tmp_node.host != ptr->host) {
        auto it = pendings_.find(id);
        if (it != pendings_.end()) {
            st.Set(EINVAL);
            it->second->pr.set_value(st);
        }
        co_return s;
    }

    bool update = false;

    for (auto port : ptr->ports) {
        if (tmp_node.ports.erase(port)) {
            update = true;
        }
    }
    if (tmp_node.ports.empty()) {
        char key[sizeof(tmp_node.node_id) + 1];
        key[0] = static_cast<char>(ApplyType::Node);
        net::BigEndian::PutUint32(key + 1, tmp_node.node_id);
        s = co_await store_->Delete(
            Buffer(key, sizeof(key), seastar::deleter()));
        if (!s) {
            co_return s;
        }
        all_node_map_.erase(tmp_node.node_id);
        host_index_map_.erase(tmp_node.host);
        rack_index_map_[tmp_node.rack].erase(tmp_node.node_id);
        az_index_map_[tmp_node.az].erase(tmp_node.node_id);
        if (rack_index_map_[tmp_node.rack].empty()) {
            rack_index_map_.erase(tmp_node.rack);
        }
        if (az_index_map_[tmp_node.az].empty()) {
            az_index_map_.erase(tmp_node.az);
        }
    } else if (update) {
        char key[sizeof(tmp_node.node_id) + 1];
        key[0] = static_cast<char>(ApplyType::Node);
        net::BigEndian::PutUint32(key + 1, tmp_node.node_id);
        Buffer val = tmp_node.Marshal();
        s = co_await store_->Put(Buffer(key, sizeof(key), seastar::deleter()),
                                 val.share());
        if (!s) {
            co_return s;
        }
        *node_ptr = tmp_node;
    }
    auto it = pendings_.find(id);
    if (it != pendings_.end()) {
        it->second->pr.set_value(st);
    }
    co_return s;
}

seastar::future<Status<>> ExtentnodeMgr::ApplyUpdateNode(uint64_t id,
                                                         extent_node_ptr ptr) {
    Status<> s;
    Status<uint32_t> st;

    auto iter = all_node_map_.find(ptr->node_id);
    if (iter == all_node_map_.end()) {
        auto it = pendings_.find(id);
        if (it != pendings_.end()) {
            st.Set(ErrCode::ErrExtentNodeNotExist);
            it->second->pr.set_value(st);
        }
        co_return s;
    }
    if (host_index_map_.count(ptr->host)) {
        auto it = pendings_.find(id);
        if (it != pendings_.end()) {
            st.Set(EEXIST);
            it->second->pr.set_value(st);
        }
        co_return s;
    }

    extent_node_ptr node_ptr = iter->second;
    extent_node tmp_node = *node_ptr;
    tmp_node.host = ptr->host;
    tmp_node.rack = ptr->rack;
    tmp_node.az = ptr->az;

    char key[sizeof(tmp_node.node_id) + 1];
    key[0] = static_cast<char>(ApplyType::Node);
    net::BigEndian::PutUint32(key + 1, tmp_node.node_id);
    Buffer val = tmp_node.Marshal();
    s = co_await store_->Put(Buffer(key, sizeof(key), seastar::deleter()),
                             val.share());
    if (!s) {
        co_return s;
    }

    host_index_map_.erase(node_ptr->host);
    host_index_map_[tmp_node.host] = tmp_node.node_id;
    auto rack_iter = rack_index_map_.find(node_ptr->rack);
    if (rack_iter != rack_index_map_.end()) {
        rack_iter->second.erase(node_ptr->node_id);
        if (rack_iter->second.empty()) {
            rack_index_map_.erase(rack_iter);
        }
    }
    rack_index_map_[tmp_node.rack].insert(tmp_node.node_id);

    auto az_iter = az_index_map_.find(node_ptr->az);
    if (az_iter != az_index_map_.end()) {
        az_iter->second.erase(node_ptr->node_id);
        if (az_iter->second.empty()) {
            az_index_map_.erase(az_iter);
        }
    }
    *node_ptr = tmp_node;
    az_index_map_[tmp_node.az].insert(tmp_node.node_id);
    auto it = pendings_.find(id);
    if (it != pendings_.end()) {
        it->second->pr.set_value(st);
    }
    co_return s;
}

seastar::future<Status<>> ExtentnodeMgr::Apply(Buffer reqid, uint64_t id,
                                               Buffer ctx, Buffer data) {
    Status<> s;
    Status<uint32_t> st;
    OP op = static_cast<ExtentnodeMgr::OP>(ctx[0]);
    extent_node_ptr node_ptr = seastar::make_lw_shared<extent_node>();
    if (!node_ptr->Unmarshal(std::string_view(data.get(), data.size()))) {
        s.Set(EBADMSG);
        co_return s;
    }

    switch (op) {
        case OP::ADD_NODE:
            s = co_await ApplyAddNode(id, node_ptr);
            break;
        case OP::REMOVE_NODE:
            s = co_await ApplyRemoveNode(id, node_ptr);
            break;
        case OP::UPDATE_NODE:
            s = co_await ApplyUpdateNode(id, node_ptr);
            break;
        default:
            s.Set(EINVAL);
            break;
    }
    co_return s;
}

void ExtentnodeMgr::Reset() {
    next_node_id_ = 1;
    pendings_.clear();
    all_node_map_.clear();
    host_index_map_.clear();
    rack_index_map_.clear();
    az_index_map_.clear();
}

void ExtentnodeMgr::Restore(Buffer key, Buffer val) {
    if (key == node_id_key_) {
        next_node_id_ = net::BigEndian::Uint32(val.get());
        return;
    }

    extent_node_ptr node = seastar::make_lw_shared<extent_node>();
    node->Unmarshal(std::string_view(val.get(), val.size()));
    all_node_map_[node->node_id] = node;
    host_index_map_[node->host] = node->node_id;
    rack_index_map_[node->rack].insert(node->node_id);
    az_index_map_[node->az].insert(node->node_id);
}

seastar::future<Status<uint32_t>> ExtentnodeMgr::Propose(
    RaftServerPtr raft, Buffer reqid, OP op, Buffer body,
    std::chrono::milliseconds timeout) {
    Status<uint32_t> s;

    std::unique_ptr<ExtentnodeMgr::apply_item> item(new apply_item);
    uint64_t id = id_gen_->Next();
    char ctx[1];
    ctx[0] = static_cast<char>(op);
    pendings_[id] = item.get();

    seastar::timer<seastar::lowres_clock> timer(
        [this, id, item = item.get()]() {
            Status<uint32_t> s;
            s.Set(ETIME);
            item->pr.set_value(s);
            pendings_.erase(id);
        });
    timer.arm(timeout);
    auto st = co_await store_->Propose(raft, reqid.share(), id, type_,
                                       Buffer(ctx, 1, seastar::deleter()),
                                       body.share());
    if (!st) {
        pendings_.erase(id);
        s.Set(st.Code(), st.Reason());
        timer.cancel();
        co_return s;
    }
    s = co_await item->pr.get_future();
    pendings_.erase(id);
    timer.cancel();
    co_return s;
}

seastar::future<Status<uint32_t>> ExtentnodeMgr::AddNode(
    RaftServerPtr raft, Buffer reqid, std::string_view host, uint16_t port,
    std::string_view rack, std::string_view az,
    std::chrono::milliseconds timeout) {
    Status<uint32_t> s;
    extent_node node;

    node.host = host;
    node.rack = rack;
    node.az = az;
    node.ports.insert(port);
    Buffer body = node.Marshal();

    s = co_await Propose(raft, reqid.share(), OP::ADD_NODE, body.share(),
                         timeout);
    co_return s;
}

seastar::future<Status<>> ExtentnodeMgr::RemoveNode(
    RaftServerPtr raft, Buffer reqid, uint32_t node_id, std::string_view host,
    uint16_t port, std::chrono::milliseconds timeout) {
    Status<> s;
    extent_node node;

    node.node_id = node_id;
    node.host = host;
    node.ports.insert(port);

    Buffer body = node.Marshal();
    auto st = co_await Propose(raft, reqid.share(), OP::REMOVE_NODE,
                               body.share(), timeout);
    if (!st) {
        s.Set(st.Code(), st.Reason());
    }
    co_return s;
}

seastar::future<Status<>> ExtentnodeMgr::UpdateNode(
    RaftServerPtr raft, Buffer reqid, uint32_t node_id, std::string_view host,
    std::string_view rack, std::string_view az,
    std::chrono::milliseconds timeout) {
    Status<> s;
    extent_node node;

    node.node_id = node_id;
    node.host = host;
    node.rack = rack;
    node.az = az;

    Buffer body = node.Marshal();
    auto st = co_await Propose(raft, reqid.share(), OP::UPDATE_NODE,
                               body.share(), timeout);
    if (!st) {
        s.Set(st.Code(), st.Reason());
    }
    co_return s;
}

}  // namespace stream
}  // namespace snail
