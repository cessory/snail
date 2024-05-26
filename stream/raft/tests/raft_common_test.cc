#include <random>
#include <seastar/core/coroutine.hh>

#include "raft_test.h"

namespace snail {
namespace raft {

StateMachinePtr BlackHoleSM::MakeStateMachine() {
    seastar::shared_ptr<BlackHoleSM> ptr = seastar::make_shared<BlackHoleSM>();
    return seastar::dynamic_pointer_cast<StateMachine>(ptr);
}

seastar::future<Status<>> BlackHoleSM::Step(snail::raft::MessagePtr m) {
    return seastar::make_ready_future<Status<>>();
}
std::vector<snail::raft::MessagePtr> BlackHoleSM::ReadMessage() {
    std::vector<snail::raft::MessagePtr> msgs;
    return msgs;
}

StateMachineType BlackHoleSM::Type() {
    return StateMachineType::BlackHoleStateMachine;
}

seastar::future<Status<>> RaftSM::Step(snail::raft::MessagePtr m) {
    return raft_->Step(m);
}

std::vector<snail::raft::MessagePtr> RaftSM::ReadMessage() {
    std::vector<snail::raft::MessagePtr> msgs = std::move(raft_->msgs_);
    return msgs;
}

StateMachineType RaftSM::Type() { return StateMachineType::RaftStateMachine; }

StateMachinePtr RaftSM::MakeStateMachine(snail::raft::RaftPtr r) {
    seastar::shared_ptr<RaftSM> ptr = seastar::make_shared<RaftSM>(r);
    return seastar::dynamic_pointer_cast<StateMachine>(ptr);
}

StateMachinePtr nopStepper = BlackHoleSM::MakeStateMachine();

seastar::future<> Network::Send(
    const std::vector<snail::raft::MessagePtr>& msgs) {
    std::deque<snail::raft::MessagePtr> ms(msgs.begin(), msgs.end());
    while (!ms.empty()) {
        auto m = ms.front();
        ms.pop_front();
        auto p = peers_[m->to];
        co_await p->Step(m);
        auto tmp = Filter(p->ReadMessage());
        for (auto e : tmp) {
            ms.push_back(e);
        }
    }
    co_return;
}

void Network::Drop(uint64_t from, uint64_t to, float perc) {
    connem key;
    key.from = from;
    key.to = to;
    dropm_[key] = perc;
}

void Network::Cut(uint64_t one, uint64_t other) {
    Drop(one, other, 2.0);
    Drop(other, one, 2.0);
}

void Network::Isolate(uint64_t id) {
    for (int i = 0; i < peers_.size(); i++) {
        uint64_t nid = i + 1;
        if (nid != id) {
            Drop(id, nid, 1.0);
            Drop(nid, id, 1.0);
        }
    }
}

void Network::Ignore(snail::raft::MessageType t) { ignorem_[t] = true; }

void Network::Recover() {
    dropm_.clear();
    ignorem_.clear();
}

std::vector<snail::raft::MessagePtr> Network::Filter(
    const std::vector<snail::raft::MessagePtr>& msgs) {
    static std::default_random_engine e(time(0));
    static std::uniform_real_distribution<float> u(0.0, 0.9);
    std::vector<snail::raft::MessagePtr> mm;
    for (auto m : msgs) {
        if (ignorem_[m->type]) {
            continue;
        }
        bool drop = false;
        switch (m->type) {
            case snail::raft::MessageType::MsgHup:
                throw std::runtime_error("unexpected msgHup");
            default: {
                connem key;
                key.from = m->from;
                key.to = m->to;
                auto perc = dropm_[key];
                if (u(e) < perc) {
                    drop = true;
                }
            }
        }
        if (drop) continue;
        if (msg_hook_) {
            if (!msg_hook_(m)) {
                continue;
            }
        }
        mm.push_back(m);
    }
    return mm;
}

StateMachinePtr entsWithConfig(
    std::function<void(snail::raft::Raft::Config*)> configFunc,
    const std::vector<uint64_t>& terms) {
    seastar::shared_ptr<snail::raft::MemoryStorage> storage =
        seastar::make_shared<snail::raft::MemoryStorage>();
    for (int i = 0; i < terms.size(); i++) {
        std::vector<snail::raft::EntryPtr> ents;
        auto ent = snail::raft::make_entry();
        ent->set_index(i + 1);
        ent->set_term(terms[i]);
        ents.push_back(ent);
        storage->Append(ents);
    }
    auto cfg = newTestConfig(
        1, 5, 1, seastar::dynamic_pointer_cast<snail::raft::Storage>(storage));
    if (configFunc) {
        configFunc(&cfg);
    }
    snail::raft::RaftPtr raft = seastar::make_lw_shared<snail::raft::Raft>();
    raft->Init(cfg).get();
    raft->Reset(terms.back());
    return RaftSM::MakeStateMachine(raft);
}

StateMachinePtr votedWithConfig(
    std::function<void(snail::raft::Raft::Config*)> configFunc, uint64_t vote,
    uint64_t term) {
    seastar::shared_ptr<snail::raft::MemoryStorage> storage =
        seastar::make_shared<snail::raft::MemoryStorage>();
    snail::raft::HardState hs(term, vote, 0);
    storage->SetHardState(hs);
    auto cfg = newTestConfig(
        1, 5, 1, seastar::dynamic_pointer_cast<snail::raft::Storage>(storage));
    if (configFunc) {
        configFunc(&cfg);
    }
    snail::raft::RaftPtr raft = seastar::make_lw_shared<snail::raft::Raft>();
    raft->Init(cfg).get();
    return RaftSM::MakeStateMachine(raft);
}

void preVoteConfig(snail::raft::Raft::Config* cfg) { cfg->pre_vote = true; }

NetworkPtr newNetwork(const std::vector<StateMachinePtr>& peers) {
    return newNetworkWithConfig(nullptr, peers);
}

NetworkPtr newNetworkWithConfig(
    std::function<void(snail::raft::Raft::Config*)> configFunc,
    const std::vector<StateMachinePtr>& peers) {
    auto size = peers.size();
    auto peer_addrs = idsBySize(size);
    std::unordered_map<uint64_t, StateMachinePtr> npeers;
    std::unordered_map<uint64_t,
                       seastar::shared_ptr<snail::raft::MemoryStorage>>
        nstorage;

    for (int i = 0; i < peers.size(); i++) {
        auto id = peer_addrs[i];
        if (!peers[i]) {
            auto store = newTestMemoryStorage();
            auto ptr =
                seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(
                    store);
            ptr->snapshot_->metadata().conf_state().set_voters(peer_addrs);
            nstorage[id] = ptr;
            auto cfg = newTestConfig(id, 10, 1, store);
            if (configFunc) {
                configFunc(&cfg);
            }
            auto raft = seastar::make_lw_shared<snail::raft::Raft>();
            raft->Init(cfg).get();
            npeers[id] = RaftSM::MakeStateMachine(raft);
        } else if (peers[i]->Type() == StateMachineType::RaftStateMachine) {
            std::unordered_map<uint64_t, bool> learners;
            auto raft = seastar::dynamic_pointer_cast<RaftSM>(peers[i])->raft_;
            for (auto i : raft->prs_->learners()) {
                learners[i] = true;
            }
            raft->id_ = id;
            size_t max_inflight = raft->prs_->max_inflight();
            raft->prs_ = seastar::make_lw_shared<snail::raft::ProgressTracker>(
                max_inflight);
            if (!learners.empty()) {
                raft->prs_->learners().clear();
            }
            for (int i = 0; i < size; i++) {
                auto pr = seastar::make_lw_shared<snail::raft::Progress>(
                    max_inflight);
                auto iter = learners.find(peer_addrs[i]);
                if (iter != learners.end()) {
                    pr->set_learner(true);
                    raft->prs_->learners().insert(peer_addrs[i]);
                } else {
                    raft->prs_->voters()[0].insert(peer_addrs[i]);
                }
                raft->prs_->progress()[peer_addrs[i]] = pr;
            }
            raft->Reset(raft->term_);
            npeers[id] = peers[i];
        } else if (peers[i]->Type() ==
                   StateMachineType::BlackHoleStateMachine) {
            npeers[id] = peers[i];
        } else {
            throw std::runtime_error("unexpected state machine type");
        }
    }

    auto net = seastar::make_lw_shared<Network>();
    net->peers_ = std::move(npeers);
    net->storage_ = std::move(nstorage);
    return net;
}

snail::raft::Raft::Config newTestConfig(uint64_t id, int election,
                                        int heartbeat,
                                        snail::raft::StoragePtr store) {
    snail::raft::Raft::Config cfg;
    cfg.id = id;
    cfg.election_tick = election;
    cfg.heartbeat_tick = heartbeat;
    cfg.storage = store;
    cfg.max_size_per_msg = std::numeric_limits<uint64_t>::max();
    cfg.max_inflight_msgs = 256;
    return cfg;
}

seastar::future<snail::raft::RaftPtr> newTestRaft(
    uint64_t id, int election, int heartbeat, snail::raft::StoragePtr store) {
    snail::raft::RaftPtr raft = seastar::make_lw_shared<snail::raft::Raft>();
    co_await raft->Init(newTestConfig(id, election, heartbeat, store));
    co_return raft;
}

snail::raft::StoragePtr newTestMemoryStorage() {
    seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
        seastar::make_shared<snail::raft::MemoryStorage>();
    return seastar::dynamic_pointer_cast<snail::raft::Storage>(ptr);
}

std::vector<uint64_t> idsBySize(int size) {
    std::vector<uint64_t> ids;
    for (int i = 0; i < size; i++) {
        ids.push_back(1 + i);
    }
    return ids;
}

}  // namespace raft
}  // namespace snail
