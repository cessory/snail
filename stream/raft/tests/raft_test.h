#pragma once

#include "raft/raft.h"

namespace snail {
namespace raft {

enum StateMachineType {
    RaftStateMachine = 0,
    BlackHoleStateMachine = 1,
};

class StateMachine {
   public:
    virtual seastar::future<Status<>> Step(snail::raft::MessagePtr m) = 0;
    virtual std::vector<snail::raft::MessagePtr> ReadMessage() = 0;
    virtual StateMachineType Type() = 0;
};
using StateMachinePtr = seastar::shared_ptr<StateMachine>;

class BlackHoleSM : public StateMachine {
   public:
    static StateMachinePtr MakeStateMachine();

    seastar::future<Status<>> Step(snail::raft::MessagePtr m) override;
    std::vector<snail::raft::MessagePtr> ReadMessage() override;
    StateMachineType Type() override;
};

class RaftSM : public StateMachine {
   public:
    snail::raft::RaftPtr raft_;

   public:
    static StateMachinePtr MakeStateMachine(snail::raft::RaftPtr r);

    RaftSM(snail::raft::RaftPtr r) : raft_(r) {}
    seastar::future<Status<>> Step(snail::raft::MessagePtr m) override;
    std::vector<snail::raft::MessagePtr> ReadMessage() override;
    StateMachineType Type() override;
};

extern StateMachinePtr nopStepper;

struct connem {
    uint64_t from;
    uint64_t to;
    bool operator==(const connem& x) const {
        return from == x.from && to == x.to;
    }
};

struct connem_hash {
    size_t operator()(const connem& c) const { return c.from << 32 | c.to; }
};

class Network {
   public:
    std::unordered_map<uint64_t, StateMachinePtr> peers_;
    std::unordered_map<uint64_t,
                       seastar::shared_ptr<snail::raft::MemoryStorage>>
        storage_;
    std::unordered_map<connem, float, connem_hash> dropm_;
    std::unordered_map<snail::raft::MessageType, bool> ignorem_;
    std::function<bool(snail::raft::MessagePtr)> msg_hook_;

    seastar::future<> Send(const std::vector<snail::raft::MessagePtr>& msgs);

    void Drop(uint64_t from, uint64_t to, float perc);

    void Cut(uint64_t one, uint64_t other);

    void Isolate(uint64_t id);

    void Ignore(snail::raft::MessageType t);

    void Recover();

    std::vector<snail::raft::MessagePtr> Filter(
        const std::vector<snail::raft::MessagePtr>& msgs);
};
using NetworkPtr = seastar::lw_shared_ptr<Network>;

StateMachinePtr entsWithConfig(
    std::function<void(snail::raft::Raft::Config*)> configFunc,
    const std::vector<uint64_t>& terms);

StateMachinePtr votedWithConfig(
    std::function<void(snail::raft::Raft::Config*)> configFunc, uint64_t vote,
    uint64_t term);

void preVoteConfig(snail::raft::Raft::Config* cfg);

NetworkPtr newNetwork(const std::vector<StateMachinePtr>& peers);

NetworkPtr newNetworkWithConfig(
    std::function<void(snail::raft::Raft::Config*)> configFunc,
    const std::vector<StateMachinePtr>& peers);

snail::raft::Raft::Config newTestConfig(uint64_t id, int election,
                                        int heartbeat,
                                        snail::raft::StoragePtr store);

seastar::future<snail::raft::RaftPtr> newTestRaft(
    uint64_t id, int election, int heartbeat, snail::raft::StoragePtr store);

snail::raft::StoragePtr newTestMemoryStorage();

std::vector<uint64_t> idsBySize(int size);

}  // namespace raft
}  // namespace snail
