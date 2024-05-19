#include "confchange.h"

#include "raft_errno.h"

namespace snail {
namespace raft {

static bool joint(const TrackerConfig& cfg) {
    return cfg.voters()[1].size() > 0;
}

static int symdiff(const MajorityConfig& l, const MajorityConfig& r) {
    int n = 0;
    for (auto& id : l) {
        auto it = r.find(id);
        if (it == r.end()) {
            n++;
        }
    }

    for (auto& id : r) {
        auto it = l.find(id);
        if (it == l.end()) {
            n++;
        }
    }
    return n;
}

static std::tuple<std::vector<ConfChangeSingle>, std::vector<ConfChangeSingle>>
toConfChangeSingle(const ConfState& cs) {
    std::vector<ConfChangeSingle> out;
    std::vector<ConfChangeSingle> in;

    for (auto& id : cs.voters_outgoing()) {
        out.push_back(ConfChangeSingle(ConfChangeType::ConfChangeAddNode, id));
        in.push_back(
            ConfChangeSingle(ConfChangeType::ConfChangeRemoveNode, id));
    }

    for (auto& id : cs.voters()) {
        in.push_back(ConfChangeSingle(ConfChangeType::ConfChangeAddNode, id));
    }

    for (auto& id : cs.learners()) {
        in.push_back(
            ConfChangeSingle(ConfChangeType::ConfChangeAddLearnerNode, id));
    }

    for (auto& id : cs.learners_next()) {
        in.push_back(
            ConfChangeSingle(ConfChangeType::ConfChangeAddLearnerNode, id));
    }

    return std::make_tuple<std::vector<ConfChangeSingle>,
                           std::vector<ConfChangeSingle>>(std::move(out),
                                                          std::move(in));
}

std::tuple<TrackerConfig, ProgressMap, int> Changer::EnterJoint(
    bool auto_leave, const std::vector<ConfChangeSingle>& ccs) {
    TrackerConfig cfg;
    ProgressMap prs;
    int err;
    auto res = CheckAndCopy();
    std::tie(cfg, prs, err) = res;
    if (err != RAFT_OK) {
        return std::make_tuple(cfg, prs, err);
    }
    if (joint(cfg)) {
        RAFT_LOG_ERROR(logger_, "[{}-{}] config is already joint", group_, id_);
        return std::make_tuple(TrackerConfig(), ProgressMap(),
                               RAFT_ERR_CONFCHANGE);
    }

    if (cfg.voters()[0].empty()) {
        RAFT_LOG_ERROR(logger_, "[{}-{}] can't make a zero-voter config joint",
                       group_, id_);
        return std::make_tuple(TrackerConfig(), ProgressMap(),
                               RAFT_ERR_CONFCHANGE);
    }
    cfg.voters()[1].clear();
    cfg.voters()[1] = cfg.voters()[0];

    auto st1 = Apply(&cfg, prs, ccs);
    if (!st1) {
        return std::make_tuple(TrackerConfig(), ProgressMap(),
                               RAFT_ERR_CONFCHANGE);
    }
    cfg.set_auto_leave(auto_leave);
    return CheckAndReturn(std::move(cfg), std::move(prs));
}

std::tuple<TrackerConfig, ProgressMap, int> Changer::LeaveJoint() {
    TrackerConfig cfg;
    ProgressMap prs;
    int err;
    auto res = CheckAndCopy();
    if (err != RAFT_OK) {
        return std::make_tuple(cfg, prs, err);
    }

    if (!joint(cfg)) {
        RAFT_LOG_ERROR(logger_, "[{}-{}] can't leave a non-joint config",
                       group_, id_);
        return std::make_tuple(TrackerConfig(), ProgressMap(),
                               RAFT_ERR_CONFCHANGE);
    }

    if (cfg.voters()[1].empty()) {
        RAFT_LOG_ERROR(logger_, "[{}-{}] configuration is not joint", group_,
                       id_);
        return std::make_tuple(TrackerConfig(), ProgressMap(),
                               RAFT_ERR_CONFCHANGE);
    }
    for (auto& id : cfg.learners_next()) {
        cfg.learners().insert(id);
        auto it = prs.find(id);
        if (it != prs.end()) {
            it->second->set_learner(true);
        }
    }
    cfg.learners_next().clear();
    for (auto& id : cfg.voters()[1]) {
        auto it = cfg.voters()[0].find(id);
        if (it != cfg.voters()[0].end()) {
            continue;
        }
        auto iter = cfg.learners().find(id);
        if (iter != cfg.learners().end()) {
            continue;
        }
        prs.erase(id);
    }
    cfg.voters()[1].clear();
    cfg.set_auto_leave(false);
    return CheckAndReturn(std::move(cfg), std::move(prs));
}

Status<std::tuple<TrackerConfig, ProgressMap>> Changer::Simple(
    const std::vector<ConfChangeSingle>& ccs) {
    Status<std::tuple<TrackerConfig, ProgressMap>> s;
    TrackerConfig cfg;
    ProgressMap prs;
    int err;
    auto st = CheckAndCopy();
    if (!st) {
        s.Set(st.Code());
        return s;
    }
    std::tie(cfg, prs) = st.Value();

    if (joint(cfg)) {
        LOG_ERROR("[{}-{}] can't apply simple config change in joint config",
                  group_, id_);
        s.Set(ErrCode::ErrRaftConfChange);
        return s;
    }

    auto st1 = Apply(&cfg, prs, ccs);
    if (!st1) {
        s.Set(ErrCode::ErrRaftConfChange);
        return s;
    }

    auto n = symdiff(tracker_->voters()[0], cfg.voters()[0]);
    if (n > 1) {
        LOG_ERROR(
            "[{}-{}] more than one voter changed without entering joint config",
            group_, id_);
        s.Set(ErrCode::ErrRaftConfChange);
        return s;
    }
    return CheckAndReturn(std::move(cfg), std::move(prs));
}

void Changer::InitProgress(TrackerConfig* cfg, ProgressMap& prs, uint64_t id,
                           bool is_learner) {
    if (!is_learner) {
        cfg->voters()[0].insert(id);
    } else {
        cfg->learners().insert(id);
    }

    ProgressPtr pr =
        seastar::make_lw_shared<Progress>(tracker_->max_inflight());
    pr->set_next(last_index_);
    pr->set_match(0);
    pr->set_learner(is_learner);
    pr->set_recent_active(true);
    prs[id] = pr;
}

void Changer::MakeVoter(TrackerConfig* cfg, ProgressMap& prs, uint64_t id) {
    auto iter = prs.find(id);
    if (iter == prs.end()) {
        InitProgress(cfg, prs, id, false);
        return;
    }
    iter->second->set_learner(false);
    cfg->learners().erase(id);
    cfg->learners_next().erase(id);
    cfg->voters()[0].insert(id);
}

void Changer::MakeLearner(TrackerConfig* cfg, ProgressMap& prs, uint64_t id) {
    auto iter = prs.find(id);
    if (iter == prs.end()) {
        InitProgress(cfg, prs, id, true);
        return;
    }

    auto pr = iter->second;

    if (pr->is_learner()) {
        return;
    }
    Remove(cfg, prs, id);
    prs[id] = pr;

    auto it = cfg->voters()[1].find(id);
    if (it != cfg->voters()[1].end()) {
        cfg->learners_next().insert(id);
    } else {
        pr->set_learner(true);
        cfg->learners().insert(id);
    }
}

void Changer::Remove(TrackerConfig* cfg, ProgressMap& prs, uint64_t id) {
    auto iter = prs.find(id);
    if (iter == prs.end()) {
        return;
    }

    cfg->voters()[0].erase(id);
    cfg->learners().erase(id);
    cfg->learners_next().erase(id);

    auto it = cfg->voters()[1].find(id);
    if (it == cfg->voters()[1].end()) {
        prs.erase(id);
    }
}

Status<> Changer::Apply(TrackerConfig* cfg, ProgressMap& prs,
                        const std::vector<ConfChangeSingle>& ccs) {
    Status<> s;
    for (auto& cc : ccs) {
        if (cc.node_id() == 0) {
            continue;
        }

        switch (cc.type()) {
            case ConfChangeType::ConfChangeAddNode:
                MakeVoter(cfg, prs, cc.node_id());
                break;
            case ConfChangeType::ConfChangeAddLearnerNode:
                MakeLearner(cfg, prs, cc.node_id());
                break;
            case ConfChangeType::ConfChangeRemoveNode:
                Remove(cfg, prs, cc.node_id());
                break;
            default:
                LOG_ERROR("[{}-{}] unexpected conf type {}", group_, id_,
                          (int)cc.type());
                s.Set(ErrCode::ErrRaftConfChange);
                return s;
        }
    }

    if (cfg->voters()[0].empty()) {
        LOG_ERROR("[{}-{}] remove all voters", group_, id_);
        s.Set(ErrCode::ErrRaftConfChange);
    }
    return s;
}

Status<std::tuple<TrackerConfig, ProgressMap>> Changer::Restore(
    const ConfState& cs) {
    Status<std::tuple<TrackerConfig, ProgressMap>> s;
    auto r = toConfChangeSingle(cs);
    std::vector<ConfChangeSingle> outgoing;
    std::vector<ConfChangeSingle> incoming;
    std::tie(outgoing, incoming) = r;

    auto fn = [this](const std::vector<ConfChangeSingle>& ccs) -> Status<> {
        Status<> s;
        for (auto& cc : ccs) {
            std::vector<ConfChangeSingle> vec;
            vec.push_back(cc);
            auto st = Simple(vec);
            if (!st) {
                s.Set(st.Code());
                return s;
            }
            TrackerConfig cfg;
            ProgressMap prs;
            std::tie(cfg, prs) = st.Value();
            tracker_->set_config(std::move(cfg));
            tracker_->set_progress(std::move(prs));
        }
        return s;
    };

    TrackerConfig cfg;
    ProgressMap prs;

    if (outgoing.empty()) {
        // No outgoing config, so just apply the incoming changes one by one.
        auto err = fn(incoming);
        if (err != RAFT_OK) {
            return std::make_tuple(TrackerConfig(), ProgressMap(), err);
        }
        TrackerConfig* pcfg = dynamic_cast<TrackerConfig*>(tracker_.get());
        cfg = *pcfg;
        prs = tracker_->progress();
        return std::make_tuple(std::move(cfg), std::move(prs), err);
    }
    auto err = fn(outgoing);
    if (err != RAFT_OK) {
        return std::make_tuple(TrackerConfig(), ProgressMap(), err);
    }

    auto res = EnterJoint(cs.auto_leave(), incoming);
    std::tie(cfg, prs, err) = res;
    if (err != RAFT_OK) {
        return std::make_tuple(TrackerConfig(), ProgressMap(), err);
    }
    tracker_->set_config(cfg);
    tracker_->set_progress(prs);
    return std::make_tuple(std::move(cfg), std::move(prs), err);
}

Status<> Changer::CheckInvariants(const TrackerConfig& cfg,
                                  const ProgressMap& prs) {
    Status<> s;
    auto voters = cfg.voters();
    for (int i = 0; i < voters.size(); i++) {
        for (auto id : voters[i]) {
            auto iter = prs.find(id);
            if (iter == prs.end()) {
                LOG_ERROR("[{}-{}] no progress for voter: {}", group_, id_, id);
                s.Set(ErrCode::ErrRaftConfChange);
                return s;
            }
        }
    }

    auto learners = cfg.learners();
    for (auto id : learners) {
        auto iter = prs.find(id);
        if (iter == prs.end()) {
            LOG_ERROR("[{}-{}] no progress for learner: {}", group_, id_, id);
            s.Set(ErrCode::ErrRaftConfChange);
            return s;
        } else if (!iter->second->is_learner()) {
            LOG_ERROR("[{}-{}] {} is in Learners, but is not marked as learner",
                      group_, id_, id);
            s.Set(ErrCode::ErrRaftConfChange);
            return s;
        }

        auto it = voters[1].find(id);
        if (it != voters[1].end()) {
            LOG_ERROR("[{}-{}] {} is in Learners and Voters[1]", group_, id_,
                      id);
            s.Set(ErrCode::ErrRaftConfChange);
            return s;
        }

        it = voters[0].find(id);
        if (it != voters[0].end()) {
            LOG_ERROR("[{}-{}] {} is in Learners and Voters[0]", group_, id_,
                      id);
            s.Set(ErrCode::ErrRaftConfChange);
            return s;
        }
    }

    if (!joint(cfg)) {
        if (!voters[1].empty()) {
            LOG_ERROR("[{}-{}] cfg.Voters[1] must be nil when not joint",
                      group_, id_);
            s.Set(ErrCode::ErrRaftConfChange);
            return s;
        }

        if (!cfg.learners_next().empty()) {
            LOG_ERROR("[{}-{}] cfg.LearnersNext must be nil when not joint",
                      group_, id_);
            s.Set(ErrCode::ErrRaftConfChange);
            return s;
        }
        if (cfg.auto_leave()) {
            LOG_ERROR("[{}-{}] AutoLeave must be false when not joint", group_,
                      id_);
            s.Set(ErrCode::ErrRaftConfChange);
            return s;
        }
    }
    return s;
}

Status<std::tuple<TrackerConfig, ProgressMap>> Changer::CheckAndCopy() {
    Status<std::tuple<TrackerConfig, ProgressMap>> s;
    auto cfg = tracker_->Clone();
    ProgressMap prs;
    prs = tracker_->progress();

    auto st = CheckInvariants(cfg, prs);
    if (!st) {
        s.Set(st.Code());
    } else {
        s.SetValue(std::make_tuple(cfg, prs));
    }
    return s;
}

Status<std::tuple<TrackerConfig, ProgressMap>> Changer::CheckAndReturn(
    TrackerConfig cfg, ProgressMap prs) {
    auto st = CheckInvariants(cfg, prs);
    if (!st) {
        s.Set(st.Code());
    } else {
        s.SetValue(std::make_tuple(std::move(cfg), std::move(prs)));
    }
    return s;
}

}  // namespace raft
}  // namespace snail
