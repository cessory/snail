#include <yaml-cpp/yaml.h>

#include <boost/program_options.hpp>
#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>

#include "extentnode_mgr.h"
#include "id_allocator.h"
#include "id_generator.h"
#include "proto/master.pb.h"
#include "raft_server.h"
#include "server.h"
#include "storage.h"
#include "util/logger.h"

struct RaftConfig {
    uint64_t id = 0;
    std::string raft_wal_path;
    uint32_t retain_wal_entries = 20000;
    uint32_t tick_interval = 1;
    uint32_t heartbeat_tick = 1;
    uint32_t election_tick = 2;
    std::vector<snail::stream::RaftNode> raft_nodes;

    void ParseRaftConfig(const YAML::Node& node) {
        raft_wal_path = node["wal_path"].as<std::string>();
        if (raft_wal_path.empty()) {
            throw std::runtime_error("wal_path is empty");
        }
        if (node["retain_wal_entries"]) {
            retain_wal_entries = node["retain_wal_entries"].as<uint32_t>();
            if (retain_wal_entries == 0) {
                throw std::runtime_error("retain_wal_entries is zero");
            }
        }
        if (node["tick_interval"]) {
            tick_interval = node["tick_interval"].as<uint32_t>();
            if (tick_interval == 0) {
                throw std::runtime_error("tick_interval is zero");
            }
        }
        if (node["heartbeat_tick"]) {
            heartbeat_tick = node["heartbeat_tick"].as<uint32_t>();
            if (heartbeat_tick == 0) {
                throw std::runtime_error("heartbeat_tick is zero");
            }
        }
        if (node["election_tick"]) {
            election_tick = node["election_tick"].as<uint32_t>();
        }
        if (election_tick <= heartbeat_tick) {
            throw std::runtime_error(
                "election_tick is lesser than heartbeat_tick");
        }
        YAML::Node raft_nodes_doc = node["nodes"];

        std::unordered_set<uint64_t> id_set;
        for (auto child : raft_nodes_doc) {
            snail::stream::RaftNode raft_node;
            auto id = child["id"].as<uint64_t>();
            if (id_set.count(id)) {
                throw std::runtime_error("has duplicate id in raft config");
            }
            id_set.insert(id);
            raft_node.set_id(id);
            if (id == 0) {
                throw std::runtime_error("invalid id in raft config");
            }
            raft_node.set_raft_host(child["raft_host"].as<std::string>());
            raft_node.set_raft_port(child["raft_port"].as<uint16_t>());
            if (raft_node.raft_host().empty() || raft_node.raft_port() == 0) {
                throw std::runtime_error(
                    "invalid raft_host or raft_port in raft config");
            }
            raft_node.set_host(child["host"].as<std::string>());
            raft_node.set_port(child["port"].as<uint16_t>());
            if (raft_node.host().empty() || raft_node.port() == 0) {
                throw std::runtime_error("invalid host or port in raft config");
            }
            if (child["learner"]) {
                raft_node.set_learner(child["learner"].as<bool>());
            }
            raft_nodes.push_back(raft_node);
        }
    }
};

struct Config {
    uint32_t cluster_id = 0;
    std::string host;
    uint16_t port = 0;
    bool poll_mode = false;
    std::string log_file;
    std::string log_level;
    std::string hugedir;
    std::string memory;
    std::string db_path;
    RaftConfig raft_cfg;

    void ParseConfigFile(const std::string& name) {
        YAML::Node doc = YAML::LoadFile(name);

        if (doc["cluster_id"]) {
            cluster_id = doc["cluster_id"].as<uint32_t>();
        } else {
            throw std::runtime_error("not found cluster_id in config file");
        }
        if (doc["host"]) {
            host = doc["host"].as<std::string>();
        } else {
            throw std::runtime_error("not found host in config file");
        }
        if (doc["port"]) {
            port = doc["port"].as<uint16_t>();
        } else {
            throw std::runtime_error("not found port in config file");
        }
        if (doc["poll_mode"]) {
            poll_mode = doc["poll_mode"].as<bool>();
        }
        if (doc["log_file"]) {
            log_file = doc["log_file"].as<std::string>();
        }
        if (doc["log_level"]) {
            log_level = doc["log_level"].as<std::string>();
        }
        if (doc["memory"]) {
            memory = doc["memory"].as<std::string>();
        }
        if (doc["hugedir"]) {
            hugedir = doc["hugedir"].as<std::string>();
        }
        db_path = doc["db_path"].as<std::string>();
        raft_cfg.ParseRaftConfig(doc["raft"]);
        bool found = false;
        for (int i = 0; i < raft_cfg.raft_nodes.size(); i++) {
            if (host == raft_cfg.raft_nodes[i].host() &&
                port == raft_cfg.raft_nodes[i].port()) {
                found = true;
                raft_cfg.id = raft_cfg.raft_nodes[i].id();
                break;
            }
        }
        if (!found) {
            throw std::runtime_error(
                "not found our self raft_id in raft nodes");
        }
    }
};

namespace bpo = boost::program_options;

static void ServerStart(Config cfg) {
    auto st1 =
        seastar::smp::submit_to(
            1,
            seastar::coroutine::lambda(
                [&cfg]() -> seastar::future<snail::Status<
                             seastar::foreign_ptr<snail::stream::StoragePtr>>> {
                    snail::Status<
                        seastar::foreign_ptr<snail::stream::StoragePtr>>
                        s;

                    snail::stream::RaftServerOption opt;
                    opt.node_id = cfg.raft_cfg.id;
                    opt.tick_interval = cfg.raft_cfg.tick_interval;
                    opt.heartbeat_tick = cfg.raft_cfg.heartbeat_tick;
                    opt.election_tick = cfg.raft_cfg.election_tick;
                    opt.wal_dir = cfg.raft_cfg.raft_wal_path;
                    auto st = co_await snail::stream::Storage::Create(
                        cfg.db_path, opt, cfg.raft_cfg.retain_wal_entries,
                        cfg.raft_cfg.raft_nodes);
                    if (!st) {
                        LOG_ERROR("create storage error: {}", st);
                        s.Set(st.Code(), st.Reason());
                        co_return s;
                    }
                    seastar::foreign_ptr<snail::stream::StoragePtr> ptr =
                        seastar::make_foreign(std::move(st.Value()));
                    s.SetValue(std::move(ptr));
                    co_return s;
                }))
            .get0();
    if (!st1) {
        return;
    }
    LOG_INFO("create storage succeed...");
    seastar::foreign_ptr<snail::stream::StoragePtr> foreign_store =
        std::move(st1.Value());
    seastar::foreign_ptr<snail::stream::IDGeneratorPtr> foreign_id_gen =
        seastar::make_foreign(
            seastar::make_shared<snail::stream::IDGenerator>(cfg.raft_cfg.id));

    auto st2 = snail::stream::IdAllocator::CreateDiskIdAllocator(
                   foreign_store.get(), foreign_id_gen.get())
                   .get0();
    if (!st2) {
        foreign_store->Close().get();
        LOG_ERROR("create diskid allocator error: {}", st2);
        return;
    }
    snail::stream::IdAllocatorPtr diskid_allocator = st2.Value();
    LOG_INFO("create diskid allocator succeed...");

    auto st3 = snail::stream::ExtentnodeMgr::Create(foreign_store.get(),
                                                    foreign_id_gen.get())
                   .get0();
    if (!st3) {
        foreign_store->Close().get();
        LOG_ERROR("create extentnode mgr error: {}", st3);
        return;
    }
    snail::stream::ExtentnodeMgrPtr extentnode_mgr = st3.Value();
    LOG_INFO("create extentnode manager succeed...");
    // register apply handler
    foreign_store->RegisterApplyHandler(diskid_allocator.get());
    foreign_store->RegisterApplyHandler(extentnode_mgr.get());

    auto ft = foreign_store.get()->Start();
    while (!foreign_store->HasLeader()) {
        foreign_store->WaitLeaderChange().get();
    }
    foreign_store->Reload().get();
    LOG_INFO("start raft server succeed...");

    seastar::smp::submit_to(
        2, seastar::coroutine::lambda(
               [&cfg, diskid_allocator = diskid_allocator.get(),
                extentnode_mgr = extentnode_mgr.get()]() -> seastar::future<> {
                   snail::stream::ServicePtr service =
                       seastar::make_lw_shared<snail::stream::Service>(
                           cfg.cluster_id,
                           cfg.raft_cfg.tick_interval *
                               cfg.raft_cfg.heartbeat_tick * 1000,
                           diskid_allocator, extentnode_mgr);
                   LOG_INFO("create service succeed...");
                   // create tcp server
                   snail::stream::ServerPtr tcp_server =
                       seastar::make_lw_shared<snail::stream::Server>(
                           cfg.host, cfg.port, service.get());

                   LOG_INFO("start tcp server on shard {}...",
                            seastar::this_shard_id());
                   co_await tcp_server->Start();
                   co_await tcp_server->Close();
                   co_return;
               }))
        .get();
    foreign_store->Close().get();
    ft.get();
    return;
}

int main(int argc, char* argv[]) {
    boost::program_options::options_description desc;
    desc.add_options()("help,h", "show help message");
    desc.add_options()("config,c", bpo::value<std::string>(), "config file");
    bpo::variables_map vm;
    try {
        bpo::store(bpo::command_line_parser(argc, argv).options(desc).run(),
                   vm);
        bpo::notify(vm);
    } catch (std::exception& e) {
        std::cout << "parse command line error: " << e.what() << std::endl;
        return -1;
    }

    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }

    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%f][%l][%s:%#] %v");
    Config cfg;
    try {
        cfg.ParseConfigFile(vm["config"].as<std::string>());
    } catch (std::exception& e) {
        LOG_ERROR("parse config file error: {}", e.what());
        return 0;
    }
    if (!cfg.log_file.empty()) {
        LOG_INIT(cfg.log_file, 1073741824, 3);
    }
    LOG_SET_LEVEL(cfg.log_level);

    seastar::app_template::seastar_options opts;
    opts.auto_handle_sigint_sigterm = false;
    opts.reactor_opts.abort_on_seastar_bad_alloc.set_value();
    if (cfg.poll_mode) {
        opts.reactor_opts.poll_mode.set_value();
    }
    if (!cfg.memory.empty()) {
        opts.smp_opts.memory.set_value(cfg.memory);
    }
    if (!cfg.hugedir.empty()) {
        opts.smp_opts.hugepages.set_value(cfg.hugedir);
    }

    seastar::app_template app(std::move(opts));

    char* args[2] = {argv[0]};
    args[1] = strdup("--reactor-backend=epoll");
    return app.run(2, args, [cfg]() -> seastar::future<> {
        return seastar::async([cfg] { ServerStart(cfg); });
    });
}
