#include <yaml-cpp/yaml.h>

#include "proto/master.pb.h"
#include "raft_server.h"

struct RaftConfig {
    uint64_t id;
    std::string raft_wal_path;
    uint32_t ticket_interval;
    uint32_t heartbeat_tick;
    uint32_t election_tick;
    std::vector<snail::stream::RaftNode> raft_nodes;

    void ParseRaftConfig(const YAML::Node& node) {
        raft_wal_path = node["wal_path"].as<std::string>();
        ticket_interval = node["ticket_interval"].as<uint32_t>();
        heartbeat_tick = node["heartbeat_tick"].as<uint32_t>();
        heartbeat_tick = node["election_tick"].as<uint32_t>();
        YAML::Node raft_nodes_doc = node["nodes"];
        for (auto child : raft_nodes_doc) {
            RaftNode raft_node;
            raft_node.set_id(child["id"].as<uint64_t>());
            raft_node.set_raft_host(child["raft_host"].as<std::string>());
            raft_node.set_raft_port(child["raft_port"].as<uint16_t>());
            raft_node.set_host(child["host"].as<std::string>());
            raft_node.set_port(child["port"].as<std::string>());
            raft_node.set_learner(child["learner"].as<bool>());
            raft_nodes.push_back(raft_node);
        }
    }
};

struct Config {
    std::string host;
    uint16_t port;
    bool poll_mode = false;
    std::string log_file;
    std::string log_level;
    std::string hugedir;
    std::string memory;
    std::string db_path;
    RaftConfig raft_cfg;

    void ParseConfigFile(const std::string& name) {
        YAML::Node doc = YAML::LoadFile(name);

        if (doc["host"]) {
            host = doc["host"].as<std::string>();
        }
        if (doc["port"]) {
            host = doc["port"].as<uint16_t>();
        }
        if (doc["poll_mode"]) {
            poll_mode = doc["poll_mode"].as<bool>();
        }
        if (doc["log_file"]) {
            log_file = doc["log_file"].as<std::string>();
        }
        if (doc["log_level"]) {
            log_file = doc["log_level"].as<std::string>();
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
                raft_cfg.id = raft_nodes[i].id();
                break;
            }
        }
        if (!found) {
            throw std::system_error("not found our self raft_id in raft nodes");
        }
    }
};

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

    seastar::app_template::seastar_options opts;
    opts.auto_handle_sigint_sigterm = false;
    opts.reactor_opts.abort_on_seastar_bad_alloc.set_value();
    opts.smp_opts.smp.set_value(1);
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

    char* args[1] = {argv[0]};
    return app.run(1, args, []() -> seastar::future<> {
        return seastar::async([] {
            auto st =
                snail::stream::Storage::Create(cfg.db_path, cfg.raft_cfg.id)
                    .get0();
            if (!st) {
                LOG_ERROR("create storage error: {}", st);
                return;
            }
            snail::stream::StoragePtr store = st.Value();
            snail::stream::IDGeneratorPtr id_gen =
                seastar::make_lw_shared<snail::stream::IDGenerator>(
                    cfg.raft_cfg.id);

            snail::stream::RaftServerOption opt;
            opt.node_id = cfg.raft_cfg.id;
            opt.tick_interval = cfg.raft_cfg.tick_interval;
            opt.heartbeat_tick = cfg.raft_cfg.heartbeat_tick;
            opt.election_tick = cfg.raft_cfg.election_tick;
            opt.wal_dir = cfg.raft_cfg.raft_wal_path;

            uint64_t applied = store->Applied();
            std::vector<snail::stream::RaftNode> raft_nodes =
                store->GetRaftNodes();
            if (raft_nodes.empty()) {
                raft_nodes = cfg.raft_cfg.raft_nodes;
            }
            auto st1 =
                snail::stream::RaftServer::Create(
                    opt, applied, std::move(raft_nodes),
                    seastar::dynamic_pointer_cast<StatemachinePtr, StoragePtr>(
                        store))
                    .get0();
            if (!st1) {
                LOG_ERROR("create raft server error: {}", st1);
                store->Close().get();
                return;
            }
            snail::stream::RaftServerPtr raft = st1.Value();
            for (;;) {
                // load wal
                auto st2 = raft->ReadIndex().get0();
                if (st2) {
                    break;
                }
            }
        });
    });
}
