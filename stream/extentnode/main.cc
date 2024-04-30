#include <pthread.h>
#include <spdk/env.h>
#include <yaml-cpp/yaml.h>

#include <seastar/core/app-template.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>

#include "tcp_server.h"
#include "util/logger.h"

namespace bpo = boost::program_options;
using namespace snail;
using namespace snail::stream;

struct DiskConfig {
    std::string name;
    bool spdk_nvme = false;
    unsigned shard = 0;
    unsigned ns_id = 1;  // if spdk_nvme is true, this is aviliable
};

struct NetConfig {
    std::string host = "0.0.0.0";
    uint16_t port = 9000;
    std::set<unsigned> shards;
};

struct Config {
    bool poll_mode = false;
    std::set<unsigned> cpuset;
    std::string log_file;
    std::string log_level;
    std::string hugedir;
    std::string memory;
    NetConfig net_cfg;
    std::vector<DiskConfig> disks_cfg;
};

static void ParseDiskConfig(const YAML::Node& node, DiskConfig& cfg) {
    if (node["name"]) {
        cfg.name = node["name"].as<std::string>();
    }
    if (node["spdk_nvme"]) {
        cfg.spdk_nvme = node["spdk_nvme"].as<bool>();
    }
    if (node["shard"]) {
        cfg.shard = node["shard"].as<unsigned>();
    }
    if (node["ns_id"]) {
        cfg.ns_id = node["ns_id"].as<unsigned>();
    }
}

static void ParseNetConfig(const YAML::Node& node, NetConfig& cfg) {
    if (node["host"]) {
        cfg.host = node["host"].as<std::string>();
    }
    if (node["port"]) {
        cfg.port = node["port"].as<uint16_t>();
    }

    auto& child = node["shards"];
    if (child && child.IsSequence()) {
        for (auto& i : child) {
            cfg.shards.insert(i.as<unsigned>());
        }
    }
}

static bool ParseConfigFile(const std::string& name, Config& cfg) {
    YAML::Node doc = YAML::LoadFile(name);

    if (doc["poll_mode"]) {
        cfg.poll_mode = doc["poll_mode"].as<bool>();
    }
    if (doc["log_file"]) {
        cfg.log_file = doc["log_file"].as<std::string>();
    }
    if (doc["log_level"]) {
        cfg.log_file = doc["log_level"].as<std::string>();
    }
    if (doc["memory"]) {
        cfg.memory = doc["memory"].as<std::string>();
    }
    if (doc["hugedir"]) {
        cfg.hugedir = doc["hugedir"].as<std::string>();
    }

    if (doc["cpuset"] && doc["cpuset"].IsSequence()) {
        for (const auto& node : doc["cpuset"]) {
            cfg.cpuset.insert(node.as<unsigned>());
        }
    }

    unsigned max_thread_n =
        std::max(static_cast<unsigned>(1), (unsigned)cfg.cpuset.size());

    if (doc["net"] && doc["net"].IsMap()) {
        ParseNetConfig(doc["net"], cfg.net_cfg);
        auto it = cfg.net_cfg.shards.rbegin();
        if (it != cfg.net_cfg.shards.rend() && *it >= max_thread_n) {
            LOG_ERROR("shard id {} in net is too large", *it);
            return false;
        } else if (it == cfg.net_cfg.shards.rend()) {
            for (unsigned i = 0; i < max_thread_n; i++) {
                cfg.net_cfg.shards.insert(i);
            }
        }
    }

    if (doc["disks"] && doc["disks"].IsSequence()) {
        for (const auto& node : doc["disks"]) {
            DiskConfig disk_cfg;
            ParseDiskConfig(node, disk_cfg);
            if (disk_cfg.name == "") {
                LOG_ERROR("disk name is empty");
                return false;
            }
            if (disk_cfg.shard >= max_thread_n) {
                LOG_ERROR("invalid shard={} in disk-{}", disk_cfg.shard,
                          disk_cfg.name);
                return false;
            }
            cfg.disks_cfg.push_back(disk_cfg);
            if (disk_cfg.spdk_nvme && !cfg.poll_mode) {
                // if we have a spdk nvme, we must set poll mode
                cfg.poll_mode = true;
            }
        }
    }
    return true;
}

static seastar::future<Status<seastar::foreign_ptr<ServicePtr>>> CreateService(
    const DiskConfig disk_cfg) {
    Status<seastar::foreign_ptr<ServicePtr>> s;
    if (disk_cfg.shard == seastar::this_shard_id()) {
        auto store = co_await Store::Load(disk_cfg.name, disk_cfg.spdk_nvme);
        if (!store) {
            s.Set(ErrCode::ErrUnExpect);
            co_return s;
        }
        auto service =
            seastar::make_foreign(seastar::make_lw_shared<Service>(store));
        s.SetValue(std::move(service));
    } else {
        s = co_await seastar::smp::submit_to(
            disk_cfg.shard,
            [disk_cfg]()
                -> seastar::future<Status<seastar::foreign_ptr<ServicePtr>>> {
                Status<seastar::foreign_ptr<ServicePtr>> s;
                auto store =
                    co_await Store::Load(disk_cfg.name, disk_cfg.spdk_nvme);
                if (!store) {
                    s.Set(ErrCode::ErrUnExpect);
                    co_return s;
                }
                auto service = seastar::make_foreign(
                    seastar::make_lw_shared<Service>(store));
                s.SetValue(std::move(service));
                co_return s;
            });
    }
    co_return s;
}

int main(int argc, char** argv) {
    boost::program_options::options_description desc;
    desc.add_options()("help,h", "show help message");
    desc.add_options()("config,c", bpo::value<std::string>(), "config file");
    desc.add_options()("format", bpo::value<std::string>(), "format disk");
    desc.add_options()("spdk_nvme", bpo::value<bool>(),
                       "if spdk nvme, set true");

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
    bool is_format = false;
    bool spdk_nvme = false;
    std::string format_disk;
    if (vm.count("format")) {
        is_format = true;
        format_disk = vm["format"].as<std::string>();
    }
    if (vm.count("spdk_nvme")) {
        spdk_nvme = vm["spdk_nvme"].as<bool>();
    }
    if (!is_format && vm.count("config")) {
        try {
            if (!ParseConfigFile(vm["config"].as<std::string>(), cfg)) {
                return 0;
            }
        } catch (std::exception& e) {
            LOG_ERROR("parse config file error: {}", e.what());
            return 0;
        }
    }

    if (!is_format) {
        if (!cfg.log_file.empty()) {
            LOG_INIT(cfg.log_file, 1073741824, 10);
        }
        if (!cfg.log_level.empty()) {
            LOG_SET_LEVEL(cfg.log_level);
        }
    }

    seastar::app_template::seastar_options opts;
    opts.auto_handle_sigint_sigterm = false;
    opts.reactor_opts.abort_on_seastar_bad_alloc.set_value();
    if (cfg.poll_mode) {
        opts.reactor_opts.poll_mode.set_value();
    }
    if (is_format) {
        opts.smp_opts.smp.set_value(1);
    } else {
        if (!cfg.cpuset.empty()) {
            opts.smp_opts.smp.set_value(cfg.cpuset.size());
            opts.smp_opts.cpuset.set_value(cfg.cpuset);
        } else {
            opts.smp_opts.smp.set_value(1);
        }
    }
    if (!cfg.memory.empty()) {
        opts.smp_opts.memory.set_value(cfg.memory);
    }
    if (!cfg.hugedir.empty()) {
        opts.smp_opts.hugepages.set_value(cfg.hugedir);
    }
    seastar::app_template app(std::move(opts));

#ifdef HAS_SPDK
    struct spdk_env_opts spdk_opts;
    spdk_env_opts_init(&spdk_opts);
    spdk_opts.name = argv[0];
    if (!cfg.hugedir.empty()) {
        spdk_opts.hugedir = cfg.hugedir.c_str();
    }
    if (spdk_env_init(&spdk_opts) < 0) {
        LOG_ERROR("Unable to initialize SPDK env");
        return 0;
    }
    spdk_unaffinitize_thread();
#endif

    char* args[1] = {argv[0]};
    return app.run(
        1, args,
        [is_format, format_disk, spdk_nvme, cfg]() -> seastar::future<> {
            return seastar::async([is_format, format_disk, spdk_nvme, cfg]() {
                if (is_format) {
                    auto ok =
                        snail::stream::Store::Format(
                            format_disk, 1, snail::stream::DevType::HDD, 1)
                            .get();
                    if (!ok) {
                        LOG_ERROR("format {} error", format_disk);
                    } else {
                        LOG_INFO("format {} success", format_disk);
                    }
                    return;
                }
                TcpServer* server = nullptr;
                try {
                    server = new TcpServer(cfg.net_cfg.host, cfg.net_cfg.port,
                                           cfg.net_cfg.shards);
                } catch (std::exception& e) {
                    LOG_ERROR("new tcp server error: {}", e.what());
                    return;
                }
                std::vector<
                    seastar::future<Status<seastar::foreign_ptr<ServicePtr>>>>
                    fu_vec;
                for (const auto& disk_cfg : cfg.disks_cfg) {
                    auto fu = CreateService(disk_cfg);
                    fu_vec.emplace_back(std::move(fu));
                }
                for (int i = 0; i < fu_vec.size(); i++) {
                    auto s = fu_vec[i].get0();
                    if (s) {
                        server->RegisterService(std::move(s.Value())).get();
                    }
                }
                server->Start().get();

                server->Close().get();
                delete server;
                // engine().at_exit([server] { return server->Stop(); });
            });
        });
}

