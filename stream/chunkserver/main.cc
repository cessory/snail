#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/routes.hh>

#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/spdlog.h"
#include "store.h"

namespace bpo = boost::program_options;

class stop_signal {
    bool _caught = false;
    seastar::condition_variable _cond;

   private:
    void signaled() {
        if (_caught) {
            return;
        }
        _caught = true;
        _cond.broadcast();
    }

   public:
    stop_signal() {
        seastar::engine().handle_signal(SIGINT, [this] { signaled(); });
        seastar::engine().handle_signal(SIGTERM, [this] { signaled(); });
    }
    ~stop_signal() {
        // There's no way to unregister a handler yet, so register a no-op
        // handler instead.
        seastar::engine().handle_signal(SIGINT, [] {});
        seastar::engine().handle_signal(SIGTERM, [] {});
    }
    seastar::future<> wait() {
        return _cond.wait([this] { return _caught; });
    }
    bool stopping() const { return _caught; }
};

int main(int argc, char** argv) {
    boost::program_options::options_description desc;
    desc.add_options()("help,h", "show help message");
    desc.add_options()("format", bpo::value<std::string>(), "format disk");
    desc.add_options()("disk", bpo::value<std::string>(), "disk path");
    desc.add_options()("logfile", bpo::value<std::string>(), "log file");
    desc.add_options()("loglevel", bpo::value<std::string>(), "log level");

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
    if (vm.count("logfile")) {
        auto logger = spdlog::rotating_logger_mt(
            "chunkserver", vm["logfile"].as<std::string>(), 1073741824, 10);
        spdlog::set_default_logger(logger);
    }

    if (vm.count("loglevel")) {
        spdlog::set_level(
            spdlog::level::from_str(vm["loglevel"].as<std::string>()));
    }

    seastar::app_template::seastar_options opts;
    opts.reactor_opts.poll_mode.set_value();
    opts.smp_opts.smp.set_value(1);
    seastar::app_template app(std::move(opts));
    char* args[1] = {argv[0]};
    return app.run(
        1, args, [vm = std::move(vm)]() mutable -> seastar::future<> {
            return seastar::async([vm = std::move(vm)]() mutable {
                stop_signal signal;
                if (vm.count("format")) {
                    std::string path = vm["format"].as<std::string>();
                    auto ok = snail::stream::Store::Format(
                                  path, 1, snail::stream::DevType::HDD, 1)
                                  .get();
                    if (!ok) {
                        SPDLOG_ERROR("format {} error", path);
                    } else {
                        SPDLOG_INFO("format {} success", path);
                    }
                    return;
                }

                if (!vm.count("disk")) {
                    SPDLOG_ERROR("not found disk path");
                    return;
                }
                std::string disk_path = vm["disk"].as<std::string>();
                return;
            });
        });
}
