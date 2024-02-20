#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/when_all.hh>

#include "tcp_connection.h"
#include "tcp_session.h"

namespace bpo = boost::program_options;

seastar::future<> test_stream(snail::net::StreamPtr stream) {
    seastar::temporary_buffer<char> data(32 << 10);
    for (;;) {
        std::vector<seastar::future<snail::Status<>>> fu_vec;
        for (int i = 0; i < 8; i++) {
            std::vector<iovec> iov;
            iov.push_back({data.get_write(), data.size() - 4});
            iov.push_back({data.get_write() + data.size() - 4, 4});
            auto ft = stream->WriteFrame(std::move(iov));
            fu_vec.emplace_back(std::move(ft));
        }
        auto res_vec =
            co_await seastar::when_all_succeed(fu_vec.begin(), fu_vec.end());
        bool has_error = false;
        for (int i = 0; i < 8; i++) {
            if (!res_vec[i].OK()) {
                std::cout << "write frame error: " << res_vec[i].Reason()
                          << std::endl;
                has_error = true;
                break;
            }
        }
        if (has_error) {
            break;
        }
    }
    co_await stream->Close();
    co_return;
}

seastar::future<> test_client(uint16_t port) {
    seastar::socket_address sa(seastar::ipv4_addr("127.0.0.1", port));

    auto fd = seastar::engine().make_pollable_fd(sa, 0);
    try {
        co_await seastar::engine().posix_connect(fd, sa,
                                                 seastar::socket_address());
    } catch (std::exception& e) {
        std::cout << "connect server error: " << e.what() << std::endl;
        co_return;
    }
    auto conn = snail::net::TcpConnection::make_connection(std::move(fd), sa);

    snail::net::Option opt;
    opt.version = 2;
    auto sess = snail::net::Session::make_session(opt, conn, true);

    std::vector<seastar::future<>> fu_vec;
    for (int i = 0; i < 10; i++) {
        auto s = co_await sess->OpenStream();
        if (!s.OK()) {
            std::cout << "open stream error: " << s.Reason() << std::endl;
            break;
        }
        auto fu = test_stream(s.Value());
        fu_vec.emplace_back(std::move(fu));
    }
    std::cout << "has " << fu_vec.size() << " streams" << std::endl;
    co_await seastar::when_all_succeed(fu_vec.begin(), fu_vec.end());
    co_await sess->Close();
    co_return;
}

int main(int argc, char** argv) {
    boost::program_options::options_description desc;
    desc.add_options()("help,h", "show help message");
    desc.add_options()("port", bpo::value<uint16_t>(), "Server port");
    desc.add_options()("cpu", bpo::value<unsigned>()->default_value(1),
                       "bind cpu");

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

    seastar::app_template::seastar_options opts;
    // opts.reactor_opts.poll_mode.set_value();
    opts.smp_opts.smp.set_value(1);
    opts.smp_opts.cpuset.set_value({vm["cpu"].as<unsigned>()});
    opts.auto_handle_sigint_sigterm = false;
    seastar::app_template app(std::move(opts));
    char* args[1] = {argv[0]};
    return app.run(1, args,
                   [vm = std::move(vm)]() mutable -> seastar::future<> {
                       return seastar::async([vm = std::move(vm)]() mutable {
                           if (!vm.count("port")) {
                               return;
                           }
                           uint16_t port = vm["port"].as<uint16_t>();
                           test_client(port).get();
                           return;
                       });
                   });
}
