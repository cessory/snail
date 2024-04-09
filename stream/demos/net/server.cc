#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/when_all.hh>

#include "net/tcp_connection.h"
#include "net/tcp_session.h"
#include "util/status.h"

namespace bpo = boost::program_options;

static size_t recv_bytes = 0;

seastar::future<> handle_stream(snail::net::StreamPtr stream) {
    for (;;) {
        auto s = co_await stream->ReadFrame();

        if (!s.OK()) {
            std::cout << "sid=" << stream->ID()
                      << " read frame error: " << s.Reason() << std::endl;
            break;
        }
        recv_bytes += s.Value().size();
    }
    co_await stream->Close();
    co_return;
}

seastar::future<> handle_sess(snail::net::SessionPtr sess) {
    seastar::timer t(
        [sess] { std::cout << "recv_bytes=" << recv_bytes << std::endl; });
    t.arm_periodic(std::chrono::seconds(1));
    for (;;) {
        auto s = co_await sess->AcceptStream();
        if (!s.OK()) {
            std::cout << "accept stream error: " << s.Reason() << std::endl;
            break;
        }
        auto stream = s.Value();
        (void)handle_stream(std::move(stream));
    }
    t.cancel();
    std::cout << "close sess" << std::endl;
    co_await sess->Close();
    co_return;
}

seastar::future<> test_server(uint16_t port) {
    seastar::socket_address sa(seastar::ipv4_addr("127.0.0.1", port));

    auto fd = seastar::engine().posix_listen(sa);
    for (;;) {
        auto ar = co_await fd.accept();
        auto conn = snail::net::TcpConnection::make_connection(
            std::move(std::get<0>(ar)), std::get<1>(ar));
        snail::net::Option opt;
        auto sess = snail::net::TcpSession::make_session(opt, conn, false);
        (void)handle_sess(sess);
    }
    co_return;
}

int main(int argc, char** argv) {
    boost::program_options::options_description desc;
    desc.add_options()("help,h", "show help message");
    desc.add_options()("port", bpo::value<uint16_t>(), "Server port");
    desc.add_options()("cpu", bpo::value<unsigned>()->default_value(2),
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
                           test_server(port).get();

                           return;
                       });
                   });
}
