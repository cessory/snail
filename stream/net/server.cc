#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>

#include "session.h"
#include "tcp_connection.h"

namespace bpo = boost::program_options;

seastar::future<> handle_stream(snail::net::StreamPtr stream) {
    for (;;) {
        auto s = co_await stream->ReadFrame();
        if (!s.OK()) {
            std::cout << "read frame error: " << s.Reason() << std::endl;
            break;
        }
    }
    co_await stream->Close();
    co_return;
}

seastar::future<> handle_sess(snail::net::SessionPtr sess) {
    for (;;) {
        auto s = co_await sess->AcceptStream();
        if (!s.OK()) {
            std::cout << "accept stream error: " << s.Reason() << std::endl;
            break;
        }
        auto stream = s.Value();
        (void)handle_stream(stream);
    }
    std::cout << "close sess" << std::endl;
    co_await sess->Close();
    co_return;
}

seastar::future<> test_server(uint16_t port) {
    seastar::socket_address sa(seastar::ipv4_addr("127.0.0.1", port));

    auto socket = seastar::listen(sa);
    for (;;) {
        auto ar = co_await socket.accept();
        auto conn = snail::net::TcpConnection::make_connection(
            std::move(ar.connection), ar.remote_address);
        snail::net::Option opt;
        opt.keep_alive_disabled = true;
        auto sess = snail::net::Session::make_session(opt, conn, false);
        (void)handle_sess(sess);
    }
    co_return;
}

int main(int argc, char** argv) {
    boost::program_options::options_description desc;
    desc.add_options()("help,h", "show help message");
    desc.add_options()("port", bpo::value<uint16_t>(), "Server port");

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
