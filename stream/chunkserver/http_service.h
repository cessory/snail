#pragma once
#include <seastar/http/handlers.hh>

#include "disk.h"
#include "lease_mgr.h"

namespace snail {
namespace stream {

class HttpWriteHandler : public seastar::httpd::handler_base {
    DiskPtr disk_ptr_;
    LeaseMgrPtr lease_mgr_ptr_;

   public:
    HttpWriteHandler(DiskPtr disk_ptr, LeaseMgrPtr lease_mgr_ptr)
        : disk_ptr_(disk_ptr), lease_mgr_ptr_(lease_mgr_ptr) {}

    seastar::future<std::unique_ptr<seastar::http::reply>> handle(
        const seastar::sstring& path,
        std::unique_ptr<seastar::http::request> req,
        std::unique_ptr<seastar::http::reply> rep) override;

    virtual ~HttpWriteHandler() {}

   private:
    void Reply(std::unique_ptr<seastar::http::reply>& rep, size_t offset);
};

class HttpReadHandler : public seastar::httpd::handler_base {
    DiskPtr disk_ptr_;

   public:
    HttpReadHandler(DiskPtr disk_ptr) : disk_ptr_(disk_ptr) {}

    seastar::future<std::unique_ptr<seastar::http::reply>> handle(
        const seastar::sstring& path,
        std::unique_ptr<seastar::http::request> req,
        std::unique_ptr<seastar::http::reply> rep) override;

    virtual ~HttpReadHandler() {}
};

class HttpCreateHandler : public seastar::httpd::handler_base {
    DiskPtr disk_ptr_;
    LeaseMgrPtr lease_mgr_ptr_;

   public:
    HttpCreateHandler(DiskPtr disk_ptr, LeaseMgrPtr lease_mgr_ptr)
        : disk_ptr_(disk_ptr), lease_mgr_ptr_(lease_mgr_ptr) {}

    virtual ~HttpCreateHandler() {}

    seastar::future<std::unique_ptr<seastar::http::reply>> handle(
        const seastar::sstring& path,
        std::unique_ptr<seastar::http::request> req,
        std::unique_ptr<seastar::http::reply> rep) override;
};

class HttpRemoveHandler : public seastar::httpd::handler_base {
    DiskPtr disk_ptr_;
    LeaseMgrPtr lease_mgr_ptr_;

   public:
    HttpRemoveHandler(DiskPtr disk_ptr, LeaseMgrPtr lease_mgr_ptr)
        : disk_ptr_(disk_ptr), lease_mgr_ptr_(lease_mgr_ptr) {}

    virtual ~HttpRemoveHandler() {}

    seastar::future<std::unique_ptr<seastar::http::reply>> handle(
        const seastar::sstring& path,
        std::unique_ptr<seastar::http::request> req,
        std::unique_ptr<seastar::http::reply> rep) override;
};

}  // namespace stream
}  // namespace snail
