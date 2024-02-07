#include "device.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/when_all.hh>

#include "types.h"
#include "util/logger.h"

namespace snail {
namespace stream {

class KernelDevice : public Device {
    std::string name_;
    seastar::file fp_;
    size_t capacity_;

   public:
    KernelDevice() {}

    virtual ~KernelDevice() {}

    static seastar::future<DevicePtr> Open(const std::string_view name) {
        seastar::shared_ptr<KernelDevice> ptr =
            seastar::make_shared<KernelDevice>();
        try {
            ptr->fp_ =
                co_await seastar::open_file_dma(name, seastar::open_flags::rw);
            ptr->capacity_ = co_await ptr->fp_.size();
        } catch (std::exception& e) {
            LOG_ERROR("open disk {} error: {}", name, e.what());
            if (ptr->fp_) {
                co_await ptr->fp_.close();
            }
            co_return seastar::coroutine::return_exception(std::move(e));
        }
        ptr->name_ = name;
        co_return dynamic_pointer_cast<Device, KernelDevice>(ptr);
    }

    const std::string& Name() const override { return name_; }

    size_t Capacity() const override { return capacity_; }

    seastar::temporary_buffer<char> Get(size_t n) override {
        size_t len = std::max(n, kMemoryAlignment);
        auto buf =
            seastar::temporary_buffer<char>::aligned(kMemoryAlignment, len);
        return std::move(buf.share(0, n));
    }

    seastar::future<Status<>> Write(uint64_t pos, const char* b,
                                    size_t len) override {
        Status<> s;
        try {
            auto n = co_await fp_.dma_write(pos, b, len);
            if (n != len) {
                s.Set(ErrCode::ErrUnExpect, "return unexpect bytes");
            }
        } catch (std::system_error& e) {
            s.Set(e.code().value());
        } catch (std::exception& e) {
            s.Set(ErrCode::ErrUnExpect, e.what());
        }
        co_return s;
    }

    seastar::future<Status<>> Write(uint64_t pos,
                                    std::vector<iovec> iov) override {
        Status<> s;
        std::vector<seastar::future<size_t>> fu_vec;
        try {
            int n = iov.size();
            for (int i = 0; i < n; i++) {
                auto fu = fp_.dma_write(
                    pos, reinterpret_cast<const char*>(iov[i].iov_base),
                    iov[i].iov_len);
                pos += iov[i].iov_len;
                fu_vec.emplace_back(std::move(fu));
            }
            auto res = co_await seastar::when_all_succeed(fu_vec.begin(),
                                                          fu_vec.end());
            for (int i = 0; i < n; i++) {
                if (res[i] != iov[i].iov_len) {
                    s.Set(ErrCode::ErrUnExpect, "return unexpect bytes");
                    break;
                }
            }
        } catch (std::system_error& e) {
            s.Set(e.code().value());
        } catch (std::exception& e) {
            s.Set(ErrCode::ErrUnExpect, e.what());
        }
        co_return s;
    }

    seastar::future<Status<size_t>> Read(uint64_t pos, char* b,
                                         size_t len) override {
        Status<size_t> s;
        s.SetValue(0);
        try {
            size_t n = co_await fp_.dma_read<char>(pos, b, len);
            s.SetValue(n);
        } catch (std::system_error& e) {
            s.Set(e.code().value());
        } catch (std::exception& e) {
            s.Set(ErrCode::ErrUnExpect, e.what());
        }
        co_return s;
    }

    seastar::future<Status<size_t>> Read(uint64_t pos,
                                         std::vector<iovec> iov) override {
        Status<size_t> s;
        try {
            size_t n = co_await fp_.dma_read(pos, iov);
            s.SetValue(n);
        } catch (std::system_error& e) {
            s.Set(e.code().value());
        } catch (std::exception& e) {
            s.Set(ErrCode::ErrUnExpect, e.what());
        }
        co_return s;
    }

    seastar::future<> Close() override { return fp_.close(); }
};

#ifdef HAS_SPDK

class SpdkDevice : public Device, public enable_shared_from_this<SpdkDevice> {
    struct spdk_env_opts opts_;
    struct spdk_nvme_transport_id trid_;
    uint32_t ns_id_ = 0;
    struct spdk_nvme_probe_ctx* probe_ctx_ = nullptr;
    struct spdk_nvme_ctrlr* ctrlr_ = nullptr;
    struct spdk_nvme_ns* ns_ = nullptr;
    std::vector<struct spdk_nvme_qpair*> qpairs_;
    int qpairs_idx = 0;

    seastar::reactor::poller poller_;

    bool attach_ = false;
    bool stop_ = false;
    size_t capacity_ = 0;
    uint32_t sector_size_ = 512;

    struct request {
        seastar::shared_ptr<SpdkDevice> dev_ptr;
        seastar::promise<int> pr;
    };

    static bool probe_cb(void* cb_ctx,
                         const struct spdk_nvme_transport_id* trid,
                         struct spdk_nvme_ctrlr_opts* opts) {}

    static bool attach_cb(void* cb_ctx,
                          const struct spdk_nvme_transport_id* trid,
                          struct spdk_nvme_ctrlr* ctrlr,
                          struct spdk_nvme_ctrlr_opts* opts) {
        SpdkDevice::request* req =
            reinterpret_cast<SpdkDevice::request*>(cb_ctx);
        req->dev_ptr->attach_ = true;
        for (auto nsid = spdk_nvme_ctrlr_get_first_active_ns(ctrlr); nsid != 0;
             nsid = spdk_nvme_ctrlr_get_next_active_ns(ctrlr, nsid)) {
            if nsid
                == req->dev_ptr->ns_id_ {
                    auto ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
                    if (!spdk_nvme_ns_is_active(ns)) {
                        LOG_ERROR("device {} ns: {} is not active",
                                  trid->traddr, nsid)
                        req->pr.set_value(-1);
                        return
                    }
                    req->dev_ptr->ctrlr_ = ctrlr;
                    req->dev_ptr->ns_ = ns;
                    req->dev_ptr->capacity_ = spdk_nvme_ns_get_size(ns);
                    req->dev_ptr->sector_size_ =
                        spdk_nvme_ns_get_sector_size(ns);
                    req->pr.set_value(0);
                    return;
                }
        }

        req->pr.set_value(-1);
    }

    bool poll() {
        if (stop_) {
            return false;
        }

        if (!attach_) {
            spdk_nvme_probe_poll_async(probe_ctx_);
        }

        for (auto& qpair : qpairs_) {
            spdk_nvme_qpair_process_completions(qpair, 0);
        }
        return true;
    }

   public:
    static seastar::future<DevicePtr> Open(const std::string_view name,
                                           uint32_t ns_id, uint32_t qpair_n) {
        if (qpair_n == 0 || qpair_n > 16) {
            qpair_n == 16;
        }
        seastar::shared_ptr<SpdkDevice> ptr =
            seastar::make_shared<SpdkDevice>();
        spdk_nvme_trid_populate_transport(&ptr->trid_,
                                          SPDK_NVME_TRANSPORT_PCIE);
        if (0 != spdk_nvme_transport_id_parse(&ptr->trid_, name.c_str())) {
            LOG_ERROR("error parsing transport address");
            co_return nullptr;
        }

        SpdkDevice::request* req = new SpdkDevice::request;
        req->dev_ptr = ptr;
        ptr->probe_ctx_ =
            spdk_nvme_probe_async(&ptr->trid_, req, SpdkDevice::probe_cb,
                                  SpdkDevice::attach_cb, NULL);

        ptr->poller_ =
            seastar::reactor::poller::simple([ptr]() { ptr->poll(); });
        auto res = co_await req->pr.get_future();
        delete req;
        if (res == -1) {
            ptr->stop_ = true;
            co_return nullptr
        }

        for (uint32_t i = 0; i < qpair_n; i++) {
            auto qpair = spdk_nvme_ctrlr_alloc_io_qpair(ptr->ctrlr_, NULL, 0);
            if (qpair == NULL) {
                ptr->stop_ = true;
                co_return nullptr
            }
            ptr->qpairs_.push_back(qpair);
        }

        if (spdk_nvme_ns_get_csi(ptr->ns_) == SPDK_NVME_CSI_ZNS) {
            req = new SpdkDevice::request;
            req->dev_ptr = ptr;
            if (spdk_nvme_zns_reset_zone(
                    ptr->ns_, ptr->qpairs_[0], 0, false,
                    [](void* arg, const struct spdk_nvme_cpl* completion) {
                        SpdkDevice::request* r =
                            reinterpret_cast<SpdkDevice::request*>(arg);
                        if (spdk_nvme_cpl_is_error(completion)) {
                            LOG_ERROR("I/O error status: {}",
                                      spdk_nvme_cpl_get_status_string(
                                          &completion->status));
                            r->pr.set_value(-1);
                            return
                        }
                        r->pr.set_value(0);
                    },
                    req)) {
                delete req;
                ptr->stop_ = true;
                co_return nullptr
            }
            auto res = co_await req->pr.get_future();
            delete req;
            if (res == -1) {
                ptr->stop_ = true;
                co_return nullptr
            }
        }

        co_return seastar::dynamic_pointer_cast<Device, SpdkDevice>(ptr);
    }

    seastar::temporary_buffer<char> Get(size_t n) {}

    seastar::future<Status<>> Write(uint64_t pos, const char* b, size_t len) {
        if ((pos % sector_size_) != 0 || (len % sector_size_) != 0) {
            co_return Status<>(ErrCode::ErrInvalidParameter);
        }

        auto req = new SpdkDevice::request;
        req->dev_ptr = seastar::shared_from_this();
        auto res = spdk_nvme_ns_cmd_write(
            ns_, qpairs_[qpairs_idx_], b, (pos / sector_size_),
            len / sector_size_,
            [](void* arg, const struct spdk_nvme_cpl* completion) {
                SpdkDevice::request* r =
                    reinterpret_cast<SpdkDevice::request*>(arg);
                if (spdk_nvme_cpl_is_error(completion)) {
                    LOG_ERROR(
                        "I/O error status: {}",
                        spdk_nvme_cpl_get_status_string(&completion->status));
                    r->pr.set_value((int)ErrCode::ErrDisk);
                    return;
                }
                r->pr.set_value((int)ErrCode::OK);
            },
            req, 0);
        qpairs_idx = (qpairs_idx + 1) % qpairs_.size();
        if (res != 0) {
            delete req;
            if (res == ENXIO) {
                co_return Status<>(ErrCode::ErrDisk);
            }
            co_return Status<>(ErrCode::ErrSystem, strerror(res));
        }

        auto code = co_await req->pr.get_future();
        return Status<>((ErrCode)code);
    }

    seastar::future<Status<>> Write(uint64_t pos, std::vector<iovec> iov) {
        if ((pos % sector_size_) != 0) {
            co_return Status<>(ErrCode::ErrInvalidParameter);
        }

        size_t len = 0;
        for (int i = 0; i < iov.size(); i++) {
            if (((uint64_t)iov[i].iov_base % kMemoryAlignment) != 0 ||
                (iov[i].iov_len % sector_size_) != 0) {
                co_return Status<>(ErrCode::ErrInvalidParameter);
            }
            len += iov[i].iov_len;
        }

        auto req = new SpdkDevice::request;
        req->dev_ptr = seastar::shared_from_this();

        auto res = spdk_nvme_ns_cmd_writev(
            ns_, qpairs_[qpairs_idx], (pos / sector_size_),
            (len / sector_size_),
            [](void* ctx, const struct spdk_nvme_cpl* cpl) {}, req, 0,
            [](void* cb_arg, uint32_t offset) {},
            [](void* cb_arg, void** address, uint32_t* length) {});
        if (res != 0) {
            delete req;
            if (res == ENXIO) {
                co_return Status<>(ErrCode::ErrDisk);
            }
            co_return Status<>(ErrCode::ErrSystem, strerror(res));
        }
    }

    seastar::future<Status<size_t>> Read(uint64_t pos, char* b, size_t len) = 0;

    seastar::future<Status<size_t>> Read(uint64_t pos,
                                         std::vector<iovec> iov) = 0;

    seastar::future<> Close() = 0;

    size_t Capacity() const { return capacity_; }
};

#endif

seastar::future<DevicePtr> OpenKernelDevice(const std::string_view name) {
    DevicePtr ptr;
    try {
        ptr = co_await KernelDevice::Open(name);
    } catch (...) {
        co_return nullptr;
    }
    co_return ptr;
}

seastar::future<DevicePtr> OpenSpdkDevice(const std::string_view name) {
    co_return nullptr;
}

}  // namespace stream
}  // namespace snail
