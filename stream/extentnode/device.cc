#include "device.h"

#include <spdk/nvme.h>
#include <spdk/nvme_spec.h>
#include <spdk/nvme_zns.h>

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/reactor.hh>
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
    seastar::gate gate_;

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
            ptr->capacity_ = seastar::align_down(ptr->capacity_, kSectorSize);
        } catch (std::exception& e) {
            LOG_ERROR("open disk {} error: {}", name, e.what());
            if (ptr->fp_) {
                co_await ptr->fp_.close();
            }
            co_return seastar::coroutine::return_exception(std::move(e));
        }
        ptr->name_ = name;
        co_return seastar::dynamic_pointer_cast<Device, KernelDevice>(ptr);
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
        if (gate_.is_closed()) {
            s.Set(ENODEV);
            co_return s;
        }
        seastar::gate::holder holder(gate_);
        if ((pos & kSectorSizeMask) || (len & kSectorSizeMask) ||
            (reinterpret_cast<uintptr_t>(b) & kMemoryAlignmentMask)) {
            s.Set(EINVAL);
            co_return s;
        }
        if (pos + len > capacity_) {
            s.Set(ERANGE);
            co_return s;
        }
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
        if (gate_.is_closed()) {
            s.Set(ENODEV);
            co_return s;
        }
        seastar::gate::holder holder(gate_);
        if (pos & kSectorSizeMask) {
            s.Set(EINVAL);
            co_return s;
        }
        int n = iov.size();
        try {
            for (int i = 0; i < n; i++) {
                if (pos + iov[i].iov_len > capacity_) {
                    s.Set(ERANGE);
                    break;
                }
                if ((reinterpret_cast<uintptr_t>(iov[i].iov_base) &
                     kMemoryAlignmentMask) ||
                    (iov[i].iov_len & kSectorSizeMask)) {
                    s.Set(EINVAL);
                    break;
                }
                auto fu = fp_.dma_write(
                    pos, reinterpret_cast<const char*>(iov[i].iov_base),
                    iov[i].iov_len);
                pos += iov[i].iov_len;
                fu_vec.emplace_back(std::move(fu));
            }
            auto res = co_await seastar::when_all_succeed(fu_vec.begin(),
                                                          fu_vec.end());
            for (int i = 0; i < res.size(); i++) {
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
        if (gate_.is_closed()) {
            s.Set(ENODEV);
            co_return s;
        }
        seastar::gate::holder holder(gate_);
        if ((pos & kSectorSizeMask) || (len & kSectorSizeMask) ||
            (reinterpret_cast<uintptr_t>(b) & kMemoryAlignmentMask)) {
            s.Set(EINVAL);
            co_return s;
        }
        if (pos >= capacity_) {
            co_return s;
        } else if (pos + len > capacity_) {
            len = capacity_ - pos;
        }
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

    seastar::future<> Close() override {
        if (!gate_.is_closed()) {
            co_await gate_.close();
            co_await fp_.close();
        }
        co_return;
    }
};

#ifdef HAS_SPDK

struct SpdkDeleter final : public seastar::deleter::impl {
    void* obj;
    SpdkDeleter(void* obj) : impl(seastar::deleter()), obj(obj) {}
    virtual ~SpdkDeleter() override { spdk_free(obj); }
};

static seastar::deleter make_spdk_deleter(void* obj) {
    return seastar::deleter(new SpdkDeleter(obj));
}

class SpdkDevice : public Device {
    struct spdk_env_opts opts_;
    struct spdk_nvme_transport_id trid_;
    std::string name_;
    uint32_t ns_id_ = 0;
    struct spdk_nvme_probe_ctx* probe_ctx_ = nullptr;
    struct spdk_nvme_ctrlr* ctrlr_ = nullptr;
    struct spdk_nvme_ns* ns_ = nullptr;
    std::vector<struct spdk_nvme_qpair*> qpairs_;
    int qpairs_idx_ = 0;

    bool attach_ = false;
    struct spdk_nvme_detach_ctx* detach_ctx_ = nullptr;
    std::optional<seastar::promise<>> detach_pr_;
    size_t capacity_ = 0;
    seastar::gate gate_;
    std::optional<seastar::reactor::poller> poller_;

    struct request {
        SpdkDevice* dev_ptr;
        seastar::promise<int> pr;
        std::string reason;
    };

    static bool probe_cb(void* cb_ctx,
                         const struct spdk_nvme_transport_id* trid,
                         struct spdk_nvme_ctrlr_opts* opts) {
        return true;
    }

    static void attach_cb(void* cb_ctx,
                          const struct spdk_nvme_transport_id* trid,
                          struct spdk_nvme_ctrlr* ctrlr,
                          const struct spdk_nvme_ctrlr_opts* opts) {
        SpdkDevice::request* req =
            reinterpret_cast<SpdkDevice::request*>(cb_ctx);
        req->dev_ptr->attach_ = true;
        for (auto nsid = spdk_nvme_ctrlr_get_first_active_ns(ctrlr); nsid != 0;
             nsid = spdk_nvme_ctrlr_get_next_active_ns(ctrlr, nsid)) {
            if (nsid == req->dev_ptr->ns_id_) {
                auto ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
                if (!spdk_nvme_ns_is_active(ns)) {
                    LOG_ERROR("device {} ns: {} is not active", trid->traddr,
                              nsid);
                    req->pr.set_value(-1);
                    return;
                }
                req->dev_ptr->ctrlr_ = ctrlr;
                req->dev_ptr->ns_ = ns;
                req->dev_ptr->capacity_ =
                    seastar::align_down(spdk_nvme_ns_get_size(ns), kSectorSize);
                req->pr.set_value(0);
                return;
            }
        }
        LOG_WARN("device {} ns {} is not found", trid->traddr,
                 req->dev_ptr->ns_id_);
        req->pr.set_value(-1);
        return;
    }

    bool poll() {
        if (!attach_ && probe_ctx_ &&
            spdk_nvme_probe_poll_async(probe_ctx_) == 0) {
            probe_ctx_ = nullptr;
        }

        for (auto& qpair : qpairs_) {
            spdk_nvme_qpair_process_completions(qpair, 0);
        }

        if (detach_ctx_ && detach_pr_ &&
            spdk_nvme_detach_poll_async(detach_ctx_) == 0) {
            detach_ctx_ = nullptr;
            detach_pr_.value().set_value();
        }
        return true;
    }

   public:
    SpdkDevice() {}
    virtual ~SpdkDevice() {}

    static seastar::future<DevicePtr> Open(const std::string_view name,
                                           uint32_t ns_id, uint32_t qpair_n) {
        if (qpair_n == 0 || qpair_n > 16) {
            qpair_n == 16;
        }
        seastar::shared_ptr<SpdkDevice> dev_ptr =
            seastar::make_shared<SpdkDevice>();
        spdk_nvme_trid_populate_transport(&dev_ptr->trid_,
                                          SPDK_NVME_TRANSPORT_PCIE);
        if (0 != spdk_nvme_transport_id_parse(&dev_ptr->trid_, name.data())) {
            LOG_ERROR("error parsing transport address");
            co_return nullptr;
        }
        dev_ptr->name_ = name;

        std::unique_ptr<SpdkDevice::request> req(new SpdkDevice::request);
        req->dev_ptr = dev_ptr.get();
        dev_ptr->probe_ctx_ = spdk_nvme_probe_async(
            &dev_ptr->trid_, req.get(), SpdkDevice::probe_cb,
            SpdkDevice::attach_cb, NULL);

        dev_ptr->poller_ = seastar::reactor::poller::simple(
            [ptr = dev_ptr.get()]() -> bool { return ptr->poll(); });

        auto res = co_await req->pr.get_future();
        if (res == -1) {
            co_return nullptr;
        }

        for (uint32_t i = 0; i < qpair_n; i++) {
            auto qpair =
                spdk_nvme_ctrlr_alloc_io_qpair(dev_ptr->ctrlr_, NULL, 0);
            if (qpair == NULL) {
                LOG_ERROR("failed to alloc io qpair, i={}", i);
                co_await dev_ptr->Close();
                co_return nullptr;
            }
            dev_ptr->qpairs_.push_back(qpair);
        }

        if (spdk_nvme_ns_get_csi(dev_ptr->ns_) == SPDK_NVME_CSI_ZNS) {
            std::unique_ptr<SpdkDevice::request> ns_req(
                new SpdkDevice::request);
            ns_req->dev_ptr = dev_ptr.get();
            if (spdk_nvme_zns_reset_zone(
                    dev_ptr->ns_, dev_ptr->qpairs_[0], 0, false,
                    [](void* arg, const struct spdk_nvme_cpl* completion) {
                        SpdkDevice::request* r =
                            reinterpret_cast<SpdkDevice::request*>(arg);
                        if (spdk_nvme_cpl_is_error(completion)) {
                            LOG_ERROR("I/O error status: {}",
                                      spdk_nvme_cpl_get_status_string(
                                          &completion->status));
                            r->pr.set_value(-1);
                            return;
                        }
                        r->pr.set_value(0);
                    },
                    ns_req.get())) {
                co_await dev_ptr->Close();
                co_return nullptr;
            }
            auto res = co_await ns_req->pr.get_future();
            if (res == -1) {
                co_await dev_ptr->Close();
                co_return nullptr;
            }
        }

        co_return seastar::dynamic_pointer_cast<Device, SpdkDevice>(dev_ptr);
    }

    const std::string& Name() const { return name_; }

    seastar::temporary_buffer<char> Get(size_t n) {
        void* b = spdk_zmalloc(n, kMemoryAlignment, NULL,
                               SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
        return seastar::temporary_buffer<char>((char*)b, n,
                                               make_spdk_deleter(b));
    }

    seastar::future<Status<>> Write(uint64_t pos, const char* b, size_t len) {
        Status<> s;
        if (gate_.is_closed()) {
            s.Set(ENODEV);
            co_return s;
        }
        seastar::gate::holder holder(gate_);
        std::unique_ptr<request> req(new request);
        req->dev_ptr = this;
        if ((reinterpret_cast<uintptr_t>(b) & kMemoryAlignmentMask) ||
            (len & kSectorSizeMask) || (pos & kSectorSizeMask)) {
            s.Set(EINVAL);
            co_return s;
        }
        if (pos + len > capacity_) {
            s.Set(ERANGE);
            co_return s;
        }
        auto res = spdk_nvme_ns_cmd_write(
            ns_, qpairs_[qpairs_idx_], (void*)b, (pos / kSectorSize),
            len / kSectorSize,
            [](void* arg, const struct spdk_nvme_cpl* completion) {
                SpdkDevice::request* r =
                    reinterpret_cast<SpdkDevice::request*>(arg);
                if (spdk_nvme_cpl_is_error(completion)) {
                    LOG_ERROR(
                        "I/O error status: {}",
                        spdk_nvme_cpl_get_status_string(&completion->status));
                    r->pr.set_value(EIO);
                    return;
                }
                r->pr.set_value((int)ErrCode::OK);
            },
            req.get(), 0);
        qpairs_idx_ = (qpairs_idx_ + 1) % qpairs_.size();
        if (res != 0) {
            s.Set(-res);
            co_return s;
        }

        auto code = co_await req->pr.get_future();
        s.Set(code);
        co_return s;
    }

    seastar::future<Status<>> Write(uint64_t pos, std::vector<iovec> iov) {
        Status<> s;
        if (gate_.is_closed()) {
            s.Set(ENODEV);
            co_return s;
        }
        seastar::gate::holder holder(gate_);
        if (pos & kSectorSizeMask) {
            s.Set(EINVAL);
            co_return s;
        }

        std::vector<std::unique_ptr<SpdkDevice::request>> req_vec;
        std::vector<seastar::future<int>> fu_vec;

        for (int i = 0; i < iov.size(); i++) {
            if (((uint64_t)iov[i].iov_base & kMemoryAlignmentMask) ||
                (iov[i].iov_len & kSectorSizeMask)) {
                s.Set(EINVAL);
                break;
            }
            if (pos + iov[i].iov_len > capacity_) {
                s.Set(ERANGE);
                break;
            }
            std::unique_ptr<SpdkDevice::request> req(new SpdkDevice::request);
            req->dev_ptr = this;
            auto res = spdk_nvme_ns_cmd_write(
                ns_, qpairs_[qpairs_idx_], iov[i].iov_base, (pos / kSectorSize),
                iov[i].iov_len / kSectorSize,
                [](void* arg, const struct spdk_nvme_cpl* cpl) {
                    SpdkDevice::request* r =
                        reinterpret_cast<SpdkDevice::request*>(arg);
                    if (spdk_nvme_cpl_is_error(cpl)) {
                        LOG_ERROR(
                            "I/O error status: {}",
                            spdk_nvme_cpl_get_status_string(&cpl->status));
                        r->pr.set_value(EIO);
                        return;
                    }
                    r->pr.set_value((int)ErrCode::OK);
                },
                req.get(), 0);
            if (res != 0) {
                if (res == -ENXIO) {
                    s.Set(EIO);
                } else {
                    s.Set(-res);
                }
                LOG_ERROR("spdk nvme writev error: {}", s);
                break;
            }
            fu_vec.emplace_back(std::move(req->pr.get_future()));
            req_vec.emplace_back(std::move(req));
            pos += iov[i].iov_len;
        }
        qpairs_idx_ = (qpairs_idx_ + 1) % qpairs_.size();

        if (!fu_vec.empty()) {
            auto results = co_await seastar::when_all_succeed(fu_vec.begin(),
                                                              fu_vec.end());
            for (int i = 0; i < results.size(); i++) {
                if (results[i] && !s) {
                    s.Set(results[i]);
                    break;
                }
            }
        }
        co_return s;
    }

    seastar::future<Status<size_t>> Read(uint64_t pos, char* b, size_t len) {
        Status<size_t> s;
        if (gate_.is_closed()) {
            s.Set(ENODEV);
            co_return s;
        }
        seastar::gate::holder holder(gate_);
        if ((pos & kSectorSizeMask) ||
            (reinterpret_cast<uintptr_t>(b) & kMemoryAlignmentMask) ||
            (len & kSectorSizeMask)) {
            s.Set(EINVAL);
            co_return s;
        }
        if (pos >= capacity_) {
            s.SetValue(0);
            co_return s;
        } else if (pos + len > capacity_) {
            len = capacity_ - pos;
        }
        std::unique_ptr<SpdkDevice::request> req(new SpdkDevice::request);
        req->dev_ptr = this;
        auto res = spdk_nvme_ns_cmd_read(
            ns_, qpairs_[qpairs_idx_], b, pos / kSectorSize, len / kSectorSize,
            [](void* arg, const struct spdk_nvme_cpl* cpl) {
                SpdkDevice::request* r =
                    reinterpret_cast<SpdkDevice::request*>(arg);
                if (spdk_nvme_cpl_is_error(cpl)) {
                    LOG_ERROR("I/O error status: {}",
                              spdk_nvme_cpl_get_status_string(&cpl->status));
                    r->pr.set_value(EIO);
                    return;
                }
                r->pr.set_value((int)ErrCode::OK);
            },
            req.get(), 0);
        qpairs_idx_ = (qpairs_idx_ + 1) % qpairs_.size();
        if (res != 0) {
            s.Set(-res);
            LOG_ERROR("spdk nvme read error: {}", s);
            co_return s;
        }

        auto code = co_await req->pr.get_future();
        s.Set(code);
        if (s) {
            s.SetValue(len);
        }
        co_return s;
    }

    seastar::future<> Close() {
        if (!gate_.is_closed()) {
            co_await gate_.close();
            for (int i = 0; i < qpairs_.size(); ++i) {
                spdk_nvme_ctrlr_free_io_qpair(qpairs_[i]);
            }
            qpairs_.clear();
            if (ctrlr_) {
                spdk_nvme_detach_async(ctrlr_, &detach_ctx_);
                if (detach_ctx_) {
                    detach_pr_ = seastar::promise<>();
                    co_await detach_pr_.value().get_future();
                    detach_pr_.reset();
                }
            }
        }
        co_return;
    }

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

seastar::future<DevicePtr> OpenSpdkDevice(const std::string_view name,
                                          uint32_t ns_id, uint32_t qpair_n) {
#if HAS_SPDK
    auto ptr = co_await SpdkDevice::Open(name, ns_id, qpair_n);
    co_return ptr;
#else
    LOG_ERROR("not support spdk device");
    co_return nullptr;
#endif
}

}  // namespace stream
}  // namespace snail
