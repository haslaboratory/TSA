#pragma once

#include <thread>
#include <atomic>

#include "common/Cache.h"
#include "kangaroo/FwLog.h"

class RawLog: public CacheEngine {
public:
    struct Config {
        uint64_t base_offset{0};
        uint64_t zone_capacity{0};
        uint64_t page_size{4096};
        uint64_t zone_num{0};

        uint64_t gc_thread_num{10};

        Device *device;
    };

    struct PerfCounter {
        std::atomic<bool> warm_ok{true};
    };
    explicit RawLog(Config &&config);

    ~RawLog();

    Status lookup(HashedKey hk, Buffer *value);

    Status insert(HashedKey hk, BufferView value);

    Status remove(HashedKey hk);

    Status prefill(
        std::function<uint64_t ()> k_func,
        std::function<BufferView ()> v_func
    );

    PerfCounter &perf_counter();

private:
    void clean_segment();
    void gc_loop();
    void gc_wait_loop();

    struct ValidConfigTag {};
    RawLog(Config &&config, ValidConfigTag);

    // layers
    std::unique_ptr<FwLog> log_;
    // threads
    std::vector<std::thread> gc_threads_;
    // shared meta
    bool stop_flag_ = false;
    std::mutex gc_sync_;
    std::condition_variable gc_sync_cond_;
    bool gc_flag_ = false;
    uint64_t gc_sync_threads_{0};

    // config
    uint64_t gc_thread_num_{0};

    PerfCounter perf_;
};