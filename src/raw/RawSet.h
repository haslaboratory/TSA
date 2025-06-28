#pragma once

#include <thread>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <shared_mutex>
#include <vector>

#include "common/Cache.h"
#include "common/BloomFilter.h"
#include "kangaroo/RripBucket.h"
#include "kangaroo/Wren.h"

class RawSet: public CacheEngine {
public:
    struct Config {
        uint64_t base_offset{0};
        uint64_t zone_capacity{0};
        uint64_t page_size{4096};
        uint64_t zone_num{0};
        double op{0.07};

        uint32_t gc_thread_num{10};

        Device *device{nullptr};

        bool enable_bf{false};
        uint32_t bf_num_hashes{0};
        uint32_t bf_len_hash_bits{0};

        DestructorCallback destructor_cb{};
    };

    struct PerfCounter {
        std::atomic<uint64_t> objInserted{0};
        std::atomic<uint64_t> pageWritten{0};
        std::atomic<uint64_t> gcWritten{0};
        std::atomic<bool> warm_ok{false};
    };

    RawSet(const RawSet &) = delete;
    RawSet &operator=(const RawSet &) = delete;

    explicit RawSet(Config &&config);

    ~RawSet();

    Status lookup(HashedKey hk, Buffer *value);

    Status insert(HashedKey hk, BufferView value);

    Status remove(HashedKey hk);

    Status prefill(
        std::function<uint64_t ()> k_func,
        std::function<BufferView ()> v_func
    );

    PerfCounter &perf_counter();

private:
    struct ValidConfigTag {};
    RawSet(Config &&config, ValidConfigTag);

    // tool func
    void bf_rebuild(uint32_t bid, const RripBucket *bucket);
    void bf_build(uint32_t bid, const RripBucket *bucket);
    bool bf_reject(uint32_t bid, uint64_t key_hash) const;
    Buffer read_bucket(KangarooBucketId bid);
    bool write_bucket(KangarooBucketId bid, Buffer buffer);

    // back ground func
    void perform_gc();
    void gc_loop();
    void gc_wait_loop();

    uint32_t get_bid(HashedKey hk);

    // shared meta
    std::mutex write_sync_;
    std::condition_variable write_sync_cond_;
    std::mutex gc_sync_;
    std::condition_variable gc_sync_cond_;
    bool gc_flag_ = false;
    uint64_t gc_sync_threads_{0};
    bool stop_flag_ = false;
    Wren::EuIterator eu_iterator_;

    // threads
    std::vector<std::thread> gc_threads_;

    // meta
    uint64_t base_offset_{0};
    uint64_t zone_capacity_{0};
    uint64_t page_size_{4096};
    uint64_t pages_per_zone_{0};
    uint64_t zone_num_{0};
    double op_{0.05};
    uint64_t bucket_num_{0};
    uint64_t gc_thread_num_{0};

    const uint64_t MUTEX_PART = 4096;
    std::unique_ptr<std::shared_mutex[]> bucket_mutex_;
    std::unique_ptr<Wren> wren_;
    std::unique_ptr<BloomFilter> bloom_filter_;

    Device *device_;

    PerfCounter perf_;
};