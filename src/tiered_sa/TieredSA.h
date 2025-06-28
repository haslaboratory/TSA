#pragma once

#include <atomic>
#include <thread>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <unordered_set>

#include "common/Cache.h"
#include "common/BloomFilter.h"
#include "kangaroo/Wren.h"
#include "kangaroo/RripBucket.h"
#include "kangaroo/RripBitVector.h"
#include "tiered_sa/LogLayer.h"

class TieredSA: public CacheEngine {
public:
    enum SetLayerMode {
        FW,
        CROSSOVER,
    };

    struct Config {
        SetLayerMode set_mode{SetLayerMode::FW};

        uint64_t base_offset{0};
        uint64_t zone_capacity{0};
        uint64_t page_size{0};
        uint64_t log_zone_num{0};
        uint64_t set_zone_num{0};
        double log_op{0.05};
        double set_op{0.05};

        uint32_t migrate_thread_num{10};
        
        // bf
        bool enable_bf{false};
        uint32_t bf_num_hashes{0};
        uint32_t bf_len_hash_bits{0};
        // bv
        uint32_t bv_vector_size{0};

        Device *device;

        DestructorCallback destructor_cb{};

        Config &validate();
    };

    struct PerfCounter {
        std::atomic<uint64_t> logObjAbsorbed{0};
        std::atomic<uint64_t> setPageWritten{0};
        std::atomic<uint64_t> gcWritten{0};

        std::atomic<uint64_t> time_log_read{0};
        std::atomic<uint64_t> time_set_read{0};
        std::atomic<uint64_t> time_set_write{0};

        std::atomic<uint64_t> logReadmitted{0};

        std::atomic<bool> warm_ok{false};
    };

    struct ThreadLocalCounter {
        uint64_t time_log_read{0};
        uint64_t time_set_read{0};
        uint64_t time_set_write{0};
    };

    TieredSA() = delete;
    explicit TieredSA(Config &&config);
    ~TieredSA();

    TieredSA(const TieredSA &) = delete;
    TieredSA &operator=(const TieredSA &) = delete;

    Status lookup(HashedKey hk, Buffer *value);

    Status insert(HashedKey hk, BufferView value);

    Status remove(HashedKey hk);

    Status prefill(
        std::function<uint64_t ()> k_func,
        std::function<BufferView ()> v_func
    );

    Status prefill_inner(
        std::function<uint64_t ()> k_func,
        std::function<BufferView ()> v_func,
        std::unordered_set<HashedKey> *refs = nullptr
    );

    PerfCounter &perf_counter();

    LogLayer::PerfCounter &log_perf_counter();

private:
    struct ValidConfigTag {};
    TieredSA(Config &&config, ValidConfigTag);
    void init_fw(Config &&config);
    void init_x(Config &&config);

    // fairywren like func
    void move_bucket_fw(uint32_t bid, int gc_mode, ThreadLocalCounter *thc);
    void rediv_bucket_fw(RripBucket *hot_bucket, RripBucket *cold_bucket);
    // crossover func
    void move_middle_bucket_x(
        uint32_t bid,
        uint32_t bottom_bid,
        std::vector<std::unique_ptr<ObjectInfo>> *of_ois,
        ThreadLocalCounter *thc
    );
    void move_bottom_bucket_x(
        uint32_t bid,
        std::vector<std::unique_ptr<ObjectInfo>> of_ois,
        ThreadLocalCounter *thc
    );
    void rediv_bucket_x(
        RripBucket *bucket,
        uint32_t bottom_bid,
        std::vector<std::unique_ptr<ObjectInfo>> *of_ois,
        uint32_t cnt
    );
    
    // tool func
    uint32_t get_middle_bid(HashedKey hk) const;
    uint32_t get_bottom_bid(HashedKey hk) const;
    bool check_in_bottom_bid_x(HashedKey hk, uint32_t bottom_bid) const;
    uint32_t get_mutex_from_middle_bid(uint32_t middle_bid) const;
    uint32_t get_mutex_from_bottom_bid(uint32_t middle_bid) const;

    Buffer read_bucket(uint32_t bid, bool bottom);
    bool write_bucket(uint32_t bid, Buffer buffer, bool bottom);
    bool write_bucket_erase(uint32_t bid, Buffer buffer, bool bottom);
    bool bv_get_hit(uint32_t bid, uint32_t key_idx, RripBitVector *bit_vector) const;
    void bv_set_hit(uint32_t bid, uint32_t key_idx, RripBitVector *bit_vector) const;

    void bf_rebuild(uint32_t bid, const RripBucket *bucket, BloomFilter *bloom_filter);
    void bf_build(uint32_t bid, const RripBucket *bucket, BloomFilter *bloom_filter);
    bool bf_reject(uint32_t bid, uint64_t key_hash, BloomFilter *bloom_filter) const;

    // backround
    void migrate_log_slice_fw(uint64_t tid);
    void do_migrate_fw();
    void migrate_wait_loop_fw(uint64_t tid);
    void migrate_loop_fw();
    void migrate_log_slice_x(uint64_t tid);
    void do_migrate_x();
    void migrate_wait_loop_x(uint64_t tid);
    void migrate_loop_x();

    // layers
    std::unique_ptr<LogLayer> log_;
    std::unique_ptr<Wren> wren_;
    std::unique_ptr<Wren> bottom_wren_;
    std::unique_ptr<RripBitVector> bit_vector_;
    std::unique_ptr<RripBitVector> bottom_bit_vector_;
    
    // bllom filters
    std::unique_ptr<BloomFilter> bloom_filter_;
    std::unique_ptr<BloomFilter> bottom_bloom_filter_;
   
    // threads
    std::vector<std::thread> migrate_threads_;
    // shared meta
    volatile uint64_t mig_slice_id_ = 0;
    volatile uint64_t cycle_cnt_ = 0;
    volatile uint64_t mig_bid_gen_ = 0;
    volatile uint64_t mig_sync_threads_ = 0;
    std::mutex mig_sync_;
    std::condition_variable mig_sync_cond_;
    bool stop_flag_ = false;

    // config
    std::chrono::nanoseconds gen_time_{};

    SetLayerMode set_mode_{SetLayerMode::FW};
    double log_op_{0.05};
    double set_op_{0.05};
    uint64_t base_offset_{0};
    uint64_t zone_capacity_{0};
    uint64_t page_size_{4096};
    uint64_t pages_per_zone_{0};
    uint64_t log_zone_num_{0};
    uint64_t set_zone_num_{0};
    uint64_t middle_zone_num_{0};
    uint64_t bottom_zone_num_{0};
    uint64_t log_slice_num_{0};
    uint64_t middle_per_slice_{0};
    uint64_t middle_num_buckets_{0};
    uint64_t bottom_num_buckets_{0};
    uint32_t migrate_thread_num_{0};
    uint32_t set_per_thread_{0};
    const uint64_t MIDDLE_FACTOR = 2;
    const uint64_t BOTTOM_FACTOR = 8;
    Device &device_;

    std::unique_ptr<std::shared_mutex[]> mutex_{nullptr};
    PerfCounter perf_;
};