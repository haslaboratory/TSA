#pragma once

#include <memory>
#include <condition_variable>
#include <thread>

#include "common/Cache.h"
#include "raw/LogIndex.h"

class SwapLog: public CacheEngine {
public:
    struct Config {
        uint32_t page_size{4096};
        uint32_t segment_size{0};
        uint32_t zone_capacity{0};
        uint32_t data_zone_num{0};
        uint32_t swap_zone_num{0};
        uint32_t write_part_num{0};
        uint64_t log_start_off{0};
        uint64_t swap_start_off{0};

        Device *device{nullptr};
    };

    struct PerfCounter {
        std::atomic<bool> warm_ok{true};
    };

    SwapLog() = delete;

    SwapLog(Config &&config);

    ~SwapLog();

    Status lookup(HashedKey, Buffer *value);

    Status insert(HashedKey hk, BufferView value);

    Status remove(HashedKey hk);

    Status prefill(
        std::function<uint64_t()> k_func,
        std::function<BufferView()> v_func
    );

    PerfCounter &perf_counter();

private:
    // tool functions
    Status lookup_write_buffer(HashedKey hk, Buffer *value, uint32_t lpid);
    bool flush_log_segment(uint32_t index_part, bool wait);
    bool write_log_segment(LogSegmentId lsid, Buffer buffer);
    Buffer read_log_page(LogPageId lpid);
    Buffer read_log_segment(LogSegmentId lsid);
    LogSegmentId get_next_lsid(LogSegmentId lsid, uint32_t increment);
    // gc / cleaning
    bool should_clean(double threshold);
    void start_clean();
    bool finish_clean();
    bool get_next_cleaning_key(HashedKey *hk, LogPageId *lpid);
    void clean_segment();
    void gc_loop();
    void gc_wait_loop();
    // index
    std::unique_ptr<LogIndex> log_index_;

    // buffer
    const uint32_t WRITE_BUFFER_CNT = 2;
    std::unique_ptr<Buffer[]> write_buffers_;
    std::unique_ptr<std::shared_mutex[]> write_mutexes_;
    std::unique_ptr<FwLogSegment *[]> write_segments_;
    uint32_t write_buffer_offset_{0}; 

    // flush meta
    std::shared_mutex buffer_meta_mutex_;
    std::shared_mutex flush_clean_mutex_;
    std::condition_variable_any flush_clean_cv_;
    // cleaning
    LogSegmentId next_lsid_to_clean_;
    bool write_empty_{true};
    Buffer cleaning_buffer_;
    std::unique_ptr<FwLogSegment> cleaning_segment_ = nullptr;
    std::unique_ptr<FwLogSegment::Iterator> cleaning_segment_it_ = nullptr;
    // gc threads meta
    std::vector<std::thread> gc_threads_;
    std::mutex gc_sync_;
    std::condition_variable gc_sync_cond_;
    bool gc_flag_ = false;
    uint64_t gc_sync_threads_{0};
    // stop meta
    bool stop_flag_ = false;

    // config
    uint32_t page_size_{4096};
    uint32_t zone_capacity_{0};
    uint32_t data_zone_num_{0};
    uint32_t swap_zone_num_{0};
    uint32_t write_part_num_{0};
    uint64_t log_start_off_{0};
    uint32_t segment_size_{0};

    uint32_t pages_per_zone_{0};
    uint32_t pages_per_segment_{0};
    uint32_t segments_per_zone_{0};

    uint32_t gc_thread_num_{10};

    Device *device_{nullptr};

    PerfCounter perf_;
};