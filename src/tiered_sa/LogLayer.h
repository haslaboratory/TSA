#pragma once

#include <cstdint>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <atomic>
#include <unordered_set>

#include "common/Device.h"
#include "common/Types.h"
#include "tiered_sa/FlushCache.h"
#include "tiered_sa/LogWriter.h"
#include "kangaroo/ChainedLogIndex.h"
#include "tiered_sa/LogIndex.h"

class LogLayer {
public:
    struct Config {
        uint32_t page_size{0};
        uint32_t zone_size{0};
        uint32_t slice_num{0};
        uint32_t bucket_per_slice{0};
        uint64_t log_start_off{0};
        uint64_t log_zone_num{0};

        double lop_op{0.05};
        uint64_t set_num{0};
        Device *device{nullptr};

        SetNumberCallback set_number_callback{};

        Config &validate();
    };

    struct PerfCounter {
        std::atomic<uint64_t> logPageWritten{0};
    };

    explicit LogLayer(Config &&config);

    ~LogLayer();

    LogLayer(const LogLayer &) = delete;
    LogLayer &operator=(const LogLayer &) = delete;

    // 用户读
    Status lookup(HashedKey hk, Buffer *value);
    // 用户 buffer 写
    Status insert(HashedKey hk, BufferView value);

    Status remove(HashedKey hk);

    Status prefill(
        std::function<uint64_t ()> k_func,
        std::function<BufferView ()> v_func,
        std::unordered_set<HashedKey> *refs
    );
    // flush cache 读
    // Status cache_lookup(HashedKey hk, uint64_t page_offset, Buffer *value);
    // page 读
    Status read_page(uint64_t page_offset, Buffer *value);
    // flush readmit，page单位
    Status readmit(uint64_t page_offset, Buffer buf);
    // prepare clean
    Status prepare_clean_seg(uint64_t page_num, std::vector<uint64_t> *pages);
    // clean ok
    Status complete_clean_seg(uint64_t page_num);
    // get obj
    std::unordered_map<uint64_t, std::unique_ptr<ObjectInfo>> 
        get_objects_to_move(KangarooBucketId bid);
    // check migration
    bool check_migrate();
    // dec wait op
    void dec_wait_op();

    inline void dump_cache_stats() {
        flush_cache_->dump_stats();
    }

    PerfCounter &perf_counter() {
        return perf_;
    }
private:
    struct ValidConfigTag {};

    class PageWriterAgent {
    public:
        // 写 page
        uint64_t write(Buffer buffer, LogLayer *layer, bool force = false);
        // 获取当前 clean 指针
        Status load_clean_seg(LogLayer *layer, uint64_t *page_idx);
        // 移动 clean 指针
        Status complete_clean_seg(LogLayer *layer, uint64_t page_num);
        // 是否需要 migrate
        bool check_migrate(LogLayer *layer);
        
        bool check_prefill_done(LogLayer *layer);
    private:
        bool check_write_wait(LogLayer *layer);

        uint64_t write_page_id_{0};
        uint64_t clean_page_id_{0};
    };
    friend PageWriterAgent;

    LogLayer(Config &&config, ValidConfigTag);
    // 计算 slice id
    uint32_t get_slice_id(HashedKey hk);
    // 计算 page offset
    uint64_t page_idx_to_offset(uint64_t page_idx);
    uint64_t page_offset_to_idx(uint64_t page_offset);
    // 计算PartitionOffset
    PartitionOffset page_offset_to_partition(uint64_t page_offset);
    uint64_t partition_offset_to_page(PartitionOffset partition);

    // 工具函数，用于比较延迟
    Status page_lookup(uint64_t page_offset, uint8_t ver, std::function<Status(Buffer *)> read_cb);

    uint32_t page_size_{0};
    uint32_t slice_num_{0};
    uint64_t bucket_per_slice_{0};
    uint64_t log_start_off_{0};
    uint64_t log_zone_num_{0};
    uint64_t pages_per_zone_{0};
    double log_wait_op_{1.0};
    double log_threshhold{0.15};

    Device &device_;
    const SetNumberCallback set_number_cb_;

    std::unique_ptr<std::shared_mutex[]> slice_mutexs_;

    std::shared_mutex meta_mutex_;
    std::condition_variable_any clean_cv_;

    std::unique_ptr<FlushCache> flush_cache_{nullptr};
    std::unique_ptr<LogWriter> log_writer_{nullptr};
    PageBit page_bit_;
    
    ChainedLogIndex **index_;

    PageWriterAgent w_agent_;

    PerfCounter perf_;
};