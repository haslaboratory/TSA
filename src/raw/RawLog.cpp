#include <unistd.h>
#include <unordered_set>

#include "raw/RawLog.h"

RawLog::RawLog(Config &&config): RawLog(std::move(config), ValidConfigTag{}) {}

RawLog::RawLog(Config &&config, ValidConfigTag)
    : gc_thread_num_(config.gc_thread_num)
{
    uint64_t valid_page_num = config.zone_capacity / config.page_size * config.zone_num * 0.95;
    valid_page_num = valid_page_num / (8 * 256) * (8 * 256);
    
    FwLog::Config fw_config {
        .logBaseOffset = config.base_offset,
        .logSize = config.zone_capacity * config.zone_num,
        .device = config.device,
        .logPhysicalPartitions = 64,
        .logIndexPartitions = 8 * 256,
        .sizeAllocations = 2048,
        .numTotalIndexBuckets = valid_page_num,
        .setNumberCallback = [valid_page_num](HashedKey hk) {
            return KangarooBucketId(hk % valid_page_num);
        },
        .threshold = 0,
        .flushGranularity = config.zone_capacity
    };

    log_ = std::make_unique<FwLog>(std::move(fw_config));

    gc_threads_.emplace_back(std::thread(&RawLog::gc_loop, this));
    for (uint32_t i = 1; i < gc_thread_num_; i++) {
        gc_threads_.emplace_back(std::thread(&RawLog::gc_wait_loop, this));
    }
}

RawLog::~RawLog() {
    stop_flag_ = true;
    for (auto &t : gc_threads_) {
        t.join();
    }
}

Status RawLog::lookup(HashedKey hk, Buffer *value) {
    return log_->lookup(hk, value);
}

Status RawLog::insert(HashedKey hk, BufferView value) {
    return log_->insert(hk, value);
}

Status RawLog::remove(HashedKey hk) {
    return log_->remove(hk);
}

Status RawLog::prefill(
    std::function<uint64_t ()> k_func,
    std::function<BufferView ()> v_func
) {
    std::unordered_set<uint64_t> inserted_keys;
    Status status = Status::Ok;
    while (!log_->shouldClean(0.055)) {
        HashedKey hk = k_func();
        BufferView value = v_func();
        if (inserted_keys.find(hk) != inserted_keys.end()) {
            continue;
        }
        status = log_->insert(hk, value);
        if (status != Status::Ok) {
            break;
        }
        inserted_keys.insert(hk);
    }
    return status;
}

void RawLog::clean_segment() {
    {
        std::unique_lock<std::mutex> lock(gc_sync_);
        gc_sync_threads_ += 1;
    }

    std::vector<HashedKey> keys;
    std::vector<LogPageId> lpids;

    uint64_t check_cnt = 0;
    uint64_t invalid_cnt = 0;

    while (gc_flag_) {
        {
            std::unique_lock<std::mutex> lock(gc_sync_);
            HashedKey key;
            LogPageId lpid;

            for (uint32_t i = 0; i < 8; i++) {
                bool is_done = log_->get_next_cleaning_key(&key, &lpid);
                if (is_done) {
                    gc_flag_ = false;
                    break;
                } else {
                    keys.push_back(key);
                    lpids.push_back(lpid);
                }
            }
        }

        for (uint32_t i = 0; i < keys.size(); i++) {
            check_cnt++;
            Status status = log_->check_remove(keys[i], lpids[i]);
            if (status == Status::NotFound) {
                invalid_cnt++;
            }
        }

        keys.clear();
        lpids.clear();
    }
    {
        std::unique_lock<std::mutex> lock(gc_sync_);
        gc_sync_threads_ -= 1;
        if (gc_sync_threads_ == 0) {
            gc_sync_cond_.notify_all();
        }
    }

    // printf("check_cnt: %lu, invalid_cnt: %lu\n", check_cnt, invalid_cnt);
}

void RawLog::gc_loop() {
    while (!stop_flag_) {
        if (log_->shouldClean(0.05)) {
            // printf("clean segment\n");
            log_->startClean();
            gc_flag_ = true;
            gc_sync_cond_.notify_all();

            clean_segment();

            {
                std::unique_lock<std::mutex> lock(gc_sync_);
                while (gc_sync_threads_ > 0 || gc_flag_ == true) {
                    gc_sync_cond_.wait(lock);
                }
            }

            log_->finishClean();
        } else {
            usleep(2 * 1000);
        }
    }

    gc_sync_cond_.notify_all();
}

void RawLog::gc_wait_loop() {
    while (!stop_flag_) {
        if (gc_flag_) {
            clean_segment();
        }
        {
            std::unique_lock<std::mutex> lock(gc_sync_);
            gc_sync_cond_.wait(lock);
        }
    }
}

RawLog::PerfCounter &RawLog::perf_counter() {
    return perf_;
}