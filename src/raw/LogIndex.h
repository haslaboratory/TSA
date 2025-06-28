#pragma once

#include <cstdint>
#include <memory>
#include <vector>
#include <shared_mutex>
#include <atomic>
#include <optional>

#include "kangaroo/FwLogSegment.h"
#include "common/BloomFilter.h"
#include "common/Types.h"
#include "common/Device.h"

// 动态将一部分 Index 交换至 SSD 上以控制索引内存占用

// Swap Index
struct __attribute__((__packed__)) SwapIndexEntry {
    HashedKey hk;
    uint32_t valid: 1;
    uint32_t flash_index: 31;
};

constexpr uint32_t SWAP_INDEX_ENTRY_NUM = 4096 / sizeof(SwapIndexEntry);

struct SwapIndexPage {
    SwapIndexEntry entries_[SWAP_INDEX_ENTRY_NUM];
};

static_assert(sizeof(SwapIndexPage) <= 4096, "SwapIndexPage size is too large");

class SwapIndex {
public:
    struct Config {
        uint32_t swap_part_num{0};
        uint32_t page_size{4096};
        uint64_t swap_start_off{0};
        uint32_t swap_zone_num{0};
        uint32_t zone_capacity{0};

        Device *device{nullptr};
    };
    SwapIndex() = delete;
    SwapIndex(Config &&config);

    Buffer read(uint32_t lpid);

    bool write(uint32_t lpid, Buffer buf);

    bool could_exist(uint32_t lpid, HashedKey hk);

    bool swap_bkt_exist(uint32_t lpid);

private:
    bool check_gc();
    void do_gc();
    Buffer inner_read(uint32_t lpid);
    void inner_write(uint32_t lpid, Buffer buf);

    void bf_rebuild(uint32_t lpid, SwapIndexPage *page);

    std::vector<uint32_t> lpid_2_ppid_;
    std::unique_ptr<BloomFilter> bloom_filter_;
    // meta
    uint32_t write_ptr_{0};
    uint32_t gc_zone_id_{0};
    std::shared_mutex mutex_;
    // config
    uint32_t swap_part_num_{0};
    uint32_t page_size_{4096};
    uint32_t zone_capacity_{0};
    uint32_t swap_zone_num_{0};
    uint64_t swap_start_off_{0};
    uint32_t page_per_zone_{0};

    Device *device_{nullptr};
};

struct __attribute__((__packed__)) IndexEntry {
    HashedKey hk;
    uint32_t flash_index;
    uint32_t next_idx;
};

#define UNIT_CNT 1024

class LogIndexPart {
public:
    LogIndexPart() { allocate(); }

    void allocate();

    IndexEntry *get_entry(uint32_t idx);
    uint32_t allocate_entry();
    void release_entry(uint32_t idx);

private:
    // allocate banks
    std::vector<std::unique_ptr<IndexEntry[]>> entries_;

    uint32_t alloc_idx_{0};
    uint32_t free_idx_{UINT32_MAX};
};

class LogIndex {
public:
    struct Config {
        uint32_t page_size{4096};
        uint32_t zone_capacity{0};
        uint64_t swap_start_off{0};
        uint32_t data_zone_num{0};
        uint32_t swap_zone_num{0};

        uint32_t key_part_num{0};
        uint32_t swap_part_num{0};

        Device *device{nullptr};

        bool validate();
    };
    LogIndex() = delete;

    LogIndex(Config &&config);

    PartitionOffset lookup(HashedKey hk);

    Status insert(HashedKey hk, PartitionOffset po);

    Status remove(HashedKey hk, PartitionOffset lpid);

    Status pop(HashedKey hk, PartitionOffset po);

    void force_do_swap(uint32_t base_addr);
private:
    void do_swap(uint32_t swap_part_id, std::optional<HashedKey> filter_key, uint32_t base_addr);
    // utils functions
    std::shared_mutex* get_mutex(uint32_t swap_part_id);
    LogIndexPart *get_index_part(uint32_t swap_part_id);
    // index
    std::unique_ptr<SwapIndex> swap_index_;
    // buffer

    // config
    uint32_t page_size_{4096};
    uint32_t zone_capacity_{0};
    uint32_t data_zone_num_{0};

    uint32_t key_part_num_{0};
    uint32_t swap_part_num_{0};
    const uint32_t LOCK_PART_NUM{128 * 1024};
    // meta
    uint32_t pages_per_zone_{0};

    // key index
    std::unique_ptr<uint32_t[]> upper_index_;
    std::unique_ptr<uint32_t[]> lower_index_;

    // lock index
    std::unique_ptr<std::shared_mutex[]> mutexes_;
    std::unique_ptr<LogIndexPart[]> index_parts_;

    // cached lsid
    std::atomic<uint32_t> cached_lsid_{0};
    // test
    // std::vector<uint32_t> insert_cnt_;
};