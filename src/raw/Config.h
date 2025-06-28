#pragma once

#include "raw/RawSet.h"
#include "raw/RawLog.h"
#include "raw/SwapLog.h"

class RawSetProto {
public:
    virtual ~RawSetProto() = default;

    void set_layout(
        uint64_t base_offset,
        uint64_t page_size,
        uint64_t zone_capacity,
        uint64_t zone_num
    );

    void set_device(Device *device);

    void set_bloom_filter(uint32_t num_hashes, uint32_t len_hash_bits);

    std::unique_ptr<RawSet> create();
private:
    RawSet::Config config_;
};

class RawLogProto {
public:
    virtual ~RawLogProto() = default;

    void set_layout(
        uint64_t base_offset,
        uint64_t page_size,
        uint64_t zone_capacity,
        uint64_t zone_num
    );

    void set_device(Device *device);
    std::unique_ptr<RawLog> create();
private:
    RawLog::Config config_;
};

class SwapLogProto {
public:
    virtual ~SwapLogProto() = default;

    void set_layout(
        uint32_t page_size,
        uint32_t segment_size,
        uint32_t zone_capacity,
        uint32_t data_zone_num,
        uint32_t write_part_num,
        uint64_t log_start_off
    );

    void set_index(
        uint32_t swap_zone_num,
        uint64_t swap_start_off
    );
    void set_device(Device *device);
    std::unique_ptr<SwapLog> create();
private:
    SwapLog::Config config_;
};