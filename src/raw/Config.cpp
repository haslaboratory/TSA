#include "raw/Config.h"

void RawSetProto::set_layout(
    uint64_t base_offset,
    uint64_t page_size,
    uint64_t zone_capacity,
    uint64_t zone_num
) {
    config_.base_offset = base_offset;
    config_.page_size = page_size;
    config_.zone_capacity = zone_capacity;
    config_.zone_num = zone_num;
}

void RawSetProto::set_device(Device *device) {
    config_.device = device;
}

void RawSetProto::set_bloom_filter(uint32_t num_hashes, uint32_t len_hash_bits) {
    config_.enable_bf = true;
    config_.bf_num_hashes = num_hashes;
    config_.bf_len_hash_bits = len_hash_bits;
}

std::unique_ptr<RawSet> RawSetProto::create() {
    return std::make_unique<RawSet>(std::move(config_));
}

void RawLogProto::set_layout(
    uint64_t base_offset,
    uint64_t page_size,
    uint64_t zone_capacity,
    uint64_t zone_num
) {
    config_.base_offset = base_offset;
    config_.page_size = page_size;
    config_.zone_capacity = zone_capacity;
    config_.zone_num = zone_num;
}

void RawLogProto::set_device(Device *device) {
    config_.device = device;
}

std::unique_ptr<RawLog> RawLogProto::create() {
    return std::make_unique<RawLog>(std::move(config_));
}

void SwapLogProto::set_layout(
    uint32_t page_size,
    uint32_t segment_size,
    uint32_t zone_capacity,
    uint32_t data_zone_num,
    uint32_t write_part_num,
    uint64_t log_start_off
) {
    config_.page_size = page_size;
    config_.segment_size = segment_size;
    config_.zone_capacity = zone_capacity;
    config_.data_zone_num = data_zone_num;
    config_.write_part_num = write_part_num;
    config_.log_start_off = log_start_off;
}

void SwapLogProto::set_index(
    uint32_t swap_zone_num,
    uint64_t swap_start_off
) {
    config_.swap_zone_num = swap_zone_num;
    config_.swap_start_off = swap_start_off;
}

void SwapLogProto::set_device(Device *device) {
    config_.device = device;
}

std::unique_ptr<SwapLog> SwapLogProto::create() {
    return std::make_unique<SwapLog>(std::move(config_));
}