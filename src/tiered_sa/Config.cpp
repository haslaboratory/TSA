#include "common/Device.h"
#include "tiered_sa/Config.h"

void TieredSAProto::set_layout(
    uint64_t base_offset,
    uint64_t page_size,
    uint64_t zone_capacity,
    uint64_t log_zone_num,
    uint64_t set_zone_num
) {
    config_.base_offset = base_offset;
    config_.page_size = page_size;
    config_.zone_capacity = zone_capacity;
    config_.log_zone_num = log_zone_num;
    config_.set_zone_num = set_zone_num;
}

void TieredSAProto::set_mode(TieredSA::SetLayerMode mode) {
    config_.set_mode = mode;
}

void TieredSAProto::set_device(Device *device) {
    config_.device = device;
}

void TieredSAProto::set_destructor_cb(DestructorCallback cb) {
    config_.destructor_cb = std::move(cb);
}

void TieredSAProto::set_bit_vector(uint32_t vector_size) {
    config_.bv_vector_size = vector_size;
}

void TieredSAProto::set_bloom_filter(uint32_t num_hashes, uint32_t len_hash_bits) {
    config_.enable_bf = true;
    config_.bf_num_hashes = num_hashes;
    config_.bf_len_hash_bits = len_hash_bits;
}

std::unique_ptr<TieredSA> TieredSAProto::create() {
    return std::make_unique<TieredSA>(std::move(config_));
}