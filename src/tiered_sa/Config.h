#pragma once

#include <cstdint>
#include <memory>

#include "tiered_sa/TieredSA.h"

class TieredSAProto {
public:
    virtual ~TieredSAProto() = default;

    void set_layout(
        uint64_t base_offset,
        uint64_t page_size,
        uint64_t zone_capacity,
        uint64_t log_zone_num,
        uint64_t set_zone_num
    );

    void set_mode(TieredSA::SetLayerMode mode);

    void set_device(Device *device);

    void set_destructor_cb(DestructorCallback cb);

    void set_bit_vector(uint32_t vector_size);

    void set_bloom_filter(uint32_t num_hashes, uint32_t len_hash_bits);

    std::unique_ptr<TieredSA> create();
private:
    TieredSA::Config config_;
};