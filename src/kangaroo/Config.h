#pragma once

#include <cstdint>
#include <memory>

#include "common/Cache.h"
#include "kangaroo/Kangaroo.h"

// Kangaroo engine proto. kangaroo is used to cache small objects (under 2KB)
// more efficiently than BigHash by itself.
// User sets up this proto object and passes it to CacheProto::setKangaroo.
class KangarooProto {
public:
    virtual ~KangarooProto() = default;

    // Set cache layout. Cache will start at @baseOffset and will be @size bytes
    // on the device. Kangaroo divides its device spcae into a number of fixed
    // size buckets, represented by @bucketSize. All IO happens on bucket-size
    // granularity.
    void setLayout(uint64_t baseOffset, uint64_t size, uint32_t bucketSize,
                    uint32_t hotBucketSize);

    // Enable Bloom filter with @numHashes hash functions, each mapped into an
    // bit array of @hashTableBitSize bits.
    void setBloomFilter(uint32_t numHashes, uint32_t hashTableBitSize);

    void setBitVector(uint32_t vectorSize);

    // Enable part of cache space to be log
    void setLog(uint64_t logSize, uint64_t logBaseOffset, uint32_t threshold,
                uint32_t physicalPartitions,
                uint32_t indexPartitionsPerPhysical, uint64_t writeGranularity,
                uint64_t flushGranularity);

    void setDevice(Device *device);

    void setFairyOptimizations(bool enable);

    void setDestructorCb(DestructorCallback cb);

    std::unique_ptr<Kangaroo> create();

private:
    Kangaroo::Config config_;
    bool bloomFilterEnabled_{false};
    bool nruEnabled_{false};
    uint32_t numHashes_{};
    uint32_t hashTableBitSize_{};
};