#include "common/Device.h"
#include "kangaroo/Kangaroo.h"
#include "kangaroo/Config.h"

void KangarooProto::setLayout(uint64_t baseOffset, uint64_t size,
    uint32_t bucketSize, uint32_t hotBucketSize) {
    config_.cacheBaseOffset = baseOffset;
    config_.totalSetSize = size;
    config_.bucketSize = bucketSize;
    config_.hotSetSize = size * hotBucketSize / bucketSize;
    config_.hotBucketSize = hotBucketSize;
}

void KangarooProto::setBloomFilter(uint32_t numHashes,
         uint32_t hashTableBitSize) {
    // Want to make @setLayout and Bloom filter setup independent.
    bloomFilterEnabled_ = true;
    numHashes_ = numHashes;
    if (config_.hotBucketSize == 0) {
        hashTableBitSize_ = hashTableBitSize;
    } else {
        hashTableBitSize_ = hashTableBitSize * 2;
    }
}

void KangarooProto::setBitVector(uint32_t bitVectorSize) {
    if (config_.hotBucketSize == 0) {
        config_.bitVectorSize = bitVectorSize;
    } else {
        config_.bitVectorSize = bitVectorSize * 2;
    }
}

void KangarooProto::setLog(uint64_t logSize, uint64_t logBaseOffset,
 uint32_t threshold, uint32_t physicalPartitions,
 uint32_t indexPartitionsPerPhysical,
 uint64_t writeGranularity,
 uint64_t flushGranularity) {
    config_.logConfig.logSize = logSize;
    printf("setLog logSize 0x%lx kangaroo size 0x%lx\n", logSize,
    config_.totalSetSize);
    config_.totalSetSize = config_.totalSetSize - logSize;
    config_.hotSetSize =
    config_.totalSetSize * config_.hotBucketSize / config_.bucketSize;

    config_.logConfig.logBaseOffset = logBaseOffset;
    config_.logConfig.threshold = threshold;
    config_.logIndexPartitionsPerPhysical = indexPartitionsPerPhysical;
    config_.logConfig.logPhysicalPartitions = physicalPartitions;
    config_.logConfig.numTotalIndexBuckets =
    config_.numBuckets() -
    config_.numBuckets() %
    (indexPartitionsPerPhysical * physicalPartitions);
    config_.logConfig.segmentSize = writeGranularity;
    config_.logConfig.flushGranularity = flushGranularity;
    printf("Set up kangaroo with %d index partitions and %d physical "
        "partitions\n",
        indexPartitionsPerPhysical, physicalPartitions);
}

void KangarooProto::setDevice(Device *device) { config_.device = device; }

void KangarooProto::setDestructorCb(DestructorCallback cb) {
config_.destructorCb = std::move(cb);
}


void KangarooProto::setFairyOptimizations(bool enable) {
    config_.fwOptimizations = enable;
}

std::unique_ptr<Kangaroo> KangarooProto::create() {
    if (bloomFilterEnabled_) {
        if (config_.bucketSize == 0) {
            printf("invalid bucket size\n");
            exit(-1);
        }
        config_.bloomFilter = std::make_unique<BloomFilter>(
        config_.numBuckets(), numHashes_, hashTableBitSize_);
    }

    assert(config_.bitVectorSize > 0);
    config_.rripBitVector =
        std::make_unique<RripBitVector>(config_.numBuckets(), config_.bitVectorSize);
    return std::make_unique<Kangaroo>(std::move(config_));
}