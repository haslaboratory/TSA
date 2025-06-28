#pragma once

#include <atomic>
#include <cstdint>
#include <thread>
#include <condition_variable>
#include <unordered_set>

#include "common/Cache.h"
#include "common/BloomFilter.h"
#include "common/Buffer.h"
#include "common/Device.h"
#include "common/Types.h"

#include "kangaroo/FwLog.h"
#include "kangaroo/RripBitVector.h"
#include "kangaroo/RripBucket.h"
#include "kangaroo/Wren.h"

// Kangaroo is a small item flash-based cache engine. It divides the device into
// a series of buckets. One can think of it as a on-device hash table.
//
// Each item is hashed to a bucket according to its key. There is no size class,
// and each bucket is consisted of various variable-sized items. When full, we
// evict the items in their insertion order. An eviction call back is guaranteed
// to be invoked once per item. We currently do not support removeCB. That is
// coming as part of Navy eventually.
//
// Each read and write via Kangaroo happens in `bucketSize` granularity. This
// means, you will read a full bucket even if your item is only 100 bytes.
// It's also the same for writes. This makes Kangaroo inherently unsuitable for
// large items that will also need large buckets (several KB and above).
//
// However, this design gives us the ability to forgo an in-memory index and
// instead look up our items directly from disk. In practice, this means
// Kangaroo is a flash engine optimized for small items.
class Kangaroo: public CacheEngine {
public:
    struct Config {
        uint32_t bucketSize{4 * 1024};
        uint32_t hotBucketSize{0};

        // The range of device that Kangaroo will access is guaranted to be
        // with in [baseOffset, baseOffset + cacheSize)
        uint64_t cacheBaseOffset{};
        uint64_t totalSetSize{};
        uint64_t hotSetSize{};
        Device *device{nullptr};

        bool fwOptimizations{true};

        DestructorCallback destructorCb;

        // Optional bloom filter to reduce IO
        std::unique_ptr<BloomFilter> bloomFilter;

        uint64_t bitVectorSize{0};
        std::unique_ptr<RripBitVector> rripBitVector;

        uint64_t mergeThreads{10};

        // Better to underestimate, used for pre-allocating log index
        // only needed for Kangaroo
        uint32_t avgSmallObjectSize{100};
        uint32_t logIndexPartitionsPerPhysical{};

        double setOverprovisioning{.05}; // overprovisioning
        uint64_t numBuckets() const {
            return (1 - setOverprovisioning) * totalSetSize / bucketSize;
        }
        uint64_t hotBaseOffset() const {
            uint64_t totalZones = totalSetSize / device->getIOZoneCapSize();
            uint64_t hotZones = totalZones * hotBucketSize / bucketSize;

            printf("hotZones %ld, totalZones %ld, hotBucketSize 0x%x, "
                   "bucketSize: 0x%x\n",
                   hotZones, totalZones, hotBucketSize, bucketSize);

            return cacheBaseOffset +
                   (totalZones - hotZones) * device->getIOZoneSize();
        };

        FwLog::Config logConfig;

        Config &validate();
    };

    struct PerfCounter {
        std::atomic<uint64_t> objInserted{0};

        std::atomic<uint64_t> logObjAbsorbed{0};

        std::atomic<uint64_t> timeMovedRead{0};
        std::atomic<uint64_t> timeSetWritten{0};

        // 暂时不考虑重复 key， 单纯计数有效 obj 的个数
        std::atomic<uint64_t> logObjs{0};
        std::atomic<uint64_t> setObjs{0};

        std::atomic<uint64_t> setPageWritten{0};
        std::atomic<uint64_t> gcWritten{0};

        std::atomic<uint64_t> bfMissCnt{0};
        std::atomic<uint64_t> readMissCnt{0};

        std::atomic<uint64_t> gcPages{0};

        std::atomic<bool> warm_ok{false};
    };

    // Throw std::invalid_argument on bad config
    explicit Kangaroo(Config &&config);

    ~Kangaroo();

    Kangaroo(const Kangaroo &) = delete;
    Kangaroo &operator=(const Kangaroo &) = delete;

    // Check if the key could exist in bighash. This can be used as a pre-check
    // to optimize cache lookups to avoid calling lookups in an async IO
    // environment.
    //
    // @param hk   key to be checked
    //
    // @return  false if the key definitely does not exist and true if it could.
    bool couldExist(HashedKey hk);
    // Look up a key in Kangaroo. On success, it will return Status::Ok and
    // populate "value" with the value found. User should pass in a null
    // Buffer as "value" as any existing storage will be freed. If not found,
    // it will return Status::NotFound. And of course, on error, it returns
    // DeviceError.
    Status lookup(HashedKey hk, Buffer *value);

    // Inserts key and value into Kangaroo. This will replace an existing
    // key if found. If it failed to write, it will return DeviceError.
    Status insert(HashedKey hk, BufferView value);

    // Removes an entry from Kangaroo if found. Ok on success, NotFound on miss,
    // and DeviceError on error.
    // Kangaroo 算法不能保证一致性, TODO: LSM 追加删除
    Status remove(HashedKey hk);

    Status prefill(
        std::function<uint64_t ()> k_func,
        std::function<BufferView ()> v_func
    );

    void flush();

    void reset();

    // return the maximum allowed item size
    uint64_t getMaxItemSize() const;

    PerfCounter &perfCounter();

    FwLog::PerfCounter &fwLogPerfCounter();

private:
    struct ValidConfigTag {};
    Kangaroo(Config &&config, ValidConfigTag);

    Buffer readBucket(KangarooBucketId bid, bool hot);
    bool writeBucket(KangarooBucketId bid, Buffer buffer, bool hot);

    // The corresponding r/w bucket lock must be held during the entire
    // duration of the read and write operations. For example, during write,
    // if write lock is dropped after a bucket is read from device, user
    // must re-acquire the write lock and re-read the bucket from device
    // again to ensure they have the newest content. Otherwise, one thread
    // could overwrite another's writes.
    //
    // In short, just hold the lock during the entire operation!
    std::shared_mutex &getMutex(KangarooBucketId bid) const {
        return mutex_[bid.index() & (kNumMutexes - 1)];
    }

    KangarooBucketId getKangarooBucketId(HashedKey hk) const {
        return KangarooBucketId{static_cast<uint32_t>(hk % numBuckets_)};
    }

    KangarooBucketId getKangarooBucketIdFromHash(uint64_t hash) const {
        return KangarooBucketId{static_cast<uint32_t>(hash % numBuckets_)};
    }

    uint64_t getBucketOffset(KangarooBucketId bid) const {
        return cacheBaseOffset_ + bucketSize_ * bid.index();
    }

    // double bfFalsePositivePct() const;
    void bfRebuild(KangarooBucketId bid, const RripBucket *bucket);
    void bfBuild(KangarooBucketId bid,
                 const RripBucket *bucket); // doesn't clear bf
    bool bfReject(KangarooBucketId bid, uint64_t keyHash) const;

    bool bvGetHit(KangarooBucketId bid, uint32_t keyIdx) const;
    void bvSetHit(KangarooBucketId bid, uint32_t keyIdx) const;

    void moveBucket(KangarooBucketId bid, bool logFlush,
                    int gcMode); // gcMode 0 = log flush, 1 = cold, 2 = hot
    void redivideBucket(RripBucket *hotBucket, RripBucket *coldBucket);

    // Use birthday paradox to estimate number of mutexes given number of
    // parallel queries and desired probability of lock collision.
    static constexpr size_t kNumMutexes = 16 * 1024;

    // Serialization format version. Never 0. Versions < 10 reserved for
    // testing.
    static constexpr uint32_t kFormatVersion = 10;

    // Open addressing index overhead
    static constexpr double LogIndexOverhead = 2;

    // Log flushing and gc code, performed on a separate set of threads
    double flushingThreshold_ = 0.15;
    double gcUpperThreshold_ = 0.05;
    double gcLowerThreshold_ = 0.015;
    bool shouldLogFlush();        // not parallel
    bool shouldUpperGC(bool hot); // not parallel
    bool shouldLowerGC(bool hot); // not parallel
    void performGC(int gcMode);
    void performLogFlush();
    void gcSetupTeardown(bool hot);
    void cleanSegmentsLoop();
    void cleanSegmentsWaitLoop();
    std::vector<std::thread> cleaningThreads_;
    uint64_t numCleaningThreads_;

    std::mutex cleaningSync_;
    uint64_t cleaningSyncThreads_ = 0;
    std::condition_variable cleaningSyncCond_;
    bool performingLogFlush_ = false;
    int performingGC_ = 0; // 0 = fine, 1 = cold sets, 2 = hot sets
    Wren::EuIterator euIterator_;
    bool stop_flag_ = false;
    const uint32_t hotRebuildFreq_ = 5;

    const DestructorCallback destructorCb_{};
    const uint64_t bucketSize_{};
    const uint64_t hotBucketSize_{};
    const uint64_t cacheBaseOffset_{};
    const uint64_t hotCacheBaseOffset_{};
    const uint64_t numBuckets_{};
    std::unique_ptr<BloomFilter> bloomFilter_;
    std::unique_ptr<RripBitVector> bitVector_;
    std::unique_ptr<FwLog> fwLog_{nullptr};
    std::chrono::nanoseconds generationTime_{};
    Device &device_;
    std::unique_ptr<Wren> wrenDevice_{nullptr};
    std::unique_ptr<Wren> wrenHotDevice_{nullptr};
    std::unique_ptr<std::shared_mutex[]> mutex_{
        new std::shared_mutex[kNumMutexes]};

    bool fwOptimizations_ = false;
    bool enable_hot_ = false;

    static_assert((kNumMutexes & (kNumMutexes - 1)) == 0,
                  "number of mutexes must be power of two");

    PerfCounter perfCounter_;
};