#pragma once

#include <cstdint>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <atomic>

#include "common/Device.h"
#include "common/Types.h"
#include "kangaroo/ChainedLogIndex.h"
#include "kangaroo/FwLogSegment.h"

class FwLog {
public:
    struct Config {
        uint32_t readSize{4 * 1024};
        uint32_t segmentSize{256 * 1024};

        // The range of device that Log will access is guaranted to be
        // with in [logBaseOffset, logBaseOffset + logSize)
        uint64_t logBaseOffset{};
        uint64_t logSize{0};
        Device *device{nullptr};

        // log partition, 用来减少索引位数
        uint64_t logPhysicalPartitions{4};

        // for index
        // Log Index 的总体 partition 数量
        uint64_t logIndexPartitions{4};
        // index 预分配单元大小
        uint16_t sizeAllocations{2048};
        // 这个才是真正的 partition num
        uint64_t numTotalIndexBuckets{};
        SetNumberCallback setNumberCallback{};

        // for merging to sets
        uint32_t threshold;
        SetMultiInsertCallback setMultiInsertCallback{};

        uint64_t flushGranularity{};

        Config &validate();
    };

    struct PerfCounter {
        std::atomic<uint64_t> pendingCnt{0};
        std::atomic<uint64_t> logicalReadmitCnt{0};
        std::atomic<uint64_t> physicalReadmitCnt{0};

        std::atomic<uint64_t> logPageWritten{0};
    };

    // Throw std::invalid_argument on bad config
    explicit FwLog(Config &&config);

    ~FwLog();

    FwLog(const FwLog &) = delete;
    FwLog &operator=(const FwLog &) = delete;

    bool couldExist(HashedKey hk);

    // Look up a key in KangarooLog. On success, it will return Status::Ok and
    // populate "value" with the value found. User should pass in a null
    // Buffer as "value" as any existing storage will be freed. If not found,
    // it will return Status::NotFound. And of course, on error, it returns
    // DeviceError.
    Status lookup(HashedKey hk, Buffer *value);

    // Inserts key and value into KangarooLog. This will replace an existing
    // key if found. If it failed to write, it will return DeviceError.
    Status insert(HashedKey hk, BufferView value);

    // Removes an entry from Kangaroo if found. Ok on success, NotFound on miss,
    // and DeviceError on error.
    Status remove(HashedKey hk);

    Status prefill(
        std::function<uint64_t()> k_func,
        std::function<BufferView()> v_func
    );

    void flush();

    void reset();
    void readmit(std::unique_ptr<ObjectInfo> &oi);

    bool shouldClean(double cleaningThreshold);
    bool cleaningDone();
    void startClean();
    bool finishClean();
    KangarooBucketId getNextCleaningBucket();
    std::vector<std::unique_ptr<ObjectInfo>>
    getObjectsToMove(KangarooBucketId bid, bool checkThreshold);

    LogSegmentId nextLsidToClean();

    // raw log function
    Status check_remove(HashedKey hk, LogPageId lpid);
    bool get_next_cleaning_key(HashedKey *hk, LogPageId *lpid);

    PerfCounter &perfCounter();

    // TODO: persist and recover not implemented

private:
    struct ValidConfigTag {};
    FwLog(Config &&config, ValidConfigTag);

    Buffer readLogPage(LogPageId lpid);
    Buffer readLogSegment(LogSegmentId lsid);
    bool writeLogSegment(LogSegmentId lsid, Buffer buffer);
    bool eraseSegments(LogSegmentId startLsid);

    bool isBuffered(LogPageId lpid,
                    uint32_t buffer); // does not grab logSegmentMutex mutex
    Status lookupBuffered(HashedKey hk, Buffer *value, LogPageId lpid);
    Status lookupBufferedTag(uint32_t tag, HashedKey &hk, Buffer *value,
                             LogPageId lpid);

    uint64_t getPhysicalPartition(LogPageId lpid) const {
        return (lpid.index() % pagesPerSegment_) / pagesPerPartitionSegment_;
    }
    uint64_t getLogSegmentOffset(LogSegmentId lsid) const {
        return logBaseOffset_ + segmentSize_ * lsid.offset() +
               device_.getIOZoneSize() * lsid.zone();
    }

    uint64_t getPhysicalPartition(HashedKey hk) const {
        return getIndexPartition(hk) % logPhysicalPartitions_;
    }
    uint64_t getPhysicalPartition(KangarooBucketId kbid) const {
        return getIndexPartition(kbid) % logPhysicalPartitions_;
    }
    uint64_t getIndexPartition(HashedKey hk) const {
        return getLogIndexEntry(hk) % logIndexPartitions_;
    }
    uint64_t getIndexPartition(KangarooBucketId kbid) const {
        return getLogIndexEntry(kbid) % logIndexPartitions_;
    }
    uint64_t getLogIndexEntry(HashedKey hk) const {
        return getLogIndexEntry(setNumberCallback_(hk));
    }
    uint64_t getLogIndexEntry(KangarooBucketId kbid) const {
        return kbid.index() % numIndexEntries_;
    }

    uint64_t getLogPageOffset(LogPageId lpid) const {
        uint64_t zone_in_region = lpid.index() / pagesPerZone_;
        uint64_t page_in_zone = lpid.index() % pagesPerZone_;
        return logBaseOffset_ + zone_in_region * device_.getIOZoneSize() + page_in_zone * pageSize_;
    }

    LogPageId getLogPageId(PartitionOffset po, uint32_t physicalPartition) {
        uint32_t compressed_page_in_segment = po.index() % pagesPerPartitionSegment_; // partition 内部索引
        uint32_t segment_in_region = po.index() / pagesPerPartitionSegment_;   // partition 外部索引

        uint32_t zone_in_region = segment_in_region / numSegmentsPerZone_;
        uint32_t segment_in_zone = segment_in_region % numSegmentsPerZone_;
        uint64_t index = zone_in_region * pagesPerZone_ +
                        segment_in_zone * pagesPerSegment_ + compressed_page_in_segment +
                         physicalPartition * pagesPerPartitionSegment_;
        return LogPageId(index, po.isValid());
    }
    PartitionOffset getPartitionOffset(LogPageId lpid) {
        uint32_t page_in_segment =
            (lpid.index() % pagesPerSegment_);

        uint32_t zone_in_region = lpid.index() / pagesPerZone_;
        uint32_t page_in_zone = lpid.index() % pagesPerZone_;

        uint32_t segment_in_zone = page_in_zone / pagesPerSegment_;
        uint32_t segment_in_region = zone_in_region * numSegmentsPerZone_ + segment_in_zone;
        return PartitionOffset(segment_in_region * pagesPerPartitionSegment_ +
                                   page_in_segment % pagesPerPartitionSegment_,
                               lpid.isValid());
    }
    LogSegmentId getSegmentId(LogPageId lpid) {
        uint32_t segment_in_region = lpid.index() / pagesPerSegment_;
        uint32_t segment_in_zone = segment_in_region % numSegmentsPerZone_;
        uint32_t zone_in_region = lpid.index() / pagesPerZone_;
        return LogSegmentId(segment_in_zone, zone_in_region);
    }
    LogPageId getLogPageId(LogSegmentId lsid, int32_t pageOffset) {
        return LogPageId(lsid.zone() * pagesPerZone_ + 
                        lsid.offset() * pagesPerSegment_ + pageOffset,
                         pageOffset >= 0);
    }

    LogSegmentId getNextLsid(LogSegmentId lsid);
    LogSegmentId getNextLsid(LogSegmentId lsid, uint32_t increment);

    // locks based on zone offset, concurrent read, single modify
    std::shared_mutex &getMutexFromSegment(LogSegmentId lsid) const {
        return mutex_[(lsid.offset()) & (NumMutexes - 1)];
    }
    std::shared_mutex &getMutexFromPage(LogPageId lpid) {
        return getMutexFromSegment(getSegmentId(lpid));
    }

    double cleaningThreshold_ = .1;
    Buffer cleaningBuffer_;
    std::unique_ptr<FwLogSegment> cleaningSegment_ = nullptr;
    std::unique_ptr<FwLogSegment::Iterator> cleaningSegmentIt_ = nullptr;
    void moveBucket(HashedKey hk, uint64_t count, LogSegmentId lsidToFlush);
    void readmit(HashedKey hk, BufferView value);
    bool flushLogSegment(uint32_t partition, bool wait);

    // Use birthday paradox to estimate number of mutexes given number of
    // parallel queries and desired probability of lock collision.
    static constexpr size_t NumMutexes = 16 * 1024;

    // Serialization format version. Never 0. Versions < 10 reserved for
    // testing.
    static constexpr uint32_t kFormatVersion = 10;

    const uint64_t pageSize_{};
    const uint64_t segmentSize_{};
    const uint64_t logBaseOffset_{};
    const uint64_t logSize_{};
    const uint64_t pagesPerSegment_{};
    const uint64_t numSegments_{};
    const uint64_t flushGranularity_{};

    Device &device_;
    std::unique_ptr<std::shared_mutex[]> mutex_{
        new std::shared_mutex[NumMutexes]};
    const uint64_t logIndexPartitions_{};
    ChainedLogIndex **index_;
    const SetNumberCallback setNumberCallback_{};

    const uint64_t numLogZones_{};
    const uint64_t numSegmentsPerZone_{};
    const uint64_t pagesPerZone_{};
    const uint64_t logPhysicalPartitions_{};
    const uint64_t physicalPartitionSize_{};
    const uint64_t pagesPerPartitionSegment_{};
    const uint64_t numIndexEntries_{};
    FwLogSegment **currentLogSegments_;
    const uint32_t numBufferedLogSegments_ = 2;
    uint32_t bufferedSegmentOffset_ = 0;
    double overflowLimit_ = .5;
    /* prevent access to log segment while it's being switched out
     * to disk, one for each buffered log Segment */
    std::unique_ptr<std::shared_mutex[]> logSegmentMutexs_;
    std::shared_mutex bufferMetadataMutex_;
    // std::condition_variable_any flushLogCv_;
    std::condition_variable_any cleaningLogCv_;
    std::shared_mutex cleaningMutex_;
    std::shared_mutex writeMutex_;
    Buffer *logSegmentBuffers_;

    LogSegmentId nextLsidToClean_;
    uint32_t threshold_{0};

    PerfCounter perfCounter_;

    bool writeEmpty_ = true;
};