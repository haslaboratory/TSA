#pragma once

#include <cstdint>
#include <mutex>
#include <shared_mutex>

#include "common/Buffer.h"
#include "common/Types.h"
#include "kangaroo/LogBucket.h"

class FwLogSegment {
public:
    class Iterator {
        // only iterates through one partition of log segment
    public:
        bool done() const { return done_; }

        HashedKey key() const { return itr_.hashedKey(); }

        BufferView value() const { return itr_.value(); }

        uint32_t bucket() const { return bucketNum_; }

        uint32_t partition() const { return bucketNum_ / bucketsPerPartition_; }

    private:
        friend FwLogSegment;

        explicit Iterator(uint64_t bucketNum, LogBucket::Iterator itr,
                          uint64_t bucketsPerPartition)
            : bucketNum_{bucketNum},
              bucketsPerPartition_{bucketsPerPartition}, itr_{itr} {
            if (itr_.done()) {
                done_ = true;
            }
        }

        uint64_t bucketNum_;
        uint64_t bucketsPerPartition_;
        LogBucket::Iterator itr_;
        bool done_ = false;
    };

    explicit FwLogSegment(uint64_t segmentSize, uint64_t pageSize,
                          LogSegmentId lsid, uint32_t numPartitions,
                          MutableBufferView mutableView, bool newBucket);

    ~FwLogSegment();

    FwLogSegment(const FwLogSegment &) = delete;
    FwLogSegment &operator=(const FwLogSegment &) = delete;

    // Look up for the value corresponding to a key.
    // BufferView::isNull() == true if not found.
    BufferView find(HashedKey hk, LogPageId lpid);
    BufferView findTag(uint32_t tag, HashedKey &hk, LogPageId lpid);

    // Insert into the segment. Returns negative bucket offset if there is no
    // room.
    int32_t insert(HashedKey hk, BufferView value, uint32_t partition);

    LogSegmentId getLogSegmentId();

    void clear(LogSegmentId newLsid);

    std::unique_ptr<Iterator> getFirst();
    std::unique_ptr<Iterator> getNext(std::unique_ptr<Iterator> itr);

    double getFullness(uint32_t partition);

private:
    uint32_t bucketOffset(LogPageId lpid);

    // allocation on Kangaroo Bucket lock
    std::shared_mutex *allocationMutexes_;

    uint64_t segmentSize_;
    uint64_t pageSize_;
    uint64_t numBuckets_;
    uint32_t numPartitions_;
    uint64_t bucketsPerPartition_;
    LogSegmentId lsid_;
    // pointer to array of pointers to LogBuckets
    LogBucket **buckets_;
};