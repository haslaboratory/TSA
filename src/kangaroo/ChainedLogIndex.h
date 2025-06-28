#pragma once

#include <mutex>
#include <shared_mutex>
#include <stdint.h>
#include <vector>

#include "common/Types.h"

// valid：1
// flash index: total_page_bits_ / phy_part_num
// tag: >= total_obj_bits / bucket_part_num, 假阳性率怎么计算
// hits: 3
// next: total_obj_bits / index_part_num

class ChainedLogIndex;

class __attribute__((__packed__)) ChainedLogIndexEntry {
public:
    ChainedLogIndexEntry() : valid_{false} {}
    ~ChainedLogIndexEntry() = default;

    bool operator==(const ChainedLogIndexEntry &rhs) const noexcept {
        return valid_ && rhs.valid_ && tag_ == rhs.tag_;
    }
    bool operator!=(const ChainedLogIndexEntry &rhs) const noexcept {
        return !(*this == rhs);
    }

    void populateEntry(PartitionOffset po, uint32_t tag, uint8_t hits) {
        flash_index_ = po.index();
        tag_ = tag;
        valid_ = 1;
        hits_ = hits;
    }

    void incrementHits() {
        if (hits_ < ((1 << 3) - 1)) {
            hits_++;
        }
    }
    uint32_t hits() { return hits_; }
    uint32_t tag() { return tag_; }
    void invalidate() { valid_ = 0; }
    bool isValid() { return valid_; }
    PartitionOffset offset() { return PartitionOffset(flash_index_, valid_); }
    uint32_t next() { return next_; }

private:
    friend ChainedLogIndex;

    uint32_t valid_ : 1;
    uint32_t hits_ : 3;
    uint32_t flash_index_: 28;
    uint32_t tag_ : 12;
    uint32_t next_: 20;
};

// ChainedLogIndex is a hash-based log index optimized for allowing easy
// threshold lookups
//
// It primarily is a chained hash table that chains so that
// items in the same on-flash Kangaroo bucket will end up in the
// same hash bucket in the index. This way we avoid scans to
// see if items can end up in the same set.
class ChainedLogIndex {
public:
    // BucketIterator gives hashed key for each valid
    // element corresponding to a given kangaroo bucket
    // Read only
    class BucketIterator {
    public:
        BucketIterator() : end_{true} {}

        bool done() const { return end_; }

        uint32_t tag() const { return tag_; }

        uint32_t hits() const { return hits_; }

        PartitionOffset offset() const { return offset_; }

    private:
        friend ChainedLogIndex;

        BucketIterator(KangarooBucketId id, ChainedLogIndexEntry *firstKey)
            : bucket_{id}, tag_{firstKey->tag()}, hits_{firstKey->hits()},
              offset_{firstKey->offset()}, nextEntry_{firstKey->next_} {}

        KangarooBucketId bucket_{0};
        uint32_t tag_;
        uint32_t hits_;
        PartitionOffset offset_{0, 0};
        uint32_t nextEntry_;
        bool end_{false};
    };

    explicit ChainedLogIndex(uint64_t numHashBuckets, uint32_t allocationSize,
                             SetNumberCallback setNumberCb);

    ~ChainedLogIndex();

    ChainedLogIndex(const ChainedLogIndex &) = delete;
    ChainedLogIndex &operator=(const ChainedLogIndex &) = delete;

    // Look up a key in Index.
    // If not found, return will not be valid.
    PartitionOffset lookup(HashedKey hk, bool hit, uint32_t *hits);

    // Inserts key into index.
    Status insert(HashedKey hk, PartitionOffset po, uint8_t hits = 0);
    Status insert(uint32_t tag, KangarooBucketId bid, PartitionOffset po,
                  uint8_t hits);

    // Removes entry's valid bit if it's in the log
    Status remove(HashedKey hk, PartitionOffset lpid);
    Status remove(uint64_t tag, KangarooBucketId bid, PartitionOffset lpid);

    // does not create a hit, for log flush lookups
    PartitionOffset find(KangarooBucketId bid, uint64_t tag);

    // Counts number of items in log corresponding to set
    // bucket for the hashed key
    uint64_t countBucket(HashedKey hk);

    // Get iterator for all items in the same bucket
    BucketIterator getHashBucketIterator(HashedKey hk);
    BucketIterator getHashBucketIterator(KangarooBucketId bid);
    BucketIterator getNext(BucketIterator bi);

private:
    friend BucketIterator;

    class LogIndexBucket {
    public:
        explicit LogIndexBucket(uint32_t idx) : idx_{idx} {}

        bool operator==(const LogIndexBucket &rhs) const noexcept {
            return idx_ == rhs.idx_;
        }
        bool operator!=(const LogIndexBucket &rhs) const noexcept {
            return !(*this == rhs);
        }

        uint32_t index() const noexcept { return idx_; }

    private:
        uint32_t idx_;
    };

    Status remove(uint64_t tag, LogIndexBucket lib, PartitionOffset lpid);
    Status insert(uint32_t tag, LogIndexBucket lib, PartitionOffset po,
                  uint8_t hits);

    LogIndexBucket getLogIndexBucket(HashedKey hk);
    // LogIndexBucket getLogIndexBucket(uint64_t hk);
    LogIndexBucket getLogIndexBucketFromSetBucket(KangarooBucketId bid);

    // locks based on log index hash bucket, concurrent read, single modify
    std::shared_mutex &getMutex(LogIndexBucket lib) const {
        return mutexes_[lib.index() & (numMutexes_ - 1)];
    }

    const uint64_t numMutexes_{};
    const uint64_t numHashBuckets_{};
    const SetNumberCallback setNumberCb_{};
    std::unique_ptr<std::shared_mutex[]> mutexes_;
    std::vector<uint32_t> index_;

    std::shared_mutex allocationMutex_;
    const uint32_t allocationSize_{};
    uint32_t maxSlotUsed_{0};
    uint32_t nextEmpty_{0};
    uint32_t numAllocations_{0};

    void allocate();
    ChainedLogIndexEntry *findEntry(uint32_t offset);
    ChainedLogIndexEntry *findEntryNoLock(uint32_t offset);
    ChainedLogIndexEntry *allocateEntry(uint32_t &offset);
    uint32_t releaseEntry(uint32_t offset);

    std::vector<ChainedLogIndexEntry *> allocations;
};