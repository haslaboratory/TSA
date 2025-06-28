#pragma once

#include "common/Buffer.h"
#include "common/Types.h"

// TODO: (beyondsora) T31519237 Change KangarooBucketStorage to allocate
// backwards
//                              for better performance
// This is a very simple FIFO allocator that once full the only
// way to free up more space is by removing entries at the
// front. It is used for managing alloactions inside a bucket.
class __attribute__((__packed__)) KangarooBucketStorage {
public:
    // This is an allocation that is returned to user when they
    // allocate from the KangarooBucketStorage. "view" is for reading
    // and modifying data allocated from the storage. "position"
    // indicates where it is in the storage and is used internally to
    // iterate to the next allocation.
    //
    // User should only have reference to one "allocation" at a time.
    // Calling remove or removeUntil API on an allocation will invalidate
    // all the other references to allocations.
    class Allocation {
    public:
        Allocation() = default;

        bool done() const { return view_.isNull(); }

        MutableBufferView view() const { return view_; }

        uint32_t position() const { return position_; }

    private:
        friend KangarooBucketStorage;

        Allocation(MutableBufferView v, uint32_t p) : view_{v}, position_{p} {}

        MutableBufferView view_{};
        uint32_t position_{};
    };

    static uint32_t slotSize(uint32_t size) {
        return kAllocationOverhead + size;
    }

    explicit KangarooBucketStorage(uint32_t capacity) : capacity_{capacity} {}

    Allocation allocate(uint32_t size);

    uint32_t capacity() const { return capacity_; }

    uint32_t remainingCapacity() const { return capacity_ - endOffset_; }

    uint32_t numAllocations() const { return numAllocations_; }

    void clear() {
        endOffset_ = 0;
        numAllocations_ = 0;
    }

    void remove(Allocation alloc);

    // Removes every single allocation from the beginning, including this one.
    void removeUntil(Allocation alloc);

    Allocation getFirst() const;
    Allocation getNext(Allocation alloc) const;

private:
    // Slot represents a physical slot in the storage. User does not use
    // this directly but instead uses Allocation.
    struct __attribute__((__packed__)) Slot {
        uint16_t size{};
        uint8_t data[];
        explicit Slot(uint16_t s) : size{s} {}
    };

    bool canAllocate(uint32_t size) const {
        return static_cast<uint64_t>(endOffset_) + slotSize(size) <= capacity_;
    }

    static const uint32_t kAllocationOverhead;

    const uint32_t capacity_{};
    uint32_t numAllocations_{};
    uint32_t endOffset_{};
    mutable uint8_t data_[];
};

// BigHash is a series of buckets where each item is hashed to one of the
// buckets. A bucket is the fundamental unit of read and write onto the device.
// On read, we read an entire bucket from device and then search for the key
// we need. On write, we read the entire bucket first to insert the new entry
// in memory and then write back to the device. Same for remove.
//
// To ensure the validity of a bucket, on reading, we first check if the
// checksum is what we expect. If it's unexpected, we will reinitialize
// the bucket, finish our operation, compute a new checksum, and finally
// store the checksum in the bucket. Checksum protects us from any device
// corruption. In addition, on first start-up, this is a convenient way
// to let us know a bucket had not been initialized.
//
// Each bucket has a generation timestamp associated with it. On reading, user
// must ensure the generation is what they expect. E.g. in BigHash, to trigger
// a ice roll, we'll update the global generation and then on next startup,
// we'll lazily invalidate each bucket as we read it as the generation will
// be a mismatch.
class __attribute__((__packed__)) LogBucket {
public:
    // Iterator to bucket's items.
    class Iterator {
    public:
        bool done() const { return itr_.done(); }

        BufferView key() const;
        HashedKey hashedKey() const;
        uint64_t keyHash() const;
        BufferView value() const;

        bool keyEqualsTo(HashedKey hk) const;
        // bool keyEqualsTo(uint64_t keyHash) const;

    private:
        friend LogBucket;

        Iterator() = default;
        explicit Iterator(KangarooBucketStorage::Allocation itr) : itr_{itr} {}

        KangarooBucketStorage::Allocation itr_;
    };

    // User will pass in a view that contains the memory that is a
    // KangarooBucket
    static uint32_t computeChecksum(BufferView view);

    // Initialize a brand new LogBucket given a piece of memory in the case
    // that the existing bucket is invalid. (I.e. checksum or generation
    // mismatch). Refer to comments at the top on what do we use checksum
    // and generation time for.
    static LogBucket &initNew(MutableBufferView view, uint64_t generationTime);

    uint32_t getChecksum() const { return checksum_; }

    void setChecksum(uint32_t checksum) { checksum_ = checksum; }

    uint64_t generationTime() const { return generationTime_; }

    uint32_t size() const { return storage_.numAllocations(); }
    uint32_t capacity() const { return storage_.capacity(); }
    uint32_t remainingCapacity() const { return storage_.remainingCapacity(); }

    // Look up for the value corresponding to a key.
    // BufferView::isNull() == true if not found.
    BufferView find(HashedKey hk) const;

    BufferView findTag(uint32_t tag, HashedKey &hk) const;

    // Note: this does *not* replace an existing key! User must make sure to
    //       remove an existing key before calling insert.
    //
    // Insert into the bucket. Trigger eviction and invoke @destructorCb if
    // not enough space. Return number of entries evicted.
    uint32_t insert(HashedKey hk, BufferView value,
                    const DestructorCallback &destructorCb);

    // Remove an entry corresponding to the key. If found, invoke @destructorCb
    // before returning true. Return number of entries removed.
    uint32_t remove(HashedKey hk, const DestructorCallback &destructorCb);

    // Reorders entries in bucket based on NRU bit vector callback results
    void reorder(BitVectorReadVisitor isHitCallback);

    // Needed for log buckets, allocate does not remove objects
    bool isSpace(HashedKey hk, BufferView value);
    KangarooBucketStorage::Allocation allocate(HashedKey hk, BufferView value);
    void insert(KangarooBucketStorage::Allocation alloc, HashedKey hk,
                BufferView value);
    void clear();

    void collect_keys(std::vector<HashedKey> &keys) const;

    Iterator getFirst() const;
    Iterator getNext(Iterator itr) const;

private:
    LogBucket(uint64_t generationTime, uint32_t capacity)
        : generationTime_{generationTime}, storage_{capacity} {}

    // Reserve enough space for @size by evicting. Return number of evictions.
    uint32_t makeSpace(uint32_t size, const DestructorCallback &destructorCb);

    uint32_t checksum_{};
    uint64_t generationTime_{};
    KangarooBucketStorage storage_;
};