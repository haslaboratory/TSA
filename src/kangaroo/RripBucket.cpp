#include "common/Utils.h"
#include "kangaroo/RripBucket.h"

static_assert(sizeof(RripBucketStorage) == 12,
              "RripBucketStorage overhead. Changing this may require changing "
              "the sizes used in unit tests as well");

const uint32_t RripBucketStorage::kAllocationOverhead =
    sizeof(RripBucketStorage::Slot);

// This is very simple as it only tries to allocate starting from the
// tail of the storage. Returns null view() if we don't have any more space.
RripBucketStorage::Allocation RripBucketStorage::allocate(uint32_t size,
                                                          uint8_t rrip) {
    // Allocate at the beginning of the right rrip value
    //
    //              tail
    // |-6--|3|--0--|~~~~~~~~~~~~~|
    //
    // after allocating object with 3
    //                  tail
    // |-6--|3|NEW|--0--|~~~~~~~~~|
    if (!canAllocate(size)) {
        return {};
    }

    auto itr = getFirst();
    uint32_t position = 0;
    while (!itr.done() && itr.rrip() >= rrip) {
        itr = getNext(itr);
        position++;
    }

    uint32_t totalNewSize = slotSize(size);
    uint8_t *start = data_ + endOffset_;
    if (!itr.done()) {
        start = itr.view().data() - kAllocationOverhead;
    }
    std::memmove(start + totalNewSize, start, (data_ + endOffset_) - start);

    auto *slot = new (start) Slot(size, rrip);
    endOffset_ += totalNewSize;
    numAllocations_++;
    return {MutableBufferView{slot->size, slot->data}, position,
            (uint8_t)slot->rrip};
}

RripBucketStorage::Allocation RripBucketStorage::remove(Allocation alloc) {
    // Remove triggers a compaction.
    //
    //                         tail
    //  |--------|REMOVED|-----|~~~~|
    //
    // after compaction
    //                  tail
    //  |---------------|~~~~~~~~~~~|
    if (alloc.done()) {
        return alloc;
    }

    const uint32_t removedSize = slotSize(alloc.view().size());
    uint8_t *removed = alloc.view().data() - kAllocationOverhead;
    uint32_t position = alloc.position();
    std::memmove(removed, removed + removedSize,
                 (data_ + endOffset_) - removed - removedSize);
    endOffset_ -= removedSize;
    numAllocations_--;

    auto *current = reinterpret_cast<Slot *>(removed);
    if (reinterpret_cast<uint8_t *>(current) - data_ >= endOffset_) {
        return {};
    }
    return {MutableBufferView{current->size, current->data}, position,
            (uint8_t)current->rrip};
}

void RripBucketStorage::removeUntil(Allocation alloc) {
    // Remove everything until (and include) "alloc"
    //
    //                         tail
    //  |----------------|-----|~~~~|
    //  ^                ^
    //  begin            offset
    //  remove this whole range
    //
    //        tail
    //  |-----|~~~~~~~~~~~~~~~~~~~~~|
    if (alloc.done()) {
        return;
    }

    uint32_t offset = alloc.view().data() + alloc.view().size() - data_;
    if (offset > endOffset_) {
        return;
    }

    std::memmove(data_, data_ + offset, endOffset_ - offset);
    endOffset_ -= offset;
    numAllocations_ -= alloc.position() + 1;
}

RripBucketStorage::Allocation RripBucketStorage::getFirst() const {
    if (endOffset_ == 0) {
        return {};
    }
    auto *slot = reinterpret_cast<Slot *>(data_);
    return {MutableBufferView{slot->size, slot->data}, 0, (uint8_t)slot->rrip};
}

RripBucketStorage::Allocation
RripBucketStorage::getNext(RripBucketStorage::Allocation alloc) const {
    if (alloc.done()) {
        return {};
    }

    auto *next =
        reinterpret_cast<Slot *>(alloc.view().data() + alloc.view().size());
    if (!next || !next->size || !next->data ||
        reinterpret_cast<uint8_t *>(next) - data_ >= endOffset_) {
        return {};
    }
    return {MutableBufferView{next->size, next->data}, alloc.position() + 1,
            (uint8_t)next->rrip};
}

void RripBucketStorage::incrementRrip(Allocation alloc, int8_t increment) {
    uint8_t *current_slot = alloc.view().data() - kAllocationOverhead;
    auto *slot = reinterpret_cast<Slot *>(current_slot);
    assert(increment + slot->rrip <= 7);
    slot->rrip += increment;
}

static_assert(
    sizeof(RripBucket) == 24,
    "RripBucket overhead. If this changes, you may have to adjust the "
    "sizes used in unit tests.");

namespace {
// This maps to exactly how an entry is stored in a bucket on device.
class __attribute__((__packed__)) RripBucketEntry {
public:
    static uint32_t computeSize(uint32_t valueSize) {
        return sizeof(RripBucketEntry) + valueSize;
    }

    static RripBucketEntry &create(MutableBufferView storage, HashedKey hk,
                                   BufferView value) {
        new (storage.data()) RripBucketEntry{hk, value};
        return reinterpret_cast<RripBucketEntry &>(*storage.data());
    }

    // BufferView key() const { return {keySize_, data_}; }

    HashedKey hashedKey() const { return hashedKey_; }

    bool keyEqualsTo(HashedKey hk) const { return hk == hashedKey(); }

    //   bool keyEqualsTo(uint64_t keyHash) const {
    //     return keyHash == keyHash_;
    //   }

    // uint64_t keyHash() const { return keyHash_; }

    BufferView value() const { return {valueSize_, data_}; }

private:
    RripBucketEntry(HashedKey hk, BufferView value)
        : hashedKey_{hk},
          valueSize_{static_cast<uint16_t>(value.size())} {
        static_assert(sizeof(RripBucketEntry) == 12,
                      "RripBucketEntry overhead");
        value.copyTo(data_);
    }

    const uint64_t hashedKey_{};
    const uint32_t valueSize_{};
    uint8_t data_[];
};

const RripBucketEntry *getIteratorEntry(RripBucketStorage::Allocation itr) {
    return reinterpret_cast<const RripBucketEntry *>(itr.view().data());
}
} // namespace

// BufferView RripBucket::Iterator::key() const {
//   return getIteratorEntry(itr_)->key();
// }

uint8_t RripBucket::Iterator::rrip() const { return itr_.rrip(); }

// uint64_t RripBucket::Iterator::keyHash() const {
//   return getIteratorEntry(itr_)->keyHash();
// }

HashedKey RripBucket::Iterator::hashedKey() const {
    return getIteratorEntry(itr_)->hashedKey();
}

BufferView RripBucket::Iterator::value() const {
    return getIteratorEntry(itr_)->value();
}

bool RripBucket::Iterator::keyEqualsTo(HashedKey hk) const {
    return getIteratorEntry(itr_)->keyEqualsTo(hk);
}

// bool RripBucket::Iterator::keyEqualsTo(uint64_t keyHash) const {
//   return getIteratorEntry(itr_)->keyEqualsTo(keyHash);
// }

uint32_t RripBucket::computeChecksum(BufferView view) {
    constexpr auto kChecksumStart = sizeof(checksum_);
    auto data = view.slice(kChecksumStart, view.size() - kChecksumStart);
    return simple_checksum(data.data(), data.size());
}

RripBucket &RripBucket::initNew(MutableBufferView view,
                                uint64_t generationTime) {
    return *new (view.data())
        RripBucket(generationTime, view.size() - sizeof(RripBucket));
}

BufferView RripBucket::find(HashedKey hk,
                            BitVectorUpdateVisitor addHitCallback) const {
    auto itr = storage_.getFirst();
    uint32_t keyIdx = 0;
    while (!itr.done()) {
        auto *entry = getIteratorEntry(itr);
        if (entry->keyEqualsTo(hk)) {
            if (addHitCallback) {
                addHitCallback(keyIdx);
            }
            return entry->value();
        }
        itr = storage_.getNext(itr);
        keyIdx++;
    }
    return {};
}

uint32_t RripBucket::insert(HashedKey hk, BufferView value, uint8_t hits,
                            const DestructorCallback &destructorCb) {
    const auto size = RripBucketEntry::computeSize(value.size());
    assert(size <= storage_.capacity());

    const auto evictions = makeSpace(size, destructorCb);
    uint8_t rrip = getRripValue(hits);
    auto alloc = storage_.allocate(size, rrip);
    assert(!alloc.done());
    RripBucketEntry::create(alloc.view(), hk, value);

    return evictions;
}

uint32_t RripBucket::insertRrip(HashedKey hk, BufferView value, uint8_t rrip,
                                const DestructorCallback &destructorCb) {
    const auto size = RripBucketEntry::computeSize(value.size());
    assert(size <= storage_.capacity());

    const auto evictions = makeSpace(size, destructorCb);
    auto alloc = storage_.allocate(size, rrip);
    assert(!alloc.done());
    RripBucketEntry::create(alloc.view(), hk, value);

    return evictions;
}

bool RripBucket::isSpace(HashedKey hk, BufferView value, uint8_t hits) {
    const auto size = RripBucketEntry::computeSize(value.size());
    const auto requiredSize = RripBucketStorage::slotSize(size);
    if (unlikely(requiredSize > storage_.capacity())) {
        printf("requiredSize: %u, capacity: %u\n", requiredSize, storage_.capacity());
        assert(false);
    }

    auto curFreeSpace = storage_.remainingCapacity();
    uint8_t rrip = getRripValue(hits);

    auto itr = storage_.getFirst();
    while (curFreeSpace < requiredSize) {
        if (itr.done()) {
            return false;
        } else if (itr.rrip() < rrip) {
            return false;
        }
        curFreeSpace += RripBucketStorage::slotSize(itr.view().size());
        itr = storage_.getNext(itr);
    }
    return (curFreeSpace >= requiredSize);
}

bool RripBucket::isSpaceRrip(HashedKey hk, BufferView value, uint8_t rrip) {
    const auto size = RripBucketEntry::computeSize(value.size());
    const auto requiredSize = RripBucketStorage::slotSize(size);
    assert(requiredSize <= storage_.capacity());

    auto curFreeSpace = storage_.remainingCapacity();

    auto itr = storage_.getFirst();
    while (curFreeSpace < requiredSize) {
        if (itr.done()) {
            return false;
        } else if (itr.rrip() <= rrip) {
            return false;
        }
        curFreeSpace += RripBucketStorage::slotSize(itr.view().size());
        itr = storage_.getNext(itr);
    }
    return (curFreeSpace >= requiredSize);
}

bool RripBucket::isSpacePrefill(HashedKey hk, BufferView value) {
    const auto size = RripBucketEntry::computeSize(value.size());
    const auto requiredSize = RripBucketStorage::slotSize(size);
    assert(requiredSize <= storage_.capacity());

    auto curFreeSpace = storage_.remainingCapacity();

    return (curFreeSpace >= requiredSize);
}

uint32_t RripBucket::makeSpace(uint32_t size,
                               const DestructorCallback &destructorCb) {
    const auto requiredSize = RripBucketStorage::slotSize(size);
    assert(requiredSize <= storage_.capacity());

    auto curFreeSpace = storage_.remainingCapacity();
    if (curFreeSpace >= requiredSize) {
        return 0;
    }

    uint32_t evictions = 0;
    auto itr = storage_.getFirst();
    while (true) {
        evictions++;

        if (destructorCb) {
            auto *entry = getIteratorEntry(itr);
            destructorCb(entry->hashedKey(), entry->value(),
                         DestructorEvent::Recycled);
        }

        curFreeSpace += RripBucketStorage::slotSize(itr.view().size());
        if (curFreeSpace >= requiredSize) {
            storage_.removeUntil(itr);
            break;
        }
        itr = storage_.getNext(itr);
        assert(!itr.done());
    }
    return evictions;
}

uint32_t RripBucket::makeSpace(HashedKey hk, BufferView value,
                               const RedivideCallback &redivideCb) {
    const auto size = RripBucketEntry::computeSize(value.size());
    const auto requiredSize = RripBucketStorage::slotSize(size);
    assert(requiredSize <= storage_.capacity());

    auto curFreeSpace = storage_.remainingCapacity();
    if (curFreeSpace >= requiredSize) {
        return 0;
    }

    uint32_t evictions = 0;
    auto itr = storage_.getFirst();
    while (true) {
        evictions++;

        auto *entry = getIteratorEntry(itr);
        redivideCb(entry->hashedKey(), entry->value(), itr.rrip());

        curFreeSpace += RripBucketStorage::slotSize(itr.view().size());
        if (curFreeSpace >= requiredSize) {
            storage_.removeUntil(itr);
            break;
        }
        itr = storage_.getNext(itr);
        assert(!itr.done());
    }
    return evictions;
}

uint32_t RripBucket::remove(HashedKey hk,
                            const DestructorCallback &destructorCb) {
    auto itr = storage_.getFirst();
    while (!itr.done()) {
        auto *entry = getIteratorEntry(itr);
        if (entry->keyEqualsTo(hk)) {
            if (destructorCb) {
                destructorCb(entry->hashedKey(), entry->value(),
                             DestructorEvent::Removed);
            }
            storage_.remove(itr);
            return 1;
        }
        itr = storage_.getNext(itr);
    }
    return 0;
}

void RripBucket::reorder(BitVectorReadVisitor isHitCallback) {
    uint32_t keyIdx = 0;
    HashedKey firstMovedKey;
    bool movedKey = false;
    int8_t increment = -1;

    auto itr = storage_.getFirst();
    // 理论上是 RRIP 从高到低排序
    while (!itr.done()) {
        auto *entry = getIteratorEntry(itr);
        bool hit = isHitCallback(keyIdx);
        if (hit) {
            // 命中则将插入最后数据，同时把 RRIP 设为 1
            auto key = entry->hashedKey();
            if (!movedKey) {
                movedKey = true;
                firstMovedKey = key;
            } else if (firstMovedKey == key) {
                break;
            }

            auto value = Buffer(entry->value());
            HashedKey hk = entry->hashedKey();
            BufferView valueView = value.view();
            itr = storage_.remove(itr);
            const auto size = RripBucketEntry::computeSize(valueView.size());
            // promotion rrip so a hit promotes to 0
            auto alloc = storage_.allocate(size, 0);
            RripBucketEntry::create(alloc.view(), hk, valueView);
        } else {
            // 未命中则提高 increment，increment 值按照第一个的未命中的 obj 的 rrip 和最大值的的差值计算
            if (increment == -1) {
                increment = (int8_t)getIncrement(itr.rrip());
            }
            storage_.incrementRrip(itr, increment);
            itr = storage_.getNext(itr);
        }

        keyIdx++;
    }
}

RripBucket::Iterator RripBucket::getFirst() const {
    return Iterator{storage_.getFirst()};
}

RripBucket::Iterator RripBucket::getNext(Iterator itr) const {
    return Iterator{storage_.getNext(itr.itr_)};
}