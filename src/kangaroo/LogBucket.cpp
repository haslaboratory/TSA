#include "kangaroo/LogBucket.h"
#include "common/Utils.h"

static_assert(
    sizeof(KangarooBucketStorage) == 12,
    "KangarooBucketStorage overhead. Changing this may require changing "
    "the sizes used in unit tests as well");

const uint32_t KangarooBucketStorage::kAllocationOverhead =
    sizeof(KangarooBucketStorage::Slot);

// This is very simple as it only tries to allocate starting from the
// tail of the storage. Returns null view() if we don't have any more space.
KangarooBucketStorage::Allocation
KangarooBucketStorage::allocate(uint32_t size) {
    if (!canAllocate(size)) {
        return {};
    }

    auto *slot = new (data_ + endOffset_) Slot(size);
    endOffset_ += slotSize(size);
    numAllocations_++;
    return {MutableBufferView{slot->size, slot->data}, numAllocations_ - 1};
}

void KangarooBucketStorage::remove(Allocation alloc) {
    // Remove triggers a compaction.
    //
    //                         tail
    //  |--------|REMOVED|-----|~~~~|
    //
    // after compaction
    //                  tail
    //  |---------------|~~~~~~~~~~~|
    if (alloc.done()) {
        return;
    }

    const uint32_t removedSize = slotSize(alloc.view().size());
    uint8_t *removed = alloc.view().data() - kAllocationOverhead;
    std::memmove(removed, removed + removedSize,
                 (data_ + endOffset_) - removed - removedSize);
    endOffset_ -= removedSize;
    numAllocations_--;
}

void KangarooBucketStorage::removeUntil(Allocation alloc) {
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

KangarooBucketStorage::Allocation KangarooBucketStorage::getFirst() const {
    if (endOffset_ == 0) {
        return {};
    }
    auto *slot = reinterpret_cast<Slot *>(data_);
    return {MutableBufferView{slot->size, slot->data}, 0};
}

KangarooBucketStorage::Allocation
KangarooBucketStorage::getNext(KangarooBucketStorage::Allocation alloc) const {
    if (alloc.done()) {
        return {};
    }

    auto *next =
        reinterpret_cast<Slot *>(alloc.view().data() + alloc.view().size());
    if (reinterpret_cast<uint8_t *>(next) - data_ >= endOffset_) {
        return {};
    } else if (next->size + reinterpret_cast<uint8_t *>(next) - data_ >=
               endOffset_) {
        return {};
    } else if (next->size == 0) {
        return {};
    }

    return {MutableBufferView{next->size, next->data}, alloc.position() + 1};
}

namespace {
// This maps to exactly how an entry is stored in a bucket on device.
class __attribute__((__packed__)) LogBucketEntry {
public:
    static uint32_t computeSize(uint32_t valueSize) {
        return sizeof(LogBucketEntry) + valueSize;
    }

    static LogBucketEntry &create(MutableBufferView storage, HashedKey hk,
                                  BufferView value) {
        new (storage.data()) LogBucketEntry{hk, value};
        return reinterpret_cast<LogBucketEntry &>(*storage.data());
    }

    HashedKey hashedKey() const { return hashedKey_; }

    bool keyEqualsTo(HashedKey hk) const { return hk == hashedKey_; }

    BufferView value() const { return {valueSize_, data_}; }

private:
    LogBucketEntry(HashedKey hk, BufferView value)
        : hashedKey_{hk}, valueSize_{static_cast<uint16_t>(value.size())} {
        static_assert(sizeof(LogBucketEntry) == 12, "LogBucketEntry overhead");
        value.copyTo(data_);
    }

    const uint64_t hashedKey_{};
    const uint32_t valueSize_{};
    uint8_t data_[];
};

const LogBucketEntry *getIteratorEntry(KangarooBucketStorage::Allocation itr) {
    return reinterpret_cast<const LogBucketEntry *>(itr.view().data());
}
} // namespace

// BufferView LogBucket::Iterator::key() const {
//   return getIteratorEntry(itr_)->key();
// }

HashedKey LogBucket::Iterator::hashedKey() const {
    return getIteratorEntry(itr_)->hashedKey();
}

// uint64_t LogBucket::Iterator::keyHash() const {
//   return getIteratorEntry(itr_)->keyHash();
// }

BufferView LogBucket::Iterator::value() const {
    return getIteratorEntry(itr_)->value();
}

bool LogBucket::Iterator::keyEqualsTo(HashedKey hk) const {
    return getIteratorEntry(itr_)->keyEqualsTo(hk);
}

// bool LogBucket::Iterator::keyEqualsTo(uint64_t keyHash) const {
//   return getIteratorEntry(itr_)->keyEqualsTo(keyHash);
// }

uint32_t LogBucket::computeChecksum(BufferView view) {
    constexpr auto kChecksumStart = sizeof(checksum_);
    auto data = view.slice(kChecksumStart, view.size() - kChecksumStart);
    return simple_checksum(data.data(), data.size());
}

LogBucket &LogBucket::initNew(MutableBufferView view, uint64_t generationTime) {
    return *new (view.data())
        LogBucket(generationTime, view.size() - sizeof(LogBucket));
}

BufferView LogBucket::find(HashedKey hk) const {
    auto itr = storage_.getFirst();
    uint32_t keyIdx = 0;
    while (!itr.done()) {
        auto *entry = getIteratorEntry(itr);
        if (entry->keyEqualsTo(hk)) {
            return entry->value();
        }
        itr = storage_.getNext(itr);
        keyIdx++;
    }
    return {};
}

BufferView LogBucket::findTag(uint32_t tag, HashedKey &hk) const {
    auto itr = storage_.getFirst();
    while (!itr.done()) {
        auto *entry = getIteratorEntry(itr);
        hk = entry->hashedKey();
        if (createTag(hk, 12) == tag) {
            return entry->value();
        }
        itr = storage_.getNext(itr);
    }
    return {};
}

uint32_t LogBucket::insert(HashedKey hk, BufferView value,
                           const DestructorCallback &destructorCb) {
    const auto size = LogBucketEntry::computeSize(value.size());
    assert(size <= storage_.capacity());

    const auto evictions = makeSpace(size, destructorCb);
    auto alloc = storage_.allocate(size);
    assert(!alloc.done());
    LogBucketEntry::create(alloc.view(), hk, value);

    return evictions;
}

void LogBucket::insert(KangarooBucketStorage::Allocation alloc, HashedKey hk,
                       BufferView value) {
    assert(!alloc.done());
    LogBucketEntry::create(alloc.view(), hk, value);
}

KangarooBucketStorage::Allocation LogBucket::allocate(HashedKey hk,
                                                      BufferView value) {
    const auto size = LogBucketEntry::computeSize(value.size());
    assert(size <= storage_.remainingCapacity());

    auto alloc = storage_.allocate(size);
    assert(!alloc.done());
    return alloc;
}

void LogBucket::clear() { storage_.clear(); }

void LogBucket::collect_keys(std::vector<HashedKey> &keys) const {
    auto itr = storage_.getFirst();
    uint32_t keyIdx = 0;
    while (!itr.done()) {
        auto *entry = getIteratorEntry(itr);
        keys.push_back(entry->hashedKey());
        itr = storage_.getNext(itr);
        keyIdx++;
    }
    return;
}

bool LogBucket::isSpace(HashedKey hk, BufferView value) {
    const auto size = LogBucketEntry::computeSize(value.size());
    const auto requiredSize = KangarooBucketStorage::slotSize(size);
    assert(requiredSize <= storage_.capacity());

    auto curFreeSpace = storage_.remainingCapacity();
    return (curFreeSpace >= requiredSize);
}

uint32_t LogBucket::makeSpace(uint32_t size,
                              const DestructorCallback &destructorCb) {
    const auto requiredSize = KangarooBucketStorage::slotSize(size);
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

        curFreeSpace += KangarooBucketStorage::slotSize(itr.view().size());
        if (curFreeSpace >= requiredSize) {
            storage_.removeUntil(itr);
            break;
        }
        itr = storage_.getNext(itr);
        assert(!itr.done());
    }
    return evictions;
}

uint32_t LogBucket::remove(HashedKey hk,
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

void LogBucket::reorder(BitVectorReadVisitor isHitCallback) {
    uint32_t keyIdx = 0;
    auto itr = storage_.getFirst();
    while (!itr.done()) {
        auto *entry = getIteratorEntry(itr);
        bool hit = isHitCallback(keyIdx);
        if (hit) {
            // auto key = Buffer(entry->key());
            auto value = Buffer(entry->value());
            HashedKey hk = entry->hashedKey();
            BufferView valueView = value.view();
            storage_.remove(itr);
            const auto size = LogBucketEntry::computeSize(valueView.size());
            auto alloc = storage_.allocate(size);
            LogBucketEntry::create(alloc.view(), hk, valueView);
        }

        keyIdx++;
        itr = storage_.getNext(itr);
    }
}

LogBucket::Iterator LogBucket::getFirst() const {
    return Iterator{storage_.getFirst()};
}

LogBucket::Iterator LogBucket::getNext(Iterator itr) const {
    return Iterator{storage_.getNext(itr.itr_)};
}