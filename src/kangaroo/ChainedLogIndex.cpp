#include "common/Utils.h"
#include "kangaroo/ChainedLogIndex.h"

void ChainedLogIndex::allocate() {
    {
        assert((numAllocations_+1) * allocationSize_ < ((1 << 20) - 1));
        numAllocations_++;
        allocations.resize(numAllocations_);
        allocations[numAllocations_ - 1] =
            new ChainedLogIndexEntry[allocationSize_];
        // printf("allocate %d\n", numAllocations_);
    }
}

ChainedLogIndex::ChainedLogIndex(uint64_t numHashBuckets,
                                 uint32_t allocationSize,
                                 SetNumberCallback setNumberCb)
    : numMutexes_{numHashBuckets / 10 + 1}, numHashBuckets_{numHashBuckets},
      setNumberCb_{setNumberCb}, allocationSize_{allocationSize} {
    mutexes_ = std::make_unique<std::shared_mutex[]>(numMutexes_);
    index_.resize(numHashBuckets_, -1);
    {
        std::unique_lock<std::shared_mutex> lock{allocationMutex_};
        allocate();
    }
}

ChainedLogIndex::~ChainedLogIndex() {}

ChainedLogIndexEntry *ChainedLogIndex::findEntryNoLock(uint32_t offset) {
    /*if (offset >= maxSlotUsed_) {
      return nullptr;
    }*/
    uint32_t arrayOffset = offset % allocationSize_;
    uint32_t vectorOffset = offset / allocationSize_;
    if (vectorOffset > numAllocations_) {
        return nullptr;
    }
    return &allocations[vectorOffset][arrayOffset];
}

ChainedLogIndexEntry *ChainedLogIndex::findEntry(uint32_t offset) {
    std::shared_lock<std::shared_mutex> lock{allocationMutex_};
    return findEntryNoLock(offset);
}

ChainedLogIndexEntry *ChainedLogIndex::allocateEntry(uint32_t &offset) {
    std::unique_lock<std::shared_mutex> lock{allocationMutex_};
    if (nextEmpty_ >= numAllocations_ * allocationSize_) {
        allocate();
    }
    offset = nextEmpty_;
    ChainedLogIndexEntry *entry = findEntryNoLock(offset);
    if (nextEmpty_ == maxSlotUsed_) {
        nextEmpty_++;
        maxSlotUsed_++;
    } else {
        nextEmpty_ = entry->next_;
    }
    entry->next_ = 0xfffff;
    return entry;
}

uint32_t ChainedLogIndex::releaseEntry(uint32_t offset) {
    std::unique_lock<std::shared_mutex> lock{allocationMutex_};
    ChainedLogIndexEntry *entry = findEntryNoLock(offset);
    uint32_t ret = entry->next_;
    entry->invalidate();
    entry->next_ = nextEmpty_;
    nextEmpty_ = offset;
    return ret;
}

// problem: 查询数据无一致性
PartitionOffset ChainedLogIndex::lookup(HashedKey hk, bool hit,
                                        uint32_t *hits) {
    const auto lib = getLogIndexBucket(hk);
    uint32_t tag = createTag(hk, 12);
    {
        std::shared_lock<std::shared_mutex> lock{getMutex(lib)};
        ChainedLogIndexEntry *currentHead = findEntry(index_[lib.index()]);
        while (currentHead) {
            if (currentHead && currentHead->isValid() &&
                currentHead->tag() == tag) {
                if (hit) {
                    currentHead->incrementHits();
                }
                if (hits != nullptr) {
                    *hits = currentHead->hits();
                }
                return currentHead->offset();
            } else if (!currentHead) {
                break;
            }
            currentHead = findEntry(currentHead->next());
        }
    }
    hits = 0;
    return PartitionOffset(0, false);
}

// problem: 插入数据无一致性
Status ChainedLogIndex::insert(HashedKey hk, PartitionOffset po, uint8_t hits) {
    const auto lib = getLogIndexBucket(hk);
    uint32_t tag = createTag(hk, 12);
    insert(tag, lib, po, hits);
    return Status::Ok;
}

Status ChainedLogIndex::insert(uint32_t tag, KangarooBucketId bid,
                               PartitionOffset po, uint8_t hits) {
    const auto lib = getLogIndexBucketFromSetBucket(bid);
    insert(tag, lib, po, hits);
    return Status::Ok;
}

// 不会删除
Status ChainedLogIndex::insert(uint32_t tag, LogIndexBucket lib,
                               PartitionOffset po, uint8_t hits) {
    {
        std::unique_lock<std::shared_mutex> lock{getMutex(lib)};

        ChainedLogIndexEntry *oldEntry = nullptr;
        ChainedLogIndexEntry *nextEntry = findEntry(index_[lib.index()]);
        while (nextEntry) {
            if (nextEntry->isValid() && nextEntry->tag() == tag) {
                // 并不应当覆盖老值，而是应当最后选择最老的值
                nextEntry->populateEntry(po, tag, hits);
                return Status::Ok;
            }
            oldEntry = nextEntry;
            nextEntry = findEntry(oldEntry->next_);
        }

        uint32_t entryOffset;
        ChainedLogIndexEntry *newEntry = allocateEntry(entryOffset);
        newEntry->populateEntry(po, tag, hits);
        if (oldEntry != nullptr) {
            oldEntry->next_ = entryOffset;
        } else {
            index_[lib.index()] = entryOffset;
        }
    }
    return Status::Ok;
}

Status ChainedLogIndex::remove(HashedKey hk, PartitionOffset po) {
    uint64_t tag = createTag(hk, 12);
    const auto lib = getLogIndexBucket(hk);
    return remove(tag, lib, po);
}

Status ChainedLogIndex::remove(uint64_t tag, KangarooBucketId bid,
                               PartitionOffset po) {
    auto lib = getLogIndexBucketFromSetBucket(bid);
    return remove(tag, lib, po);
}

Status ChainedLogIndex::remove(uint64_t tag, LogIndexBucket lib,
                               PartitionOffset po) {
    {
        std::unique_lock<std::shared_mutex> lock{getMutex(lib)};

        ChainedLogIndexEntry *oldEntry = nullptr;
        ChainedLogIndexEntry *nextEntry = findEntry(index_[lib.index()]);
        while (nextEntry) {
            if (nextEntry->isValid() && nextEntry->tag() == tag &&
                nextEntry->offset() == po) {
                // *oldNext = releaseEntry(*oldNext);
                if (oldEntry!= nullptr) {
                    oldEntry->next_ = releaseEntry(oldEntry->next_);
                } else {
                    index_[lib.index()] = releaseEntry(index_[lib.index()]);
                }
                return Status::Ok;
            }
            oldEntry = nextEntry;
            nextEntry = findEntry(nextEntry->next_);
        }
    }
    return Status::NotFound;
}

// Counts number of items in log corresponding to bucket
uint64_t ChainedLogIndex::countBucket(HashedKey hk) {
    const auto lib = getLogIndexBucket(hk);
    uint64_t count = 0;
    {
        std::shared_lock<std::shared_mutex> lock{getMutex(lib)};
        ChainedLogIndexEntry *nextEntry = findEntry(index_[lib.index()]);
        while (nextEntry) {
            if (nextEntry->isValid()) {
                count++;
            }
            nextEntry = findEntry(nextEntry->next_);
        }
    }
    return count;
}

// Get iterator for all items in the same bucket
ChainedLogIndex::BucketIterator
ChainedLogIndex::getHashBucketIterator(HashedKey hk) {
    auto idx = setNumberCb_(hk);
    return getHashBucketIterator(idx);
}

ChainedLogIndex::BucketIterator
ChainedLogIndex::getHashBucketIterator(KangarooBucketId bid) {
    const auto lib = getLogIndexBucketFromSetBucket(bid);
    {
        std::shared_lock<std::shared_mutex> lock{getMutex(lib)};
        auto currentHead = findEntry(index_[lib.index()]);
        while (currentHead) {
            if (currentHead->isValid()) {
                return BucketIterator(bid, currentHead);
            }
            currentHead = findEntry(currentHead->next_);
        }
    }
    return BucketIterator();
}

ChainedLogIndex::BucketIterator
ChainedLogIndex::getNext(ChainedLogIndex::BucketIterator bi) {
    if (bi.done()) {
        return bi;
    }
    auto lib = getLogIndexBucketFromSetBucket(bi.bucket_);
    {
        std::shared_lock<std::shared_mutex> lock{getMutex(lib)};
        auto currentHead = findEntry(bi.nextEntry_);
        if (currentHead && currentHead->isValid()) {
            return BucketIterator(bi.bucket_, currentHead);
        } else {
            return BucketIterator();
        }
    }
}

PartitionOffset ChainedLogIndex::find(KangarooBucketId bid, uint64_t tag) {
    auto lib = getLogIndexBucketFromSetBucket(bid);
    {
        std::shared_lock<std::shared_mutex> lock{getMutex(lib)};
        // ChainedLogIndexEntry *oldEntry = nullptr;
        ChainedLogIndexEntry *nextEntry = findEntry(index_[lib.index()]);
        while (nextEntry) {
            if (nextEntry->isValid() && nextEntry->tag() == tag) {
                PartitionOffset po = nextEntry->offset();
                return po;
            }
            // oldEntry = nextEntry;
            nextEntry = findEntry(nextEntry->next_);
        }
    }
    return PartitionOffset(0, false);
}

ChainedLogIndex::LogIndexBucket
ChainedLogIndex::getLogIndexBucket(HashedKey hk) {
    return getLogIndexBucketFromSetBucket(setNumberCb_(hk));
}

//   ChainedLogIndex::LogIndexBucket ChainedLogIndex::getLogIndexBucket(uint64_t
//   key) {
//     return getLogIndexBucketFromSetBucket(setNumberCb_(key));
//   }

ChainedLogIndex::LogIndexBucket
ChainedLogIndex::getLogIndexBucketFromSetBucket(KangarooBucketId bid) {
    return LogIndexBucket(bid.index() % numHashBuckets_);
}