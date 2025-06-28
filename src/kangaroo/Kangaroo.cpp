#include "kangaroo/Kangaroo.h"
#include "common/Rand.h"

namespace {
constexpr uint64_t kMinSizeDistribution = 64;
constexpr uint64_t kMinThresholdSizeDistribution = 8;
constexpr double kSizeDistributionGranularityFactor = 1.25;
} // namespace

constexpr uint32_t Kangaroo::kFormatVersion;

Kangaroo::Config &Kangaroo::Config::validate() {
    if (totalSetSize < bucketSize) {
        printf("cache size: %ld cannot be smaller than bucket size: %d\n",
               totalSetSize, bucketSize);
        exit(-1);
    }

    //   if (!folly::isPowTwo(bucketSize)) {
    //     throw std::invalid_argument(
    //         folly::sformat("invalid bucket size: {}", bucketSize));
    //   }

    if (totalSetSize > uint64_t{bucketSize} << 32) {
        printf("Can't address Kangaroo with 32 bits. Cache size: %ld, bucket "
               "size: "
               "%d\n",
               totalSetSize, bucketSize);
        exit(-1);
    }

    if (cacheBaseOffset % bucketSize != 0 || totalSetSize % bucketSize != 0) {
        printf("cacheBaseOffset and totalSetSize need to be a multiple of "
               "bucketSize. "
               "cacheBaseOffset: %ld, totalSetSize:%ld, bucketSize: %d.\n",
               cacheBaseOffset, totalSetSize, bucketSize);
        exit(-1);
    }

    if (device == nullptr) {
        printf("device cannot be null\n");
        exit(-1);
    }

    if (rripBitVector == nullptr) {
        printf("need a RRIP bit vector\n");
        exit(-1);
    }

    if (bloomFilter && bloomFilter->numFilters() != numBuckets()) {
        printf("bloom filter #filters mismatch #buckets: %d vs %ld\n",
               bloomFilter->numFilters(), numBuckets());
        exit(-1);
    }

    if (logConfig.logSize > 0 && avgSmallObjectSize == 0) {
        printf("Need an avgSmallObjectSize for the log\n");
        exit(-1);
    }

    if (hotBucketSize >= bucketSize) {
        printf("Hot bucket size %d needs to be less than bucket size %d\n",
               hotBucketSize, bucketSize);
        exit(-1);
    }

    if (hotSetSize >= totalSetSize) {
        printf("Hot cache size %ld needs to be less than total set size %ld\n",
               hotSetSize, totalSetSize);
        exit(-1);
    }
    return *this;
}

Kangaroo::Kangaroo(Config &&config)
    : Kangaroo{std::move(config.validate()), ValidConfigTag{}} {}

Kangaroo::Kangaroo(Config &&config, ValidConfigTag)
    : numCleaningThreads_{config.mergeThreads},
      destructorCb_{[this, cb = std::move(config.destructorCb)](
                        HashedKey hk, BufferView value, DestructorEvent event) {
          // sizeDist_.removeSize(hk.key().size() + value.size());
          if (cb) {
              printf("Calling destructor callback\n");
              cb(hk, value, event);
          }
      }},
      bucketSize_{config.bucketSize}, hotBucketSize_{config.hotBucketSize},
      cacheBaseOffset_{config.cacheBaseOffset},
      hotCacheBaseOffset_{config.hotBaseOffset()},
      numBuckets_{config.numBuckets()}, bloomFilter_{std::move(
                                            config.bloomFilter)},
      bitVector_{std::move(config.rripBitVector)}, device_{*config.device},
      wrenDevice_{new Wren(device_, numBuckets_, bucketSize_ - hotBucketSize_,
                           config.totalSetSize - config.hotSetSize,
                           cacheBaseOffset_)},
      wrenHotDevice_{new Wren(device_, numBuckets_, (hotBucketSize_ == 0 ? 4096:hotBucketSize_),
                              config.hotSetSize, hotCacheBaseOffset_)},
      fwOptimizations_{config.fwOptimizations},
      enable_hot_(config.hotBucketSize > 0)
{
    printf("Kangaroo created: buckets: 0x%lx, bucket size(hot + cold): 0x%lx, "
           "base offset: 0x%lx, "
           "hot base offset 0x%lx\n",
           numBuckets_, bucketSize_, cacheBaseOffset_, hotCacheBaseOffset_);
    printf(
        "Kangaroo created: base offset zone: %lf, hot base offset zone %lf\n",
        cacheBaseOffset_ / (double)device_.getIOZoneSize(),
        hotCacheBaseOffset_ / (double)device_.getIOZoneSize());

    printf("Kangaroo enbale-hot: %d\n", enable_hot_);
    if (config.logConfig.logSize) {
        SetNumberCallback cb = [&](uint64_t hk) {
            return getKangarooBucketIdFromHash(hk);
        };
        config.logConfig.setNumberCallback = cb;
        config.logConfig.logIndexPartitions =
            config.logIndexPartitionsPerPhysical *
            config.logConfig.logPhysicalPartitions;
        // uint64_t bytesPerIndex =
        //     config.logConfig.logSize / config.logConfig.logIndexPartitions;
        config.logConfig.device = config.device;
        fwLog_ = std::make_unique<FwLog>(std::move(config.logConfig));
    }

    reset();

    cleaningThreads_.reserve(numCleaningThreads_);
    for (uint64_t i = 0; i < numCleaningThreads_; i++) {
        if (!i) {
            cleaningThreads_.push_back(
                std::thread(&Kangaroo::cleanSegmentsLoop, this));
        } else {
            cleaningThreads_.push_back(
                std::thread(&Kangaroo::cleanSegmentsWaitLoop, this));
        }
    }
}

void Kangaroo::reset() {
    printf("Reset Kangaroo\n");
    generationTime_ = getSteadyClock();

    if (bloomFilter_) {
        bloomFilter_->reset();
    }
}

// move objects from cold bucket to hot bucket
void Kangaroo::redivideBucket(RripBucket *hotBucket, RripBucket *coldBucket) {
    std::vector<std::unique_ptr<ObjectInfo>>
        movedKeys; // using hits as rrip value
    RedivideCallback moverCb = [&](HashedKey hk, BufferView value,
                                   uint8_t rrip_value) {
        auto ptr =
            std::make_unique<ObjectInfo>(hk, value, rrip_value, LogPageId(), 0);
        movedKeys.push_back(std::move(ptr));
        return;
    };

    RripBucket::Iterator it = coldBucket->getFirst();
    while (!it.done()) {

        // lower rrip values (ie highest pri objects) are at the end
        // 找到最低 RRIP 值的高优先级对象
        RripBucket::Iterator nextIt = coldBucket->getNext(it);
        while (!nextIt.done()) {
            nextIt = coldBucket->getNext(nextIt);
        }

        bool space =
            hotBucket->isSpaceRrip(it.hashedKey(), it.value(), it.rrip());
        if (!space) {
            break;
        }

        hotBucket->makeSpace(it.hashedKey(), it.value(), moverCb);
        hotBucket->insertRrip(it.hashedKey(), it.value(), it.rrip(), nullptr);
        coldBucket->remove(it.hashedKey(), nullptr);
        it = coldBucket->getFirst(); // just removed the last object
    }

    // move objects into cold bucket (if there's any overflow evict lower pri
    // items)
    for (auto &oi : movedKeys) {
        coldBucket->insertRrip(oi->key, oi->value.view(), oi->hits, nullptr);
    }
}

// TODO: bf rebulid 和 clear 有什么关系
void Kangaroo::moveBucket(KangarooBucketId kbid, bool logFlush, int gcMode) {
    uint64_t insertCount = 0;
    uint64_t evictCount = 0;
    uint64_t removedCount = 0;
    uint64_t dropCount = 0;

    // uint64_t passedItemSize = 0;
    uint64_t passedCount = 0;

    std::vector<std::unique_ptr<ObjectInfo>> ois;
    {
        std::unique_lock<std::shared_mutex> lock{getMutex(kbid)};

        uint64_t read_start = RdtscTimer::instance().us();
        if (logFlush || fwOptimizations_) {
            // 查询 Index 获取相同 kbid 中的 objects，**会尝试吸收整个 Log
            // 区的数据**
            ois = fwLog_->getObjectsToMove(kbid, logFlush);
        }
        if (!ois.size() && logFlush) {
            return; // still need to move bucket if gc caused
        }

        auto coldBuffer = readBucket(kbid, false);
        if (coldBuffer.isNull()) {
            printf("coldBuffer is null\n");
            return;
        }
        auto *coldBucket = reinterpret_cast<RripBucket *>(coldBuffer.data());

        auto hotBuffer = readBucket(kbid, true);
        if (hotBuffer.isNull()) {
            printf("hotBuffer is null\n");
            return;
        }
        auto *hotBucket = reinterpret_cast<RripBucket *>(hotBuffer.data());

        perfCounter_.timeMovedRead.fetch_add(RdtscTimer::instance().us() - read_start);

        uint32_t hotCount = hotBucket->size();

        coldBucket->reorder(
            [&](uint32_t keyIdx) { return bvGetHit(kbid, hotCount + keyIdx); });

        uint32_t random = rand32() % hotRebuildFreq_;
        if (!random && enable_hot_) {
            // need to rebuild hot-cold seperation

            hotBucket->reorder(
                [&](uint32_t keyIdx) { return bvGetHit(kbid, keyIdx); });
            redivideBucket(hotBucket, coldBucket);
        }
        bitVector_->clear(kbid.index());

        if ((!random && enable_hot_)|| gcMode == 2) {
            // rewrite hot bucket if either gc caused by hot sets or redivide
            uint64_t hotCount = hotBucket->size();

            if (bloomFilter_) {
                // 如果需要迁移 hot，则清空并重建 bloom filter
                bfRebuild(kbid, hotBucket);
            }

            uint64_t write_start = RdtscTimer::instance().us();
            const auto res = writeBucket(kbid, std::move(hotBuffer), true);
            uint64_t write_end = RdtscTimer::instance().us();
            perfCounter_.timeSetWritten.fetch_add(write_end - write_start);
            // printf("write time: %ld\n", write_end - write_start);
            if (!res) {
                printf("write hot bucket failed\n");
                dropCount += hotCount;
            }
            // if (res && (logFlush || fwOptimizations_)) {
            if (res) {
                perfCounter_.setPageWritten.fetch_add(1);
            }
            if (res && (gcMode != 0)) {
                perfCounter_.gcWritten.fetch_add(1);
            }
        } else {
            if (bloomFilter_) {
                // 如果不需要迁移 hot，则只清空 bloom filter
                bloomFilter_->clear(kbid.index());
            }
        }

        // only insert new objects into cold set
        // 直接访问频次按照插入
        for (auto &oi : ois) {
            // passedItemSize += oi->key.key().size() + oi->value.size();
            passedCount++;

            if (coldBucket->isSpace(oi->key, oi->value.view(), oi->hits)) {
                removedCount += coldBucket->remove(oi->key, nullptr);
                evictCount += coldBucket->insert(oi->key, oi->value.view(),
                                                 oi->hits, nullptr);
                // sizeDist_.addSize(oi->key.key().size() + oi->value.size());
                insertCount++;
            } else {
                fwLog_->readmit(oi);
            }
        }

        uint64_t coldCount = coldBucket->size();

        if (bloomFilter_) {
            // 之前已经清空，只需要重填 bloom filter 即可
            bfBuild(kbid, coldBucket);
        }

        uint64_t write_start = RdtscTimer::instance().us();
        const auto res = writeBucket(kbid, std::move(coldBuffer), false);
        uint64_t write_end = RdtscTimer::instance().us();
        perfCounter_.timeSetWritten.fetch_add(write_end - write_start);
        // printf("write time: %ld\n", write_end - write_start);
        if (!res) {
            printf("write cold bucket failed\n");
            dropCount += coldCount;
        }

        // if (res && (logFlush || fwOptimizations_)) {
        if (res) {
            perfCounter_.setPageWritten.fetch_add(1);
        }
        if (res && (gcMode != 0)) {
            perfCounter_.gcWritten.fetch_add(1);
        }

        // if (res && (logFlush || fwOptimizations_)) {
        if (res) {
            perfCounter_.logObjAbsorbed.fetch_add(insertCount);
        }
    }
    perfCounter_.logObjs.fetch_sub(insertCount);
    perfCounter_.setObjs.fetch_add(insertCount);
    perfCounter_.setObjs.fetch_sub(evictCount + removedCount + dropCount);
    return;
}

uint64_t Kangaroo::getMaxItemSize() const {
    // does not include per item overhead
    return bucketSize_ - sizeof(RripBucket);
}

Status Kangaroo::insert(HashedKey hk, BufferView value) {
    // const auto bid = getKangarooBucketId(hk);

    Status ret = fwLog_->insert(hk, value);
    if (ret == Status::Ok) {
        perfCounter_.objInserted.fetch_add(1);
        perfCounter_.logObjs.fetch_add(1);
    }
    return ret;
}

Status Kangaroo::lookup(HashedKey hk, Buffer *value) {
    const auto bid = getKangarooBucketId(hk);
    // lookupCount_.inc();

    // first check log if it exists
    if (fwLog_) {
        Status ret = fwLog_->lookup(hk, value);
        if (ret == Status::Ok) {
            //   succLookupCount_.inc();
            //   logHits_.inc();
            return ret;
        }
    }

    RripBucket *bucket{nullptr};
    Buffer buffer;
    BufferView valueView;
    // scope of the lock is only needed until we read and mutate state for the
    // bucket. Once the bucket is read, the buffer is local and we can find
    // without holding the lock.
    {
        std::shared_lock<std::shared_mutex> lock{getMutex(bid)};

        if (bfReject(bid, hk)) {
            perfCounter_.bfMissCnt.fetch_add(1);
            return Status::NotFound;
        }

        buffer = readBucket(bid, true);
        if (buffer.isNull()) {
            // ioErrorCount_.inc();
            return Status::DeviceError;
        }

        bucket = reinterpret_cast<RripBucket *>(buffer.data());

        /* TODO: moving this inside lock could cause performance problem */
        valueView =
            bucket->find(hk, [&](uint32_t keyIdx) { bvSetHit(bid, keyIdx); });

        // uint64_t hotItems = 0;
        if (valueView.isNull()) {
            uint64_t hotItems = bucket->size();
            buffer = readBucket(bid, false);
            if (buffer.isNull()) {
                // ioErrorCount_.inc();
                return Status::DeviceError;
            }

            bucket = reinterpret_cast<RripBucket *>(buffer.data());

            /* TODO: moving this inside lock could cause performance problem */
            valueView = bucket->find(
                hk, [&](uint32_t keyIdx) { bvSetHit(bid, keyIdx + hotItems); });
        } else {
            // hotSetHits_.inc();
        }
    }

    if (valueView.isNull()) {
        // bfFalsePositiveCount_.inc();
        // if (bid.index() % 20000 == 10) {
        //     printf("valueView is null bid %d, key %ld\n", bid.index(), hk);
        // }
        perfCounter_.readMissCnt.fetch_add(1);
        return Status::NotFound;
    }
    // if (bid.index() % 20000 == 10) {
    //     printf("Found a set hit bucket %d, key %ld\n", bid.index(), hk);
    // }
    *value = Buffer{valueView};
    //   succLookupCount_.inc();
    //   setHits_.inc();
    return Status::Ok;
}

Status Kangaroo::remove(HashedKey hk) {
    const auto bid = getKangarooBucketId(hk);
    // removeCount_.inc();

    if (fwLog_) {
        Status ret = fwLog_->remove(hk);
        if (ret == Status::Ok) {
            return ret;
        }
    }

    {
        std::unique_lock<std::shared_mutex> lock{getMutex(bid)};
        if (bfReject(bid, hk)) {
            perfCounter_.bfMissCnt.fetch_add(1);
            return Status::NotFound;
        }

        // Get hot bucket first
        auto buffer = readBucket(bid, true);
        if (buffer.isNull()) {
            // ioErrorCount_.inc();
            return Status::DeviceError;
        }
        auto *bucket = reinterpret_cast<RripBucket *>(buffer.data());
        bucket->reorder([&](uint32_t keyIdx) { return bvGetHit(bid, keyIdx); });

        // Get cold bucket
        uint64_t hotItems = bucket->size();
        auto coldBuffer = readBucket(bid, false);
        if (coldBuffer.isNull()) {
            // ioErrorCount_.inc();
            return Status::DeviceError;
        }
        auto *coldBucket = reinterpret_cast<RripBucket *>(coldBuffer.data());
        coldBucket->reorder(
            [&](uint32_t keyIdx) { return bvGetHit(bid, keyIdx + hotItems); });

        if (!bucket->remove(hk, destructorCb_)) {
            if (!coldBucket->remove(hk, destructorCb_)) {
                // bfFalsePositiveCount_.inc();
                return Status::NotFound;
            }
        }

        bool res;
        if (coldBucket) {
            res = writeBucket(bid, std::move(coldBuffer), false);
        } else {
            res = writeBucket(bid, std::move(buffer), true);
        }

        if (!res) {
            if (bloomFilter_) {
                bloomFilter_->clear(bid.index());
            }
            // ioErrorCount_.inc();
            return Status::DeviceError;
        }

        if (bloomFilter_) {
            bfRebuild(bid, bucket);
            bfBuild(bid, coldBucket);
        }
        bitVector_->clear(bid.index());
    }

    // We do not bump logicalWrittenCount_ because logically a
    // remove operation does not write, but for Kangaroo, it does
    // incur physical writes.
    //   physicalWrittenCount_.add(bucketSize_);
    //   succRemoveCount_.inc();
    return Status::Ok;
}

Status Kangaroo::prefill(
    std::function<uint64_t ()> k_func,
    std::function<BufferView ()> v_func
) {
    Status status = Status::Ok;

    status = fwLog_->prefill(k_func, v_func);
    if (status != Status::Ok) {
        return status;
    }

    // middle layer
    for (uint32_t i = 0; i < numBuckets_; i++) {
        Buffer buffer = readBucket(KangarooBucketId{i}, false);
        std::unordered_set<HashedKey> keys;
        RripBucket *bucket = reinterpret_cast<RripBucket *>(buffer.data());
        while (true) {
            HashedKey hk = k_func();
            // 将 key 和对应 set 对齐
            hk = hk - (hk % numBuckets_) + i;
            assert(getKangarooBucketId(hk).index() == i);
            if (keys.find(hk) != keys.end()) {
                continue;
            }
            BufferView v = v_func();
            if (bucket->isSpacePrefill(hk, v)) {
                bucket->remove(hk, nullptr);
                bucket->insert(hk, v, 0, nullptr);
                keys.insert(hk);
            } else {
                break;
            }
        }
        bfRebuild(KangarooBucketId{i}, bucket);
        auto res = writeBucket(KangarooBucketId{i}, std::move(buffer), false);
        if (!res) {
            return Status::DeviceError;
        }

        if (!enable_hot_) {
            continue;
        }

        // bottom layer
        buffer = readBucket(KangarooBucketId{i}, true);
        bucket = reinterpret_cast<RripBucket *>(buffer.data());
        while (true) {
            HashedKey hk = k_func();
            // 将 key 和对应 set 对齐
            hk = hk - (hk % numBuckets_) + i;
            assert(getKangarooBucketId(hk).index() == i);
            if (keys.find(hk) != keys.end()) {
                continue;
            }
            BufferView v = v_func();
            if (bucket->isSpacePrefill(hk, v)) {
                bucket->remove(hk, nullptr);
                bucket->insert(hk, v, 0, nullptr);
                keys.insert(hk);
            } else {
                break;
            }
        }
        bfBuild(KangarooBucketId{i}, bucket);
        res = writeBucket(KangarooBucketId{i}, std::move(buffer), true);
        if (!res) {
            return Status::DeviceError;
        }
    }

    printf("Prefill done!\n");

    return status;
}

bool Kangaroo::couldExist(HashedKey hk) {
    const auto bid = getKangarooBucketId(hk);
    bool canExist = false;

    if (fwLog_) {
        canExist = fwLog_->couldExist(hk);
    }

    if (!canExist) {
        std::shared_lock<std::shared_mutex> lock{getMutex(bid)};
        canExist = !bfReject(bid, hk);
    }

    // the caller is not likely to issue a subsequent lookup when we return
    // false. hence tag this as a lookup. If we return the key can exist, the
    // caller will perform a lookupAsync and will be counted within lookup api.
    if (!canExist) {
        // lookupCount_.inc();
    }

    return canExist;
}

bool Kangaroo::bfReject(KangarooBucketId bid, uint64_t keyHash) const {
    if (bloomFilter_) {
        // bfProbeCount_.inc();
        if (!bloomFilter_->couldExist(bid.index(), keyHash)) {
            // bfRejectCount_.inc();
            return true;
        }
    }
    return false;
}

bool Kangaroo::bvGetHit(KangarooBucketId bid, uint32_t keyIdx) const {
    if (bitVector_) {
        return bitVector_->get(bid.index(), keyIdx);
    }
    return false;
}

void Kangaroo::bvSetHit(KangarooBucketId bid, uint32_t keyIdx) const {
    if (bitVector_) {
        bitVector_->set(bid.index(), keyIdx);
    }
}

void Kangaroo::bfRebuild(KangarooBucketId bid, const RripBucket *bucket) {
    assert(bloomFilter_ != nullptr);
    assert(bucket != nullptr);
    bloomFilter_->clear(bid.index());
    auto itr = bucket->getFirst();
    uint32_t i = 0;
    uint32_t total = bucket->size();
    while (!itr.done() && i < total) {
        if (i >= total) {
            printf("Bucket %d: has only %d items, iterating through %d, not "
                   "done %d\n",
                   bid.index(), total, i, itr.done());
        }
        bloomFilter_->set(bid.index(), itr.hashedKey());
        itr = bucket->getNext(itr);
        i++;
    }
}

void Kangaroo::bfBuild(KangarooBucketId bid, const RripBucket *bucket) {
    assert(bloomFilter_ != nullptr);
    auto itr = bucket->getFirst();
    while (!itr.done()) {
        bloomFilter_->set(bid.index(), itr.hashedKey());
        itr = bucket->getNext(itr);
    }
}

void Kangaroo::flush() {
    printf("Flush big hash\n");
    device_.flush();
}

Buffer Kangaroo::readBucket(KangarooBucketId bid, bool hot) {
    Buffer buffer;
    bool newBuffer = false;
    if (hot) {
        buffer = wrenHotDevice_->read(bid, newBuffer);
    } else {
        buffer = wrenDevice_->read(bid, newBuffer);
    }

    if (buffer.isNull()) {
        return {};
    }

    auto *bucket = reinterpret_cast<RripBucket *>(buffer.data());

    uint32_t checksum = RripBucket::computeChecksum(buffer.view());

    const auto checksumSuccess = (checksum == bucket->getChecksum());
    // We can only know for certain this is a valid checksum error if bloom
    // filter is already initialized. Otherwise, it could very well be because
    // we're reading the bucket for the first time.
    if (!newBuffer && !checksumSuccess && bloomFilter_) {
        printf("Checksum error for bucket %d compute: %d actual: %d\n", bid.index(), checksum, bucket->getChecksum());
        // checksumErrorCount_.inc();
    }

    if (!checksumSuccess || newBuffer ||
        static_cast<uint64_t>(generationTime_.count()) !=
            bucket->generationTime()) {
        RripBucket::initNew(buffer.mutableView(), generationTime_.count());
    }
    return buffer;
}

bool Kangaroo::writeBucket(KangarooBucketId bid, Buffer buffer, bool hot) {
    bool ret = false;
    auto *bucket = reinterpret_cast<RripBucket *>(buffer.data());
    bucket->setChecksum(RripBucket::computeChecksum(buffer.view()));
    if (hot) {
        ret = wrenHotDevice_->write(bid, std::move(buffer));
    } else {
        ret = wrenDevice_->write(bid, std::move(buffer));
    }

    return ret;
}

Kangaroo::PerfCounter &Kangaroo::perfCounter() { return perfCounter_; }

bool Kangaroo::shouldLogFlush() {
    return fwLog_->shouldClean(flushingThreshold_);
}

bool Kangaroo::shouldUpperGC(bool hot) {
    if (hot) {
        return wrenHotDevice_->shouldClean(gcUpperThreshold_);
    } else {
        return wrenDevice_->shouldClean(gcUpperThreshold_);
    }
}

bool Kangaroo::shouldLowerGC(bool hot) {
    if (hot) {
        return wrenHotDevice_->shouldClean(gcLowerThreshold_);
    } else {
        return wrenDevice_->shouldClean(gcLowerThreshold_);
    }
}

// 实际只有一个 Flush 线程或者一个 GC 线程获取到锁
void Kangaroo::performLogFlush() {
    {
        std::unique_lock<std::mutex> lock{cleaningSync_};
        cleaningSyncThreads_++;
    }

    while (true) {
        KangarooBucketId kbid = KangarooBucketId(0);
        {
            std::unique_lock<std::mutex> lock{cleaningSync_};
            if (performingLogFlush_ && !fwLog_->cleaningDone()) {
                // 只是遍历 bucket 而已，未尝试合并相同 hash key
                kbid = fwLog_->getNextCleaningBucket();
            } else {
                // 说明 Log Flush 完成
                performingLogFlush_ = false;
                break;
            }
        }
        moveBucket(kbid, true, 0);
    }

    {
        std::unique_lock<std::mutex> lock{cleaningSync_};
        cleaningSyncThreads_--;
        if (cleaningSyncThreads_ == 0) {
            cleaningSyncCond_.notify_all();
        }
    }
}

void Kangaroo::performGC(int gcMode) {
    {
        std::unique_lock<std::mutex> lock{cleaningSync_};
        cleaningSyncThreads_++;
    }

    bool done = false;
    while (!done) {
        KangarooBucketId kbid = KangarooBucketId(0);
        {
            // 以 Bucket 为单位进行 GC
            std::unique_lock<std::mutex> lock{cleaningSync_};
            if (performingGC_ && !euIterator_.done()) {
                kbid = euIterator_.getBucket();

                perfCounter_.gcPages.fetch_add(1);

                if (gcMode == 2) {
                    euIterator_ = wrenHotDevice_->getNext(euIterator_);
                } else {
                    euIterator_ = wrenDevice_->getNext(euIterator_);
                }
            } else {
                // 说明 GC 完成
                performingGC_ = 0;
                break;
            }
        }
        assert(gcMode != 0);
        moveBucket(kbid, false, gcMode);
    }

    {
        std::unique_lock<std::mutex> lock{cleaningSync_};
        cleaningSyncThreads_--;
        if (cleaningSyncThreads_ == 0) {
            cleaningSyncCond_.notify_all();
        }
    }
}

void Kangaroo::gcSetupTeardown(bool hot) {
    perfCounter_.warm_ok.store(true);
    uint64_t gc_start = RdtscTimer::instance().us();
    if (hot) {
        // XLOG(INFO, "Starting to clean HOT wren device");
        euIterator_ = wrenHotDevice_->getEuIterator();
        performingGC_ = 2;
    } else {
        // XLOG(INFO, "Starting to clean cold wren device");
        euIterator_ = wrenDevice_->getEuIterator();
        performingGC_ = 1;
    }
    cleaningSyncCond_.notify_all();
    performGC(hot ? 2:1);

    {
        std::unique_lock<std::mutex> lock{cleaningSync_};
        while (cleaningSyncThreads_ != 0) {
            cleaningSyncCond_.wait(lock);
        }
    }

    uint64_t gc_pages = perfCounter_.gcPages.exchange(0);
    printf("GC %s Success(%d): time %lf s!, gc ratio: %lf\n",
        hot ? "hot":"cold",
        fwOptimizations_,
        (RdtscTimer::instance().us() - gc_start) / 1000.0 / 1000.0,
        (double)gc_pages / ((double)device_.getIOZoneCapSize() / 4096)
    );
    if (hot) {
        wrenHotDevice_->erase();
    } else {
        wrenDevice_->erase();
    }
}

void Kangaroo::cleanSegmentsLoop() {
    // XLOG(INFO) << "Starting cleanSegmentsLooop";
    while (true) {
        if (stop_flag_) {
            break;
        }

        if (shouldLowerGC(false)) {
            gcSetupTeardown(false);
        } else if (shouldLowerGC(true)) {
            // sometimes need to clean hot sets
            gcSetupTeardown(true);
        } else if (shouldLogFlush()) {
            // TODO: update locking mechanism
            // uint64_t read_start = clock();
            fwLog_->startClean();
            // perfCounter_.timeMovedRead.fetch_add(clock() - read_start);

            performingLogFlush_ = true;
            cleaningSyncCond_.notify_all();

            performLogFlush();
            {
                std::unique_lock<std::mutex> lock{cleaningSync_};
                while (cleaningSyncThreads_ != 0) {
                    cleaningSyncCond_.wait(lock);
                }
            }
            if (fwLog_->finishClean()) {
                printf("Set Written: %ld, Log Obj Absorbed: %ld, Log Obj Inserted: %ld\n",
                       perfCounter_.setPageWritten.load(),
                       perfCounter_.logObjAbsorbed.load(),
                       perfCounter_.objInserted.load());
                
                FwLog::PerfCounter &fwLogPerfCounter = fwLog_->perfCounter();
                printf("logicalReadmitCnt: %ld, pysicalReadmitCnt: %ld\n",
                    fwLogPerfCounter.logicalReadmitCnt.load(), fwLogPerfCounter.physicalReadmitCnt.load());

                printf("Move Read Time: %lf, Move Write Time: %lf\n",
                       perfCounter_.timeMovedRead.load() / 1000.0 / 1000.0,
                       perfCounter_.timeSetWritten.load() / 1000.0 / 1000.0);
            }
        } else if (shouldUpperGC(false)) {
            gcSetupTeardown(false);
        }
    }
    cleaningSyncCond_.notify_all();
}

void Kangaroo::cleanSegmentsWaitLoop() {
    // TODO: figure out locking
    while (true) {
        if (stop_flag_) {
            break;
        }

        if (performingGC_) {
            performGC(performingGC_);
        } else if (performingLogFlush_) {
            performLogFlush();
        }

        {
            std::unique_lock<std::mutex> lock{cleaningSync_};
            cleaningSyncCond_.wait(lock);
        }
    }
    exit(0);
}

Kangaroo::~Kangaroo() {
    stop_flag_ = true;
    for (uint32_t i = 0; i < numCleaningThreads_; i++) {
        cleaningThreads_[i].join();
    }
}

FwLog::PerfCounter &Kangaroo::fwLogPerfCounter() { return fwLog_->perfCounter(); }