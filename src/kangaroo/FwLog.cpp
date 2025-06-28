#include <cassert>
#include <cstdlib>
#include <unistd.h>

#include "common/Rand.h"
#include "kangaroo/FwLog.h"

Buffer FwLog::readLogPage(LogPageId lpid) {
    auto buffer = device_.makeIOBuffer(pageSize_);
    assert(!buffer.isNull());

    const bool res =
        device_.read(getLogPageOffset(lpid), buffer.size(), buffer.data());
    if (!res) {
        return {};
    }
    // TODO: checksumming & generations
    return buffer;
}

Buffer FwLog::readLogSegment(LogSegmentId lsid) {
    auto buffer = device_.makeIOBuffer(segmentSize_);
    assert(!buffer.isNull());

    uint64_t offset = getLogSegmentOffset(lsid);
    const bool res = device_.read(offset, buffer.size(), buffer.data());

    if (!res) {
        printf("Read log segment failed\n");
        return {};
    }
    // TODO: checksumming & generations
    return buffer;
}

bool FwLog::writeLogSegment(LogSegmentId lsid, Buffer buffer) {
    // TODO: set checksums
    uint64_t offset = getLogSegmentOffset(lsid);
    if (lsid.offset() == 0) {
        printf("FwLog Write: reseting zone %lf\n",
               offset / (double)device_.getIOZoneSize());
        uint64_t align_offset = offset - (offset % device_.getIOZoneSize());
        device_.reset(align_offset, device_.getIOZoneCapSize());
    }
    // printf("Write: writing to zone %d offset 0x%x, actual zone # %ld, loc
    // 0x%lx, len 0x%lx\n",
    //        lsid.zone(), lsid.offset(), offset / device_.getIOZoneSize(),
    //        offset, buffer.size());
    bool ret = device_.write(getLogSegmentOffset(lsid), std::move(buffer));
    if (!ret) {
        printf(
            "FwLog Write Failed: writing to zone %d offset %d, actual zone # "
            "%ld, loc %ld\n",
            lsid.zone(), lsid.offset(), offset / device_.getIOZoneSize(),
            offset);
    }
    if (getNextLsid(lsid).offset() == 0) {
        uint64_t zone_offset =
            getLogSegmentOffset(LogSegmentId(0, lsid.zone()));
        printf("FwLog Write: finishing zone %lf\n",
               zone_offset / (double)device_.getIOZoneSize());

        uint64_t align_offset = offset - (offset % device_.getIOZoneSize());
        device_.finish(align_offset, device_.getIOZoneCapSize());
    }

    perfCounter_.logPageWritten.fetch_add(pagesPerSegment_);

    return ret;
}

bool FwLog::eraseSegments(LogSegmentId startLsid) {
    return device_.reset(getLogSegmentOffset(startLsid), flushGranularity_);
}

// only flushes buffer to flash if needed
bool FwLog::flushLogSegment(uint32_t partition, bool wait) {
    // XLOGF(INFO, "Flushing Log Segment because of partition {}", partition);
    if (!bufferMetadataMutex_.try_lock()) {
        if (wait) {
            sleep(.001);
        }
        return false;
    } else {
        uint32_t oldOffset = bufferedSegmentOffset_;
        uint32_t updatedOffset =
            (bufferedSegmentOffset_ + 1) % numBufferedLogSegments_;

        // 如果下一个部分 buffer 空间足够，没有overflow，则暂时不需要下刷
        {
            std::shared_lock<std::shared_mutex> nextLock{
                logSegmentMutexs_[updatedOffset]};
            if (currentLogSegments_[updatedOffset]->getFullness(partition) <
                overflowLimit_) {
                bufferMetadataMutex_.unlock();
                // flushLogCv_.notify_all();
                return true;
            }
        }

        // 如果下刷的 segment 所在的 zone 正在被 clean， 等待 flush 线程通知
        // 由于是顺序写，所以只有 oldLsid < cleanLsid 才需要等 Flush
        LogSegmentId oldLsid =
            currentLogSegments_[oldOffset]->getLogSegmentId();
        {
            std::unique_lock<std::shared_mutex> lock{cleaningMutex_};
            uint64_t startTime = RdtscTimer::instance().ms();
            bool writeStall = false;
            while (oldLsid.zone() == nextLsidToClean_.zone() &&
                   oldLsid.offset() <= nextLsidToClean_.offset() && !writeEmpty_) {
                writeStall = true;
                cleaningLogCv_.wait(lock);
            }
            if (writeStall) {
                printf("FwLog write stall by cleaning time %lf\n",
                       (RdtscTimer::instance().ms() - startTime) / 1000.0);
            }
        }

        // refresh pointers if had to wait
        oldOffset = bufferedSegmentOffset_;
        updatedOffset = (bufferedSegmentOffset_ + 1) % numBufferedLogSegments_;
        bufferedSegmentOffset_ = updatedOffset;
        oldLsid = currentLogSegments_[oldOffset]->getLogSegmentId();

        LogSegmentId nextLsid;
        // 下刷一个 segmant
        {
            std::unique_lock<std::shared_mutex> oldSegmentLock{
                getMutexFromSegment(oldLsid)};
            std::unique_lock<std::shared_mutex> oldBufferLock{
                logSegmentMutexs_[oldOffset]};

            nextLsid = getNextLsid(oldLsid, numBufferedLogSegments_);

            writeLogSegment(
                oldLsid, std::move(Buffer(logSegmentBuffers_[oldOffset].view(),
                                          pageSize_)));
            currentLogSegments_[oldOffset]->clear(nextLsid);
            writeEmpty_ = false;
        }
    }

    bufferMetadataMutex_.unlock();
    // flushLogCv_.notify_all();

    return true;
}

LogSegmentId FwLog::getNextLsid(LogSegmentId lsid, uint32_t increment) {
    // partitions within segment so partition # doesn't matter
    assert(increment < numSegmentsPerZone_); // not correct otherwise
    if (lsid.offset() + increment >= numSegmentsPerZone_) {
        return LogSegmentId((lsid.offset() + increment) % numSegmentsPerZone_,
                            (lsid.zone() + 1) % numLogZones_);
    }
    return LogSegmentId(lsid.offset() + increment, lsid.zone());
}

LogSegmentId FwLog::getNextLsid(LogSegmentId lsid) {
    return getNextLsid(lsid, 1);
}

FwLog::~FwLog() {
    for (uint64_t i = 0; i < logIndexPartitions_; i++) {
        delete index_[i];
    }
    delete index_;
    for (uint32_t i = 0; i < numBufferedLogSegments_; i++) {
        delete currentLogSegments_[i];
    }
}

FwLog::FwLog(Config &&config)
    : FwLog{std::move(config.validate()), ValidConfigTag{}} {}

FwLog::Config &FwLog::Config::validate() {
    if (logSize < readSize) {
        printf("Log size: %ld cannot be smaller than read size: %d\n", logSize,
               readSize);
        exit(-1);
    }

    if (logSize < segmentSize) {
        printf("Log size: %ld cannot be smaller than segment size: %d\n",
               logSize, segmentSize);
        exit(-1);
    }

    // if (!folly::isPowTwo(readSize)) {
    //   throw std::invalid_argument(
    //       folly::sformat("invalid read size: {}", readSize));
    // }

    if (logSize > uint64_t{readSize} << 32) {
        printf("Log size: %ld cannot be greater than 2^32 * read size: %d\n",
               logSize, readSize);
        exit(-1);
    }

    if (segmentSize % readSize != 0 || logSize % readSize != 0) {
        printf("logSize and segmentSize need to be a multiple of readSize. "
               "segmentSize: %d, logSize: %ld, readSize: %d\n",
               segmentSize, logSize, readSize);
        exit(-1);
    }

    if (logSize % segmentSize != 0) {
        printf("logSize must be a multiple of segmentSize. segmentSize: %d, "
               "logSize: %ld",
               segmentSize, logSize);
        exit(-1);
    }

    if (logPhysicalPartitions == 0) {
        printf("number physical partitions needs to be greater than 0\n");
        exit(-1);
    }

    if (logIndexPartitions % logPhysicalPartitions != 0) {
        printf(
            "the number of index partitions must be a multiple of the physical "
            "partitions\n");
        exit(-1);
    }

    if (logSize / logPhysicalPartitions % readSize != 0) {
        printf("physical partition size must be a multiple of read size\n");
        exit(-1);
    }

    if (numTotalIndexBuckets % logIndexPartitions != 0) {
        printf("index entries %ld must be a multiple of index partitions %ld\n",
               numTotalIndexBuckets, logIndexPartitions);
        exit(-1);
    }

    if (device == nullptr) {
        printf("device cannot be null\n");
        exit(-1);
    }

    if (numTotalIndexBuckets == 0) {
        printf("need to have a number of index buckets\n");
        exit(-1);
    }

    if (flushGranularity % segmentSize != 0) {
        printf("flushGranularity should be a multiple of segmentSize\n");
        exit(-1);
    }

    return *this;
}

FwLog::FwLog(Config &&config, ValidConfigTag)
    : pageSize_{config.readSize}, segmentSize_{config.segmentSize},
      logBaseOffset_{config.logBaseOffset}, logSize_{config.logSize},
      pagesPerSegment_{segmentSize_ / pageSize_}, numSegments_{logSize_ /
                                                               segmentSize_},
      flushGranularity_{config.flushGranularity}, device_{*config.device},
      logIndexPartitions_{config.logIndexPartitions},
      index_{new ChainedLogIndex *[logIndexPartitions_]},
      setNumberCallback_{config.setNumberCallback},
      numLogZones_{logSize_ / device_.getIOZoneCapSize()},
      numSegmentsPerZone_{numSegments_ / numLogZones_},
      pagesPerZone_{device_.getIOZoneCapSize() / pageSize_},
      logPhysicalPartitions_{config.logPhysicalPartitions},
      physicalPartitionSize_{logSize_ / logPhysicalPartitions_},
      pagesPerPartitionSegment_{pagesPerSegment_ / logPhysicalPartitions_},
      // nextLsidToClean_{std::make_unique<LogSegmentId>(0, 0)},
      numIndexEntries_{config.numTotalIndexBuckets},
      currentLogSegments_{new FwLogSegment *[numBufferedLogSegments_]},
      logSegmentBuffers_{new Buffer[numBufferedLogSegments_]},
      nextLsidToClean_{LogSegmentId(0, 0)}, threshold_{config.threshold} {
    printf("FwLog created: size: %ld, read size: %ld, segment size: %ld, base "
           "offset: %ld, pages per partition segment %ld\n",
           logSize_, pageSize_, segmentSize_, logBaseOffset_,
           pagesPerPartitionSegment_);
    printf("FwLog: num zones %ld, segments per zone %ld\n", numLogZones_,
           numSegmentsPerZone_);
    printf("FwLog: zone size %ld, offset %ld, segment size %ld page size %ld\n",
           device_.getIOZoneSize(), logBaseOffset_, segmentSize_, pageSize_);
    printf("FwLog: pages per zone %ld, pages per segment %ld\n", device_.getIOZoneCapSize() / pageSize_,
           pagesPerSegment_);
    printf("FwLog: num index entries %ld, log index partitions %ld\n", 
           numIndexEntries_, logIndexPartitions_);
    for (uint64_t i = 0; i < logIndexPartitions_; i++) {
        index_[i] =
            new ChainedLogIndex(numIndexEntries_ / logIndexPartitions_,
                                config.sizeAllocations, setNumberCallback_);
    }
    logSegmentMutexs_ =
        std::make_unique<std::shared_mutex[]>(logPhysicalPartitions_);
    reset();
}

bool FwLog::shouldClean(double cleaningThreshold) {
    LogSegmentId currentLsid = LogSegmentId(0, 0);
    {
        std::shared_lock<std::shared_mutex> nextLock{
            logSegmentMutexs_[bufferedSegmentOffset_]};
        currentLsid =
            currentLogSegments_[bufferedSegmentOffset_]->getLogSegmentId();
    }

    auto nextLsid = getNextLsid(currentLsid);
    uint64_t nextWriteLoc =
        nextLsid.offset() + nextLsid.zone() * numSegmentsPerZone_;
    uint64_t nextCleaningLoc = nextLsidToClean_.offset() +
                               nextLsidToClean_.zone() * numSegmentsPerZone_;
    uint64_t freeSegments = 0;
    if (nextCleaningLoc >= nextWriteLoc) {
        freeSegments = nextCleaningLoc - nextWriteLoc;
    } else {
        freeSegments = nextCleaningLoc + (numSegments_ - nextWriteLoc);
    }
    // 小于 threshhold 则需要下刷
    if (freeSegments <= (numSegments_ * cleaningThreshold)) {
        return true;
    }
    // 小于一个 zone 则需要下刷
    if (freeSegments < numSegmentsPerZone_) {
        return true;
    }
    return false;
}

std::vector<std::unique_ptr<ObjectInfo>>
FwLog::getObjectsToMove(KangarooBucketId bid, bool checkThreshold) {
    // XLOGF(INFO, "Getting objects for {}", bid.index());
    uint64_t indexPartition = getIndexPartition(bid);
    uint64_t physicalPartition = getPhysicalPartition(bid);
    ChainedLogIndex::BucketIterator indexIt;

    // TODO: performance, stop early if threshold if not reached
    std::vector<std::unique_ptr<ObjectInfo>> objects;
    BufferView value;
    HashedKey key = HashedKey("");
    uint8_t hits;
    LogPageId lpid;
    uint32_t tag;

    /* allow reinsertion to index if not enough objects to move */
    indexIt = index_[indexPartition]->getHashBucketIterator(bid);
    while (!indexIt.done()) {

        hits = indexIt.hits();
        tag = indexIt.tag();
        indexIt = index_[indexPartition]->getNext(indexIt);
        lpid = getLogPageId(index_[indexPartition]->find(bid, tag),
                            physicalPartition);
        if (!lpid.isValid()) {
            continue;
        }

        // Find value, could be in in-memory buffer or on nvm
        Buffer buffer;
        Status status = lookupBufferedTag(tag, key, &buffer, lpid);
        if (status != Status::Ok) {
            buffer = readLogPage(lpid);
            if (buffer.isNull()) {
                continue;
            }
            LogBucket *page = reinterpret_cast<LogBucket *>(buffer.data());
            value = page->findTag(tag, key);
        } else {
            value = buffer.view();
        }

        if (value.isNull()) {
            index_[indexPartition]->remove(tag, bid, getPartitionOffset(lpid));
            continue;
        } else if (setNumberCallback_(key) != bid) {
            continue;
        }
        // 拿出来后会尝试 remove
        index_[indexPartition]->remove(tag, bid, getPartitionOffset(lpid));
        auto ptr = std::make_unique<ObjectInfo>(key, value, hits, lpid, tag);
        objects.push_back(std::move(ptr));
    }

    if (objects.size() < threshold_ && checkThreshold) {
        // thresholdNotHit_.inc();
        for (auto &item : objects) {
            readmit(item);
        }
        objects.resize(0);
    } else {
        // moveBucketCalls_.inc();
    }

    return objects;
}

void FwLog::readmit(std::unique_ptr<ObjectInfo> &oi) {
    /* reinsert items attempted to be moved into index unless in segment to
     * flush
     */
    // moveBucketSuccessfulRets_.inc();
    perfCounter_.logicalReadmitCnt.fetch_add(1);
    if (cleaningSegment_ &&
        getSegmentId(oi->lpid) != cleaningSegment_->getLogSegmentId()) {
        uint64_t indexPartition = getIndexPartition(oi->key);
        KangarooBucketId bid = setNumberCallback_(oi->key);
        index_[indexPartition]->insert(oi->tag, bid,
                                       getPartitionOffset(oi->lpid), oi->hits);
    } else if (oi->hits) {
        readmit(oi->key, oi->value.view());
    } else if (rand32() % 10 < 1) {
        // 先偷个懒
        // 正确做法是查 Bloom Filter
        // readmit(oi->key, oi->value.view());
        perfCounter_.pendingCnt.fetch_add(1);
    }
    return;
}

void FwLog::startClean() {
    {
        std::shared_lock<std::shared_mutex> segmentLock{
            getMutexFromSegment(nextLsidToClean_)};
        cleaningBuffer_ = readLogSegment(nextLsidToClean_);
        if (cleaningBuffer_.isNull()) {
            // ioErrorCount_.inc();
            cleaningSegment_ = nullptr;
            return;
        }
    }
    cleaningSegment_ = std::make_unique<FwLogSegment>(
        segmentSize_, pageSize_, nextLsidToClean_, logPhysicalPartitions_,
        cleaningBuffer_.mutableView(), false);
    cleaningSegmentIt_ = cleaningSegment_->getFirst();
}

bool FwLog::finishClean() {
    LogSegmentId next_lisd = nextLsidToClean_;
    {
        std::unique_lock<std::shared_mutex> lock{cleaningMutex_};
        nextLsidToClean_ = getNextLsid(nextLsidToClean_);
        cleaningLogCv_.notify_all();
    }
    if (next_lisd.zone() != nextLsidToClean_.zone()) {
        printf("FwLog finish cleaning zone %d\n", next_lisd.zone());
    }

    cleaningSegmentIt_.reset();
    cleaningSegment_.reset();

    return next_lisd.zone() != nextLsidToClean_.zone();
}

bool FwLog::cleaningDone() {
    while (!cleaningSegmentIt_->done()) {
        uint64_t indexPartition = getIndexPartition(cleaningSegmentIt_->key());
        uint32_t hits = 0;
        auto po = index_[indexPartition]->lookup(cleaningSegmentIt_->key(),
                                                 false, &hits);
        LogPageId lpid = getLogPageId(po, cleaningSegmentIt_->partition());

        if (!lpid.isValid() || nextLsidToClean_ != getSegmentId(lpid)) {
            if (lpid.isValid()) {
            }
            cleaningSegmentIt_ =
                cleaningSegment_->getNext(std::move(cleaningSegmentIt_));
        } else {
            return false;
        }
    }

    return true;
}

KangarooBucketId FwLog::getNextCleaningBucket() {
    // need external lock
    KangarooBucketId kbid = setNumberCallback_(cleaningSegmentIt_->key());
    cleaningSegmentIt_ =
        cleaningSegment_->getNext(std::move(cleaningSegmentIt_));
    return kbid;
}

Status FwLog::lookup(HashedKey hk, Buffer *value) {
    // lookupCount_.inc();
    uint64_t indexPartition = getIndexPartition(hk);
    uint64_t physicalPartition = getPhysicalPartition(hk);
    LogPageId lpid = getLogPageId(
        index_[indexPartition]->lookup(hk, true, nullptr), physicalPartition);
    if (!lpid.isValid()) {
        return Status::NotFound;
    }

    Buffer buffer;
    BufferView valueView;
    LogBucket *page;
    {
        std::shared_lock<std::shared_mutex> lock{getMutexFromPage(lpid)};

        // check if page is buffered in memory and read it
        Status ret = lookupBuffered(hk, value, lpid);
        if (ret != Status::Retry) {
            return ret;
        }

        buffer = readLogPage(lpid);
        if (buffer.isNull()) {
            // ioErrorCount_.inc();
            return Status::DeviceError;
        }
    }

    page = reinterpret_cast<LogBucket *>(buffer.data());

    valueView = page->find(hk);
    if (valueView.isNull()) {
        return Status::NotFound;
    }

    *value = Buffer{valueView};
    // succLookupCount_.inc();
    return Status::Ok;
}

bool FwLog::couldExist(HashedKey hk) {
    uint64_t indexPartition = getIndexPartition(hk);
    uint64_t physicalPartition = getPhysicalPartition(hk);
    LogPageId lpid = getLogPageId(
        index_[indexPartition]->lookup(hk, true, nullptr), physicalPartition);
    if (!lpid.isValid()) {
        // lookupCount_.inc();
        return false;
    }

    return true;
}

Status FwLog::insert(HashedKey hk, BufferView value) {
    // TODO: update for fw log
    //
    LogPageId lpid;
    LogSegmentId lsid;
    uint64_t physicalPartition = getPhysicalPartition(hk);
    Status ret = Status::Ok;

    while (!lpid.isValid()) {
        uint32_t buffer = numBufferedLogSegments_ + 1;
        // bool flushSegment = false;
        for (uint32_t i = 0; i < numBufferedLogSegments_; i++) {
            // logSegment handles concurrent inserts
            // lock to prevent write out of segment
            uint32_t bufferNum =
                (i + bufferedSegmentOffset_) % numBufferedLogSegments_;
            std::shared_lock<std::shared_mutex> lock{
                logSegmentMutexs_[bufferNum]};

            LogSegmentId id = currentLogSegments_[bufferNum]->getLogSegmentId();
            uint32_t pageOffset = currentLogSegments_[bufferNum]->insert(
                hk, value, physicalPartition);
            lpid = getLogPageId(id, pageOffset);
            if (lpid.isValid()) {
                buffer = bufferNum;

                break;
            }
        }

        if (lpid.isValid()) {
            uint64_t indexPartition = getIndexPartition(hk);
            auto ret =
                index_[indexPartition]->insert(hk, getPartitionOffset(lpid));
            if (ret == Status::NotFound) {
                ret = Status::Ok;
            }
            if (ret == Status::Ok) {
            }
        }

        if (buffer != bufferedSegmentOffset_) {
            // only wait if could not allocate lpid
            flushLogSegment(physicalPartition, !lpid.isValid());
        }
    }
    return ret;
    /*
      if (lpid.isValid()) {
          return ret;
      } else if (!lpid.isValid()) {
      return insert(hk, value);
    } else {
      return Status::Rejected;
    }
    */
}

void FwLog::readmit(HashedKey hk, BufferView value) {
    LogPageId lpid;
    LogSegmentId lsid;
    uint64_t physicalPartition = getPhysicalPartition(hk);

    {
        // logSegment handles concurrent inserts
        // lock to prevent write out of segment
        std::shared_lock<std::shared_mutex> lock{
            logSegmentMutexs_[bufferedSegmentOffset_]};
        lpid = getLogPageId(
            currentLogSegments_[bufferedSegmentOffset_]->getLogSegmentId(),
            currentLogSegments_[bufferedSegmentOffset_]->insert(
                hk, value, physicalPartition));
        if (!lpid.isValid()) {
            // no room in segment so will not insert
            printf("WARN: no room in segment, not readmit!\n");
            return;
        }
    }

    uint64_t indexPartition = getIndexPartition(hk);
    auto ret = index_[indexPartition]->insert(hk, getPartitionOffset(lpid));
    if (ret != Status::Ok) {
    }
}

Status FwLog::remove(HashedKey hk) {
    uint64_t indexPartition = getIndexPartition(hk);
    uint64_t physicalPartition = getPhysicalPartition(hk);
    LogPageId lpid;

    lpid = getLogPageId(index_[indexPartition]->lookup(hk, false, nullptr),
                        physicalPartition);
    if (!lpid.isValid()) {
        return Status::NotFound;
    }

    Buffer buffer;
    BufferView valueView;
    LogBucket *page;
    {
        std::shared_lock<std::shared_mutex> lock{getMutexFromPage(lpid)};

        // check if page is buffered in memory and read it
        Status ret = lookupBuffered(hk, &buffer, lpid);
        if (ret == Status::Ok) {
            Status status =
                index_[indexPartition]->remove(hk, getPartitionOffset(lpid));
            if (status == Status::Ok) {
                // succRemoveCount_.inc();
            }
            return status;
        } else if (ret == Status::Retry) {
            buffer = readLogPage(lpid);
            if (buffer.isNull()) {
                // ioErrorCount_.inc();
                return Status::DeviceError;
            }
        } else {
            return ret;
        }
    }

    page = reinterpret_cast<LogBucket *>(buffer.data());

    valueView = page->find(hk);
    if (valueView.isNull()) {
        // keyCollisionCount_.inc();
        return Status::NotFound;
    }

    Status status =
        index_[indexPartition]->remove(hk, getPartitionOffset(lpid));
    if (status == Status::Ok) {
        // succRemoveCount_.inc();
    }
    return status;
}

Status FwLog::prefill(
    std::function<uint64_t ()> k_func,
    std::function<BufferView ()> v_func
) {
    uint32_t buckets_per_zone = numIndexEntries_ / (numLogZones_ - 1);
    uint32_t buckets_range = buckets_per_zone;
    uint32_t start_zone = 0;
    
    while (!shouldClean(0.155)) {
        HashedKey hk = k_func();
        BufferView value = v_func();
        if (value.size() == 0) {
            return Status::Ok;
        }
        Status status = insert(hk, value);
        if (status != Status::Ok) {
            return status;
        }
        // 模拟 Migration
        uint32_t now_bucket = setNumberCallback_(hk).index();
        if (now_bucket >= buckets_range) {
            remove(hk);
        }
        uint32_t now_zone = currentLogSegments_[0]->getLogSegmentId().zone();
        if (now_zone != start_zone) {
            buckets_range += buckets_per_zone;
            start_zone = now_zone;
        }
    }

    return Status::Ok;
}

void FwLog::flush() {
    // TODO: should probably flush buffered part of log
    return;
}

void FwLog::reset() {
    printf("FwLog has segment size: 0x%lx\n", segmentSize_);
    for (uint64_t i = 0; i < numBufferedLogSegments_; i++) {
        logSegmentBuffers_[i] = device_.makeIOBuffer(segmentSize_);
        currentLogSegments_[i] = new FwLogSegment(
            segmentSize_, pageSize_, LogSegmentId(i, 0), logPhysicalPartitions_,
            logSegmentBuffers_[i].mutableView(), true);
    }
    nextLsidToClean_ = LogSegmentId(0, 0);
}

Status FwLog::lookupBuffered(HashedKey hk, Buffer *value, LogPageId lpid) {
    // uint64_t partition = getPhysicalPartition(hk);
    uint32_t buffer = numBufferedLogSegments_ + 1;
    LogSegmentId lsid = getSegmentId(lpid);
    for (uint32_t i = 0; i < numBufferedLogSegments_; i++) {
        if (lsid == currentLogSegments_[i]->getLogSegmentId()) {
            buffer = i;
            break;
        }
    }
    if (buffer >= numBufferedLogSegments_) {
        return Status::Retry;
    }

    BufferView view;
    {
        std::shared_lock<std::shared_mutex> lock{logSegmentMutexs_[buffer]};
        if (!isBuffered(lpid, buffer)) {
            return Status::Retry;
        }
        view = currentLogSegments_[buffer]->find(hk, lpid);
        if (view.isNull()) {
            // keyCollisionCount_.inc();
            return Status::NotFound;
        }
        *value = Buffer{view};
    }
    return Status::Ok;
}

Status FwLog::lookupBufferedTag(uint32_t tag, HashedKey &hk, Buffer *value,
                                LogPageId lpid) {
    // uint64_t partition = getPhysicalPartition(lpid);
    uint32_t buffer = numBufferedLogSegments_ + 1;
    LogSegmentId lsid = getSegmentId(lpid);
    for (uint32_t i = 0; i < numBufferedLogSegments_; i++) {
        if (lsid == currentLogSegments_[i]->getLogSegmentId()) {
            buffer = i;
            break;
        }
    }
    if (buffer >= numBufferedLogSegments_) {
        return Status::Retry;
    }

    BufferView view;
    {
        std::shared_lock<std::shared_mutex> lock{logSegmentMutexs_[buffer]};
        if (!isBuffered(lpid, buffer)) {
            return Status::Retry;
        }
        view = currentLogSegments_[buffer]->findTag(tag, hk, lpid);
        if (view.isNull()) {
            return Status::NotFound;
        }
        *value = Buffer{view};
    }
    return Status::Ok;
}

bool FwLog::isBuffered(LogPageId lpid, uint32_t buffer) {
    return getSegmentId(lpid) == currentLogSegments_[buffer]->getLogSegmentId();
}

LogSegmentId FwLog::nextLsidToClean() { return nextLsidToClean_; }

FwLog::PerfCounter &FwLog::perfCounter() { return perfCounter_; }

Status FwLog::check_remove(HashedKey hk, LogPageId lpid) {
    uint64_t indexPartition = getIndexPartition(hk);
    uint64_t physicalPartition = getPhysicalPartition(hk);
    LogPageId index_lpid = getLogPageId(
        index_[indexPartition]->lookup(hk, true, nullptr), physicalPartition);

    // no need remove
    if (!index_lpid.isValid() || index_lpid != lpid) {
        return Status::NotFound;
    }

    Status status =
        index_[indexPartition]->remove(hk, getPartitionOffset(lpid));
    if (status == Status::Ok) {
        // succRemoveCount_.inc();
    }
    return status;
}

bool FwLog::get_next_cleaning_key(HashedKey *hk, LogPageId *lpid) {
    if (cleaningSegmentIt_->done()) {
        return true;
    }
    *hk = cleaningSegmentIt_->key();
    *lpid = getLogPageId(nextLsidToClean_,
                         cleaningSegmentIt_->bucket());
    cleaningSegmentIt_ = cleaningSegment_->getNext(std::move(cleaningSegmentIt_));
    return false;
}