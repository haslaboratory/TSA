#include "kangaroo/RripBucket.h"
#include "raw/RawSet.h"

RawSet::RawSet(Config &&config): RawSet(std::move(config), ValidConfigTag{}) {}

RawSet::RawSet(Config &&config, ValidConfigTag)
    : base_offset_(config.base_offset),
    zone_capacity_(config.zone_capacity),
    page_size_(config.page_size),
    pages_per_zone_(config.zone_capacity / config.page_size),
    zone_num_(config.zone_num),
    op_(config.op),
    gc_thread_num_(config.gc_thread_num),
    device_(config.device)
{
    bucket_num_ = zone_num_ * pages_per_zone_ * (1 - op_);

    bucket_mutex_ = std::unique_ptr<std::shared_mutex[]>(new std::shared_mutex[MUTEX_PART]);

    wren_ = std::make_unique<Wren>(
        *config.device,
        bucket_num_,
        page_size_,
        zone_num_ * zone_capacity_,
        base_offset_
    );

    if (config.enable_bf) {
        bloom_filter_ = std::make_unique<BloomFilter>(
            bucket_num_,
            config.bf_num_hashes,
            config.bf_len_hash_bits
        );
    }

    // init
    // for (uint32_t i = 0; i < bucket_num_; i++) {
    //     bool must = (i / pages_per_zone_) == (bucket_num_ / pages_per_zone_);
    //     wren_->pre_write(KangarooBucketId{i}, must);
    // }

    assert(gc_thread_num_ > 0);

    gc_threads_.emplace_back(std::thread(&RawSet::gc_loop, this));
    for (uint64_t i = 1; i < gc_thread_num_; i++) {
        gc_threads_.emplace_back(std::thread(&RawSet::gc_wait_loop, this));
    }
}

Status RawSet::lookup(HashedKey hk, Buffer *value) {
    uint32_t bid = get_bid(hk);

    std::shared_lock<std::shared_mutex> lock(bucket_mutex_[bid % MUTEX_PART]);

    if (bf_reject(bid, hk)) {
        return Status::NotFound;
    }

    Buffer buffer = read_bucket(KangarooBucketId{bid});
    if (buffer.isNull()) {
        return Status::NotFound;
    }
    RripBucket *bucket = reinterpret_cast<RripBucket *>(buffer.data());
    BufferView view = bucket->find(hk, [&](uint32_t key_idx) {});
    if (view.isNull()) {
        return Status::NotFound;
    }

    *value = Buffer{view};
    return Status::Ok;
}

Status RawSet::insert(HashedKey hk, BufferView value)
{
    uint32_t bid = get_bid(hk);

    // TODO: 暂时简单处理并发问题，极端情况可能存在风险
    while (wren_->shouldClean(0.01)) {
        std::unique_lock<std::mutex> lock(write_sync_);
        write_sync_cond_.wait(lock);
    }

    std::unique_lock<std::shared_mutex> lock(bucket_mutex_[bid % MUTEX_PART]);
    
    Buffer buffer = read_bucket(KangarooBucketId{bid});

    RripBucket *bucket = reinterpret_cast<RripBucket *>(buffer.data());

    bucket->remove(hk, nullptr);
    bucket->insert(hk, value, 0, nullptr);

    if (bloom_filter_ != nullptr) {
        bf_rebuild(bid, bucket);
    }

    const auto res = write_bucket(KangarooBucketId{bid}, std::move(buffer));
    
    perf_.objInserted.fetch_add(1);

    return res ? Status::Ok : Status::DeviceError;
}

Status RawSet::remove(HashedKey hk) {
    uint32_t bid = get_bid(hk);

    std::unique_lock<std::shared_mutex> lock(bucket_mutex_[bid % MUTEX_PART]);

    Buffer buffer = read_bucket(KangarooBucketId{bid});

    RripBucket *bucket = reinterpret_cast<RripBucket *>(buffer.data());

    bucket->remove(hk, nullptr);

    const auto res = write_bucket(KangarooBucketId{bid}, std::move(buffer));

    return res ? Status::Ok : Status::DeviceError;
}

Status RawSet::prefill(
    std::function<uint64_t ()> k_func,
    std::function<BufferView ()> v_func
) {
    for (uint32_t i = 0; i < bucket_num_; i++) {
        Buffer buffer = read_bucket(KangarooBucketId{i});
        RripBucket *bucket = reinterpret_cast<RripBucket *>(buffer.data());
        while (true) {
            HashedKey hk = k_func();
            // 将 key 和对应 set 对齐
            hk = hk - (hk % bucket_num_) + i;
            BufferView v = v_func();
            if (bucket->isSpacePrefill(hk, v)) {
                bucket->remove(hk, nullptr);
                bucket->insert(hk, v, 0, nullptr);
            } else {
                break;
            }
        }
        if (bloom_filter_ != nullptr) {
            bf_rebuild(i, bucket);
        }
        const auto res = write_bucket(KangarooBucketId{i}, std::move(buffer));
        if (!res) {
            return Status::DeviceError;
        }
    }

    return Status::Ok;
}

void RawSet::bf_rebuild(uint32_t bid, const RripBucket *bucket) {
    assert(bloom_filter_!= nullptr);
    assert(bucket != nullptr);
    bloom_filter_->clear(bid);
    auto iter = bucket->getFirst();
    uint32_t i = 0;
    uint32_t total = bucket->size();
    while (!iter.done() && i < total) {
        bloom_filter_->set(bid, iter.hashedKey());
        iter = bucket->getNext(iter);
        i++;
    }
}

void RawSet::bf_build(uint32_t bid, const RripBucket *bucket) {
    assert(bloom_filter_ != nullptr);
    auto iter = bucket->getFirst();
    while (!iter.done()) {
        bloom_filter_->set(bid, iter.hashedKey());
        iter = bucket->getNext(iter);
    }
}

bool RawSet::bf_reject(uint32_t bid, uint64_t key_hash) const {
    if (bloom_filter_ == nullptr) {
        return false;
    }
    return !bloom_filter_->couldExist(bid, key_hash);
}

Buffer RawSet::read_bucket(KangarooBucketId bid) {
    bool new_buffer = false;
    Buffer buffer = wren_->read(bid, new_buffer);

    if (buffer.isNull()) {
        return Buffer();
    }

    if (new_buffer) {
        RripBucket::initNew(buffer.mutableView(), 0);
    }

    return buffer;
}

void RawSet::perform_gc() {
    {
        std::unique_lock<std::mutex> lock(gc_sync_);
        gc_sync_threads_++;
    }
    while (gc_flag_) {
        KangarooBucketId kbid = KangarooBucketId{0};
        {
            std::unique_lock<std::mutex> lock(gc_sync_);
            if (!eu_iterator_.done()) {
                kbid = eu_iterator_.getBucket();
                eu_iterator_ = wren_->getNext(eu_iterator_);
            } else {
                gc_flag_ = false;
                break;
            }
        }
        // gc bucket
        perf_.gcWritten.fetch_add(1);
        std::unique_lock<std::shared_mutex> lock(bucket_mutex_[kbid.index() % MUTEX_PART]);
        Buffer buffer = read_bucket(kbid);
        write_bucket(kbid, std::move(buffer));
    }

    {
        std::unique_lock<std::mutex> lock(gc_sync_);
        gc_sync_threads_--;
        if (gc_sync_threads_ == 0) {
            gc_sync_cond_.notify_all();
        }
    }
}

void RawSet::gc_loop() {
    while (!stop_flag_) {
        if (!wren_->shouldClean(0.01)) {
            continue;
        }

        printf("gc start\n");
        while (wren_->shouldClean(0.01)) {
            perf_.warm_ok.store(true);
            eu_iterator_ = wren_->getEuIterator();
            gc_flag_ = true;
            gc_sync_cond_.notify_all();
            perform_gc();
    
            {
                std::unique_lock<std::mutex> lock(gc_sync_);
                // printf("gc wait\n");
                while (gc_flag_ || gc_sync_threads_ > 0) {
                    gc_sync_cond_.wait(lock);
                }
            }
    
            wren_->erase();
        }
        printf("gc end\n");
        write_sync_cond_.notify_all();
    }

    gc_sync_cond_.notify_all();
}

void RawSet::gc_wait_loop() {
    while (!stop_flag_) {
        if (gc_flag_) {
            // printf("start wait gc\n");
            perform_gc();
        }
        {
            std::unique_lock<std::mutex> lock{gc_sync_};
            gc_sync_cond_.wait(lock);
        }
    }
}

bool RawSet::write_bucket(KangarooBucketId bid, Buffer buf) {
    perf_.pageWritten.fetch_add(buf.size() / page_size_);
    return wren_->write(bid, std::move(buf));
}

uint32_t RawSet::get_bid(HashedKey hk) {
    return hk % bucket_num_;
}

RawSet::~RawSet() {
    stop_flag_ = true;
    for (auto &thread : gc_threads_) {
        thread.join();
    }
}

RawSet::PerfCounter &RawSet::perf_counter() {
    return perf_;
}