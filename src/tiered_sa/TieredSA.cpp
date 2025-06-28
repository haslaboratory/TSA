#include <algorithm>
#include <cmath>
#include <unistd.h>
#include <unordered_set>

#include "tiered_sa/TieredSA.h"

TieredSA::Config &TieredSA::Config::validate() {
    if (zone_capacity == 0 || page_size == 0) {
        printf("zone_size and page_size cannot be 0\n");
        exit(-1);
    }

    if (zone_capacity % page_size != 0) {
        printf("zone_size must be a multiple of page_size\n");
        exit(-1);
    }

    if (log_zone_num == 0 || set_zone_num == 0) {
        printf("log_zone_num, set_zone_num cannot be 0\n");
        exit(-1);
    }

    if (device == nullptr) {
        printf("device cannot be null\n");
        exit(-1);
    }
    return *this;
}

TieredSA::TieredSA(Config &&config) : TieredSA(std::move(config.validate()), ValidConfigTag{}) {}

// 先按 fairywren 的方法实现 set 区
TieredSA::TieredSA(Config &&config, ValidConfigTag)
    : set_mode_(config.set_mode),
    log_op_(config.log_op),
    set_op_(config.set_op),
    base_offset_(config.base_offset),
    zone_capacity_(config.zone_capacity),
    page_size_(config.page_size),
    pages_per_zone_(config.zone_capacity / config.page_size),
    log_zone_num_(config.log_zone_num),
    set_zone_num_(config.set_zone_num),
    migrate_thread_num_(config.migrate_thread_num),
    device_(*config.device)
{
    log_slice_num_ = std::sqrt(log_zone_num_ * pages_per_zone_ * (1 - log_op_));

    printf("log slice num: %ld\n", log_slice_num_);
    
    switch (set_mode_) {
    case SetLayerMode::FW:
        init_fw(std::move(config));
        break;
    case SetLayerMode::CROSSOVER:
        init_x(std::move(config));
        break;
    }
}

void TieredSA::init_fw(Config &&config) {
    // fw-like 默认按照两层大小相同配置
    assert(set_zone_num_ % 2 == 0);
    middle_zone_num_ = set_zone_num_ / 2;
    bottom_zone_num_ = set_zone_num_ / 2;

    middle_per_slice_ = middle_zone_num_ * pages_per_zone_ * (1 - set_op_) / log_slice_num_;
    if (middle_per_slice_ * log_slice_num_ > (middle_zone_num_ - 1) * pages_per_zone_) {
        middle_per_slice_ = (middle_zone_num_ - 1) * pages_per_zone_ / log_slice_num_ - 30;
    }

    middle_num_buckets_ = middle_per_slice_ * log_slice_num_;
    bottom_num_buckets_ = middle_num_buckets_;

    uint64_t middle_offset = base_offset_ + log_zone_num_ * device_.getIOZoneSize();
    uint64_t bottom_offset = middle_offset + middle_zone_num_ * device_.getIOZoneSize();

    assert((middle_zone_num_ - 1) * pages_per_zone_ >= middle_num_buckets_);
    assert((bottom_zone_num_ - 1) * pages_per_zone_ >= bottom_num_buckets_);

    uint64_t num_buckets = middle_num_buckets_;

    LogLayer::Config log_config {
        uint32_t(page_size_),
        uint32_t(zone_capacity_),
        uint32_t(log_slice_num_),
        uint32_t(middle_per_slice_),
        base_offset_,
        log_zone_num_,
        log_op_,
        middle_num_buckets_,
        &device_,
        [num_buckets](uint64_t hk) {
            return KangarooBucketId{static_cast<uint32_t>(hk % num_buckets)};
        }
    };

    log_ = std::make_unique<LogLayer>(std::move(log_config));
    wren_ = std::unique_ptr<Wren>(new Wren(
        device_,
        middle_num_buckets_,
        page_size_,
        middle_zone_num_ * zone_capacity_,
        middle_offset
    ));
    bottom_wren_ = std::unique_ptr<Wren>(new Wren(
        device_,
        bottom_num_buckets_,
        page_size_,
        bottom_zone_num_ * zone_capacity_,
        bottom_offset
    ));
    assert(config.bv_vector_size > 0);
    bit_vector_ = std::unique_ptr<RripBitVector>(new RripBitVector(
        middle_num_buckets_, config.bv_vector_size
    ));
    bottom_bit_vector_ = std::unique_ptr<RripBitVector>(new RripBitVector(
        bottom_num_buckets_, config.bv_vector_size
    ));

    if (config.enable_bf) {
        bloom_filter_ = std::make_unique<BloomFilter>(
            middle_num_buckets_,
            config.bf_num_hashes,
            config.bf_len_hash_bits
        );

        bottom_bloom_filter_ = std::make_unique<BloomFilter>(
            bottom_num_buckets_,
            config.bf_num_hashes,
            config.bf_len_hash_bits
        );
    }

    mutex_ = std::unique_ptr<std::shared_mutex[]>(new std::shared_mutex[log_slice_num_]);

    // TODO: 暂时仅支持一个迁移线程

    set_per_thread_ = (middle_per_slice_ + migrate_thread_num_ - 1) / migrate_thread_num_;

    migrate_threads_.emplace_back(std::thread(&TieredSA::migrate_loop_fw, this));
    for (uint64_t i = 1; i < migrate_thread_num_; i++) {
        migrate_threads_.emplace_back(std::thread(&TieredSA::migrate_wait_loop_fw, this, i));
    }

    printf("TieredSA set bucket num: %ld\n", middle_num_buckets_ * 2);
    printf("TieredSA init done\n");
}

void TieredSA::init_x(Config &&config) {
    // 计算中层和底层结构
    // 默认 中层(2) 对应 底层(8)

    middle_zone_num_ = set_zone_num_ * MIDDLE_FACTOR / (MIDDLE_FACTOR + BOTTOM_FACTOR) + 2;
    bottom_zone_num_ = set_zone_num_ - middle_zone_num_;

    uint64_t base_bucket_num = pages_per_zone_ * set_zone_num_ * (1 - set_op_);
    uint64_t middle_base_bucket_num = base_bucket_num * MIDDLE_FACTOR / (MIDDLE_FACTOR + BOTTOM_FACTOR);

    middle_per_slice_ = (middle_base_bucket_num + log_slice_num_ - 1) / log_slice_num_;
    // 对齐 MIDDLE_FACTOR
    middle_per_slice_ = middle_per_slice_ / MIDDLE_FACTOR * MIDDLE_FACTOR;

    middle_num_buckets_ = middle_per_slice_ * log_slice_num_;
    bottom_num_buckets_ = middle_num_buckets_ * BOTTOM_FACTOR / MIDDLE_FACTOR;

    // printf("middle zone num: %ld, middle bucket num: %ld\n", (middle_zone_num_) * (zone_capacity_ / page_size_), middle_num_buckets_);

    assert((middle_zone_num_ - 1) * pages_per_zone_ >= middle_num_buckets_);
    assert((bottom_zone_num_ - 1) * pages_per_zone_ >= bottom_num_buckets_);

    uint64_t num_buckets = middle_num_buckets_;
    uint64_t middle_offset = base_offset_ + log_zone_num_ * device_.getIOZoneSize();
    uint64_t bottom_offset = middle_offset + middle_zone_num_ * device_.getIOZoneSize();
    LogLayer::Config log_config {
        uint32_t(page_size_),
        uint32_t(zone_capacity_),
        uint32_t(log_slice_num_),
        uint32_t(middle_per_slice_),
        base_offset_,
        log_zone_num_,
        log_op_,
        middle_num_buckets_,
        &device_,
        [num_buckets](uint64_t hk) {
            return KangarooBucketId{static_cast<uint32_t>(hk % num_buckets)};
        }
    };
    log_ = std::make_unique<LogLayer>(std::move(log_config));
    wren_ = std::unique_ptr<Wren>(new Wren(
        device_,
        middle_num_buckets_,
        page_size_,
        middle_zone_num_ * zone_capacity_,
        middle_offset
    ));
    bottom_wren_ = std::unique_ptr<Wren>(new Wren(
        device_,
        bottom_num_buckets_,
        page_size_,
        bottom_zone_num_ * zone_capacity_,
        bottom_offset
    ));
    bit_vector_ = std::unique_ptr<RripBitVector>(new RripBitVector(
        middle_num_buckets_, config.bv_vector_size
    ));
    bottom_bit_vector_ = std::unique_ptr<RripBitVector>(new RripBitVector(
        bottom_num_buckets_, config.bv_vector_size
    ));

    if (config.enable_bf) {
        bloom_filter_ = std::make_unique<BloomFilter>(
            middle_num_buckets_,
            config.bf_num_hashes,
            config.bf_len_hash_bits
        );

        bottom_bloom_filter_ = std::make_unique<BloomFilter>(
            bottom_num_buckets_,
            config.bf_num_hashes,
            config.bf_len_hash_bits
        );
    }

    mutex_ = std::unique_ptr<std::shared_mutex[]>(new std::shared_mutex[log_slice_num_]);
    // 计算每个后台线程负责的 set 数量
    set_per_thread_ = (middle_per_slice_ + migrate_thread_num_ - 1) / migrate_thread_num_;
    if (set_per_thread_ % 2 == 1) {
        set_per_thread_++;
    }

    migrate_threads_.emplace_back(std::thread(&TieredSA::migrate_loop_x, this));
    for (uint64_t i = 1; i < migrate_thread_num_; i++) {
        migrate_threads_.emplace_back(std::thread(&TieredSA::migrate_wait_loop_x, this, i));
    }


    printf("TieredSA set bucket num: %ld\n", middle_num_buckets_ + bottom_num_buckets_);
    printf("TieredSA init done\n");
}

TieredSA::~TieredSA() {
    stop_flag_ = true;
    for (auto &t : migrate_threads_) {
        t.join();
    }
}

Status TieredSA::lookup(HashedKey hk, Buffer *value)
{
    Status ret = log_->lookup(hk, value);
    if (ret == Status::Ok) {
        return ret;
    }

    uint32_t middle_bid = get_middle_bid(hk);
    uint32_t bottom_bid = get_bottom_bid(hk);

    RripBucket *bucket{nullptr};
    Buffer buffer;
    BufferView view;
    {
        std::shared_lock<std::shared_mutex> lock(mutex_[get_mutex_from_middle_bid(middle_bid)]);

        if (!bf_reject(middle_bid, hk, bloom_filter_.get())) {
            buffer = read_bucket(middle_bid, false);
            if (buffer.isNull()) {
                return Status::DeviceError;
            }
            // check middle bucket
            bucket = reinterpret_cast<RripBucket *>(buffer.data());
            view = bucket->find(hk, [&](uint32_t key_idx) {
                return bv_set_hit(middle_bid, key_idx, bit_vector_.get());
            });
        }

        if (view.isNull() &&
                !bf_reject(bottom_bid, hk, bottom_bloom_filter_.get())) {
            // check bottom bucket
            buffer = read_bucket(bottom_bid, true);
            if (buffer.isNull()) {
                return Status::DeviceError;
            }
            bucket = reinterpret_cast<RripBucket *>(buffer.data());

            view = bucket->find(hk, [&](uint32_t key_idx) {
                return bv_set_hit(bottom_bid, key_idx, bottom_bit_vector_.get());
            });
        }
    }

    if (!view.isNull()) {
        *value = Buffer{view};
        return Status::Ok;
    }

    return Status::NotFound;
}

Status TieredSA::insert(HashedKey hk, BufferView value)
{
    Status ret = log_->insert(hk, value);
    return ret;
}

Status TieredSA::remove(HashedKey hk)
{
    if (log_->remove(hk) == Status::Ok) {
        return Status::Ok;
    }

    uint64_t middle_bid = get_middle_bid(hk);
    uint64_t bottom_bid = get_bottom_bid(hk);
    {
        std::unique_lock<std::shared_mutex> lock(mutex_[get_mutex_from_middle_bid(middle_bid)]);
        if (!bf_reject(middle_bid, hk, bloom_filter_.get())) {
            Buffer middle_buffer = read_bucket(middle_bid, false);
            RripBucket *bucket = reinterpret_cast<RripBucket *>(middle_buffer.data());
            bucket->remove(hk, nullptr);
            bf_rebuild(middle_bid, bucket, bloom_filter_.get());
            bool ret = write_bucket(middle_bid, std::move(middle_buffer), false);
            if (!ret) {
                return Status::DeviceError;
            }
        }
        if (!bf_reject(bottom_bid, hk, bottom_bloom_filter_.get())) {
            Buffer bottom_buffer = read_bucket(bottom_bid, true);
            RripBucket *bucket = reinterpret_cast<RripBucket *>(bottom_buffer.data());
            bucket->remove(hk, nullptr);
            bf_rebuild(bottom_bid, bucket, bottom_bloom_filter_.get());
            bool ret = write_bucket(bottom_bid, std::move(bottom_buffer), true);
            if (!ret) {
                return Status::DeviceError;
            }
        }
    }

    return Status::Ok;
}

Status TieredSA::prefill(
    std::function<uint64_t ()> k_func,
    std::function<BufferView ()> v_func
) {
    return prefill_inner(k_func, v_func);
}

Status TieredSA::prefill_inner(
    std::function<uint64_t ()> k_func,
    std::function<BufferView ()> v_func,
    std::unordered_set<HashedKey> *refs
) {
    Status status = Status::Ok;
    status = log_->prefill(k_func, v_func, refs);
    if (status != Status::Ok) {
        return status;
    }

    for (uint64_t i = 0; i < middle_num_buckets_; i++) {
        Buffer buffer = read_bucket(i, false);
        RripBucket *bucket = reinterpret_cast<RripBucket *>(buffer.data());
        while (true) {
            HashedKey hk = k_func();
            // 将 key 和对应 set 对齐
            hk = hk - (hk % middle_num_buckets_) + i;
            BufferView v = v_func();
            if (bucket->isSpacePrefill(hk, v)) {
                bucket->remove(hk, nullptr);
                bucket->insert(hk, v, 0, nullptr);
                if (refs != nullptr) {
                    refs->insert(hk);
                }
            } else {
                break;
            }
        }
        bf_rebuild(i, bucket, bloom_filter_.get());
        const auto res = write_bucket(i, std::move(buffer), false);
        if (!res) {
            return Status::DeviceError;
        }
    }

    auto bottom_func = [&](uint64_t bottom_bid) {
        Buffer buffer = read_bucket(bottom_bid, true);
        RripBucket *bucket = reinterpret_cast<RripBucket *>(buffer.data());
        while (true) {
            HashedKey hk = k_func();
            // 将 key 和对应 set 对齐
            if (set_mode_ == SetLayerMode::FW) {
                hk = hk - (hk % bottom_num_buckets_) + bottom_bid;
            } else {
                uint64_t h_bits = hk - hk % (middle_num_buckets_ * BOTTOM_FACTOR);
                uint64_t l_bits = hk % MIDDLE_FACTOR;
                hk = h_bits + bottom_bid * MIDDLE_FACTOR + l_bits;
                assert(check_in_bottom_bid_x(hk, bottom_bid));
                assert(get_bottom_bid(hk) == bottom_bid);
            }
            // 如果中层可能存在 hk,则重新生成
            if (!bf_reject(get_middle_bid(hk), hk, bloom_filter_.get())) {
                continue;
            }
            BufferView v = v_func();
            if (bucket->isSpacePrefill(hk, v)) {
                bucket->remove(hk, nullptr);
                bucket->insert(hk, v, 0, nullptr);
                if (refs != nullptr) {
                    refs->insert(hk);
                }
            } else {
                break;
            }
        }
        bf_rebuild(bottom_bid, bucket, bottom_bloom_filter_.get());
        const auto res = write_bucket(bottom_bid, std::move(buffer), true);
        if (!res) {
            return Status::DeviceError;
        }

        return Status::Ok;
    };

    if (set_mode_ == SetLayerMode::FW) {
        uint64_t now_slice = 4;
        uint64_t prefill_slice = 0;
        while (prefill_slice < log_slice_num_) {
            assert(now_slice < log_slice_num_);
            for (uint64_t i = 0; i < middle_per_slice_; i++) {
                uint64_t bid = now_slice * middle_per_slice_ + i;
                status = bottom_func(bid);
                if (status != Status::Ok) {
                    return status;
                }
            }
            prefill_slice += 1;
            if (now_slice + 5 >= log_slice_num_) {
                now_slice = (now_slice - 1) % 5;
            } else {
                now_slice += 5;
            }
        }
    } else {
        for (uint64_t i = 0; i < bottom_num_buckets_; i++) {
            status = bottom_func(i);
            if (status != Status::Ok) {
                return status;
            }
        }
    }

    printf("Prefill done!\n");
    return status;
}

TieredSA::PerfCounter &TieredSA::perf_counter() { return perf_; }

LogLayer::PerfCounter &TieredSA::log_perf_counter() { return log_->perf_counter(); }

// gc mode
// 0: middle
// 1: middle + bottom
void TieredSA::move_bucket_fw(uint32_t bid, int gc_mode, ThreadLocalCounter *thc)
{
    uint64_t read_start, write_start;
    std::unordered_map<uint64_t, std::unique_ptr<ObjectInfo>> ois;

    read_start = RdtscTimer::instance().us();
    ois = log_->get_objects_to_move(KangarooBucketId{bid});
    thc->time_log_read += RdtscTimer::instance().us() - read_start;

    perf_.logObjAbsorbed.fetch_add(ois.size());

    // TODO: lock
    std::unique_lock<std::shared_mutex> lock(mutex_[get_mutex_from_middle_bid(bid)]);

    read_start = RdtscTimer::instance().us();
    auto middle_buf = read_bucket(bid, false);
    thc->time_set_read += RdtscTimer::instance().us() - read_start;
    if (middle_buf.isNull()) {
        assert(false);
    }
    auto *middle_bucket = reinterpret_cast<RripBucket *>(middle_buf.data());

    middle_bucket->reorder([&](uint32_t key_idx) {
        return bv_get_hit(bid, key_idx, bit_vector_.get());
    });

    if (gc_mode > 0) {
        // read_start = RdtscTimer::instance().us();
        auto bottom_buf = read_bucket(bid, true);
        // thc->time_set_read += RdtscTimer::instance().us() - read_start;
        if (bottom_buf.isNull()) {
            assert(false);
        }

        auto *bottom_bucket = reinterpret_cast<RripBucket *>(bottom_buf.data());
    
        bottom_bucket->reorder([&](uint32_t key_idx) {
            return bv_get_hit(bid, key_idx, bottom_bit_vector_.get());
        });

        rediv_bucket_fw(bottom_bucket, middle_bucket);

        // rewrite hot bucket if either gc caused by hot sets or redivide

        // rebuild bottom bf
        if (bottom_bloom_filter_ != nullptr) {
            bf_rebuild(bid, bottom_bucket, bottom_bloom_filter_.get());
        }

        write_start = RdtscTimer::instance().us();
        const auto res = write_bucket_erase(bid, std::move(bottom_buf), true);
        thc->time_set_write += RdtscTimer::instance().us() - write_start;
        if (!res) {
            printf("write bottom bucket failed\n");
            exit(-1);
        }
        if (res) {
            perf_.setPageWritten.fetch_add(1);
        }
    }
    bit_vector_->clear(bid);

    // only insert new objects into cold set
    // 直接访问频次按照插入
    for (auto &oi : ois) {
        if (middle_bucket->isSpace(oi.first, oi.second->value.view(), oi.second->hits)) {
            middle_bucket->remove(oi.first, nullptr);
            middle_bucket->insert(oi.first, oi.second->value.view(),
                                             oi.second->hits, nullptr);
            // sizeDist_.addSize(oi->key.key().size() + oi->value.size());
        } else {
            // drop
        }
    }

    if (bloom_filter_!= nullptr) {
        bf_rebuild(bid, middle_bucket, bloom_filter_.get());
    }

    write_start = RdtscTimer::instance().us();
    const auto res = write_bucket_erase(bid, std::move(middle_buf), false);
    thc->time_set_write += RdtscTimer::instance().us() - write_start;
    if (!res) {
        printf("write middle bucket failed\n");
        exit(-1);
    }
    if (res) {
        perf_.setPageWritten.fetch_add(1);
    }
}

void TieredSA::rediv_bucket_fw(RripBucket *hot_bucket, RripBucket *cold_bucket) {
    std::vector<std::unique_ptr<ObjectInfo>>
        movedKeys; // using hits as rrip value
    RedivideCallback moverCb = [&](HashedKey hk, BufferView value,
                                   uint8_t rrip_value) {
        auto ptr =
            std::make_unique<ObjectInfo>(hk, value, rrip_value, LogPageId(), 0);
        movedKeys.push_back(std::move(ptr));
        return;
    };

    RripBucket::Iterator it = cold_bucket->getFirst();
    while (!it.done()) {
        // lower rrip values (ie highest pri objects) are at the end
        // 找到最低 RRIP 值的高优先级对象
        RripBucket::Iterator nextIt = cold_bucket->getNext(it);
        while (!nextIt.done()) {
            nextIt = cold_bucket->getNext(nextIt);
        }

        bool space =
            hot_bucket->isSpaceRrip(it.hashedKey(), it.value(), it.rrip());
        if (!space) {
            break;
        }

        hot_bucket->makeSpace(it.hashedKey(), it.value(), moverCb);
        hot_bucket->insertRrip(it.hashedKey(), it.value(), it.rrip(), nullptr);
        cold_bucket->remove(it.hashedKey(), nullptr);
        it = cold_bucket->getFirst(); // just removed the last object
    }

    // move objects into cold bucket (if there's any overflow evict lower pri
    // items)
    for (auto &oi : movedKeys) {
        cold_bucket->insertRrip(oi->key, oi->value.view(), oi->hits, nullptr);
    }
}

void TieredSA::move_middle_bucket_x(
    uint32_t bid,
    uint32_t bottom_bid,
    std::vector<std::unique_ptr<ObjectInfo>> *of_ois,
    ThreadLocalCounter *thc
) {
    uint64_t read_start, write_start;
    std::unordered_map<uint64_t, std::unique_ptr<ObjectInfo>> ois;

    assert(get_mutex_from_middle_bid(bid) == get_mutex_from_bottom_bid(bottom_bid));
    std::unique_lock<std::shared_mutex> lock(mutex_[get_mutex_from_middle_bid(bid)]);
    
    read_start = RdtscTimer::instance().us();
    ois = log_->get_objects_to_move(KangarooBucketId{bid});
    thc->time_log_read += RdtscTimer::instance().us() - read_start;

    perf_.logObjAbsorbed.fetch_add(ois.size());

    read_start = RdtscTimer::instance().us();
    auto middle_buf = read_bucket(bid, false);
    thc->time_set_read += RdtscTimer::instance().us() - read_start;
    if (middle_buf.isNull()) {
        assert(false);
    }
    auto *middle_bucket = reinterpret_cast<RripBucket *>(middle_buf.data());

    middle_bucket->reorder([&](uint32_t key_idx) {
        return bv_get_hit(bid, key_idx, bit_vector_.get());
    });

    {
        // pre calc data
        std::vector<uint64_t> of_keys;
        for (auto &oi : ois) {
            if (check_in_bottom_bid_x(oi.first, bottom_bid)) {
                of_keys.push_back(oi.first);
                of_ois->push_back(std::move(oi.second));
            }
        }

        for (auto &key : of_keys) {
            ois.erase(key);
        }
    }

    rediv_bucket_x(middle_bucket, bottom_bid, of_ois, ois.size());

    // only insert new objects into cold set
    // 直接访问频次按照插入
    for (auto &oi : ois) {
        if (middle_bucket->isSpace(oi.first, oi.second->value.view(), oi.second->hits)) {
            middle_bucket->remove(oi.first, nullptr);
            middle_bucket->insert(oi.first, oi.second->value.view(),
                                             oi.second->hits, nullptr);
            // sizeDist_.addSize(oi->key.key().size() + oi->value.size());
        } else {
            // drop
        }
    }

    bf_rebuild(bid, middle_bucket, bloom_filter_.get());

    write_start = RdtscTimer::instance().us();
    const auto res = write_bucket_erase(bid, std::move(middle_buf), false);
    thc->time_set_write += RdtscTimer::instance().us() - write_start;
    if (!res) {
        printf("write middle bucket failed\n");
        exit(-1);
    }

    if (res) {
        perf_.setPageWritten.fetch_add(1);
    }
}

void TieredSA::move_bottom_bucket_x(
    uint32_t bid,
    std::vector<std::unique_ptr<ObjectInfo>> of_ois,
    ThreadLocalCounter *thc
) {
    // TODO: lock
    uint64_t read_start, write_start;
    std::unique_lock<std::shared_mutex> lock(mutex_[get_mutex_from_bottom_bid(bid)]);

    read_start = RdtscTimer::instance().us();
    auto bottom_buf = read_bucket(bid, true);
    thc->time_set_read += RdtscTimer::instance().us() - read_start;
    if (bottom_buf.isNull()) {
        assert(false);
    }
    auto *bottom_bucket = reinterpret_cast<RripBucket *>(bottom_buf.data());

    bottom_bucket->reorder([&](uint32_t key_idx) {
        return bv_get_hit(bid, key_idx, bottom_bit_vector_.get());
    });

    // only insert new objects into cold set
    // 直接访问频次按照插入
    for (auto &oi : of_ois) {
        if (bottom_bucket->isSpace(oi->key, oi->value.view(), oi->hits)) {
            bottom_bucket->remove(oi->key, nullptr);
            bottom_bucket->insert(oi->key, oi->value.view(),
                                             oi->hits, nullptr);
            // sizeDist_.addSize(oi->key.key().size() + oi->value.size());
        } else {
            // drop
        }
    }

    bf_rebuild(bid, bottom_bucket, bottom_bloom_filter_.get());

    write_start = RdtscTimer::instance().us();
    const auto res = write_bucket_erase(bid, std::move(bottom_buf), true);
    thc->time_set_write += RdtscTimer::instance().us() - write_start;
    if (!res) {
        printf("write bottom bucket failed\n");
        exit(-1);
    }

    if (res) {
        perf_.setPageWritten.fetch_add(1);
    }
}

void TieredSA::rediv_bucket_x(
    RripBucket *bucket,
    uint32_t bottom_bid,
    std::vector<std::unique_ptr<ObjectInfo>> *of_ois,
    uint32_t cnt
) {
    RedivideCallback moverCb = [&](HashedKey hk, BufferView value,
        uint8_t rrip_value) {
        auto ptr =
        std::make_unique<ObjectInfo>(hk, value, rrip_value, LogPageId(), 0);
        of_ois->push_back(std::move(ptr));
        return;
    };
    
    RripBucket::Iterator it = bucket->getFirst();
    while (!it.done()) {
        if (cnt == 0) {
            break;
        }
        if (check_in_bottom_bid_x(it.hashedKey(), bottom_bid)) {
            moverCb(it.hashedKey(), it.value(), it.rrip());
            cnt--;
            bucket->remove(it.hashedKey(), nullptr);
            it = bucket->getFirst();
        } else {
            it = bucket->getNext(it);
        }
    }
}

uint32_t TieredSA::get_middle_bid(HashedKey hk) const {
    return hk % middle_num_buckets_;
}

uint32_t TieredSA::get_bottom_bid(HashedKey hk) const {
    if (set_mode_ == SetLayerMode::FW) {
        return hk % middle_num_buckets_;
    } else {
        return (hk % (middle_num_buckets_ * BOTTOM_FACTOR)) / MIDDLE_FACTOR;
    }
}

bool TieredSA::check_in_bottom_bid_x(HashedKey hk, uint32_t bottom_bid) const {
    return ((hk % (middle_num_buckets_ * BOTTOM_FACTOR))) / MIDDLE_FACTOR == bottom_bid;
}

uint32_t TieredSA::get_mutex_from_middle_bid(uint32_t middle_bid) const {
    return (middle_bid / MIDDLE_FACTOR) % log_slice_num_;
}

uint32_t TieredSA::get_mutex_from_bottom_bid(uint32_t bottom_bid) const {
    return bottom_bid % log_slice_num_;
}

Buffer TieredSA::read_bucket(uint32_t bid, bool bottom) {
    Buffer buffer;
    bool new_buf = false;
    if (bottom) {
        buffer = bottom_wren_->read(KangarooBucketId{bid}, new_buf);
    } else {
        buffer = wren_->read(KangarooBucketId{bid}, new_buf);
    }

    if (buffer.isNull()) {
        printf("read bucket %d failed\n", bid);
        return {};
    }

    auto *bucket = reinterpret_cast<RripBucket *>(buffer.data());

    uint32_t checksum = RripBucket::computeChecksum(buffer.view());

    const auto checksum_equal = (checksum == bucket->getChecksum());
    // We can only know for certain this is a valid checksum error if bloom
    // filter is already initialized. Otherwise, it could very well be because
    // we're reading the bucket for the first time.
    if (!new_buf && !checksum_equal) {
        printf("Checksum error for bucket %d compute: %d actual: %d\n", bid, checksum, bucket->getChecksum());
        // checksumErrorCount_.inc();
    }

    if (!checksum_equal || new_buf ||
        static_cast<uint64_t>(gen_time_.count()) !=
            bucket->generationTime()) {
        RripBucket::initNew(buffer.mutableView(), gen_time_.count());
    }
    return buffer;
}

bool TieredSA::write_bucket(uint32_t bid, Buffer buffer, bool bottom) {
    bool ret = false;
    auto *bucket = reinterpret_cast<RripBucket *>(buffer.data());
    bucket->setChecksum(RripBucket::computeChecksum(buffer.view()));
    if (bottom) {
        ret = bottom_wren_->write(KangarooBucketId{bid}, std::move(buffer));
    } else {
        ret = wren_->write(KangarooBucketId{bid}, std::move(buffer));
    }

    return ret;
}

bool TieredSA::write_bucket_erase(uint32_t bid, Buffer buffer, bool bottom) {
    bool ret = false;
    auto *bucket = reinterpret_cast<RripBucket *>(buffer.data());
    bucket->setChecksum(RripBucket::computeChecksum(buffer.view()));
    
    // write
    if (bottom) {
        ret = bottom_wren_->write(KangarooBucketId{bid}, std::move(buffer), true);
    } else {
        ret = wren_->write(KangarooBucketId{bid}, std::move(buffer), true);
    }

    if (!ret) {
        exit(-1);
    }

    return ret;
}

bool TieredSA::bv_get_hit(uint32_t bid, uint32_t key_idx, RripBitVector *bit_vector) const {
    if (bit_vector) {
        return bit_vector->get(bid, key_idx);
    }
    return false;
}

void TieredSA::bv_set_hit(uint32_t bid, uint32_t key_idx, RripBitVector *bit_vector) const {
    if (bit_vector) {
        bit_vector->set(bid, key_idx);
    }
}

void TieredSA::bf_rebuild(uint32_t bid, const RripBucket *bucket, BloomFilter *bloom_filter) {
    assert(bloom_filter != nullptr);
    assert(bucket != nullptr);
    bloom_filter->clear(bid);
    auto iter = bucket->getFirst();
    uint32_t i = 0;
    uint32_t total = bucket->size();
    while (!iter.done() && i < total) {
        bloom_filter->set(bid, iter.hashedKey());
        iter = bucket->getNext(iter);
    }
}

void TieredSA::bf_build(uint32_t bid, const RripBucket *bucket, BloomFilter *bloom_filter) {
    assert(bloom_filter!= nullptr);
    auto iter = bucket->getFirst();
    while (!iter.done()) {
        bloom_filter->set(bid, iter.hashedKey());
        iter = bucket->getNext(iter);
    }
}

bool TieredSA::bf_reject(uint32_t bid, uint64_t key_hash, BloomFilter *bloom_filter) const {
    if (bloom_filter == nullptr) {
        return false;
    }
    return !bloom_filter->couldExist(bid, key_hash);
}

void TieredSA::migrate_log_slice_fw(uint64_t tid) {
    {
        std::unique_lock<std::mutex> lock(mig_sync_);
        mig_sync_threads_++;
    }
    ThreadLocalCounter thc;
    uint64_t mig_head = 0;
    uint64_t mig_cnt = 0;

    while (true) {
        {
            std::unique_lock<std::mutex> lock(mig_sync_);
            if (mig_bid_gen_ == 0) {
                break;
            }
            mig_cnt = 32LL > mig_bid_gen_ ? mig_bid_gen_ : 32LL;
            mig_bid_gen_ -= mig_cnt;
            mig_head = mig_bid_gen_;
        }

        for (uint64_t i = 0; i < mig_cnt; i++) {
            uint64_t bid = mig_slice_id_ * middle_per_slice_ + mig_head + i;
            move_bucket_fw(
                bid,
                (cycle_cnt_ + mig_slice_id_) % 5 == 4 ? 1 : 0,
                &thc
            );
        }
    }

    perf_.time_log_read.fetch_add(thc.time_log_read);
    perf_.time_set_read.fetch_add(thc.time_set_read);
    perf_.time_set_write.fetch_add(thc.time_set_write);

    {
        std::unique_lock<std::mutex> lock(mig_sync_);
        mig_sync_threads_--;
    }
}

// 暂时按 log zone 为单位处理，后面可以考虑使用更小的粒度
void TieredSA::do_migrate_fw() {
    printf("TieredSA migrate log slice start\n");
    uint64_t start_ts = RdtscTimer::instance().ms();

    uint64_t mig_cnt = 0;
    for (mig_cnt = 0; mig_cnt < log_slice_num_ * 1.0 / log_zone_num_; mig_cnt++) {
        // notify all threads
        mig_bid_gen_ = middle_per_slice_;
        mig_sync_cond_.notify_all();
        // start itself
        migrate_log_slice_fw(0);
        // wait until all threads done
        while (mig_bid_gen_ > 0 || mig_sync_threads_ > 0) {
            // mig_sync_cond_.notify_one();
        }

        // update slice
        mig_slice_id_ = (mig_slice_id_ + 1) % log_slice_num_;
        if (mig_slice_id_ == 0) {
            perf_.warm_ok.store(true);
        }
        if (mig_slice_id_ == 0) {
            cycle_cnt_ = (cycle_cnt_ + 1) % 5;
        }
    }
    printf("TieredSA migrate %ld log slices, time %lf s\n", mig_cnt, (RdtscTimer::instance().ms() - start_ts) / 1000.0);
    printf("TieredSA time log read: %lf s, time set read: %lf s, time set write: %lf s\n",
        perf_.time_log_read.exchange(0) / 1000000.0,
        perf_.time_set_read.exchange(0) / 1000000.0,
        perf_.time_set_write.exchange(0) / 1000000.0
    );
    // log_->dump_cache_stats();
    // 清理 log zone
    const uint64_t SET_PAGE_NUM = 256;
    Buffer buffer;
    
    for (uint64_t seg = 0; seg < pages_per_zone_; seg += SET_PAGE_NUM) {
        uint64_t seg_page_num = std::min(SET_PAGE_NUM, pages_per_zone_ - seg);
        std::vector<uint64_t> pages;
        std::vector<uint64_t> obj_keys;
        log_->prepare_clean_seg(seg_page_num, &pages);
        for (uint64_t i = 0; i < pages.size(); i++) {
            log_->read_page(pages[i], &buffer);
            log_->readmit(pages[i], std::move(buffer));
            perf_.logReadmitted.fetch_add(1);
        }
        log_->complete_clean_seg(seg_page_num);
    }
    printf("TieredSA migrate log slice end, time %lf s\n", (RdtscTimer::instance().ms() - start_ts) / 1000.0);
    printf("TieredSA SetWritten: %ld readmitted: %ld, absorbed: %ld\n",
        perf_.setPageWritten.load(), perf_.logReadmitted.load(), perf_.logObjAbsorbed.load());
    log_->dec_wait_op();
}

void TieredSA::migrate_loop_fw() {
    while (!stop_flag_) {
        usleep(20 * 1000);
        if (log_->check_migrate()) {
            do_migrate_fw();
        }
    }
}

void TieredSA::migrate_wait_loop_fw(uint64_t tid) {
    while (!stop_flag_) {
        if (mig_bid_gen_ > 0) {
            migrate_log_slice_fw(tid);
        }
        {
            std::unique_lock<std::mutex> lock(mig_sync_);
            mig_sync_cond_.wait(lock);
        }
    }
}

void TieredSA::migrate_log_slice_x(uint64_t tid) {
    {
        std::unique_lock<std::mutex> lock(mig_sync_);
        mig_sync_threads_ ++;
    }

    ThreadLocalCounter thc;
    std::vector<std::unique_ptr<ObjectInfo>> of_objs;
    uint64_t mig_head = 0;
    uint64_t mig_cnt = 0;
    while (true) {
        {
            std::unique_lock<std::mutex> lock(mig_sync_);
            if (mig_bid_gen_ == 0) {
                break;
            }
            mig_cnt = 16LL * MIDDLE_FACTOR > mig_bid_gen_ ? mig_bid_gen_ : 16LL * MIDDLE_FACTOR;
            mig_bid_gen_ -= mig_cnt;
            mig_head = mig_bid_gen_;
        }

        for (uint64_t i = 0; i < mig_cnt; i++) {
            uint64_t middle_bid = mig_slice_id_ * middle_per_slice_ + mig_head + i;
            uint64_t bottom_bid = (cycle_cnt_ * middle_num_buckets_ + middle_bid) / MIDDLE_FACTOR;
            move_middle_bucket_x(middle_bid, bottom_bid, &of_objs, &thc);

            if (i % MIDDLE_FACTOR == 1) {
                move_bottom_bucket_x(bottom_bid, std::move(of_objs), &thc);
                of_objs.clear();
            }
        }
    }

    perf_.time_log_read.fetch_add(thc.time_log_read);
    perf_.time_set_read.fetch_add(thc.time_set_read);
    perf_.time_set_write.fetch_add(thc.time_set_write);
    {
        std::unique_lock<std::mutex> lock(mig_sync_);
        mig_sync_threads_ --;
    }
}

// 暂时按 log zone 为单位处理，后面可以考虑使用更小的粒度
void TieredSA::do_migrate_x() {
    printf("TieredSA migrate log slice start\n");
    uint64_t start_ts = RdtscTimer::instance().ms();

    uint64_t mig_cnt = 0;
    for (mig_cnt = 0; mig_cnt < log_slice_num_ * 1.0 / log_zone_num_; mig_cnt++) {
        // notify all threads
        mig_bid_gen_ = middle_per_slice_;
        mig_sync_cond_.notify_all();
        // start itself
        migrate_log_slice_x(0);
        // wait until all threads done
        while (mig_bid_gen_ > 0 || mig_sync_threads_ > 0) {
            // mig_sync_cond_.notify_one();
        }

        // update slice
        mig_slice_id_ = (mig_slice_id_ + 1) % log_slice_num_;
        if (mig_slice_id_ == 0) {
            perf_.warm_ok.store(true);
        }
        if (mig_slice_id_ == 0) {
            cycle_cnt_ = (cycle_cnt_ + 1) % BOTTOM_FACTOR;
        }
    }
    printf("TieredSA migrate %ld log slices, time %lf s\n", mig_cnt, (RdtscTimer::instance().ms() - start_ts) / 1000.0);
    printf("TieredSA time log read: %lf s, time set read: %lf s, time set write: %lf s\n",
        perf_.time_log_read.exchange(0) / 1000000.0,
        perf_.time_set_read.exchange(0) / 1000000.0,
        perf_.time_set_write.exchange(0) / 1000000.0
    );
    //log_->dump_cache_stats();
    // 清理 log zone
    const uint64_t SEG_PAGE_NUM = 256;
    Buffer buffer;

    uint64_t readmit_pages = 0;
    
    for (uint64_t seg = 0; seg < pages_per_zone_; seg += SEG_PAGE_NUM) {
        uint64_t seg_page_num = std::min(SEG_PAGE_NUM, pages_per_zone_ - seg);
        std::vector<uint64_t> pages;
        std::vector<uint64_t> obj_keys;
        log_->prepare_clean_seg(seg_page_num, &pages);
        for (uint64_t i = 0; i < pages.size(); i++) {
            log_->read_page(pages[i], &buffer);
            log_->readmit(pages[i], std::move(buffer));
            perf_.logReadmitted.fetch_add(1);
            readmit_pages++;
        }
        log_->complete_clean_seg(seg_page_num);
    }
    // perf_.logProcessed.fetch_add(pages_per_zone_);
    printf("TieredSA migrate log slice end, time: %lf s\n",
        (RdtscTimer::instance().ms() - start_ts) / 1000.0);
    
    printf("TieredSA SetWritten: %ld readmitted: %ld, absorbed: %ld\n",
        perf_.setPageWritten.load(), perf_.logReadmitted.load(), perf_.logObjAbsorbed.load());
    log_->dec_wait_op();
}

void TieredSA::migrate_loop_x() {
    while (!stop_flag_) {
        usleep(1 * 1000);
        if (log_->check_migrate()) {
            do_migrate_x();
        }
    }
    mig_sync_cond_.notify_all();
}

void TieredSA::migrate_wait_loop_x(uint64_t tid) {
    while (!stop_flag_) {
        if (mig_bid_gen_ > 0) {
            migrate_log_slice_x(tid);
        }
        {
            std::unique_lock<std::mutex> lock(mig_sync_);
            mig_sync_cond_.wait(lock);
        }
    }
}