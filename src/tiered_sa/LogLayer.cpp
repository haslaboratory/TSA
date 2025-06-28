#include "tiered_sa/LogLayer.h"

LogLayer::Config &LogLayer::Config::validate() {
    if (set_number_callback == nullptr) {
        printf("set_number_callback is nullptr\n");
        exit(-1);
    }

    if (device == nullptr) {
        printf("device is nullptr\n");
        exit(-1);
    }

    if (page_size == 0 || zone_size == 0) {
        printf("page_size is 0\n");
        exit(-1);
    }

    if (slice_num == 0) {
        printf("slice_num is 0\n");
        exit(-1);
    }


    return *this;
}

LogLayer::LogLayer(Config &&config)
    : LogLayer(std::move(config.validate()), ValidConfigTag()) {}

LogLayer::LogLayer(Config &&config, ValidConfigTag)
    : page_size_(config.page_size),
      slice_num_(config.slice_num),
      bucket_per_slice_(config.bucket_per_slice),
      log_start_off_(config.log_start_off),
      log_zone_num_(config.log_zone_num),
      pages_per_zone_(config.zone_size / config.page_size),
      device_(*config.device),
      set_number_cb_(config.set_number_callback),
      page_bit_(config.log_zone_num * config.zone_size / config.page_size),
      index_{new ChainedLogIndex *[config.slice_num]}
{
    FlushCache::Config flush_cache_config {
        uint32_t(slice_num_ * 1.5),
        128,
        page_size_,
        &device_
    };
    LogWriter::Config log_writer_config {
        slice_num_,
        page_size_
    };

    flush_cache_ = std::make_unique<FlushCache>(std::move(flush_cache_config));
    log_writer_ = std::make_unique<LogWriter>(std::move(log_writer_config));
    
    slice_mutexs_ = std::make_unique<std::shared_mutex[]>(config.slice_num);

    for (uint64_t i = 0; i < config.slice_num; i++) {
        index_[i] =
            new ChainedLogIndex(config.set_num / config.slice_num,
                                2048, config.set_number_callback);
    }
}

LogLayer::~LogLayer() {
    for (uint64_t i = 0; i < slice_num_; i++) {
        delete index_[i];
    }
    delete[] index_;
}

Status LogLayer::lookup(HashedKey hk, Buffer *value) {
    uint32_t slice_id = get_slice_id(hk);
    // index 读
    PartitionOffset offset = index_[slice_id]->lookup(hk, true, nullptr);

    Buffer buf;
    BufferView view;
    LogBucket *page;
    
    {
        std::shared_lock<std::shared_mutex> lock{slice_mutexs_[slice_id]};
        // buffer 读
        if (log_writer_->lookup(hk, value, slice_id) == Status::Ok) {
            return Status::Ok;
        }

        if (!offset.isValid()) {
            return Status::NotFound;
        }

        Status status = read_page(partition_offset_to_page(offset), &buf);
        if (status != Status::Ok) {
            return status;
        }
    }

    page = reinterpret_cast<LogBucket *>(buf.data());
    view = page->find(hk);
    if (view.isNull()) {
        return Status::NotFound;
    }

    *value = Buffer{view};
    return Status::Ok;
}

// TODO: log 区写合并到一个 superpage
Status LogLayer::insert(HashedKey hk, BufferView value) {
    uint32_t slice_id = get_slice_id(hk);
    uint64_t page_idx = 0;
    std::unique_lock<std::shared_mutex> lock{slice_mutexs_[slice_id]};

    while (true) {
        Status status = log_writer_->insert(hk, value, slice_id, &page_idx);
        if (status == Status::Ok) {
            break;
        }
        if (status != Status::Retry) {
            printf("unexpected status: %d\n", uint32_t(status));
            exit(-1);
        }
        // wait flush
        status = log_writer_->flush_log(slice_id, [&](Buffer buf, std::vector<HashedKey> &keys) {
            uint64_t page_offset = w_agent_.write(std::move(buf), this);
            for (auto &key : keys) {
                Status status = index_[slice_id]->insert(key, page_offset_to_partition(page_offset));
                if (status != Status::Ok) {
                    printf("unexpected status: %d\n", uint32_t(status));
                    exit(-1);
                }
            }
        });

        if (status != Status::Ok) {
            printf("unexpected status: %d\n", uint32_t(status));
            exit(-1);
        }
    }

    return Status::Ok;
}

Status LogLayer::remove(HashedKey hk) {
    uint32_t slice_id = get_slice_id(hk);
    std::unique_lock<std::shared_mutex> lock{slice_mutexs_[slice_id]};
    Status status = log_writer_->remove(hk, slice_id);
    if (status!= Status::Ok) {
        return status;
    }
    status = index_[slice_id]->remove(hk, PartitionOffset{0, false});
    if (status!= Status::Ok) {
        return status;
    }
    return Status::Ok;
}

Status LogLayer::prefill(
    std::function<uint64_t ()> k_func,
    std::function<BufferView ()> v_func,
    std::unordered_set<HashedKey> *refs
) {
    while (!w_agent_.check_prefill_done(this)) {
        HashedKey hk = k_func();
        BufferView v = v_func();
        Status status = insert(hk, v);
        if (status!= Status::Ok) {
            return status;
        }
        if (refs != nullptr) {
            refs->insert(hk);
        }
    }
    return Status::Ok;
}

// Status LogLayer::cache_lookup(HashedKey hk, uint64_t page_offset, Buffer *value) {
//     return flush_cache_->read(page_offset, &device_, [&](Buffer *buf) -> Status {
//         LogBucket *page = reinterpret_cast<LogBucket *>(buf->data());

//         BufferView found = page->find(hk);
//         if (found.isNull()) {
//             return Status::NotFound;
//         }
//         page_bit_.clear_bit(page_offset);
//         value->copyFrom(0, found);
//         return Status::Ok;
//     });
// }

Status LogLayer::read_page(uint64_t page_offset, Buffer *buf) {
    if (buf->isNull()) {
        *buf = device_.makeIOBuffer(page_size_);
    }

    assert(buf->size() >= page_size_);
    device_.read(page_offset, page_size_, buf->data());
    return Status::Ok;
}

// 此处暂时简单处理，考虑一致性问题怎么解决
Status LogLayer::readmit(uint64_t page_offset, Buffer buf) {
    LogBucket *bucket = reinterpret_cast<LogBucket *>(buf.data());
    std::vector<HashedKey> keys;
    bucket->collect_keys(keys);

    uint64_t slice_id = get_slice_id(keys[0]);

    uint64_t check_cnt = 0;
    uint64_t readmit_cnt = 0;

    uint64_t new_page_offset = w_agent_.write(std::move(buf), this, true);
    for (auto key: keys) {
        check_cnt += 1;
        Status status = index_[slice_id]->remove(key, page_offset_to_partition(page_offset));
        if (status == Status::Ok) {
            readmit_cnt += 1;
            index_[slice_id]->insert(key, page_offset_to_partition(new_page_offset));
        }
    }
    // printf("readmit: %ld, check_cnt: %ld, readmit_cnt: %ld\n", page_offset, check_cnt, readmit_cnt);

    return Status::Ok;
}

Status LogLayer::prepare_clean_seg(uint64_t page_num, std::vector<uint64_t> *pages) {
    uint64_t page_idx = 0;
    Status status = w_agent_.load_clean_seg(this, &page_idx);
    if (status != Status::Ok) {
        return status;
    }
    page_bit_.check_valid_pages(page_idx, page_num, pages);
    for (uint32_t i = 0; i < pages->size(); i++) {
        (*pages)[i] = page_idx_to_offset((*pages)[i]);
    }
    return Status::Ok;
}

Status LogLayer::complete_clean_seg(uint64_t page_num) {
    uint64_t page_idx = 0;
    Status status = w_agent_.load_clean_seg(this, &page_idx);
    if (status != Status::Ok) {
        return status;
    }
    page_bit_.set_bits(page_idx, page_num);

    if ((page_idx + page_num) % pages_per_zone_ == 0) {
        printf("LogLayer clean zone: %lu\n", page_idx / pages_per_zone_);
    }

    return w_agent_.complete_clean_seg(this, page_num);
}

std::unordered_map<uint64_t, std::unique_ptr<ObjectInfo>> LogLayer::get_objects_to_move(KangarooBucketId bid) {
    uint64_t slice_id = get_slice_id(bid.index());
    
    std::unordered_map<uint64_t, std::unique_ptr<ObjectInfo>> objects;
    HashedKey key = HashedKey("");
    uint8_t hits;
    uint32_t tag;

    PartitionOffset offset;

    ChainedLogIndex::BucketIterator it = index_[slice_id]->getHashBucketIterator(bid);
    
    // Find value, could be in in-memory buffer or on nvm
    // TODO: buffer 里的暂时不管，因为需要处理删除，留待之后处理
    
    // 从索引里拿的一定是 on nvm
    while (!it.done()) {
        hits = it.hits();
        tag = it.tag();
        it = index_[slice_id]->getNext(it);
        offset = index_[slice_id]->find(KangarooBucketId{bid}, tag);
        if (!offset.isValid()) {
            continue;
        }
        Buffer value;
        Status status = flush_cache_->read(partition_offset_to_page(offset), 0, [&](Buffer *buf) -> Status {
            LogBucket *bucket = reinterpret_cast<LogBucket *>(buf->data());
            BufferView view = bucket->findTag(tag, key);
            value.resize(view.size());
            value.copyFrom(0, view);
            return Status::Ok;
        });
        if (status != Status::Ok) {
            printf("read_page error %ld\n", uint64_t(status));
            continue;
        }

        if (value.isNull()) {
            index_[slice_id]->remove(tag, bid, offset);
            continue;
        } else if (set_number_cb_(key).index() != bid.index()) {
            continue;
        }

        // 标记 page bit
        page_bit_.clear_bit(offset.index());
        // 拿出来后会尝试 remove
        index_[slice_id]->remove(tag, bid, offset);
        // obj_index_test_->remove(key);

        // TieredSA 不需要 LogPageId，直接设置为无效值占位
        auto ptr = std::make_unique<ObjectInfo>(key, std::move(value), hits, LogPageId{0, false}, tag);
        objects.insert({key, std::move(ptr)});
    }

    return objects;
}

bool LogLayer::check_migrate() {
    return w_agent_.check_migrate(this);
}

void LogLayer::dec_wait_op() {
    std::unique_lock<std::shared_mutex> lock(meta_mutex_);
    double dec = 1.0 / log_zone_num_;
    if (log_wait_op_ > 0.2 + dec) {
        log_wait_op_ -= dec;
    } else {
        log_wait_op_ = 0.2;
    }
    clean_cv_.notify_all();
}

uint32_t LogLayer::get_slice_id(HashedKey hk) {
    return (hk / bucket_per_slice_) % slice_num_;
}

uint64_t LogLayer::page_idx_to_offset(uint64_t page_idx) {
    uint32_t zone_in_region = page_idx / pages_per_zone_;
    uint32_t page_in_zone = page_idx % pages_per_zone_;
    return log_start_off_ + zone_in_region * device_.getIOZoneSize() + page_size_ * page_in_zone;
}

uint64_t LogLayer::page_offset_to_idx(uint64_t page_offset) {
    uint32_t zone_in_region = (page_offset - log_start_off_) / device_.getIOZoneSize();
    uint32_t page_in_zone = ((page_offset - log_start_off_) % device_.getIOZoneSize()) / page_size_;
    return zone_in_region * pages_per_zone_ + page_in_zone;
}

PartitionOffset LogLayer::page_offset_to_partition(uint64_t page_offset) {
    return PartitionOffset(page_offset_to_idx(page_offset), true);
}

uint64_t LogLayer::partition_offset_to_page(PartitionOffset offset) {
    return page_idx_to_offset(offset.index());
}

Status LogLayer::page_lookup(uint64_t page_offset, uint8_t _ver, std::function<Status(Buffer *)> read_cb) {
    Buffer buffer;
    Status status = read_page(page_offset, &buffer);
    if (status != Status::Ok) {
        return status;
    }
    return read_cb(&buffer);
}

uint64_t LogLayer::PageWriterAgent::write(Buffer buf, LogLayer *layer, bool force) {
    std::unique_lock<std::shared_mutex> lock(layer->meta_mutex_);
    while (check_write_wait(layer) && !force) {
        // wait until ok
        layer->clean_cv_.wait(lock);
    }

    if (write_page_id_< clean_page_id_ && (write_page_id_ / layer->pages_per_zone_ 
            == clean_page_id_ / layer->pages_per_zone_)) {
        printf("LogLayer write overflow!\n");
        exit(-1);
    }

    uint64_t page_offset = layer->page_idx_to_offset(write_page_id_);

    if (write_page_id_ % layer->pages_per_zone_ == 0) {
        printf("LogLayer reset zone %lu\n", write_page_id_ / layer->pages_per_zone_);
        uint64_t zone_start = page_offset - page_offset % layer->device_.getIOZoneSize();
        layer->device_.reset(zone_start, layer->page_size_ * layer->pages_per_zone_);
    }

    bool ret = layer->device_.write(page_offset, std::move(buf));

    if (!ret) {
        printf("LogLayer write error 0x%lx, write_page_id_: %ld, force: %d\n", page_offset, write_page_id_, force);
        exit(-1);
    }

    if (write_page_id_ % layer->pages_per_zone_ == layer->pages_per_zone_ - 1) {
        printf("LogLayer finish zone %lu\n", write_page_id_ / layer->pages_per_zone_);
        uint64_t zone_start = page_offset - page_offset % layer->device_.getIOZoneSize();
        layer->device_.finish(zone_start, layer->page_size_ * layer->pages_per_zone_);
    }

    // inc page id
    write_page_id_++;
    if (write_page_id_ >= layer->pages_per_zone_ * layer->log_zone_num_) {
        write_page_id_ = 0;
    }

    lock.unlock();
    layer->perf_.logPageWritten.fetch_add(1);

    return page_offset;
}

Status LogLayer::PageWriterAgent::load_clean_seg(LogLayer *layer, uint64_t *page_idx) {
    std::shared_lock<std::shared_mutex> lock(layer->meta_mutex_);
    *page_idx = clean_page_id_;
    return Status::Ok;
}

Status LogLayer::PageWriterAgent::complete_clean_seg(LogLayer *layer, uint64_t page_num) {
    // printf("complete clean\n");
    {
        std::unique_lock<std::shared_mutex> lock(layer->meta_mutex_);
        clean_page_id_ = (clean_page_id_ + page_num) % (layer->log_zone_num_ * layer->pages_per_zone_);
    }
    if (!check_write_wait(layer)) {
        layer->clean_cv_.notify_all();
    }
    return Status::Ok;
}

bool LogLayer::PageWriterAgent::check_migrate(LogLayer *layer) {
    if (check_write_wait(layer)) {
        return true;
    }
    uint64_t free_pages = 0;
    if (clean_page_id_ > write_page_id_) {
        free_pages = clean_page_id_ - write_page_id_;
    } else {
        free_pages = (layer->pages_per_zone_ * layer->log_zone_num_) + clean_page_id_ - write_page_id_;
    }

    if (free_pages <= layer->log_threshhold * layer->pages_per_zone_ * layer->log_zone_num_) {
        return true;
    }
    return false;
}

bool LogLayer::PageWriterAgent::check_prefill_done(LogLayer *layer) {
    uint64_t free_pages = 0;
    uint64_t clean_page_id = clean_page_id_;
    clean_page_id = clean_page_id - clean_page_id % layer->pages_per_zone_;
    if (clean_page_id > write_page_id_) {
        free_pages = clean_page_id - write_page_id_;
    } else {
        free_pages = (layer->pages_per_zone_ * layer->log_zone_num_) + clean_page_id_ - write_page_id_;
    }

    if (free_pages <= 2 * layer->pages_per_zone_) {
        return true;
    }

    return false;
}

bool LogLayer::PageWriterAgent::check_write_wait(LogLayer *layer) {
    uint64_t free_pages = 0;

    uint64_t clean_page_id = clean_page_id_;
    clean_page_id = clean_page_id - clean_page_id % layer->pages_per_zone_;

    if (clean_page_id > write_page_id_) {
        free_pages = clean_page_id - write_page_id_;
    } else {
        free_pages = (layer->pages_per_zone_ * layer->log_zone_num_) + clean_page_id_ - write_page_id_;
    }

    if (free_pages <= layer->log_wait_op_ * layer->pages_per_zone_) {
        return true;
    }

    return false;
}