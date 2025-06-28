#include "tiered_sa/LogWriter.h"

LogWriter::Config &LogWriter::Config::validate() {
    if (page_size != 4096) {
        printf("page_size must be 4096 bytes\n");
        exit(-1);
    }

    if (slice_num == 0) {
        printf("slice_num cannot be 0\n");
        exit(-1);
    }

    if (slice_num * page_size > 1024 * 1024 * 1024) {
        printf("slice_num * page_size cannot exceed 1GB\n");
        exit(-1);
    }
    return *this;
}

LogWriter::LogWriter(Config &&config)
    : config_(std::move(config.validate())),
      bucket_buffer_(config_.page_size * config_.slice_num),
      buckets_(new LogBucket *[config_.slice_num]),
      page_cnt_(new uint32_t[config_.slice_num])
{
    for (uint32_t i = 0; i < config_.slice_num; i++) {
        // TODO: fix generation time
        uint64_t offset = i * config_.page_size;
        auto view = MutableBufferView(config_.page_size, bucket_buffer_.data() + offset);

        LogBucket::initNew(view, 0);
        buckets_[i] =
            reinterpret_cast<LogBucket *>(bucket_buffer_.data() + offset);
    }
}

Status LogWriter::lookup(HashedKey hk, Buffer *value, uint32_t slice) {
    assert(slice < config_.slice_num);
    BufferView view = buckets_[slice]->find(hk);
    if (view.isNull()) {
        return Status::NotFound;
    } else {
        if (value->isNull()) {
            value->resize(view.size());
        }
        value->copyFrom(0, view);
        return Status::Ok;
    }
}

Status LogWriter::insert(HashedKey hk, BufferView value, uint32_t slice, uint64_t *page_idx) {
    assert(slice < config_.slice_num);
    assert(page_idx != nullptr);
    
    if (buckets_[slice]->isSpace(hk, value)) {
        KangarooBucketStorage::Allocation alloc 
            = buckets_[slice]->allocate(hk, value);
        buckets_[slice]->insert(alloc, hk, value);
        *page_idx = calc_page_idx(slice);
        return Status::Ok;
    } else {
        return Status::Retry;
    }
}

Status LogWriter::remove(HashedKey hk, uint32_t slice) {
    assert(slice < config_.slice_num);
    if (buckets_[slice]->remove(hk, DestructorCallback{})) {
        return Status::Ok;
    } else {
        return Status::NotFound;
    }
}

Status LogWriter::flush_log(uint32_t slice, std::function<void(Buffer, std::vector<HashedKey> &collected)> flush_cb) {
    std::vector<HashedKey> keys;
    buckets_[slice]->collect_keys(keys);
    
    auto view = BufferView(config_.page_size, bucket_buffer_.data() + slice * config_.page_size);
    flush_cb(Buffer(view, config_.page_size), keys);

    buckets_[slice]->clear();
    page_cnt_[slice]++;

    return Status::Ok;
}

uint64_t LogWriter::calc_page_idx(uint32_t slice) {
    return page_cnt_[slice] * config_.slice_num + slice;
}