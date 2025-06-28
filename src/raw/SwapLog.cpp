#include <cstdlib>
#include <unistd.h>
#include <cassert>
#include <unordered_set>

#include "raw/SwapLog.h"

SwapLog::SwapLog(Config &&config)
    : page_size_(config.page_size)
    , zone_capacity_(config.zone_capacity)
    , data_zone_num_(config.data_zone_num)
    , swap_zone_num_(config.swap_zone_num)
    , write_part_num_(config.write_part_num)
    , log_start_off_(config.log_start_off)
    , segment_size_(config.segment_size)
    , pages_per_zone_(zone_capacity_ / page_size_)
    , pages_per_segment_(segment_size_ / page_size_)
    , segments_per_zone_(zone_capacity_ / segment_size_)
    , device_(config.device)
{
    // index
    uint64_t swap_part_num = (uint64_t)data_zone_num_ * zone_capacity_ / 256 / (2 * SWAP_INDEX_ENTRY_NUM);
    uint64_t key_part_num = swap_part_num * 100;

    printf("SwapLog swap part num: %lu key part num: %lu\n", swap_part_num, key_part_num);

    assert(swap_part_num < UINT32_MAX);
    assert(key_part_num < UINT32_MAX);

    LogIndex::Config index_config {
        .page_size = page_size_,
        .zone_capacity = zone_capacity_,
        .swap_start_off = config.swap_start_off,
        .data_zone_num = data_zone_num_,
        .swap_zone_num = swap_zone_num_,
        .key_part_num = (uint32_t)key_part_num,
        .swap_part_num = (uint32_t)swap_part_num,
        .device = device_
    };
    log_index_ = std::make_unique<LogIndex>(std::move(index_config));
    // write
    write_buffers_ = std::make_unique<Buffer[]>(WRITE_BUFFER_CNT);
    write_mutexes_ = std::make_unique<std::shared_mutex[]>(WRITE_BUFFER_CNT);
    write_segments_ = std::make_unique<FwLogSegment *[]>(WRITE_BUFFER_CNT);
    for (uint32_t i = 0; i < WRITE_BUFFER_CNT; i++) {
        write_buffers_[i] = device_->makeIOBuffer(segment_size_);
        write_segments_[i] = new FwLogSegment(
            segment_size_, page_size_, LogSegmentId(i, 0), write_part_num_,
            write_buffers_[i].mutableView(), true
        );
    }
    next_lsid_to_clean_ = LogSegmentId(0, 0);
    // gc threads
    gc_threads_.push_back(std::thread(&SwapLog::gc_loop, this));
    for (uint32_t i = 1; i < gc_thread_num_; i++) {
        gc_threads_.push_back(std::thread(&SwapLog::gc_wait_loop, this));
    }

    printf("SwapLog init done\n");
}

SwapLog::~SwapLog() {
    stop_flag_ = true;

    for (uint32_t i = 0; i < gc_threads_.size(); i++) {
        gc_threads_[i].join();
    }
}

Status SwapLog::lookup_write_buffer(HashedKey hk, Buffer *value, uint32_t lpid) {
    uint32_t buffer_sel = WRITE_BUFFER_CNT + 1;

    uint32_t zone_in_region = lpid / pages_per_zone_;
    uint32_t segment_in_zone = (lpid % pages_per_zone_) / pages_per_segment_;
    LogSegmentId lsid(zone_in_region, segment_in_zone);

    // uint32_t page_in_segment = (lpid % pages_per_zone_) % pages_per_segment_;
    for (uint32_t i = 0; i < WRITE_BUFFER_CNT; i++) {
        if (lsid == write_segments_[i]->getLogSegmentId()) {
            buffer_sel = i;
            break;
        }
    }
    if (buffer_sel >= WRITE_BUFFER_CNT) {
        return Status::Retry;
    }

    BufferView view;
    {
        std::shared_lock lock(write_mutexes_[buffer_sel]);
        if (lsid != write_segments_[buffer_sel]->getLogSegmentId()) {
            return Status::Retry;
        }
        view = write_segments_[buffer_sel]->find(hk, LogPageId(lpid, true));
        if (view.isNull()) {
            return Status::NotFound;
        }
        *value = Buffer{view};
    }
    return Status::Ok;
}

Status SwapLog::lookup(HashedKey hk, Buffer *value) {
    PartitionOffset po = log_index_->lookup(hk);
    if (!po.isValid()) {
        return Status::NotFound;
    }
    Buffer buffer;
    BufferView value_view;
    LogBucket *page;
    {
        std::shared_lock lock(buffer_meta_mutex_);
        Status ret = lookup_write_buffer(hk, value, po.index());
        if (ret != Status::Retry) {
            return ret;
        }
    }
    buffer = read_log_page(LogPageId{po.index(), true});
    if (buffer.isNull()) {
        // ioErrorCount_.inc();
        return Status::DeviceError;
    }
    page = reinterpret_cast<LogBucket *>(buffer.data());

    value_view = page->find(hk);
    if (value_view.isNull()) {
        return Status::BadState;
    }

    *value = Buffer{value_view};
    return Status::Ok;
}

Status SwapLog::insert(HashedKey hk, BufferView value) {
    Status status = Status::Ok;

    LogPageId lpid;
    // LogSegmentId lsid;
    uint32_t write_part_id = hk % write_part_num_;

    while (!lpid.isValid()) {
        uint32_t buffer_sel = WRITE_BUFFER_CNT + 1;
        for (uint32_t i = 0; i < WRITE_BUFFER_CNT; i++) {
            uint32_t buffer_idx = (write_buffer_offset_ + i) % WRITE_BUFFER_CNT;
            std::shared_lock lock(write_mutexes_[buffer_idx]);

            LogSegmentId id = write_segments_[buffer_idx]->getLogSegmentId();
            int32_t page_offset = write_segments_[buffer_idx]->insert(hk, value, write_part_id);
            if (page_offset >= 0) {
                lpid = LogPageId(
                    id.zone() * pages_per_zone_ + id.offset() * pages_per_segment_ + page_offset,
                    true
                );
                buffer_sel = i;
                break;
            }
        }

        if (lpid.isValid()) {
            status = log_index_->insert(hk, PartitionOffset(lpid.index(), lpid.isValid()));
            if (status != Status::Ok) {
                return status;
            }
        }

        if (buffer_sel != write_buffer_offset_) {
            // flush
            bool ret = flush_log_segment(write_part_id, true);
            if (!ret) {
                // printf("SwapLog Flush Failed\n");
                // exit(-1);
            }
        }
    }

    return status;
}

Status SwapLog::remove(HashedKey hk) {
    return Status::Ok;
}

Status SwapLog::prefill(
    std::function<uint64_t ()> k_func,
    std::function<BufferView ()> v_func
) {
    std::unordered_set<uint64_t> inserted_keys;
    Status status = Status::Ok;
    while (!should_clean(0.055)) {
        HashedKey hk = k_func();
        BufferView value = v_func();
        if (inserted_keys.find(hk) != inserted_keys.end()) {
            continue;
        }
        status = insert(hk, value);
        if (status != Status::Ok) {
            break;
        }
        inserted_keys.insert(hk);
    }

    // trigger swap
    // log_index_->force_do_swap(
    //     next_lsid_to_clean_.zone() * pages_per_zone_ + next_lsid_to_clean_.offset() * pages_per_segment_
    // );

    return status;
}

bool SwapLog::flush_log_segment(uint32_t index_part, bool wait) {
    if (!buffer_meta_mutex_.try_lock()) {
        if (wait) {
            sleep(.001);
        }
        return false;
    } else {
        uint32_t old_offset = write_buffer_offset_;
        uint32_t new_offset = (old_offset + 1) % WRITE_BUFFER_CNT;

        // 不需要下刷
        {
            std::unique_lock lock(write_mutexes_[new_offset]);
            if (write_segments_[new_offset]->getFullness(index_part) < 0.5) {
                buffer_meta_mutex_.unlock();
                return true;
            }
        }
        // wait cleaning
        LogSegmentId old_lsid = write_segments_[old_offset]->getLogSegmentId();
        {
            std::unique_lock lock(flush_clean_mutex_);
            // bool write_stall = false;
            while (old_lsid.zone() == next_lsid_to_clean_.zone() &&
                   old_lsid.offset() <= next_lsid_to_clean_.offset() && !write_empty_) {
                // write_stall = true;
                flush_clean_cv_.wait(lock);
            }
        }

        // refresh pointers if had to wait
        old_offset = write_buffer_offset_;
        new_offset = (old_offset + 1) % WRITE_BUFFER_CNT;
        write_buffer_offset_ = new_offset;
        old_lsid = write_segments_[old_offset]->getLogSegmentId();

        LogSegmentId next_lsid;
        // 下刷一个 segmant
        {
            std::unique_lock buffer_lock(write_mutexes_[old_offset]);
            next_lsid = get_next_lsid(old_lsid, WRITE_BUFFER_CNT);

            // 下刷
            bool ret = write_log_segment(old_lsid, std::move(Buffer(write_buffers_[old_offset].view(), page_size_)));
            if (!ret) {
                printf("SwapLog Flush Failed\n");
                exit(-1);
            }
            write_segments_[old_offset]->clear(next_lsid);
            write_empty_ = false;
        }
    }

    buffer_meta_mutex_.unlock();
    return true;
}

bool SwapLog::write_log_segment(LogSegmentId lsid, Buffer buffer) {
    // TODO: set checksums
    uint64_t offset = log_start_off_ + segment_size_ * lsid.offset() +
               device_->getIOZoneSize() * lsid.zone();
    if (lsid.offset() == 0) {
        printf("SwapLog Write: reseting zone %lf\n",
               offset / (double)device_->getIOZoneSize());
        uint64_t align_offset = offset - (offset % device_->getIOZoneSize());
        device_->reset(align_offset, device_->getIOZoneCapSize());
    }
    // printf("Write: writing to zone %d offset 0x%x, actual zone # %ld, loc
    // 0x%lx, len 0x%lx\n",
    //        lsid.zone(), lsid.offset(), offset / device_.getIOZoneSize(),
    //        offset, buffer.size());
    bool ret = device_->write(offset, std::move(buffer));
    if (!ret) {
        printf(
            "SwapLog Write Failed: writing to zone %d offset %d, actual zone # "
            "%ld, loc %ld\n",
            lsid.zone(), lsid.offset(), offset / device_->getIOZoneSize(),
            offset);
    }
    if (lsid.offset() == segments_per_zone_ - 1) {
        uint64_t zone_offset = log_start_off_ + device_->getIOZoneSize() * lsid.zone();
        printf("SwapLog Write: finishing zone %lf\n",
               zone_offset / (double)device_->getIOZoneSize());

        uint64_t align_offset = offset - (offset % device_->getIOZoneSize());
        device_->finish(align_offset, device_->getIOZoneCapSize());
    }

    return ret;
}

Buffer SwapLog::read_log_page(LogPageId lpid) {
    auto buffer = device_->makeIOBuffer(page_size_);
    assert(!buffer.isNull());

    uint64_t offset = log_start_off_ + (lpid.index() % pages_per_zone_) * page_size_ +
               device_->getIOZoneSize() * (lpid.index() / pages_per_zone_);
    bool ret = device_->read(offset, buffer.size(), buffer.data());
    if (!ret) {
        printf("Read log page failed\n");
        return {};
    }
    return buffer;
}

Buffer SwapLog::read_log_segment(LogSegmentId lsid) {
    auto buffer = device_->makeIOBuffer(segment_size_);
    assert(!buffer.isNull());
    uint64_t offset = log_start_off_ + segment_size_ * lsid.offset() +
               device_->getIOZoneSize() * lsid.zone();
    bool ret = device_->read(offset, buffer.size(), buffer.data());
    if (!ret) {
        printf("Read log segment failed\n");
        return {};
    }
    return buffer;
}

LogSegmentId SwapLog::get_next_lsid(LogSegmentId lsid, uint32_t increment) {
    // partitions within segment so partition # doesn't matter
    assert(increment < segments_per_zone_); // not correct otherwise
    if (lsid.offset() + increment >= segments_per_zone_) {
        return LogSegmentId((lsid.offset() + increment) % segments_per_zone_,
                            (lsid.zone() + 1) % data_zone_num_);
    }
    return LogSegmentId(lsid.offset() + increment, lsid.zone());
}

bool SwapLog::should_clean(double threshold) {
    LogSegmentId cur_lsid = LogSegmentId(0, 0);
    {
        std::shared_lock lock(write_mutexes_[write_buffer_offset_]);
        cur_lsid = write_segments_[write_buffer_offset_]->getLogSegmentId();
    }
    LogSegmentId next_lsid = get_next_lsid(cur_lsid, 1);

    uint32_t next_write_loc = next_lsid.offset() + next_lsid.zone() * segments_per_zone_;
    uint32_t next_clean_loc = next_lsid_to_clean_.offset() + next_lsid_to_clean_.zone() * segments_per_zone_;

    uint32_t free_segments = 0;
    if (next_clean_loc >= next_write_loc) {
        free_segments = next_clean_loc - next_write_loc;
    } else {
        free_segments = (segments_per_zone_ * data_zone_num_ - next_write_loc) + next_clean_loc;
    }
    if (free_segments < segments_per_zone_ * data_zone_num_ * 0.05) {
        return true;
    }
    if (free_segments < segments_per_zone_) {
        return true;
    }
    return false;
}

void SwapLog::start_clean() {
    cleaning_buffer_ = read_log_segment(next_lsid_to_clean_);
    cleaning_segment_ = std::make_unique<FwLogSegment>(
        segment_size_, page_size_, next_lsid_to_clean_, write_part_num_,
        cleaning_buffer_.mutableView(), false);
    cleaning_segment_it_ = cleaning_segment_->getFirst();
}

bool SwapLog::finish_clean() {
    LogSegmentId next_lsid = next_lsid_to_clean_;
    {
        std::unique_lock lock(flush_clean_mutex_);
        next_lsid_to_clean_ = get_next_lsid(next_lsid, 1);
        flush_clean_cv_.notify_all();
    }
    if (next_lsid.zone() != next_lsid_to_clean_.zone()) {
        printf("SwapLog Finish Clean: finishing zone %d\n", next_lsid.zone());
    }
    cleaning_segment_it_.reset();
    cleaning_segment_.reset();
    return next_lsid.zone() != next_lsid_to_clean_.zone();
}

bool SwapLog::get_next_cleaning_key(HashedKey *hk, LogPageId *lpid) {
    if (cleaning_segment_it_->done()) {
        return true;
    }
    *hk = cleaning_segment_it_->key();
    *lpid = LogPageId(next_lsid_to_clean_.zone() * pages_per_zone_ + 
            next_lsid_to_clean_.offset() * pages_per_segment_ + cleaning_segment_it_->bucket(), true);
    cleaning_segment_it_ = cleaning_segment_->getNext(std::move(cleaning_segment_it_));
    return false;
}

void SwapLog::clean_segment() {
        {
        std::unique_lock<std::mutex> lock(gc_sync_);
        gc_sync_threads_ += 1;
    }

    std::vector<HashedKey> keys;
    std::vector<LogPageId> lpids;

    uint64_t check_cnt = 0;
    uint64_t invalid_cnt = 0;

    while (gc_flag_) {
        {
            std::unique_lock<std::mutex> lock(gc_sync_);
            HashedKey key;
            LogPageId lpid;

            for (uint32_t i = 0; i < 8; i++) {
                bool is_done = get_next_cleaning_key(&key, &lpid);
                if (is_done) {
                    gc_flag_ = false;
                    break;
                } else {
                    keys.push_back(key);
                    lpids.push_back(lpid);
                }
            }
        }

        for (uint32_t i = 0; i < keys.size(); i++) {
            check_cnt++;
            Status status = log_index_->pop(keys[i], PartitionOffset(lpids[i].index(), lpids[i].isValid()));
            if (status == Status::NotFound) {
                invalid_cnt++;
            }
        }

        keys.clear();
        lpids.clear();
    }
    {
        std::unique_lock<std::mutex> lock(gc_sync_);
        gc_sync_threads_ -= 1;
        if (gc_sync_threads_ == 0) {
            gc_sync_cond_.notify_all();
        }
    }
}

void SwapLog::gc_loop() {
    while (!stop_flag_) {
        if (should_clean(0.05)) {
            // printf("clean segment\n");
            start_clean();
            gc_flag_ = true;
            gc_sync_cond_.notify_all();

            clean_segment();

            {
                std::unique_lock<std::mutex> lock(gc_sync_);
                while (gc_sync_threads_ > 0 || gc_flag_ == true) {
                    gc_sync_cond_.wait(lock);
                }
            }

            finish_clean();
        } else {
            usleep(2 * 1000);
        }
    }

    gc_sync_cond_.notify_all();
}

void SwapLog::gc_wait_loop() {
    while (!stop_flag_) {
        if (gc_flag_) {
            clean_segment();
        }
        {
            std::unique_lock<std::mutex> lock(gc_sync_);
            gc_sync_cond_.wait(lock);
        }
    }
}

SwapLog::PerfCounter &SwapLog::perf_counter() {
    return perf_;
}