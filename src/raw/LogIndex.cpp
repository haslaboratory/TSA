#include <mutex>
#include <shared_mutex>

#include "raw/LogIndex.h"

SwapIndex::SwapIndex(Config &&config) 
    : swap_part_num_(config.swap_part_num),
    page_size_(config.page_size),
    zone_capacity_(config.zone_capacity),
    swap_zone_num_(config.swap_zone_num),
    swap_start_off_(config.swap_start_off),
    page_per_zone_(config.zone_capacity / config.page_size),
    device_(config.device)
{
    assert(swap_part_num_ < page_per_zone_);
    for (uint32_t i = 0; i < swap_part_num_; i++) {
        lpid_2_ppid_.push_back(UINT32_MAX);
    }

    bloom_filter_ = std::make_unique<BloomFilter>(swap_part_num_, 3, 2 * SWAP_INDEX_ENTRY_NUM);
}

Buffer SwapIndex::read(uint32_t lpid) {
    assert(lpid < lpid_2_ppid_.size());
    std::shared_lock lock(mutex_);

    return inner_read(lpid);
}

bool SwapIndex::write(uint32_t lpid, Buffer buf) {
    std::unique_lock lock(mutex_);

    while (check_gc()) {
        do_gc();
    }

    bf_rebuild(lpid, (SwapIndexPage *)buf.data());

    inner_write(lpid, std::move(buf));

    return true;
}

bool SwapIndex::could_exist(uint32_t lpid, HashedKey hk) {
    std::shared_lock lock(mutex_);
    return bloom_filter_->couldExist(lpid, hk);
}

bool SwapIndex::swap_bkt_exist(uint32_t lpid) {
    return lpid_2_ppid_[lpid] != UINT32_MAX;
}

Buffer SwapIndex::inner_read(uint32_t lpid) {
    Buffer buffer = device_->makeIOBuffer(page_size_);
    uint64_t ppid = lpid_2_ppid_[lpid];
    if (ppid == UINT32_MAX) {
        memset(buffer.data(), 0, buffer.size());
        return buffer;
    }
    uint64_t read_ppn = swap_start_off_ + ppid / page_per_zone_ * device_->getIOZoneSize() + (ppid % page_per_zone_) * page_size_;
    device_->read(
        read_ppn,
        buffer.size(),
        buffer.data()
    );
    return buffer;
}

void SwapIndex::inner_write(uint32_t lpid, Buffer buf) {
    uint64_t ppid = write_ptr_;
    uint64_t write_ppn = swap_start_off_ + ppid / page_per_zone_ * device_->getIOZoneSize() + (ppid % page_per_zone_) * page_size_;

    if (ppid % page_per_zone_ == 0) {
        // write new zone
        uint64_t zone_start = write_ppn - write_ppn % device_->getIOZoneSize();
        device_->reset(zone_start, zone_capacity_);
    }
    bool ret = device_->write(write_ppn, std::move(buf));
    if (ret == false) {
        printf("write failed\n");
        exit(-1);
    }

    if (ppid % page_per_zone_ == page_per_zone_ - 1) {
        // finish zone
        uint64_t zone_start = write_ppn - write_ppn % device_->getIOZoneSize();
        device_->finish(zone_start, zone_capacity_);
    }

    lpid_2_ppid_[lpid] = ppid;
    write_ptr_ = (write_ptr_ + 1) % (swap_zone_num_ * page_per_zone_);
}

void SwapIndex::bf_rebuild(uint32_t lpid, SwapIndexPage *page) {
    bloom_filter_->clear(lpid);
    for (uint32_t i = 0; i < SWAP_INDEX_ENTRY_NUM; i++) {
        if (page->entries_[i].valid) {
            bloom_filter_->set(lpid, page->entries_[i].hk);
        }
    }
}

bool SwapIndex::check_gc() {
    uint64_t gc_ptr_ = gc_zone_id_ * page_per_zone_;

    uint64_t free_pages = 0;
    if (gc_ptr_ <= write_ptr_) {
        free_pages = gc_ptr_ + page_per_zone_ - write_ptr_;
    } else {
        free_pages = gc_ptr_ - write_ptr_;
    }

    return free_pages <= swap_part_num_;
}

void SwapIndex::do_gc() {
    for (uint32_t i = 0; i < swap_part_num_; i++) {
        uint32_t ppid = lpid_2_ppid_[i];
        if (ppid / page_per_zone_ == gc_zone_id_) {
            Buffer buffer = inner_read(i);
            inner_write(i, std::move(buffer));
        }
    }
    gc_zone_id_ = (gc_zone_id_ + 1) % swap_zone_num_;
}

void LogIndexPart::allocate() {
    entries_.push_back(std::make_unique<IndexEntry[]>(UNIT_CNT));
}

IndexEntry *LogIndexPart::get_entry(uint32_t idx) {
    if (unlikely(idx >= alloc_idx_)) {
        printf("alloc_idx: %d, idx: %d\n", alloc_idx_, idx);
        assert(idx < alloc_idx_);
    }
    return entries_[idx / UNIT_CNT].get() + idx % UNIT_CNT;
}

uint32_t LogIndexPart::allocate_entry() {
    uint32_t ret_idx = 0;
    if (free_idx_ != UINT32_MAX) {
        assert(free_idx_ < alloc_idx_);
        ret_idx = free_idx_;
        IndexEntry *entry = get_entry(ret_idx);
        free_idx_ = entry->next_idx;
        entry->next_idx = UINT32_MAX;
    } else {
        if (alloc_idx_ >= UNIT_CNT * entries_.size()) {
            allocate();
        }
        ret_idx = alloc_idx_;
        alloc_idx_++;
        IndexEntry *entry = get_entry(ret_idx);
        entry->next_idx = UINT32_MAX;
    }
    return ret_idx;
}

void LogIndexPart::release_entry(uint32_t idx) {
    assert(idx < alloc_idx_);
    IndexEntry *entry = get_entry(idx);
    entry->next_idx = free_idx_;
    free_idx_ = idx;
}

bool LogIndex::Config::validate() {
    if (key_part_num % swap_part_num != 0) {
        printf("swap_part_num_ %% key_part_num_!= 0\n");
        exit(-1);
    }
    return true;
}

LogIndex::LogIndex(Config &&config)
    : page_size_(config.page_size),
    zone_capacity_(config.zone_capacity),
    data_zone_num_(config.data_zone_num),
    key_part_num_(config.key_part_num),
    swap_part_num_(config.swap_part_num),
    pages_per_zone_(config.zone_capacity / config.page_size)
{
    config.validate();
    // printf("LogIndex init\n");
    // swap index
    SwapIndex::Config swap_config {
        .swap_part_num = swap_part_num_,
        .page_size = page_size_,
        .swap_start_off = config.swap_start_off,
        .swap_zone_num = config.swap_zone_num,
        .zone_capacity = zone_capacity_,
        .device = config.device
    };
    swap_index_ = std::make_unique<SwapIndex>(std::move(swap_config));
    // allocate key parts
    upper_index_ = std::make_unique<uint32_t[]>(key_part_num_);
    lower_index_ = std::make_unique<uint32_t[]>(key_part_num_);

    for (uint32_t i = 0; i < key_part_num_; i++) {
        upper_index_[i] = UINT32_MAX;
        lower_index_[i] = UINT32_MAX;
    }

    mutexes_ = std::make_unique<std::shared_mutex[]>(LOCK_PART_NUM);
    index_parts_ = std::make_unique<LogIndexPart[]>(LOCK_PART_NUM);

    // for (uint32_t i = 0; i < swap_part_num_; i++) {
    //     insert_cnt_.push_back(0);
    // }

    printf("LogIndex init success\n");
}

std::shared_mutex *LogIndex::get_mutex(uint32_t swap_part_id) {
    return &mutexes_[swap_part_id % LOCK_PART_NUM];
}

LogIndexPart *LogIndex::get_index_part(uint32_t swap_part_id) {
    return &index_parts_[swap_part_id % LOCK_PART_NUM];
}

PartitionOffset LogIndex::lookup(HashedKey hk) {
    PartitionOffset po {0, false};
    uint32_t key_part_id = hk % key_part_num_;
    uint32_t swap_part_id = hk % swap_part_num_;
    std::shared_lock lock(*get_mutex(swap_part_id));

    LogIndexPart *index_part = get_index_part(swap_part_id);

    // upper
    uint32_t now_idx = upper_index_[key_part_id];
    while (now_idx!= UINT32_MAX) {
        IndexEntry *entry = index_part->get_entry(now_idx);
        if (entry->hk == hk) {
            po = PartitionOffset(entry->flash_index, true);
            return po;
        }
        now_idx = entry->next_idx;
    }

    // swap
    if (swap_index_->could_exist(swap_part_id, hk)) {
        Buffer buffer = swap_index_->read(swap_part_id);
        SwapIndexPage *page = (SwapIndexPage *)buffer.data();
        for (uint32_t i = 0; i < SWAP_INDEX_ENTRY_NUM; i++) {
            if (page->entries_[i].valid && page->entries_[i].hk == hk) {
                po = PartitionOffset(page->entries_[i].flash_index, true);
                return po;
            }
        }
    }
    // lower
    now_idx = lower_index_[key_part_id];
    while (now_idx!= UINT32_MAX) {
        IndexEntry *entry = index_part->get_entry(now_idx);
        if (entry->hk == hk) {
            po = PartitionOffset(entry->flash_index, true);
            return po;
        }
        now_idx = entry->next_idx;
    }

    return po;
}

Status LogIndex::insert(HashedKey hk, PartitionOffset po) {
    Status status = Status::Ok;
    uint32_t key_part_id = hk % key_part_num_;
    uint32_t swap_part_id = hk % swap_part_num_;
    std::unique_lock lock(*get_mutex(swap_part_id));

    LogIndexPart *index_part = get_index_part(swap_part_id);

    uint32_t new_idx = index_part->allocate_entry();
    IndexEntry *entry = index_part->get_entry(new_idx);
    entry->hk = hk;
    entry->flash_index = po.index();
    entry->next_idx = upper_index_[key_part_id];

    upper_index_[key_part_id] = new_idx;

    // if (!swap_index_->swap_bkt_exist(swap_part_id)) {
    //     do_swap(swap_part_id, std::nullopt, cached_lsid_.load());
    // }

    // insert_cnt_[swap_part_id] += 1;
    return status;
}

Status LogIndex::remove(HashedKey hk, PartitionOffset po) {
    Status status = Status::Ok;
    uint32_t key_part_id = hk % key_part_num_;
    uint32_t swap_part_id = hk % swap_part_num_;
    std::unique_lock lock(*get_mutex(swap_part_id));

    LogIndexPart *index_part = get_index_part(swap_part_id);

    // upper
    uint32_t now_idx = upper_index_[key_part_id];
    uint32_t pre_idx = UINT32_MAX;
    while (now_idx!= UINT32_MAX) {
        IndexEntry *entry = index_part->get_entry(now_idx);
        if (entry->hk == hk && entry->flash_index == po.index()) {
            if (pre_idx == UINT32_MAX) {
                upper_index_[key_part_id] = entry->next_idx;
            } else {
                IndexEntry *pre_entry = index_part->get_entry(pre_idx);
                pre_entry->next_idx = entry->next_idx;
            }
            index_part->release_entry(now_idx);
            break;
        }
        pre_idx = now_idx;
        now_idx = entry->next_idx;
    }
    // swap
    if (swap_index_->could_exist(swap_part_id, hk)) {
        Buffer buffer = swap_index_->read(swap_part_id);
        SwapIndexPage *page = (SwapIndexPage *)buffer.data();
        for (uint32_t i = 0; i < SWAP_INDEX_ENTRY_NUM; i++) {
            if (page->entries_[i].valid && page->entries_[i].hk == hk) {
                page->entries_[i].valid = 0;
                swap_index_->write(swap_part_id, std::move(buffer));
                break;
            }
        }
    }
    // lower
    now_idx = lower_index_[key_part_id];
    pre_idx = UINT32_MAX;
    while (now_idx!= UINT32_MAX) {
        IndexEntry *entry = index_part->get_entry(now_idx);
        if (entry->hk == hk && entry->flash_index == po.index()) {
            if (pre_idx == UINT32_MAX) {
                lower_index_[key_part_id] = entry->next_idx;
            } else {
                IndexEntry *pre_entry = index_part->get_entry(pre_idx);
                pre_entry->next_idx = entry->next_idx;
            }
            index_part->release_entry(now_idx);
            break;
        }
        pre_idx = now_idx;
        now_idx = entry->next_idx;
    }

    return status;
}

Status LogIndex::pop(HashedKey hk, PartitionOffset po) {
    uint32_t key_part_id = hk % key_part_num_;
    uint32_t swap_part_id = hk % swap_part_num_;
    std::unique_lock lock(*get_mutex(swap_part_id));

    LogIndexPart *index_part = get_index_part(swap_part_id);

    uint32_t now_idx = lower_index_[key_part_id];
    uint32_t pre_idx = UINT32_MAX;

    while (now_idx != UINT32_MAX) {
        IndexEntry *entry = index_part->get_entry(now_idx);
        if (entry->hk == hk && entry->flash_index == po.index()) {
            if (pre_idx == UINT32_MAX) {
                lower_index_[key_part_id] = entry->next_idx;
            } else {
                IndexEntry *pre_entry = index_part->get_entry(pre_idx);
                pre_entry->next_idx = entry->next_idx;
            }
            index_part->release_entry(now_idx);
            return Status::Ok;
        }

        // if (entry->hk == hk && entry->flash_index != po.index()) {
        //     printf("mismatch flash index(%ld): %d != %d\n", hk, entry->flash_index, po.index());
        // }
        pre_idx = now_idx;
        now_idx = entry->next_idx;
    }
    // not found
    if (!swap_index_->could_exist(swap_part_id, hk)) {
        // rearrage swap index
        do_swap(swap_part_id, hk, po.index());
    }

    return Status::NotFound;
}

struct TempEntry {
    uint32_t timestamp;
    HashedKey hk;
    uint32_t flash_index;
};

void LogIndex::do_swap(uint32_t swap_part_id, std::optional<HashedKey> filter_key, uint32_t base_addr) {
    std::vector<TempEntry> entries;
    LogIndexPart *index_part = get_index_part(swap_part_id);

    uint32_t key_part_per_swap = key_part_num_ / swap_part_num_;

    for (uint32_t i = 0; i < key_part_per_swap; i++) {
        uint32_t key_part_id = i * swap_part_num_ + swap_part_id;
        uint64_t pre_idx = UINT32_MAX;
        uint32_t now_idx = upper_index_[key_part_id];
        while (now_idx != UINT32_MAX) {
            IndexEntry *entry = index_part->get_entry(now_idx);
            if (!filter_key.has_value() || entry->hk != filter_key.value()) {
                assert(entry->hk % swap_part_num_ == swap_part_id);
                entries.push_back(TempEntry {
                    .timestamp = (entry->flash_index + data_zone_num_ * pages_per_zone_ - base_addr) % (data_zone_num_ * pages_per_zone_),
                    .hk = entry->hk,
                    .flash_index = entry->flash_index,
                });
            }
            pre_idx = now_idx;
            now_idx = entry->next_idx;
            // remove
            if (pre_idx != UINT32_MAX) {
                index_part->release_entry(pre_idx);
            }
        }
        // clear
        upper_index_[key_part_id] = UINT32_MAX;
    }
    // 读取 swap
    Buffer buffer = swap_index_->read(swap_part_id);
    SwapIndexPage *page = (SwapIndexPage *)buffer.data();
    for (uint32_t i = 0; i < SWAP_INDEX_ENTRY_NUM; i++) {
        if (page->entries_[i].valid && (!filter_key.has_value() || page->entries_[i].hk != filter_key.value())) {
            assert(page->entries_[i].hk % swap_part_num_ == swap_part_id);
            entries.push_back(TempEntry {
               .timestamp = (page->entries_[i].flash_index + data_zone_num_ * pages_per_zone_ - base_addr) % (data_zone_num_ * pages_per_zone_),
               .hk = page->entries_[i].hk,
               .flash_index = page->entries_[i].flash_index,
            });
        }
    }

    // 按照时间从晚到早排序
    std::sort(entries.begin(), entries.end(), [](TempEntry &a, TempEntry &b) {
        return a.timestamp > b.timestamp;
    });

    // if (entries.size() >= 2) {
    //     assert(entries[0].timestamp >= entries[entries.size()-1].timestamp);
    // }

    // assert(entries.size() > 0 || insert_cnt_[swap_part_id] == 0xFFFFFFFF);

    // 更新 swap
    memset(page, 0, sizeof(SwapIndexPage));

    uint32_t swap_cnt = std::min((uint32_t)entries.size(), (uint32_t)SWAP_INDEX_ENTRY_NUM);

    for (uint32_t i = 0; i < swap_cnt; i++) {
        if (i < entries.size()) {
            page->entries_[i] = SwapIndexEntry {
                .hk = entries[i].hk,
                .valid = true,
                .flash_index = entries[i].flash_index,
            };
        }
    }

    swap_index_->write(swap_part_id, std::move(buffer));
    // 更新 lower index
    for (uint32_t i = swap_cnt; i < entries.size(); i++) {
        uint32_t key_part_id = entries[i].hk % key_part_num_;
        assert(entries[i].hk % swap_part_num_ == swap_part_id);
        // insert to lower index
        uint32_t new_idx = index_part->allocate_entry();
        IndexEntry *entry = index_part->get_entry(new_idx);
        entry->hk = entries[i].hk;
        entry->flash_index = entries[i].flash_index;
        entry->next_idx = lower_index_[key_part_id];
        lower_index_[key_part_id] = new_idx;
    }

    // update stashed lsid
    // cached_lsid_.store(swap_part_id);

    // printf("do swap success %d, process cnt: %ld, insert cnt: %d\n", swap_part_id, entries.size() + find_filter, insert_cnt_[swap_part_id]);
    // insert_cnt_[swap_part_id] = 0xFFFFFFFF;
}

void LogIndex::force_do_swap(uint32_t base_addr) {
    for (uint32_t i = 0; i < swap_part_num_; i++) {
        std::unique_lock lock(*get_mutex(i));
        if (!swap_index_->swap_bkt_exist(i)) {
            do_swap(i, std::nullopt, base_addr);
        }
    }
}