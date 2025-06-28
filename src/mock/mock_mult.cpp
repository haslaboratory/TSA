#ifdef NDEBUG
#undef NDEBUG
#endif

#include <cstring>
#include <map>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <queue>

#include "common/Rand.h"

#define OBJ_SIZE 100
#define PAGE_SIZE 4096
#define KSEGMENT_SIZE (32 * 4096)
#define ZONE_SIZE (256 * 1024 * 1024)
#define LOG_ZONE_NUM 12
#define SET_ZONE_NUM 144

#define LOG_OP 0.15
#define SET_OP 0.05
#define SET_MAX_OP 0.10

const uint64_t OBJ_PER_PAGE = PAGE_SIZE / OBJ_SIZE;
const uint64_t PAGE_PER_ZONE = ZONE_SIZE / PAGE_SIZE;
const uint64_t PAGE_PER_KSEGMENT = KSEGMENT_SIZE / PAGE_SIZE;
const uint64_t KSEGMENT_PER_ZONE = ZONE_SIZE / KSEGMENT_SIZE;

const uint64_t BASE_SET_NUM =
    (LOG_ZONE_NUM * PAGE_PER_ZONE) * (1 - LOG_OP);

struct MockPerfCounter {
    uint64_t set_num = 0;
    uint64_t set_page_w = 0;
    uint64_t flush_log_cnt = 0;
    uint64_t flush_set_cnt = 0;
};

struct MockPage {
    uint64_t key[OBJ_PER_PAGE] = {0};
    uint64_t set_id{UINT64_MAX};
};

struct MockKSegment {
    uint64_t get_key(uint64_t obj_idx) {
        return pages[obj_idx / OBJ_PER_PAGE].key[obj_idx % OBJ_PER_PAGE];
    }

    void set_key(uint64_t obj_idx, uint64_t key) {
        pages[obj_idx / OBJ_PER_PAGE].key[obj_idx % OBJ_PER_PAGE] = key;
    }

    MockPage pages[PAGE_PER_KSEGMENT];
};

union MockZone {
    MockZone() {}
    MockPage pages[PAGE_PER_ZONE];
    MockKSegment segments[KSEGMENT_PER_ZONE];
};

class MockIndex {
public:
    struct ObjMeta {
        uint64_t page_id;
        uint64_t next;
    };

    struct SetMeta {
        uint64_t first;
        uint64_t num;
    };

    void insert(uint64_t key, uint64_t page_id) {
        assert(obj_map_.size() <= LOG_ZONE_NUM * PAGE_PER_ZONE * OBJ_PER_PAGE);
        assert(set_map_.size() <= BASE_SET_NUM);
        
        auto obj_iter = obj_map_.find(key);
        if (obj_iter != obj_map_.end()) {
            obj_iter->second.page_id = page_id;
            return;
        }
        ObjMeta meta = {page_id, UINT64_MAX};
        auto set_iter = set_map_.find(key % BASE_SET_NUM);
        if (set_iter != set_map_.end()) {
            meta.next = set_iter->second.first;
            set_iter->second.first = key;
            set_iter->second.num++;
        } else {
            set_map_.insert({key % BASE_SET_NUM, SetMeta{key, 1}});
        }
        obj_map_.insert({key, meta});
    }

    bool check_obj_newest(uint64_t key, uint64_t page_id) {
        assert(obj_map_.size() <= LOG_ZONE_NUM * PAGE_PER_ZONE * OBJ_PER_PAGE);
        assert(set_map_.size() <= BASE_SET_NUM);
        auto obj_iter = obj_map_.find(key);
        if (obj_iter == obj_map_.end()) {
            return false;
        }
        return (obj_iter->second.page_id == page_id);
    }

    std::vector<uint64_t> take_all_part(uint64_t key) {
        assert(obj_map_.size() <= LOG_ZONE_NUM * PAGE_PER_ZONE * OBJ_PER_PAGE);
        assert(set_map_.size() <= BASE_SET_NUM);
        std::vector<uint64_t> ret;
        auto set_iter = set_map_.find(key % BASE_SET_NUM);
        if (set_iter == set_map_.end()) {
            return ret;
        }

        uint64_t obj = set_iter->second.first;
        while (obj != UINT64_MAX) {
            ret.push_back(obj);
            uint64_t prev = obj;
            obj = obj_map_[prev].next;
            obj_map_.erase(prev);
        }
        set_map_.erase(key % BASE_SET_NUM);

        return ret;
    }

    SetMeta get_set_meta(uint64_t key) {
        auto iter = set_map_.find(key % BASE_SET_NUM);
        if (iter == set_map_.end()) {
            return {UINT64_MAX, 0};
        }
        return iter->second;
    }

private:
    std::unordered_map<uint64_t, SetMeta> set_map_;
    std::unordered_map<uint64_t, ObjMeta> obj_map_;
};

class MockSimpleLog {
public:
    void insert(uint64_t key) {
        index_.insert(key, 0);
    }

    bool check_flush_log(uint64_t key) {
        MockIndex::SetMeta meta = index_.get_set_meta(key);
        return (meta.num >= OBJ_PER_PAGE);
    }

    uint64_t get_set_num(uint64_t key) {
        MockIndex::SetMeta meta = index_.get_set_meta(key);
        return meta.num;
    }

    std::vector<uint64_t> take_all_part(uint64_t key) {
        return index_.take_all_part(key);
    }
private:
    MockIndex index_;
};

// 分析 GC 爆炸的原因
class MockBinarySet {
    struct SetId {
        uint32_t level;
        uint32_t base_rd;
        static SetId decode(uint64_t set_id) {
            SetId ret;
            ret.level = set_id >> 32;
            ret.base_rd = set_id & 0xffffffff;
            return ret;
        }

        uint64_t encode() {
            return (((uint64_t)this->level) << 32) | this->base_rd;
        }
    };
public:

    MockBinarySet() {
        uint64_t level_set_num = BASE_SET_NUM;
        uint64_t total_set_num = 0;

        for (cap_level_ = 0; cap_level_ < 8; cap_level_++) {
            total_set_num += level_set_num;
            if (total_set_num > TOTAL_SET_NUM_CAP) {
                break;
            }
            level_set_num *= 2;
        }

        printf("cap_level: %d SET_NUM_CAP: 0x%lx MIN_SET_NUM: 0x%lx \n", cap_level_, TOTAL_SET_NUM_CAP, TOTAL_LOW_SET_CAP);

        for (uint64_t i = 0; i < SET_ZONE_NUM; i++) {
            free_zones_.push(i);
        }
        up_ptr_.zone_id = free_zones_.front(); free_zones_.pop();
        up_ptr_.page_idx = 0;

        down_ptr_.zone_id = free_zones_.front(); free_zones_.pop();
        down_ptr_.page_idx = 0;
    }

    void set_perf(MockPerfCounter *perf) {
        perf_ = perf;
    }

    bool check_gc() {
        uint64_t free_zones = free_zones_.size();
        return free_zones <= 2;
    }

    void do_gc(MockSimpleLog *log) {
        assert(up_zones_.size() > 0);
        assert(down_zones_.size() > 0);

        uint64_t up_zone_id = *up_zones_.begin();
        uint64_t down_zone_id = *down_zones_.begin();

        for (auto iter = up_zones_.begin(); iter != up_zones_.end(); iter++) {
            if (valid_nums_[*iter] < valid_nums_[up_zone_id]) {
                up_zone_id = *iter;
            }
        }

        for (auto iter = down_zones_.begin(); iter != down_zones_.end(); iter++) {
            if (valid_nums_[*iter] < valid_nums_[down_zone_id]) {
                down_zone_id = *iter;
            }
        }


        if (perf_->set_num > TOTAL_SET_NUM_CAP) {
            gc_down_zones(down_zone_id);
        } else {
            // double up_ratio = up_valids_ * 1.0 / (up_zones_.size() * PAGE_PER_ZONE + up_ptr_.page_idx);
            // double down_ratio = down_valids_ * 1.0 / (down_zones_.size() * PAGE_PER_ZONE + down_ptr_.page_idx);
            if (valid_nums_[up_zone_id] < valid_nums_[down_zone_id]) {
                gc_up_zones(up_zone_id, log);
            } else {
                gc_down_zones(down_zone_id);
            }
        }
        
    }

    // 简化
    void do_flush_new_page(MockPage *page, uint32_t base_rd) {
        MockPage tmp_page;

        uint32_t level = 0;
        for (level = 0; level < cap_level_; level++) {
            bool flag = false;
            for (uint32_t i = 0; i < (1U << level); i++) {
                SetId raw_id = SetId {
                    .level = level,
                    .base_rd = uint32_t(base_rd + i * BASE_SET_NUM)
                };
                if (!take_set(&tmp_page, raw_id)) {
                    flag = true;
                }
            }
            if (flag) {
                break;
            }
        }

        for (uint32_t i = 0; i < (1U << level); i++) {
            SetId raw_id;
            if (level < cap_level_) {
                raw_id = SetId {
                    .level = level,
                    .base_rd = uint32_t(base_rd + i * BASE_SET_NUM)
                };
            } else {
                raw_id = select_down_set(uint32_t(base_rd + i * BASE_SET_NUM));
            }
            insert_set(&tmp_page, raw_id);
        }
        perf_->flush_log_cnt += 1;
        perf_->flush_set_cnt += (1U << level);
    }

private:
    const uint64_t TOTAL_SET_NUM_CAP = (SET_ZONE_NUM * PAGE_PER_ZONE) * (1 - SET_OP);
    const uint64_t TOTAL_LOW_SET_CAP = (SET_ZONE_NUM * PAGE_PER_ZONE) * (1 - SET_MAX_OP);
    bool take_set(MockPage *page, SetId raw_id) {
        uint64_t set_id = raw_id.encode();

        auto iter = set_ind_.find(set_id);
        if (iter == set_ind_.end()) {
            return false;
        }

        uint64_t page_id = iter->second;
        std::memcpy(
            page,
            &zones_[page_id / PAGE_PER_ZONE].pages[page_id % PAGE_PER_ZONE],
            sizeof(MockPage));

        set_ind_.erase(iter);

        valid_nums_[page_id / PAGE_PER_ZONE] -= 1;
        
        perf_->set_num--;
        return true;
    }

    void insert_set(MockPage *page, SetId raw_id) {
        uint64_t set_id = raw_id.encode();
        page->set_id = set_id;

        // 先删除
        if (set_ind_.find(set_id) != set_ind_.end()) {
            MockPage tmp_page;
            take_set(&tmp_page, raw_id);
        }

        WritePointer *w_ptr = nullptr;
        if (raw_id.level < cap_level_) {
            w_ptr = &up_ptr_;
        } else {
            w_ptr = &down_ptr_;
        }

        std::memcpy(
            &zones_[w_ptr->zone_id].pages[w_ptr->page_idx],
            page,
            sizeof(MockPage)
        );
    
        set_ind_.insert({set_id, w_ptr->zone_id * PAGE_PER_ZONE + w_ptr->page_idx});

        w_ptr->page_idx += 1;
        valid_nums_[w_ptr->zone_id] += 1;

        perf_->set_num++;
        perf_->set_page_w += 1;

        if (w_ptr->page_idx < PAGE_PER_ZONE) {
            return;
        }
        // Write Zone Finish
        printf("Write Set write zone %lu\n", w_ptr->zone_id);

        if (raw_id.level < cap_level_) {
            up_zones_.insert(w_ptr->zone_id);
        } else {
            down_zones_.insert(w_ptr->zone_id);
        }

        assert(free_zones_.size() > 0);
        w_ptr->zone_id = free_zones_.front(); free_zones_.pop();
        w_ptr->page_idx = 0;
    }

    // 有聚合效果才能保证不会 GC 爆炸，同时有足够 OP
    void gc_up_zones(uint64_t gc_zone_id, MockSimpleLog *log) {
        uint64_t inv_pages = 0;
        uint64_t mig_pages = 0;

        MockZone *gc_zone = &zones_[gc_zone_id];
        MockPage tmp_page;
        for (uint64_t i = 0; i < PAGE_PER_ZONE; i++) {
            MockPage *page = &gc_zone->pages[i];
            SetId raw_id = SetId::decode(page->set_id);
            if (!check_set_newest(page->set_id, gc_zone_id * PAGE_PER_ZONE + i)) {
                inv_pages += 1;
            } else if (check_up_merged(raw_id, log)) {
                log->take_all_part(raw_id.base_rd % BASE_SET_NUM);
                MockPage tmp_page;
                do_flush_new_page(&tmp_page, raw_id.base_rd % BASE_SET_NUM);
            } else {
                insert_set(&tmp_page, SetId::decode(page->set_id));
                mig_pages += 1;
            }
        }

        printf("Up Zone GC: %ld, inv: %ld, mig: %ld\n", gc_zone_id, inv_pages, mig_pages);
        free_zones_.push(gc_zone_id);
        up_zones_.erase(gc_zone_id);
        valid_nums_[gc_zone_id] = 0;
    }

    void gc_down_zones(uint64_t gc_zone_id) {
        uint64_t inv_pages = 0;
        uint64_t mig_pages = 0;
        uint64_t drop_pages = 0;

        MockZone *gc_zone = &zones_[gc_zone_id];
        MockPage tmp_page;
        for (uint64_t i = 0; i < PAGE_PER_ZONE; i++) {
            MockPage *page = &gc_zone->pages[i];
            if (!check_set_newest(page->set_id, gc_zone_id * PAGE_PER_ZONE + i)) {
                inv_pages += 1;
            } else if (perf_->set_num < TOTAL_LOW_SET_CAP) {
                insert_set(&tmp_page, SetId::decode(page->set_id));
                mig_pages += 1;
            } else {
                take_set(&tmp_page, SetId::decode(page->set_id));
                drop_pages += 1;
            }
        }

        printf("Down Zone GC: %ld, inv: %ld, mig: %ld, drop: %ld\n", gc_zone_id, inv_pages, mig_pages, drop_pages);
        free_zones_.push(gc_zone_id);
        down_zones_.erase(gc_zone_id);
        valid_nums_[gc_zone_id] = 0;
    }

    bool check_set_newest(uint64_t set_id, uint64_t page_id) {
        auto iter = set_ind_.find(set_id);
        if (iter == set_ind_.end()) {
            return false;
        }
        if (iter->second == page_id) {
            return true;
        }
        return false;
    }

    bool check_up_merged(SetId raw_id, MockSimpleLog *log) {

        if (log->get_set_num(raw_id.base_rd % BASE_SET_NUM) < 30) {
            return false;
        }

        uint32_t level;
        for (level = 0; level < cap_level_; level++) {
            SetId check_id = SetId {
                .level = level,
                .base_rd = uint32_t(raw_id.base_rd % BASE_SET_NUM)
            };
            if (set_ind_.find(check_id.encode()) == set_ind_.end()) {
                break;
            }
        }

        if (level > raw_id.level) {
            return true;
        }
        return false;
    }

    SetId select_down_set(uint32_t base_rd) {
        SetId set_0 = SetId {
            .level = cap_level_,
            .base_rd = base_rd,
        };
        SetId set_1 = SetId {
            .level = cap_level_ + 1,
            .base_rd = base_rd,
        };

        auto iter_0 = set_ind_.find(set_0.encode());
        auto iter_1 = set_ind_.find(set_1.encode());
        if (iter_0 == set_ind_.end()) {
            return set_0;
        } else if (iter_1 == set_ind_.end()) {
            return set_1;
        } else {
            // 随便选一个
            return iter_0->second < iter_1->second ? set_0 : set_1;
        }
    }

    std::unordered_map<uint64_t, uint64_t> set_ind_;
    MockZone zones_[SET_ZONE_NUM];
    struct WritePointer {
        uint64_t zone_id;
        uint64_t page_idx;
    };

    std::queue<uint64_t> free_zones_;
    std::unordered_set<uint64_t> up_zones_;
    WritePointer up_ptr_;
    std::unordered_set<uint64_t> down_zones_;
    WritePointer down_ptr_;

    uint64_t valid_nums_[SET_ZONE_NUM] = {0};

    // Cap Level
    uint32_t cap_level_{0};
    MockPerfCounter *perf_{nullptr};
};

class MockZ {
public:
    MockZ() {
        set_.set_perf(&perf_);
    }
    void insert(uint64_t key) {
        log_.insert(key);
        if (log_.check_flush_log(key)) {
            std::vector<uint64_t> tmp = log_.take_all_part(key);
            MockPage tmp_page;
            set_.do_flush_new_page(&tmp_page, key % BASE_SET_NUM);
        }
        
        while (set_.check_gc()) {
            set_.do_gc(&log_);
        }
    }

    MockPerfCounter *get_perf() { return &perf_; }

private:
    MockSimpleLog log_;
    MockBinarySet set_;
    MockPerfCounter perf_;
};

int main() {
    MockZ *z = new MockZ();
    MockPerfCounter *perf = z->get_perf();

    for (uint64_t i = 0; true; i++) {
        if (i % (1024 * 1024) == 0) {
            printf("Insert 0x%lx Set Page W 0x%lx(%lf) Set Num 0x%lx Avg Flush Set %lf\n", i, perf->set_page_w,
                   i == 0 ? 0 : perf->set_page_w * 40 / (double)i, perf->set_num,
                    perf->flush_set_cnt * 1.0 / (perf->flush_log_cnt == 0 ? 1 : perf->flush_log_cnt));
            // fflush(stdout);
            // sleep(1);
        }

        uint64_t key;
        key = rand64();

        z->insert(key);
    }

    delete z;
}