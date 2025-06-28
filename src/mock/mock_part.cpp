#include <cstring>
#include <map>
#include <unistd.h>
#include <unordered_map>

#include "common/Rand.h"

// #define KEY_SIZE 8
// #define VALUE_SIZE 100
#define KV_SIZE 100
#define PAGE_SIZE 4096
#define KV_PART_NUM 32
#define ZONE_SIZE (8 * 1024 * 1024) // 8 MB 小 zone
#define LOG_ZONE_NUM 12
#define SET_ZONE_NUM 122
#define TOTAL_ZONE_NUM (LOG_ZONE_NUM + SET_ZONE_NUM)
#define LOG_OP 0.15
#define SET_OP 0.05

const uint64_t KV_PER_PAGE = PAGE_SIZE / KV_SIZE;
const uint64_t PAGE_PER_ZONE = ZONE_SIZE / PAGE_SIZE;
const uint64_t SET_NUM = SET_ZONE_NUM * PAGE_PER_ZONE * (1 - SET_OP);

struct MockPage {
    uint64_t key[KV_PER_PAGE] = {0};
    uint64_t set_id{UINT64_MAX}; // UINT64_MAX 表示为 log page
    uint64_t v_num{0};
};

union MockZone {
    MockZone() {}
    MockPage pages[PAGE_PER_ZONE];
};

class MockIndex {
public:
    struct ObjMeta {
        uint64_t page_id;
        uint64_t next;
    };

    void insert(uint64_t key, uint64_t page_id) {
        assert(obj_map_.size() <= LOG_ZONE_NUM * PAGE_PER_ZONE * KV_PER_PAGE);
        assert(set_map_.size() <= SET_NUM);
        auto obj_iter = obj_map_.find(key);
        if (obj_iter != obj_map_.end()) {
            obj_iter->second.page_id = page_id;
            return;
        }
        ObjMeta meta = {page_id, UINT64_MAX};
        auto set_iter = set_map_.find(key % SET_NUM);
        if (set_iter != set_map_.end()) {
            meta.next = set_iter->second;
            set_iter->second = key;
        } else {
            set_map_.insert({key % SET_NUM, key});
        }
        obj_map_.insert({key, meta});
    }

    bool check_obj_newest(uint64_t key, uint64_t page_id) {
        assert(obj_map_.size() <= LOG_ZONE_NUM * PAGE_PER_ZONE * KV_PER_PAGE);
        assert(set_map_.size() <= SET_NUM);
        auto obj_iter = obj_map_.find(key);
        if (obj_iter == obj_map_.end()) {
            return false;
        }
        return (obj_iter->second.page_id == page_id);
    }

    std::vector<uint64_t> take_all_part(uint64_t key) {
        assert(obj_map_.size() <= LOG_ZONE_NUM * PAGE_PER_ZONE * KV_PER_PAGE);
        assert(set_map_.size() <= SET_NUM);
        std::vector<uint64_t> ret;
        auto set_iter = set_map_.find(key % SET_NUM);
        if (set_iter == set_map_.end()) {
            return ret;
        }

        uint64_t obj = set_iter->second;
        while (obj != UINT64_MAX) {
            ret.push_back(obj);
            uint64_t prev = obj;
            obj = obj_map_[prev].next;
            obj_map_.erase(prev);
        }
        set_map_.erase(key % SET_NUM);

        return ret;
    }

private:
    std::unordered_map<uint64_t, uint64_t> set_map_;
    std::unordered_map<uint64_t, ObjMeta> obj_map_;
};

struct MockPerfCounter {
    uint64_t set_page_w = 0;
    uint64_t flush_log_cnt = 0;
    uint64_t collect_cnt = 0;
};

class MockPart {
public:
    void set_perf(MockPerfCounter *perf) { perf_ = perf; }

    bool check_gc() {
        // Check Log
        if (log_page_num_ >= LOG_ZONE_NUM * PAGE_PER_ZONE * (1 - LOG_OP)) {
            // printf("gc log\n");
            return true;
        }
        uint64_t w_zone_id = w_page_id_ / PAGE_PER_ZONE;
        uint64_t free_zones;
        if (gc_zone_id_ <= w_zone_id) {
            free_zones = gc_zone_id_ + TOTAL_ZONE_NUM - w_zone_id;
        } else {
            free_zones = gc_zone_id_ - w_zone_id;
        }
        if (free_zones <= TOTAL_ZONE_NUM * SET_OP) {
            // printf("gc set\n");
            return true;
        }
        return false;
    }

    void move_bucket(uint64_t set_id) {
        std::vector<uint64_t> log_part = obj_ind_.take_all_part(set_id);

        if (log_part.size() > 0) {
            perf_->flush_log_cnt += 1;
            perf_->collect_cnt += log_part.size();
        }
        // calculate page
        MockPage page;
        get_set(&page, set_id);

        perf_->set_page_w += 1;
        insert_set(&page, set_id);
    }

    void flush_page(MockPage *page, uint64_t page_id) {
        for (uint32_t i = 0; i < page->v_num; i++) {
            uint64_t key = page->key[i];
            if (obj_ind_.check_obj_newest(key, page_id)) {
                // Do Something
                move_bucket(key % SET_NUM);
            }
        }
    }

    void do_gc() {
        MockZone *zone = &zones_[gc_zone_id_];
        for (uint64_t i = 0; i < PAGE_PER_ZONE; i++) {
            MockPage *page = &zone->pages[i];
            if (page->set_id == UINT64_MAX) {
                // LOG PAGE
                flush_page(page, gc_zone_id_ * PAGE_PER_ZONE + i);
                log_page_num_ -= 1;
            } else {
                if (check_set_newest(page->set_id,
                                     gc_zone_id_ * PAGE_PER_ZONE + i)) {
                    move_bucket(page->set_id);
                }
            }
        }
        gc_zone_id_ = (gc_zone_id_ + 1) % TOTAL_ZONE_NUM;
    }

    void insert(uint64_t key) {
        buffer_.key[buffer_.v_num] = key;
        buffer_.v_num = buffer_.v_num + 1;
        if (buffer_.v_num < KV_PER_PAGE) {
            return;
        }
        // Flush Log
        for (uint64_t i = 0; i < KV_PER_PAGE; i++) {
            obj_ind_.insert(buffer_.key[i], w_page_id_);
        }
        buffer_.set_id = UINT64_MAX;
        std::memcpy(&zones_[w_page_id_ / PAGE_PER_ZONE]
                         .pages[w_page_id_ % PAGE_PER_ZONE],
                    &buffer_, sizeof(MockPage));
        w_page_id_ = (w_page_id_ + 1) % ZONE_PAGE_CAP;
        log_page_num_ += 1;
        buffer_.v_num = 0;
    }

    static const uint64_t ZONE_PAGE_CAP = TOTAL_ZONE_NUM * PAGE_PER_ZONE;

private:
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
    void get_set(MockPage *page, uint64_t set_id) {
        auto iter = set_ind_.find(set_id);
        if (iter == set_ind_.end()) {
            return;
        }
        uint64_t page_id = iter->second;
        std::memcpy(
            page,
            &zones_[page_id / PAGE_PER_ZONE].pages[page_id % PAGE_PER_ZONE],
            sizeof(MockPage));
    }

    void insert_set(MockPage *set, uint64_t set_id) {
        set->set_id = set_id;
        zones_[w_page_id_ / PAGE_PER_ZONE]
            .pages[w_page_id_ % PAGE_PER_ZONE]
            .set_id = set_id;
        std::memcpy(&zones_[w_page_id_ / PAGE_PER_ZONE]
                         .pages[w_page_id_ % PAGE_PER_ZONE],
                    set, sizeof(MockPage));
        auto iter = set_ind_.find(set_id);
        if (iter != set_ind_.end()) {
            iter->second = w_page_id_;
        } else {
            set_ind_.insert({set_id, w_page_id_});
        }
        if (w_page_id_ % PAGE_PER_ZONE == PAGE_PER_ZONE - 1) {
            // printf("Write Set write zone %lu\n", w_page_id_ / PAGE_PER_ZONE);
        }
        w_page_id_ = (w_page_id_ + 1) % ZONE_PAGE_CAP;
    }

    MockPage buffer_;
    MockIndex obj_ind_;
    uint64_t log_page_num_;
    MockZone zones_[TOTAL_ZONE_NUM];
    std::unordered_map<uint64_t, uint64_t> set_ind_;
    uint64_t w_page_id_{0};
    uint64_t gc_zone_id_{0};

    // perf
    MockPerfCounter *perf_{nullptr};
};

class MockK {
public:
    MockK() {
        for (uint64_t i = 0; i < KV_PART_NUM; i++) {
            part_[i].set_perf(&perf_);
        }
    }
    void insert(uint64_t key) {
        uint64_t part_id = (key >> 32) % KV_PART_NUM;
        MockPart *part = &part_[part_id];

        part->insert(key);
        while (part->check_gc()) {
            part->do_gc();
        }
    }

    MockPerfCounter *get_perf() { return &perf_; }

private:
    MockPart part_[KV_PART_NUM];
    MockPerfCounter perf_;
};

int main() {
    MockK *k = new MockK();
    MockPerfCounter *perf = k->get_perf();

    for (uint64_t i = 0; true; i++) {
        if (i % (1024 * 1024) == 0) {
            printf("Insert 0x%lx Set Page W 0x%lx(%lf)\n", i, perf->set_page_w,
                   i == 0 ? 0 : perf->set_page_w * 40 / (double)i);
            // fflush(stdout);
            // sleep(1);
        }

        uint64_t key;
        key = rand64();

        k->insert(key);
    }

    delete k;
}