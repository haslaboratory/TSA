#ifdef NDEBUG
#undef NDEBUG
#endif

#include <algorithm>
#include <cstring>
#include <map>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>

#include "common/Rand.h"

// #define KEY_SIZE 8
// #define VALUE_SIZE 100
#define KV_SIZE 100
#define PAGE_SIZE 4096
#define KSEGMENT_SIZE (32 * 4096)
#define ZONE_SIZE (256 * 1024 * 1024)
#define LOG_ZONE_NUM 12
#define SET_ZONE_NUM 122
#define LOG_OP 0.15
#define SET_OP 0.05

const uint64_t KV_PER_PAGE = PAGE_SIZE / KV_SIZE;
const uint64_t PAGE_PER_ZONE = ZONE_SIZE / PAGE_SIZE;
const uint64_t PAGE_PER_KSEGMENT = KSEGMENT_SIZE / PAGE_SIZE;
const uint64_t KSEGMENT_PER_ZONE = ZONE_SIZE / KSEGMENT_SIZE;
const uint64_t SET_NUM = SET_ZONE_NUM * PAGE_PER_ZONE * (1 - SET_OP);

struct MockPage {
    uint64_t key[KV_PER_PAGE] = {0};
    uint8_t rrip[KV_PER_PAGE] = {0};
    uint64_t set_id{UINT64_MAX};
    uint64_t v_num{0};
};

struct MockKSegment {
    uint64_t get_key(uint64_t obj_idx) {
        return pages[obj_idx / KV_PER_PAGE].key[obj_idx % KV_PER_PAGE];
    }

    void set_key(uint64_t obj_idx, uint64_t key) {
        pages[obj_idx / KV_PER_PAGE].key[obj_idx % KV_PER_PAGE] = key;
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

    std::unordered_map<uint64_t, uint64_t> take_all_part(uint64_t key) {
        assert(obj_map_.size() <= LOG_ZONE_NUM * PAGE_PER_ZONE * KV_PER_PAGE);
        assert(set_map_.size() <= SET_NUM);
        std::unordered_map<uint64_t, uint64_t> ret;
        auto set_iter = set_map_.find(key % SET_NUM);
        if (set_iter == set_map_.end()) {
            // printf("set %lu not found\n", key % SET_NUM);
            return ret;
        }

        uint64_t obj = set_iter->second;
        while (obj != UINT64_MAX) {
            ret.insert(std::make_pair(obj, obj_map_[obj].page_id));
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

// 暂时简化 buffer
class MockKLog {
public:
    MockKLog() {}
    MockKSegment *get_ksegment(uint64_t sid) {
        return &zones_[sid / KSEGMENT_PER_ZONE]
                    .segments[sid % KSEGMENT_PER_ZONE];
    }

    void insert(uint64_t key) {
        MockKSegment *kseg = get_ksegment(w_seg_id_);
        kseg->set_key(buf_wp_, key);
        obj_ind_.insert(key, (w_seg_id_ * PAGE_PER_KSEGMENT) +
                                 (buf_wp_ / KV_PER_PAGE));

        buf_wp_ += 1;
        if (buf_wp_ == PAGE_PER_KSEGMENT * KV_PER_PAGE) {
            buf_wp_ = 0;
            if (w_seg_id_ % KSEGMENT_PER_ZONE == KSEGMENT_PER_ZONE - 1) {
                printf("Write Log write zone %lu\n",
                       w_seg_id_ / KSEGMENT_PER_ZONE);
            }
            w_seg_id_ = (w_seg_id_ + 1) % KSEGMENT_CAP;
        }
    }

    bool check_flush() {
        uint64_t free_segs;
        if (f_seg_id_ <= w_seg_id_) {
            free_segs = f_seg_id_ + KSEGMENT_CAP - w_seg_id_;
        } else {
            free_segs = f_seg_id_ - w_seg_id_;
        }
        if (free_segs <= KSEGMENT_CAP * LOG_OP) {
            return true;
        }

        if ((w_seg_id_ / KSEGMENT_PER_ZONE == f_seg_id_ / KSEGMENT_PER_ZONE) &&
            (w_seg_id_ < f_seg_id_)) {
            return true;
        }

        return false;
    }

    uint64_t f_seg_id() { return f_seg_id_; }

    void inc_f_seg_id() {
        if (f_seg_id_ % KSEGMENT_PER_ZONE == KSEGMENT_PER_ZONE - 1) {
            printf("Write Log flush zone %lu\n", f_seg_id_ / KSEGMENT_PER_ZONE);
        }
        f_seg_id_ = (f_seg_id_ + 1) % KSEGMENT_CAP;
    }

    MockKSegment *get_seg(uint64_t seg_id) {
        return &zones_[seg_id / KSEGMENT_PER_ZONE]
                    .segments[seg_id % KSEGMENT_PER_ZONE];
    }

    bool check_obj_newest(uint64_t key, uint64_t seg_id, uint64_t obj_idx) {
        uint64_t page_id = seg_id * PAGE_PER_KSEGMENT + obj_idx / KV_PER_PAGE;
        return obj_ind_.check_obj_newest(key, page_id);
    }

    std::unordered_map<uint64_t, uint64_t>take_all_part(uint64_t key) {
        return obj_ind_.take_all_part(key);
    }

    static const uint64_t KSEGMENT_CAP = LOG_ZONE_NUM * KSEGMENT_PER_ZONE;

private:
    MockZone zones_[LOG_ZONE_NUM];
    MockIndex obj_ind_;
    uint64_t buf_wp_{0};
    uint64_t w_seg_id_{0};
    uint64_t f_seg_id_{0};
};

// 暂时仅模拟 cold set， 不考虑 hot set，因为基本没影响
// 暂时也不考虑换出
class MockKSet {
public:
    MockKSet() = delete;
    MockKSet(const char *name) : name_(name) {}
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
            printf("Write %s Set write zone %lu\n", name_,
                   w_page_id_ / PAGE_PER_ZONE);
        }
        w_page_id_ = (w_page_id_ + 1) % SET_PAGE_CAP;
    }

    void get_set(MockPage *page, uint64_t set_id) {
        auto iter = set_ind_.find(set_id);
        if (iter != set_ind_.end()) {
            uint64_t page_id = iter->second;
            std::memcpy(
                page,
                &zones_[page_id / PAGE_PER_ZONE].pages[page_id % PAGE_PER_ZONE],
                sizeof(MockPage));
        }
        return;
    }

    bool check_gc() {
        uint64_t w_zone_id = w_page_id_ / PAGE_PER_ZONE;
        uint64_t free_zones;
        if (gc_zone_id_ <= w_zone_id) {
            free_zones = gc_zone_id_ + SET_ZONE_NUM - w_zone_id;
        } else {
            free_zones = gc_zone_id_ - w_zone_id;
        }
        return free_zones <= SET_ZONE_NUM * SET_OP;
    }

    MockZone *get_zone(uint64_t zone_id) { return &zones_[zone_id]; }

    uint64_t gc_zone_id() { return gc_zone_id_; }

    void inc_gc_zone_id() {
        printf("Write %s Set gc zone %lu\n", name_, gc_zone_id_);
        gc_zone_id_ = (gc_zone_id_ + 1) % SET_ZONE_NUM;
    }

    bool check_set_newest(uint64_t set_id, uint64_t zone_id,
                          uint64_t page_idx) {
        uint64_t page_id = zone_id * PAGE_PER_ZONE + page_idx;
        auto iter = set_ind_.find(set_id);
        if (iter == set_ind_.end()) {
            return false;
        }
        if (iter->second == page_id) {
            return true;
        }
        return false;
    }

    static const uint64_t SET_PAGE_CAP = SET_ZONE_NUM * PAGE_PER_ZONE;

private:
    const char *name_{nullptr};
    MockZone zones_[SET_ZONE_NUM];
    std::unordered_map<uint64_t, uint64_t> set_ind_;
    uint64_t w_page_id_{0};
    uint64_t gc_zone_id_{0};
};

class MockK {
public:
    MockK() : set_("Cold"), hot_set_("Hot") {}
    struct PerfCounter {
        uint64_t set_page_w = 0;
        uint64_t log_page_r = 0;
        uint64_t flush_log_cnt = 0;
        uint64_t collect_cnt = 0;
        std::unordered_map<uint64_t, uint64_t> map;
    };
    void insert(uint64_t key) {
        log_.insert(key);

        bool need_flush = false;
        while (true) {
            if (log_.check_flush()) {
                need_flush = true;
                do_flush_log();
                continue;
            }
            if (set_.check_gc()) {
                need_flush = true;
                do_gc();
                continue;
            }
            if (hot_set_.check_gc()) {
                need_flush = true;
                do_hot_gc();
                continue;
            }
            break;
        }

        if (need_flush) {
            // printf("write stall\n");
        }
    }

    PerfCounter *get_perf() { return &perf_; }

    std::vector<std::vector<uint64_t>> collect_set() {
        std::vector<std::vector<uint64_t>> ret;
        for (uint32_t i = 0; i < 10; i++) {
            uint64_t set_id = rand64() % SET_NUM;
            MockPage page;
            MockPage hot_page;
            set_.get_set(&page, set_id);
            hot_set_.get_set(&hot_page, set_id);
            ret.push_back({set_id, page.v_num, hot_page.v_num});
        }

        return ret;
    }

private:
    void do_flush_log() {
        // printf("flush log\n");
        uint64_t f_seg_id = log_.f_seg_id();
        MockKSegment *kseg = log_.get_seg(f_seg_id);
        for (uint64_t i = 0; i < MockKLog::KSEGMENT_CAP; i++) {
            uint64_t key = kseg->get_key(i);
            uint64_t set_id = key % SET_NUM;
            if (log_.check_obj_newest(key, f_seg_id, i)) {
                move_bucket(set_id, true, 0);
            }
            while (set_.check_gc()) {
                do_gc();
            }
            while (hot_set_.check_gc()) {
                do_hot_gc();
            }
        }
        log_.inc_f_seg_id();
    }

    void do_gc() {
        // printf("gc\n");
        uint64_t gc_pages = 0;
        uint64_t gc_zone_id = set_.gc_zone_id();
        MockZone *zone = set_.get_zone(gc_zone_id);
        for (uint64_t i = 0; i < PAGE_PER_ZONE; i++) {
            uint64_t set_id = zone->pages[i].set_id;
            if (set_.check_set_newest(set_id, gc_zone_id, i)) {
                gc_pages += 1;
                move_bucket(set_id, false, 1);
            }
        }
        printf("Cold GC %ld pages\n", gc_pages);
        set_.inc_gc_zone_id();
    }

    void do_hot_gc() {
        uint64_t gc_pages = 0;
        uint64_t gc_zone_id = hot_set_.gc_zone_id();
        MockZone *zone = hot_set_.get_zone(gc_zone_id);
        for (uint64_t i = 0; i < PAGE_PER_ZONE; i++) {
            uint64_t set_id = zone->pages[i].set_id;
            if (hot_set_.check_set_newest(set_id, gc_zone_id, i)) {
                gc_pages += 1;
                move_bucket(set_id, false, 2);
            }
        }
        printf("Hot GC %ld pages\n", gc_pages);
        hot_set_.inc_gc_zone_id();
    }

    void redivide(MockPage *page, MockPage *hot_page) {
        // 按照RRIP排序
        std::vector<std::pair<uint64_t, uint8_t>> kv_rrip;
        for (uint64_t i = 0; i < page->v_num; i++) {
            kv_rrip.emplace_back(page->key[i], page->rrip[i]);
        }
        for (uint64_t i = 0; i < hot_page->v_num; i++) {
            kv_rrip.emplace_back(hot_page->key[i], hot_page->rrip[i]);
        }
        std::sort(kv_rrip.begin(), kv_rrip.end(),
                  [](const std::pair<uint64_t, uint8_t> &a,
                     const std::pair<uint64_t, uint8_t> &b) {
                      return a.second > b.second;
                  });

        uint64_t i = 0;
        uint64_t idx = 0;
        for (i = 0; i < KV_PER_PAGE && idx < kv_rrip.size(); i++) {
            hot_page->key[i] = kv_rrip[idx].first;
            hot_page->rrip[i] = kv_rrip[idx].second;
        }
        hot_page->v_num = i;

        for (i = 0; i < KV_PER_PAGE && idx < kv_rrip.size(); i++) {
            page->key[i] = kv_rrip[i].first;
            page->rrip[i] = kv_rrip[i].second;
        }
        page->v_num = i;

        return;
    }

    void move_bucket(uint64_t set_id, bool flush_log, int gc_mode) {
        std::unordered_map<uint64_t, uint64_t> log_part = log_.take_all_part(set_id);
        std::unordered_set<uint64_t> log_sets;

        for (auto iter = log_part.begin(); iter != log_part.end(); iter++) {
            if (log_sets.count(iter->second) > 0) {
                continue;
            }
            perf_.log_page_r += 1;
            log_sets.insert(iter->second);
        }

        if (flush_log) {
            assert(log_part.size() > 0);
        }

        if (log_part.size() > 0) {
            perf_.flush_log_cnt += 1;
            perf_.collect_cnt += log_part.size();
        }
        // calculate page
        MockPage page;
        set_.get_set(&page, set_id);

        int random = rand64() % 5;
        if ((gc_mode == 2 || random == 0) && false) {
            MockPage hot_page;
            hot_set_.get_set(&hot_page, set_id);

            redivide(&page, &hot_page);

            perf_.set_page_w += 1;
            hot_set_.insert_set(&hot_page, set_id);
        }

        for (uint32_t i = 0; i < log_part.size() && page.v_num < KV_PER_PAGE;
             i++) {
            page.key[page.v_num] = log_part[i];
            page.v_num += 1;
        }

        perf_.set_page_w += 1;
        set_.insert_set(&page, set_id);

        perf_.map[set_id] += 1;
    }

    MockKLog log_;
    MockKSet set_;
    MockKSet hot_set_;
    PerfCounter perf_;
};

int main() {
    std::mt19937_64 rng(0);
    ZipfBucketRandom *zipf_b = new ZipfBucketRandom(1024, 0.7, SET_NUM);
    MockK *k = new MockK();
    MockK::PerfCounter *perf = k->get_perf();

    for (uint64_t i = 0; true; i++) {
        if (i % (1024 * 1024) == 0) {
            printf("Insert 0x%lx Set Page W 0x%lx(%lf) Log Page R 0x%lx(%lf) Collect Cnt %lf\n", i, 
                perf->set_page_w, i == 0 ? 0 : perf->set_page_w * 40 / (double)i,
                perf->log_page_r, i == 0 ? 0 : perf->log_page_r * 40 / (double)i,
                perf->collect_cnt * 1.0 / (perf->flush_log_cnt == 0 ? 1 : perf->flush_log_cnt)
            );
            // fflush(stdout);
        }

        // if (i % (1024 * 1024 / 4) == 0) {
        //     std::vector<std::vector<uint64_t>> data = k->collect_set();
        //     for (uint64_t j = 0; j < data.size(); j++) {
        //         printf("set_id: %ld cold num: %ld hot num: %ld (insert: %ld)\n",
        //                data[j][0], data[j][1], data[j][2],
        //                perf->map[data[j][0]]);
        //     }
        // }

        uint64_t key;
        // key = (*zipf_b)(rng);
        key = rand64();

        k->insert(key);
    }

    delete zipf_b;
    delete k;
}