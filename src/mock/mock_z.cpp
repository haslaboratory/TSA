#include <cstring>
#include <map>
#include <unistd.h>
#include <unordered_map>

#include "common/Rand.h"

#define OBJ_SIZE 100
#define PAGE_SIZE 4096
#define KSEGMENT_SIZE (32 * 4096)
#define ZONE_SIZE (256 * 1024 * 1024)
#define LOG_ZONE_NUM 12
#define SET_ZONE_NUM 122
#define LOG_OP 0.15
#define SET_OP 0.05

const uint64_t OBJ_PER_PAGE = PAGE_SIZE / OBJ_SIZE;
const uint64_t PAGE_PER_ZONE = ZONE_SIZE / PAGE_SIZE;
const uint64_t PAGE_PER_KSEGMENT = KSEGMENT_SIZE / PAGE_SIZE;
const uint64_t KSEGMENT_PER_ZONE = ZONE_SIZE / KSEGMENT_SIZE;

const uint64_t BUCKET_DEPLICATE = 4;
const uint64_t SET_NUM =
    (SET_ZONE_NUM * PAGE_PER_ZONE / BUCKET_DEPLICATE) * (1 - SET_OP);
const uint64_t BUCKET_NUM = SET_NUM * BUCKET_DEPLICATE;

struct MockPage {
    uint64_t key[OBJ_PER_PAGE] = {0};
    uint64_t bkt_id{UINT64_MAX};
    uint64_t obj_num{0};
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

    void insert(uint64_t key, uint64_t page_id) {
        assert(obj_map_.size() <= LOG_ZONE_NUM * PAGE_PER_ZONE * OBJ_PER_PAGE);
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
        assert(obj_map_.size() <= LOG_ZONE_NUM * PAGE_PER_ZONE * OBJ_PER_PAGE);
        assert(set_map_.size() <= SET_NUM);
        auto obj_iter = obj_map_.find(key);
        if (obj_iter == obj_map_.end()) {
            return false;
        }
        return (obj_iter->second.page_id == page_id);
    }

    std::vector<uint64_t> take_all_part(uint64_t key) {
        assert(obj_map_.size() <= LOG_ZONE_NUM * PAGE_PER_ZONE * OBJ_PER_PAGE);
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
                                 (buf_wp_ / OBJ_PER_PAGE));

        buf_wp_ += 1;
        if (buf_wp_ == PAGE_PER_KSEGMENT * OBJ_PER_PAGE) {
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
        uint64_t page_id = seg_id * PAGE_PER_KSEGMENT + obj_idx / OBJ_PER_PAGE;
        return obj_ind_.check_obj_newest(key, page_id);
    }

    std::vector<uint64_t> take_all_part(uint64_t key) {
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

class MockSet {
public:
    MockSet() = delete;
    MockSet(const char *name) : name_(name) {}

    bool insert_bkt(MockPage bkt, uint64_t bkt_id) {
        bool new_insert = false;
        bkt.bkt_id = bkt_id;
        zones_[w_page_id_ / PAGE_PER_ZONE]
            .pages[w_page_id_ % PAGE_PER_ZONE]
            .bkt_id = bkt_id;
        // std::memcpy(&zones_[w_page_id_ / PAGE_PER_ZONE].pages[w_page_id_ %
        // PAGE_PER_ZONE], &set, sizeof(MockPage));

        BktMeta meta = BktMeta{w_page_id_, ver_id_++};
        assert(ver_id_ == UINT64_MAX);
        auto iter = bkt_ind_.find(bkt_id);
        if (iter != bkt_ind_.end()) {
            iter->second = meta;
        } else {
            new_insert = true;
            bkt_ind_.insert({bkt_id, meta});
        }

        if (w_page_id_ % PAGE_PER_ZONE == PAGE_PER_ZONE - 1) {
            printf("Write %s Set write zone %lu\n", name_,
                   w_page_id_ / PAGE_PER_ZONE);
        }
        w_page_id_ = (w_page_id_ + 1) % SET_PAGE_CAP;
        return new_insert;
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

    bool check_bkt_newest(uint64_t bkt_id, uint64_t zone_id,
                          uint64_t page_idx) {
        uint64_t page_id = zone_id * PAGE_PER_ZONE + page_idx;
        auto iter = bkt_ind_.find(bkt_id);
        if (iter == bkt_ind_.end()) {
            return false;
        }
        if (iter->second.page_id == page_id) {
            return true;
        }
        return false;
    }

    uint64_t get_last_bucket(uint64_t set_id) {
        uint64_t last_bucket = set_id * BUCKET_DEPLICATE;
        uint64_t ver_id = UINT64_MAX;
        for (uint64_t i = 0; i < BUCKET_DEPLICATE; i++) {
            uint64_t bkt_id = set_id * BUCKET_DEPLICATE + i;
            auto iter = bkt_ind_.find(bkt_id);
            if (iter == bkt_ind_.end()) {
                last_bucket = bkt_id;
                ver_id = 0;
                break;
            }
            if (iter->second.version < ver_id) {
                last_bucket = bkt_id;
                ver_id = iter->second.version;
            }
        }

        return last_bucket;
    }

    static const uint64_t SET_PAGE_CAP = SET_ZONE_NUM * PAGE_PER_ZONE;

private:
    struct BktMeta {
        uint64_t page_id;
        uint64_t version;
    };

    uint64_t ver_id_{0};
    const char *name_{nullptr};
    MockZone zones_[SET_ZONE_NUM];
    std::unordered_map<uint64_t, BktMeta> bkt_ind_;
    uint64_t w_page_id_{0};
    uint64_t gc_zone_id_{0};
};

class MockZ {
public:
    MockZ() : set_("Cold") {}
    struct PerfCounter {
        uint64_t set_num = 0;
        uint64_t set_page_w = 0;
        uint64_t flush_log_cnt = 0;
        uint64_t collect_cnt = 0;
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
            break;
        }

        if (need_flush) {
            // printf("write stall\n");
        }
    }

    PerfCounter *get_perf() { return &perf_; }

private:
    void do_flush_log() {
        // printf("flush log\n");
        uint64_t f_seg_id = log_.f_seg_id();
        MockKSegment *kseg = log_.get_seg(f_seg_id);
        for (uint64_t i = 0; i < MockKLog::KSEGMENT_CAP; i++) {
            uint64_t key = kseg->get_key(i);
            uint64_t set_id = key % SET_NUM;
            if (log_.check_obj_newest(key, f_seg_id, i)) {
                move_bucket(set_.get_last_bucket(set_id), true);
            }
            while (set_.check_gc()) {
                do_gc();
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
            uint64_t bkt_id = zone->pages[i].bkt_id;
            if (set_.check_bkt_newest(bkt_id, gc_zone_id, i)) {
                gc_pages += 1;
                move_bucket(bkt_id, false);
            }
        }
        printf("Cold GC %ld pages\n", gc_pages);
        set_.inc_gc_zone_id();
    }

    void move_bucket(uint64_t bkt_id, bool flush_log) {
        std::vector<uint64_t> log_part =
            log_.take_all_part(bkt_id / BUCKET_DEPLICATE);

        if (log_part.size() > 0) {
            perf_.flush_log_cnt += 1;
            perf_.collect_cnt += log_part.size();
        }
        // calculate page
        MockPage page;

        perf_.set_page_w += 1;
        bool new_insert = set_.insert_bkt(page, bkt_id);
        if (new_insert) {
            perf_.set_num += 1;
        }
    }

    MockKLog log_;
    MockSet set_;
    PerfCounter perf_;
};

int main() {
    MockZ *z = new MockZ();
    MockZ::PerfCounter *perf = z->get_perf();

    for (uint64_t i = 0; true; i++) {
        if (i % (1024 * 1024) == 0) {
            printf("Insert 0x%lx Set Page W 0x%lx(%lf) Set Num 0x%lx\n", i, perf->set_page_w,
                   i == 0 ? 0 : perf->set_page_w * 40 / (double)i, perf->set_num);
            fflush(stdout);
            // sleep(1);
        }

        uint64_t key;
        key = rand64();

        z->insert(key);
    }

    delete z;
}