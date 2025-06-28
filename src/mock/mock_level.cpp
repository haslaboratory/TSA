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

#define KV_SIZE 100
#define PAGE_SIZE 4096
#define KSEGMENT_SIZE (32 * 4096)
#define ZONE_SIZE (256 * 1024 * 1024)
#define LOG_ZONE_NUM 12
#define SET_ZONE_NUM 122
#define HOT_ZONE_NUM 31
#define LOG_OP 0.15
#define SET_OP 0.05

const uint64_t KV_PER_PAGE = PAGE_SIZE / KV_SIZE;
const uint64_t PAGE_PER_ZONE = ZONE_SIZE / PAGE_SIZE;
const uint64_t PAGE_PER_KSEGMENT = KSEGMENT_SIZE / PAGE_SIZE;
const uint64_t KSEGMENT_PER_ZONE = ZONE_SIZE / KSEGMENT_SIZE;

const uint64_t BUCKET_DEPLICATE = 4;
const uint64_t SET_NUM =
    (SET_ZONE_NUM * PAGE_PER_ZONE / BUCKET_DEPLICATE) * (1 - SET_OP);
const uint64_t BUCKET_NUM = SET_NUM * BUCKET_DEPLICATE;

struct MockPage {
    uint64_t key[KV_PER_PAGE] = {0};
    uint64_t bkt_id{UINT64_MAX};
    uint64_t obj_num{0};
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

class MockLayerSet {
public:
    MockLayerSet() {
        for (uint64_t i = 0; i < SET_ZONE_NUM + HOT_ZONE_NUM; i++) {
            free_zones_.push(i);
        }

        warm_ptr_.zone_id = free_zones_.front();
        free_zones_.pop();
        hot_ptr_.zone_id = free_zones_.front();
        free_zones_.pop();
    }
    void read_bkt(MockPage *bkt, uint64_t bkt_id) {
        auto iter = bkt_idx_.find(bkt_id);
        if (iter == bkt_idx_.end()) {
            return;
        }
        std::memcpy(bkt, &zones_[iter->second / PAGE_PER_ZONE].pages[iter->second % PAGE_PER_ZONE], sizeof(MockPage));
    }

    void insert_hot_bkt(MockPage *bkt, uint64_t set_id) {
        uint64_t bkt_id = gen_bkt_id(set_id, 0);
        bkt->bkt_id = bkt_id;
        std::memcpy(&zones_[hot_ptr_.zone_id].pages[hot_ptr_.page_idx], bkt,
                    sizeof(MockPage));
        
        auto iter = bkt_idx_.find(bkt_id);
        if (iter != bkt_idx_.end()) {
            valid_nums_[iter->second / PAGE_PER_ZONE] -= 1;
            iter->second = hot_ptr_.zone_id * PAGE_PER_ZONE + hot_ptr_.page_idx;
        } else {
            bkt_idx_.insert({bkt_id, hot_ptr_.zone_id * PAGE_PER_ZONE + hot_ptr_.page_idx});
        }
        valid_nums_[hot_ptr_.zone_id] += 1;
        
        hot_ptr_.page_idx = hot_ptr_.page_idx + 1;
        if (hot_ptr_.page_idx < PAGE_PER_ZONE) {
            return;
        }
        printf("Write Hot Set write zone %lu\n", hot_ptr_.zone_id);
        hot_zones_.insert(hot_ptr_.zone_id);
        assert(free_zones_.size() > 0);
        hot_ptr_.zone_id = free_zones_.front(); free_zones_.pop();
        hot_ptr_.page_idx = 0;
    }

    void insert_warm_bkt(MockPage *bkt, uint64_t set_id, uint64_t bkt_dup) {
        assert(bkt_dup > 0);
        assert(set_2_bkt_dup_.find(set_id) != set_2_bkt_dup_.end());
        assert(set_2_bkt_dup_[set_id] == bkt_dup);
        {
            uint64_t new_bkt_dup = 0;
            if (bkt_dup == BUCKET_DEPLICATE) {
                new_bkt_dup = 1;
            } else {
                new_bkt_dup = bkt_dup + 1;
            }
            set_2_bkt_dup_[set_id] = new_bkt_dup;
        }

        uint64_t bkt_id = gen_bkt_id(set_id, bkt_dup);
        bkt->bkt_id = bkt_id;
        std::memcpy(&zones_[warm_ptr_.zone_id].pages[warm_ptr_.page_idx], bkt,
                    sizeof(MockPage));
        
        auto iter = bkt_idx_.find(bkt_id);
        if (iter != bkt_idx_.end()) {
            valid_nums_[iter->second / PAGE_PER_ZONE] -= 1;
            iter->second = warm_ptr_.zone_id * PAGE_PER_ZONE + warm_ptr_.page_idx;
        } else {
            bkt_idx_.insert({bkt_id, warm_ptr_.zone_id * PAGE_PER_ZONE + warm_ptr_.page_idx});
        }
        valid_nums_[warm_ptr_.zone_id] += 1;
        
        warm_ptr_.page_idx = warm_ptr_.page_idx + 1;
        if (warm_ptr_.page_idx < PAGE_PER_ZONE) {
            return;
        }
        printf("Write Warm Set write zone %lu\n", warm_ptr_.zone_id);
        warm_zones_.push(warm_ptr_.zone_id);
        assert(free_zones_.size() > 0);
        warm_ptr_.zone_id = free_zones_.front(); free_zones_.pop();
        warm_ptr_.page_idx = 0;
    }

    bool check_gc() {
        uint64_t free_zones = free_zones_.size();
        return free_zones <= (SET_ZONE_NUM + HOT_ZONE_NUM) * SET_OP / 2;
    }

    uint64_t prepare_gc_warm() {
        assert(warm_zones_.size() > 0);
        return warm_zones_.front();
    }

    void finish_gc_warm(uint64_t zone_id, uint64_t gc_pages) {
        assert(warm_zones_.front() == zone_id);
        assert(valid_nums_[zone_id] == 0);
        printf("GC Warm Set zone %lu, gc pages %lu\n", zone_id, gc_pages);
        warm_zones_.pop();
        free_zones_.push(zone_id);
    }

    uint64_t prepare_gc_hot() {
        assert(hot_zones_.size() > 0);
        uint64_t gc_zone_id = *hot_zones_.begin();
        for (auto iter = hot_zones_.begin(); iter != hot_zones_.end(); iter++) {
            if (valid_nums_[*iter] < valid_nums_[gc_zone_id]) {
                gc_zone_id = *iter;
            }
        }
        return gc_zone_id;
    }

    void finish_gc_hot(uint64_t zone_id, uint64_t gc_pages) {
        assert(hot_zones_.find(zone_id) != hot_zones_.end());
        assert(valid_nums_[zone_id] == 0);
        printf("GC Hot Set zone %lu, gc pages %lu\n", zone_id, gc_pages);
        hot_zones_.erase(zone_id);
        free_zones_.push(zone_id);
    }

    uint64_t get_bkt_dup(uint64_t set_id) {
        auto iter = set_2_bkt_dup_.find(set_id);
        if (iter == set_2_bkt_dup_.end()) {
            set_2_bkt_dup_.insert({set_id, 1});
            return 1;
        }
        return iter->second;
    }

    bool check_bkt_newest(uint64_t bkt_id, uint64_t page_id) {
        auto iter = bkt_idx_.find(bkt_id);
        if (iter == bkt_idx_.end()) {
            return false;
        }
        return iter->second == page_id;
    }

    uint64_t gen_bkt_id(uint64_t set_id, uint64_t bkt_dup) {
        return bkt_dup * SET_NUM + set_id;
    }

    MockZone *get_zone(uint64_t zone_id) {
        return &zones_[zone_id];
    }

    bool check_gc_warm() {
        uint64_t warm_zone_id = prepare_gc_warm();
        uint64_t hot_zone_id = prepare_gc_hot();
        if (valid_nums_[warm_zone_id] < valid_nums_[hot_zone_id]) {
            return true;
        }
        return false;
    }

private:
    struct WritePointer {
        uint64_t zone_id{0};
        uint64_t page_idx{0};
    };
    std::unordered_map<uint64_t, uint64_t> bkt_idx_;
    std::unordered_map<uint64_t, uint64_t> set_2_bkt_dup_;
    MockZone zones_[SET_ZONE_NUM + HOT_ZONE_NUM];
    uint64_t valid_nums_[SET_ZONE_NUM + HOT_ZONE_NUM] = {0};
    std::queue<uint64_t> free_zones_;
    WritePointer hot_ptr_;
    std::unordered_set<uint64_t> hot_zones_;
    WritePointer warm_ptr_;
    std::queue<uint64_t> warm_zones_;
};

// 将 hot set 和 warm set放在近临位置可能会导致不平衡的 GC
class MockAdjSet {
public:
    MockAdjSet() {
        for (int i = 0; i < SET_ZONE_NUM + HOT_ZONE_NUM; i++) {
            free_zones_.push(i);
        }
        write_ptr_.zone_id = free_zones_.front();
        free_zones_.pop();
        write_ptr_.page_idx = 0;
    }

    uint64_t gen_bkt_id(uint64_t set_id, uint64_t bkt_dup) {
        return bkt_dup * SET_NUM + set_id;
    }
    void read_bkt(MockPage *bkt, uint64_t bkt_id) {
        auto iter = bkt_idx_.find(bkt_id);
        if (iter == bkt_idx_.end()) {
            return;
        }
        std::memcpy(bkt, &zones_[iter->second / PAGE_PER_ZONE].pages[iter->second % PAGE_PER_ZONE], sizeof(MockPage));
    }

    void insert_bkt(MockPage *bkt, uint64_t bkt_id) {
        bkt->bkt_id = bkt_id;
        std::memcpy(&zones_[write_ptr_.zone_id].pages[write_ptr_.page_idx], bkt,
                    sizeof(MockPage));
        
        auto iter = bkt_idx_.find(bkt_id);
        if (iter != bkt_idx_.end()) {
            iter->second = write_ptr_.zone_id * PAGE_PER_ZONE + write_ptr_.page_idx;
        } else {
            bkt_idx_.insert({bkt_id, write_ptr_.zone_id * PAGE_PER_ZONE + write_ptr_.page_idx});
        }
        
        write_ptr_.page_idx = write_ptr_.page_idx + 1;
        if (write_ptr_.page_idx < PAGE_PER_ZONE) {
            return;
        }
        printf("Write Set write zone %lu\n", write_ptr_.zone_id);
        full_zones_.push(write_ptr_.zone_id);
        assert(free_zones_.size() > 0);
        write_ptr_.zone_id = free_zones_.front(); free_zones_.pop();
        write_ptr_.page_idx = 0;
    }

    void insert_hot_bkt(MockPage *bkt, uint64_t set_id) {
        uint64_t bkt_id = gen_bkt_id(set_id, 0);
        bkt->bkt_id = bkt_id;
        std::memcpy(&zones_[write_ptr_.zone_id].pages[write_ptr_.page_idx], bkt,
                    sizeof(MockPage));
        
        auto iter = bkt_idx_.find(bkt_id);
        if (iter != bkt_idx_.end()) {
            iter->second = write_ptr_.zone_id * PAGE_PER_ZONE + write_ptr_.page_idx;
        } else {
            bkt_idx_.insert({bkt_id, write_ptr_.zone_id * PAGE_PER_ZONE + write_ptr_.page_idx});
        }
        
        write_ptr_.page_idx = write_ptr_.page_idx + 1;
        if (write_ptr_.page_idx < PAGE_PER_ZONE) {
            return;
        }
        printf("Write Set write zone %lu\n", write_ptr_.zone_id);
        full_zones_.push(write_ptr_.zone_id);
        assert(free_zones_.size() > 0);
        write_ptr_.zone_id = free_zones_.front(); free_zones_.pop();
        write_ptr_.page_idx = 0;
    }

    void insert_warm_bkt(MockPage *bkt, uint64_t set_id, uint64_t bkt_dup) {
        assert(bkt_dup > 0);
        assert(set_2_bkt_dup_.find(set_id) != set_2_bkt_dup_.end());
        assert(set_2_bkt_dup_[set_id] == bkt_dup);
        {
            uint64_t new_bkt_dup = 0;
            if (bkt_dup == BUCKET_DEPLICATE) {
                new_bkt_dup = 1;
            } else {
                new_bkt_dup = bkt_dup + 1;
            }
            set_2_bkt_dup_[set_id] = new_bkt_dup;
        }

        uint64_t bkt_id = gen_bkt_id(set_id, bkt_dup);
        bkt->bkt_id = bkt_id;
        std::memcpy(&zones_[write_ptr_.zone_id].pages[write_ptr_.page_idx], bkt,
                    sizeof(MockPage));
        
        auto iter = bkt_idx_.find(bkt_id);
        if (iter != bkt_idx_.end()) {
            iter->second = write_ptr_.zone_id * PAGE_PER_ZONE + write_ptr_.page_idx;
        } else {
            bkt_idx_.insert({bkt_id, write_ptr_.zone_id * PAGE_PER_ZONE + write_ptr_.page_idx});
        }
        
        write_ptr_.page_idx = write_ptr_.page_idx + 1;
        if (write_ptr_.page_idx < PAGE_PER_ZONE) {
            return;
        }
        printf("Write Set write zone %lu\n", write_ptr_.zone_id);
        full_zones_.push(write_ptr_.zone_id);
        assert(free_zones_.size() > 0);
        write_ptr_.zone_id = free_zones_.front(); free_zones_.pop();
        write_ptr_.page_idx = 0;
    }

    bool check_bkt_newest(uint64_t bkt_id, uint64_t page_id) {
        auto iter = bkt_idx_.find(bkt_id);
        if (iter == bkt_idx_.end()) {
            return false;
        }
        return iter->second == page_id;
    }

    bool check_gc() {
        if (free_zones_.size() < (HOT_ZONE_NUM + SET_ZONE_NUM) * SET_OP / 2) {
            return true;
        }
        return false;
    }

    uint64_t get_bkt_dup(uint64_t set_id) {
        auto iter = set_2_bkt_dup_.find(set_id);
        if (iter == set_2_bkt_dup_.end()) {
            set_2_bkt_dup_.insert({set_id, 1});
            return 1;
        }
        return iter->second;
    }

    uint64_t prepare_gc() {
        return full_zones_.front();
    }

    void finish_gc(uint64_t zone_id, uint64_t gc_pages) {
        assert(full_zones_.front() == zone_id);
        printf("Finish GC zone %lu with %lu pages\n", zone_id, gc_pages);
        full_zones_.pop();
        free_zones_.push(zone_id);
    }

    MockZone *get_zone(uint64_t zone_id) {
        return &zones_[zone_id];
    }

    uint64_t get_set_num() {
        return bkt_idx_.size();
    }
private:
    struct WritePointer {
        uint64_t zone_id{0};
        uint64_t page_idx{0};  
    };

    std::unordered_map<uint64_t, uint64_t> bkt_idx_;
    std::unordered_map<uint64_t, uint64_t> set_2_bkt_dup_;
    MockZone zones_[SET_ZONE_NUM + HOT_ZONE_NUM];
    std::queue<uint64_t> free_zones_;

    WritePointer write_ptr_;
    std::queue<uint64_t> full_zones_;
};

// 探究 cache 一致性实现
class MockSetRegion {
public:
    MockSetRegion() {
        for (int i = 0; i < HOT_ZONE_NUM; i++) {
            free_zones_.push(i);
        }
        write_ptr_.zone_id = free_zones_.front();
        free_zones_.pop();
        write_ptr_.page_idx = 0;
    }

    void set_region_id(uint64_t region_id) {
        region_id_ = region_id;
    }

    void read_set(MockPage *bkt, uint64_t set_id) {
        auto iter = bkt_idx_.find(set_id);
        if (iter == bkt_idx_.end()) {
            return;
        }
        std::memcpy(bkt, &zones_[iter->second / PAGE_PER_ZONE].pages[iter->second % PAGE_PER_ZONE], sizeof(MockPage));
    }

    void insert_set(MockPage *bkt, uint64_t set_id) {
        zones_[write_ptr_.zone_id]
            .pages[write_ptr_.page_idx]
            .bkt_id = set_id;
        std::memcpy(&zones_[write_ptr_.zone_id]
                         .pages[write_ptr_.page_idx],
                    bkt, sizeof(MockPage));
        
        auto iter = bkt_idx_.find(set_id);
        if (iter != bkt_idx_.end()) {
            iter->second = write_ptr_.zone_id * PAGE_PER_ZONE + write_ptr_.page_idx;
        } else {
            bkt_idx_.insert({set_id, write_ptr_.zone_id * PAGE_PER_ZONE + write_ptr_.page_idx});
        }

        write_ptr_.page_idx = write_ptr_.page_idx + 1;
        if (write_ptr_.page_idx < PAGE_PER_ZONE) {
            return;
        }
        printf("Write Region %lu write zone %lu\n", region_id_, write_ptr_.zone_id);
        full_zones_.push(write_ptr_.zone_id);

        if (check_gc()) {
            do_gc();
        }

        assert(free_zones_.size() > 0);
        write_ptr_.zone_id = free_zones_.front(); free_zones_.pop();
        write_ptr_.page_idx = 0;
    }

    bool check_gc() {
        if (free_zones_.size() == 0) {
            return true;
        }
        return false;
    }

    bool check_set_newest(uint64_t set_id, uint64_t page_id) {
        auto iter = bkt_idx_.find(set_id);
        if (iter == bkt_idx_.end()) {
            return false;
        }
        return iter->second == page_id;
    }

    MockZone *get_zone(uint64_t zone_id) {
        return &zones_[zone_id];
    }

    uint64_t prepare_gc_zone() {
        return full_zones_.front();
    }

    void complete_gc_zone(uint64_t zone_id) {
        assert(full_zones_.front() == zone_id);
        printf("Region %ld Finish GC zone %lu\n", region_id_, zone_id);
        full_zones_.pop();
        free_zones_.push(zone_id);
        return;
    }

private:
    void do_gc() {
        uint64_t zone_id = prepare_gc_zone();
        MockZone *zone = get_zone(zone_id);
        for (uint64_t i = 0; i < PAGE_PER_ZONE; i++) {
            uint64_t bkt_id = zone->pages[i].bkt_id;
            assert(check_set_newest(zone_id * PAGE_PER_ZONE + i, bkt_id) == false);
        }

        complete_gc_zone(zone_id);
    }
    struct WritePointer {
        uint64_t zone_id{0};
        uint64_t page_idx{0};  
    };

    std::unordered_map<uint64_t, uint64_t> bkt_idx_;
    MockZone zones_[HOT_ZONE_NUM];
    std::queue<uint64_t> free_zones_;
    std::queue<uint64_t> full_zones_;

    WritePointer write_ptr_;

    uint64_t region_id_{UINT64_MAX};
};

class MockCursorSet {
public:
    MockCursorSet() {
        for (uint64_t i = 0; i < BUCKET_DEPLICATE + 1; i++) {
            region[i].set_region_id(i);
        }
    }

    uint64_t get_bkt_dup(uint64_t set_id) {
        auto iter = set_2_bkt_dup_.find(set_id);
        if (iter == set_2_bkt_dup_.end()) {
            set_2_bkt_dup_.insert({set_id, 1});
            return 1;
        }
        return iter->second;
    }

    void read_bkt(MockPage *bkt, uint64_t set_id, uint64_t bkt_dup) {
        region[bkt_dup].read_set(bkt, set_id);
    }

    void insert_hot_bkt(MockPage *bkt, uint64_t set_id) {
        region[0].insert_set(bkt, set_id);
    }

    void insert_warm_bkt(MockPage *bkt, uint64_t set_id, uint64_t bkt_dup) {
        assert(bkt_dup > 0);
        assert(set_2_bkt_dup_.find(set_id) != set_2_bkt_dup_.end());
        assert(set_2_bkt_dup_[set_id] == bkt_dup);
        {
            uint64_t new_bkt_dup = 0;
            if (bkt_dup == BUCKET_DEPLICATE) {
                new_bkt_dup = 1;
            } else {
                new_bkt_dup = bkt_dup + 1;
            }
            set_2_bkt_dup_[set_id] = new_bkt_dup;
        }

        region[bkt_dup].insert_set(bkt, set_id);
    }

private:
    std::unordered_map<uint64_t, uint64_t> set_2_bkt_dup_;
    MockSetRegion region[BUCKET_DEPLICATE + 1];
};

// 研究 GC 爆炸的可能性
// 如何将冷热两层的GC对齐

class MockLayerZ {
public:
    struct PerfCounter {
        uint64_t set_num = 0;
        uint64_t set_page_w = 0;
        uint64_t flush_log_cnt = 0;
        uint64_t collect_cnt = 0;
    };
    void insert(uint64_t key) {
        log_.insert(key);

        while (true) {
            if (log_.check_flush()) {
                do_flush_log();
                continue;
            }
            if (set_.check_gc()) {
                do_gc();
                continue;
            }
            break;
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
                move_bucket(set_id);
            }
            while (set_.check_gc()) {
                do_gc();
            }
        }
        log_.inc_f_seg_id();
    }

    void do_hot_gc() {
        uint64_t hot_zone_id = set_.prepare_gc_hot();
        MockZone *hot_zone = set_.get_zone(hot_zone_id);

        uint64_t gc_pages = 0;
        for (uint64_t i = 0; i < PAGE_PER_ZONE; i++) {
            uint64_t bkt_id = hot_zone->pages[i].bkt_id;
            if (set_.check_bkt_newest(bkt_id, hot_zone_id * PAGE_PER_ZONE + i)) {
                gc_pages += 1;
                move_bucket(bkt_id % SET_NUM);
            }
        }

        set_.finish_gc_hot(hot_zone_id, gc_pages);
    }

    void do_warm_gc() {
        uint64_t warm_zone_id = set_.prepare_gc_warm();
        MockZone *warm_zone = set_.get_zone(warm_zone_id);

        uint64_t gc_pages = 0;
        for (uint64_t i = 0; i < PAGE_PER_ZONE; i++) {
            uint64_t bkt_id = warm_zone->pages[i].bkt_id;
            if (set_.check_bkt_newest(bkt_id, warm_zone_id * PAGE_PER_ZONE + i)) {
                gc_pages += 1;
                move_bucket(bkt_id % SET_NUM);
            }
        }
        
        set_.finish_gc_warm(warm_zone_id, gc_pages);
    }

    void do_gc() {
        if (set_.check_gc_warm()) {
            do_warm_gc();
        } else {
            do_hot_gc();
        }
    }

    void move_bucket(uint64_t set_id) {
        std::vector<uint64_t> log_part =
            log_.take_all_part(set_id);

        MockPage hot_bkt;
        MockPage warm_bkt;
        uint64_t warm_bkt_dup = set_.get_bkt_dup(set_id);

        set_.read_bkt(&hot_bkt, set_.gen_bkt_id(set_id, 0));
        set_.read_bkt(&warm_bkt, set_.gen_bkt_id(set_id, warm_bkt_dup));

        if (log_part.size() > 0) {
            perf_.flush_log_cnt += 1;
            perf_.collect_cnt += log_part.size();
        }

        // if (hot_bkt.obj_num + log_part.size() <= KV_PER_PAGE) {
        //     hot_bkt.obj_num += log_part.size();
        //     set_.insert_hot_bkt(&hot_bkt, set_id);
        //     perf_.set_page_w += 1;
        // } else {
            hot_bkt.obj_num = KV_PER_PAGE;
            set_.insert_hot_bkt(&hot_bkt, set_id);
            set_.insert_warm_bkt(&warm_bkt, set_id, warm_bkt_dup);

            perf_.set_page_w += 2;
        // }
    }

    MockKLog log_;
    MockLayerSet set_;
    PerfCounter perf_;
};

class MockAdjZ {
public:
    struct PerfCounter {
        uint64_t set_num = 0;
        uint64_t set_page_w = 0;
        uint64_t flush_log_cnt = 0;
        uint64_t collect_cnt = 0;  
    };

    void insert(uint64_t key) {
        log_.insert(key);

        while (true) {
            if (log_.check_flush()) {
                do_flush_log();
                continue;
            }
            if (set_.check_gc()) {
                do_gc();
                continue;
            }
            break;
        }
    }

    PerfCounter *get_perf() { return &perf_; }

    uint64_t get_set_num() { return set_.get_set_num(); }
private:
    void do_flush_log() {
        // printf("flush log\n");
        uint64_t f_seg_id = log_.f_seg_id();
        MockKSegment *kseg = log_.get_seg(f_seg_id);
        for (uint64_t i = 0; i < MockKLog::KSEGMENT_CAP; i++) {
            uint64_t key = kseg->get_key(i);
            uint64_t set_id = key % SET_NUM;
            if (log_.check_obj_newest(key, f_seg_id, i)) {
                move_bucket(set_id);
            }
            while (set_.check_gc()) {
                do_gc();
            }
        }
        log_.inc_f_seg_id();
    }

    void do_gc() {
        uint64_t gc_zone_id = set_.prepare_gc();
        MockZone *gc_zone = set_.get_zone(gc_zone_id);

        uint64_t gc_pages = 0;
        uint64_t w_pages = 0;
        for (uint64_t i = 0; i < PAGE_PER_ZONE; i++) {
            uint64_t bkt_id = gc_zone->pages[i].bkt_id;
            if (!set_.check_bkt_newest(bkt_id, gc_zone_id * PAGE_PER_ZONE + i)) {
                continue;
            }
            gc_pages += 1;
            uint64_t set_id = bkt_id % SET_NUM;
            uint64_t bkt_dup = bkt_id / SET_NUM;
            if (bkt_dup == 0 || set_.get_bkt_dup(set_id) == bkt_dup) {
                move_bucket(set_id);
                w_pages += 2;
            } else {
                set_.insert_bkt(&gc_zone->pages[i], bkt_id);
                w_pages += 1;
            }
        }
        
        assert(w_pages <= gc_pages + 1);
        set_.finish_gc(gc_zone_id, gc_pages);
    }

    void move_bucket(uint64_t set_id) {
        std::vector<uint64_t> log_part =
            log_.take_all_part(set_id);

        MockPage hot_bkt;
        MockPage warm_bkt;
        uint64_t warm_bkt_dup = set_.get_bkt_dup(set_id);

        set_.read_bkt(&hot_bkt, set_.gen_bkt_id(set_id, 0));
        set_.read_bkt(&warm_bkt, set_.gen_bkt_id(set_id, warm_bkt_dup));

        if (log_part.size() > 0) {
            perf_.flush_log_cnt += 1;
            perf_.collect_cnt += log_part.size();
        }

        // if (hot_bkt.obj_num + log_part.size() <= KV_PER_PAGE) {
        //     hot_bkt.obj_num += log_part.size();
        //     set_.insert_hot_bkt(&hot_bkt, set_id);
        //     perf_.set_page_w += 1;
        // } else {
            hot_bkt.obj_num = KV_PER_PAGE;
            set_.insert_warm_bkt(&warm_bkt, set_id, warm_bkt_dup);
            set_.insert_hot_bkt(&hot_bkt, set_id);

            perf_.set_page_w += 2;
        // }
    }

    MockKLog log_;
    MockAdjSet set_;
    PerfCounter perf_;
};

class MockCursorZ {
public:
    struct PerfCounter {
        uint64_t set_num = 0;
        uint64_t set_page_w = 0;
        uint64_t flush_log_cnt = 0;
        uint64_t collect_cnt = 0;  
        uint64_t readmit_cnt = 0;
    };

    void insert(uint64_t key) {
        log_.insert(key);

        while (log_.check_flush()) {
            do_flush_log();
        }
    }

    PerfCounter *get_perf() { return &perf_; }

private:
    const uint64_t FLUSH_REUSED_UNIT = KSEGMENT_PER_ZONE;
    const uint64_t FLUSH_SET_CAP = SET_NUM * FLUSH_REUSED_UNIT * 1.0 / (KSEGMENT_PER_ZONE * LOG_ZONE_NUM);
    
    void do_flush_log() {
        // 按批次进行 flush
        uint64_t f_set_num = 0;

        for (f_set_num = 0; f_set_num <= FLUSH_SET_CAP; f_set_num++) {
            move_bucket(cursor_set_id_);
            cursor_set_id_ = (cursor_set_id_ + 1) % SET_NUM;
        }

        for (uint64_t i = 0; i < FLUSH_REUSED_UNIT; i++) {
            uint64_t f_seg_id = log_.f_seg_id();
            MockKSegment *kseg = log_.get_seg(f_seg_id);
            for (uint64_t j = 0; j < MockKLog::KSEGMENT_CAP; j++) {
                uint64_t key = kseg->get_key(j);
                if (log_.check_obj_newest(key, f_seg_id, j)) {
                    // readmit
                    log_.insert(key);
                    perf_.readmit_cnt += 1;
                }
            }
            log_.inc_f_seg_id();
        }
    }

    void move_bucket(uint64_t set_id) {
        std::vector<uint64_t> log_part =
            log_.take_all_part(set_id);

        MockPage hot_bkt;
        MockPage warm_bkt;
        uint64_t warm_bkt_dup = set_.get_bkt_dup(set_id);

        set_.read_bkt(&hot_bkt, set_id, 0);
        set_.read_bkt(&warm_bkt, set_id, warm_bkt_dup);

        if (log_part.size() > 0) {
            perf_.flush_log_cnt += 1;
            perf_.collect_cnt += log_part.size();
        }

        // if (hot_bkt.obj_num + log_part.size() <= KV_PER_PAGE) {
        //     hot_bkt.obj_num += log_part.size();
        //     set_.insert_hot_bkt(&hot_bkt, set_id);
        //     perf_.set_page_w += 1;
        // } else {
            hot_bkt.obj_num = KV_PER_PAGE;
            set_.insert_hot_bkt(&hot_bkt, set_id);
            set_.insert_warm_bkt(&warm_bkt, set_id, warm_bkt_dup);
            perf_.set_page_w += 2;
        // }
    }

    uint64_t cursor_set_id_{0};

    MockKLog log_;
    MockCursorSet set_;
    PerfCounter perf_;
};

int main() {
    MockCursorZ *z = new MockCursorZ();
    MockCursorZ::PerfCounter *perf = z->get_perf();

    for (uint64_t i = 0; true; i++) {
        if (i % (1024 * 1024) == 0 && i > 0) {
            printf("Insert 0x%lx Set Page W 0x%lx(%lf) Flush Ratio %lf\n", i, perf->set_page_w,
                   i == 0 ? 0 : perf->set_page_w * 40 / (double)i, perf->readmit_cnt * 1.0 / i);
            fflush(stdout);
            // sleep(1);
        }

        uint64_t key;
        key = rand64();

        z->insert(key);
    }

    delete z;
}