#ifdef NDEBUG
#undef NDEBUG
#endif

#include <cassert>
#include <cstring>
#include <map>
#include <queue>
#include <unistd.h>
#include <unordered_map>

#include "common/Rand.h"

#define KEY_SIZE 8
#define VALUE_SIZE 100
#define INDEX_SIZE 4
#define KEY_INDEX_SIZE (KEY_SIZE + INDEX_SIZE)
#define PAGE_SIZE 4096
#define KSEGMENT_SIZE (32 * 4096)
#define ZONE_SIZE (256 * 1024 * 1024)
// #define LOG_ZONE_NUM 12
// #define SET_ZONE_NUM 122

#define KSET_ZONE_NUM 14
#define VLOG_ZONE_NUM 120

#define LOG_OP 0.15
#define SET_OP 0.05

const uint64_t KEY_INDEX_PER_PAGE = PAGE_SIZE / KEY_INDEX_SIZE;
const uint64_t VALUE_PER_PAGE = PAGE_SIZE / VALUE_SIZE;
const uint64_t PAGE_PER_ZONE = ZONE_SIZE / PAGE_SIZE;
const uint64_t SET_NUM = KSET_ZONE_NUM * PAGE_PER_ZONE * (1 - LOG_OP);

struct MockPerfCounter {
    uint64_t set_page_w = 0;
    uint64_t flush_log_cnt = 0;
    uint64_t collect_cnt = 0;
};

struct MockKeyPage {
    uint64_t key[KEY_INDEX_PER_PAGE] = {0};
    uint64_t index[KEY_INDEX_PER_PAGE] = {0};
    uint64_t v_num{0};
    uint64_t set_id{UINT64_MAX};
};

struct MockValuePage {
    uint64_t value[VALUE_PER_PAGE] = {0};
};

struct MockKeyZone {
    MockKeyZone() {}
    MockKeyPage k_pages[PAGE_PER_ZONE];
};

struct MockValueZone {
    MockValuePage v_pages[PAGE_PER_ZONE];
};

class MockIndex {
public:
    struct ObjMeta {
        uint64_t page_id;
        uint64_t next;
    };

    struct SetMeta {
        uint64_t first;
        uint64_t ver;
    };

    // insert == 1
    bool insert(uint64_t key, uint64_t page_id) {
        // assert(obj_map_.size() <= FLUSH_CAP);
        assert(set_map_.size() <= SET_NUM);
        auto obj_iter = obj_map_.find(key);
        if (obj_iter != obj_map_.end()) {
            obj_iter->second.page_id = page_id;
            return false;
        }
        ObjMeta meta = {page_id, UINT64_MAX};
        auto set_iter = set_map_.find(key % SET_NUM);
        if (set_iter != set_map_.end()) {
            meta.next = set_iter->second.first;
            set_iter->second.first = key;
        } else {
            SetMeta set_meta = {key, ver_++};
            set_map_.insert({key % SET_NUM, set_meta});
            set_queue_.push(std::make_pair(key % SET_NUM, set_meta.ver));
        }
        obj_map_.insert({key, meta});
        return true;
    }

    bool check_obj_newest(uint64_t key, uint64_t page_id) {
        // assert(obj_map_.size() <= LOG_ZONE_NUM * PAGE_PER_ZONE *
        // KV_PER_PAGE); assert(set_map_.size() <= SET_NUM);
        auto obj_iter = obj_map_.find(key);
        if (obj_iter == obj_map_.end()) {
            return false;
        }
        return (obj_iter->second.page_id == page_id);
    }

    std::unordered_map<uint64_t, uint64_t> take_all_part(uint64_t key) {
        // assert(obj_map_.size() <= LOG_ZONE_NUM * PAGE_PER_ZONE *
        // KV_PER_PAGE); assert(set_map_.size() <= SET_NUM);
        std::unordered_map<uint64_t, uint64_t> ret;
        auto set_iter = set_map_.find(key % SET_NUM);
        if (set_iter == set_map_.end()) {
            return ret;
        }
        uint64_t obj = set_iter->second.first;
        assert(obj != UINT64_MAX);
        while (obj != UINT64_MAX) {
            ret.insert(std::make_pair(obj, obj_map_[obj].page_id));
            uint64_t prev = obj;
            obj = obj_map_[prev].next;
            obj_map_.erase(prev);
        }
        set_map_.erase(key % SET_NUM);
        // printf("take_all_part ok %ld\n", ret.size());

        return ret;
    }

    uint64_t get_flush_set() {
        while (true) {
            assert(set_queue_.size() >= set_map_.size());
            std::pair<uint64_t, uint64_t> pair = set_queue_.front();
            set_queue_.pop();
            auto iter = set_map_.find(pair.first);
            if (iter == set_map_.end()) {
                continue;
            }
            if (iter->second.ver != pair.second) {
                continue;
            }
            // printf("ok %ld\n", set_queue_.size());
            return pair.first;
        }
    }

private:
    uint64_t ver_;
    std::queue<std::pair<uint64_t, uint64_t>> set_queue_;
    std::unordered_map<uint64_t, SetMeta> set_map_;
    std::unordered_map<uint64_t, ObjMeta> obj_map_;
};

class MockKeySet {
public:
    void set_perf(MockPerfCounter *perf) { perf_ = perf; }
    bool check_key_newest(uint64_t key, uint64_t page_id) {
        if (index_.check_obj_newest(key, page_id)) {
            return true;
        }
        auto iter = set_ind_.find(key % SET_NUM);
        if (iter == set_ind_.end()) {
            return false;
        }
        uint64_t set_page_id = iter->second;
        assert(set_page_id < PAGE_PER_ZONE * KSET_ZONE_NUM);
        MockKeyPage *page = &zones_[set_page_id / PAGE_PER_ZONE]
                                 .k_pages[set_page_id % PAGE_PER_ZONE];
        for (uint64_t i = 0; i < std::min(page->v_num, KEY_INDEX_PER_PAGE);
             i++) {
            if (page->key[i] != key) {
                continue;
            }
            if (page->index[i] == page_id) {
                return true;
            } else {
                return false;
            }
        }
        return false;
    }

    void insert(uint64_t key, uint64_t page_id) {
        if (index_.insert(key, page_id)) {
            index_cnt_ += 1;
        }

        while (check_flush_idx()) {
            move_bucket(index_.get_flush_set(), true);
        }
    }

    bool check_gc() {
        uint64_t w_zone_id = w_page_id_ / PAGE_PER_ZONE;
        uint64_t free_zones;
        if (gc_zone_id_ <= w_zone_id) {
            free_zones = gc_zone_id_ + KSET_ZONE_NUM - w_zone_id;
        } else {
            free_zones = gc_zone_id_ - w_zone_id;
        }
        return free_zones <= KSET_ZONE_NUM * LOG_OP;
    }

    void do_gc() {
        uint64_t gc_pages = 0;
        MockKeyZone *zone = &zones_[gc_zone_id_];
        for (uint64_t i = 0; i < PAGE_PER_ZONE; i++) {
            uint64_t set_id = zone->k_pages[i].set_id;
            if (check_set_newest(set_id, gc_zone_id_ * PAGE_PER_ZONE + i)) {
                gc_pages += 1;
                move_bucket(set_id, false);
            }
        }
        printf("KSet GC zone %ld pages: %ld\n", gc_zone_id_, gc_pages);
        gc_zone_id_ = (gc_zone_id_ + 1) % KSET_ZONE_NUM;
    }

private:
    const uint64_t FLUSH_CAP = KSET_ZONE_NUM * PAGE_PER_ZONE *
                               KEY_INDEX_PER_PAGE * 0.052 * (1 - LOG_OP);
    const uint64_t KSET_PAGE_CAP = KSET_ZONE_NUM * PAGE_PER_ZONE;
    bool check_flush_idx() {
        if (index_cnt_ >= FLUSH_CAP) {
            return true;
        }
        return false;
    }

    void move_bucket(uint64_t set_id, bool flush_mem) {
        std::unordered_map<uint64_t, uint64_t> log_part =
            index_.take_all_part(set_id);
        index_cnt_ -= log_part.size();

        if (flush_mem) {
            assert(log_part.size() > 0);
        }

        // printf("move bucket %ld\n", set_id);
        MockKeyPage page;
        get_set(&page, set_id);

        for (uint64_t i = 0; i < std::min(KEY_INDEX_PER_PAGE, page.v_num);
             i++) {
            if (log_part.count(page.key[i]) > 0) {
                page.index[i] = log_part[page.key[i]];
                log_part.erase(page.key[i]);
            }
        }

        // 暂时 FIFO
        for (auto iter = log_part.begin(); iter != log_part.end(); iter++) {
            page.key[page.v_num % KEY_INDEX_PER_PAGE] = iter->first;
            page.index[page.v_num % KEY_INDEX_PER_PAGE] = iter->second;
            page.v_num += 1;
        }

        perf_->set_page_w += 1;
        insert_set(&page, set_id);
    }

    void insert_set(MockKeyPage *set, uint64_t set_id) {
        set->set_id = set_id;
        std::memcpy(&zones_[w_page_id_ / PAGE_PER_ZONE]
                         .k_pages[w_page_id_ % PAGE_PER_ZONE],
                    set, sizeof(MockKeyPage));
        auto iter = set_ind_.find(set_id);
        if (iter != set_ind_.end()) {
            iter->second = w_page_id_;
        } else {
            set_ind_.insert({set_id, w_page_id_});
        }
        if (w_page_id_ % PAGE_PER_ZONE == PAGE_PER_ZONE - 1) {
            printf("KSet write zone %ld\n", w_page_id_ / PAGE_PER_ZONE);
        }
        w_page_id_ = (w_page_id_ + 1) % KSET_PAGE_CAP;
    }

    void get_set(MockKeyPage *set, uint64_t set_id) {
        auto iter = set_ind_.find(set_id);
        if (iter == set_ind_.end()) {
            return;
        }
        uint64_t page_id = iter->second;
        std::memcpy(
            set,
            &zones_[page_id / PAGE_PER_ZONE].k_pages[page_id % PAGE_PER_ZONE],
            sizeof(MockKeyPage));
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

    MockIndex index_;
    uint64_t index_cnt_{0};
    std::unordered_map<uint64_t, uint64_t> set_ind_;
    MockKeyZone zones_[KSET_ZONE_NUM];
    uint64_t w_page_id_{0};
    uint64_t gc_zone_id_{0};

    MockPerfCounter *perf_{nullptr};
};

class MockValueLog {
public:
    void set_perf(MockPerfCounter *perf) { perf_ = perf; }
    uint64_t insert(uint64_t key) {
        MockValuePage *page = &zones_[w_page_id_ / PAGE_PER_ZONE]
                                   .v_pages[w_page_id_ % PAGE_PER_ZONE];
        page->value[wp_++] = key;
        uint64_t page_id = w_page_id_;
        if (wp_ >= VALUE_PER_PAGE) {
            perf_->set_page_w += 1;
            if (w_page_id_ % PAGE_PER_ZONE == PAGE_PER_ZONE - 1) {
                printf("VLog write zone %ld\n", w_page_id_ / PAGE_PER_ZONE);
            }
            w_page_id_ = (w_page_id_ + 1) % VLOG_PAGE_CAP;
            wp_ = 0;
        }
        return page_id;
    }

    bool check_gc() {
        uint64_t w_zone_id = w_page_id_ / PAGE_PER_ZONE;
        uint64_t free_zones;
        if (gc_zone_id_ <= w_zone_id) {
            free_zones = gc_zone_id_ + VLOG_ZONE_NUM - w_zone_id;
        } else {
            free_zones = gc_zone_id_ - w_zone_id;
        }
        return free_zones <= VLOG_ZONE_NUM * SET_OP;
    }

    void do_gc(MockKeySet *kset) {
        MockValueZone *zone = &zones_[gc_zone_id_];
        uint64_t gc_values = 0;

        for (uint64_t i = 0; i < PAGE_PER_ZONE; i++) {
            MockValuePage *page = &zone->v_pages[i];
            for (uint64_t j = 0; j < VALUE_PER_PAGE; j++) {
                if (kset->check_key_newest(page->value[j],
                                           gc_zone_id_ * PAGE_PER_ZONE + i)) {
                    gc_values += 1;
                    uint64_t new_page_id = insert(page->value[j]);
                    kset->insert(page->value[j], new_page_id);

                    while (kset->check_gc()) {
                        kset->do_gc();
                    }
                }
            }
        }
        printf("VLog GC: %ld, values: %ld\n", gc_zone_id_, gc_values);
        gc_zone_id_ = (gc_zone_id_ + 1) % VLOG_ZONE_NUM;
    }

private:
    const uint64_t VLOG_PAGE_CAP = VLOG_ZONE_NUM * PAGE_PER_ZONE;

    MockValueZone zones_[VLOG_ZONE_NUM];
    uint64_t wp_{0};
    uint64_t w_page_id_{0};
    uint64_t gc_zone_id_{0};

    MockPerfCounter *perf_{nullptr};
};

class MockK {
public:
    MockK() {
        kset_.set_perf(&perf_);
        vlog_.set_perf(&perf_);
    }
    void insert(uint64_t key) {
        uint64_t page_id = vlog_.insert(key);
        kset_.insert(key, page_id);
        while (kset_.check_gc()) {
            kset_.do_gc();
        }
        while (vlog_.check_gc()) {
            vlog_.do_gc(&kset_);
        }
    }

    MockPerfCounter *get_perf() { return &perf_; }

private:
    MockValueLog vlog_;
    MockKeySet kset_;
    MockPerfCounter perf_;
};

int main() {
    MockK *k = new MockK();
    MockPerfCounter *perf = k->get_perf();

    for (uint64_t i = 0; true; i++) {
        if (i % (1024 * 1024) == 0) {
            printf("Insert 0x%lx Set Page W 0x%lx(%lf)\n", i, perf->set_page_w,
                   i == 0 ? 0 : perf->set_page_w * 40 / (double)i);
            fflush(stdout);
            // sleep(1);
        }

        uint64_t key;
        key = rand64();

        k->insert(key);
    }

    delete k;
}