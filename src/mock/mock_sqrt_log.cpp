#ifdef NDEBUG
#undef NDEBUG
#endif

#include <algorithm>
#include <cstring>
#include <map>
#include <queue>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>

#include "common/Rand.h"

// #define KEY_SIZE 8
// #define VALUE_SIZE 100
#define KV_SIZE 251
#define PAGE_SIZE 4096
#define ZONE_SIZE (256 * 1024 * 1024)
#define LOG_ZONE_NUM 12
#define SET_ZONE_NUM 31
#define LOG_OP 0.05
#define SET_OP 0.05

const uint64_t KV_PER_PAGE = PAGE_SIZE / KV_SIZE;
const uint64_t PAGE_PER_ZONE = ZONE_SIZE / PAGE_SIZE;

const uint64_t LOG_PART_NUM = sqrt(LOG_ZONE_NUM * PAGE_PER_ZONE * 1.1 * (1 - LOG_OP));

const uint64_t LOG_SET_DIFF = (SET_ZONE_NUM * PAGE_PER_ZONE * (1 - SET_OP) / LOG_PART_NUM);
const uint64_t SET_NUM = LOG_PART_NUM * LOG_SET_DIFF;

struct MockPage {
    uint64_t key[KV_PER_PAGE] = {0};
    uint8_t rrip[KV_PER_PAGE] = {0};
    uint64_t set_id{UINT64_MAX};
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

// FIFO PAGE 用于捕捉局部性，暂时用于测试
class MockMemCache {
    struct DLinkedNode {
        uint64_t key;
        DLinkedNode *prev;
        DLinkedNode *next;
        DLinkedNode() : key(0), prev(nullptr), next(nullptr) {}
        DLinkedNode(uint64_t _key)
            : key(_key), prev(nullptr), next(nullptr) {}
    };
public:
    MockMemCache() = delete;
    MockMemCache(uint64_t capacity) : head(nullptr), tail(nullptr), size(0), capacity(capacity) {
        // 使用伪头部和伪尾部节点
        head = new DLinkedNode();
        tail = new DLinkedNode();
        head->next = tail;
        tail->prev = head;
    }
    bool get(uint64_t key) {
        if (!cache.count(key)) {
            return false;
        }
        // 如果 key 存在，先通过哈希表定位，再移到头部
        DLinkedNode *node = cache[key];
        moveToHead(node);
        return true;
    }

    void put(uint64_t key) {
        if (!cache.count(key)) {
            // 如果 key 不存在，创建一个新的节点
            DLinkedNode *node = new DLinkedNode(key);
            // 添加进哈希表
            cache[key] = node;
            // 添加至双向链表的头部
            addToHead(node);
            ++size;
            if (size > capacity) {
                // 如果超出容量，删除双向链表的尾部节点
                DLinkedNode *removed = removeTail();
                // 删除哈希表中对应的项
                cache.erase(removed->key);
                // 防止内存泄漏
                delete removed;
                --size;
            }
        } else {
            // 如果 key 存在，先通过哈希表定位，再修改 value，并移到头部
            DLinkedNode *node = cache[key];
            moveToHead(node);
        }
    }

    void clear() {
        while (true) {
            DLinkedNode *removed = removeTail();
            if (removed == nullptr) {
                break;
            }
            cache.erase(removed->key);
            delete removed;
            --size;
        }
    }
private:
    void addToHead(DLinkedNode *node) {
        node->prev = head;
        node->next = head->next;
        head->next->prev = node;
        head->next = node;
    }

    void removeNode(DLinkedNode *node) {
        node->prev->next = node->next;
        node->next->prev = node->prev;
    }

    void moveToHead(DLinkedNode *node) {
        removeNode(node);
        addToHead(node);
    }

    DLinkedNode *removeTail() {
        DLinkedNode *node = tail->prev;
        if (node == head) {
            return nullptr;
        }
        removeNode(node);
        return node;
    }
    std::unordered_map<uint64_t, DLinkedNode *> cache;
    DLinkedNode *head{nullptr};
    DLinkedNode *tail{nullptr};
    uint64_t size{0};
    uint64_t capacity{0};
};

struct MockLogPart {
    std::queue<uint64_t> page_inds;
    MockPage buffer;
};

// 局部性吸收理论上是有效果的
// 只要吸收是满 op 才开始的，那么吸收就是正确的
// Set 区 GC 是否可以考虑局部性选择
// Challenge： Set 区 GC 如何优雅地处理 ？
// Challenge： SSTABLE LOG 区怎么处理
class MockSqrtLog {
public:
    MockSqrtLog() {
        for (uint32_t i = 0; i < LOG_PART_NUM; i++) {
            log_parts_.push_back(MockLogPart{});
        }
    }

    void insert(uint64_t key) {
        uint64_t log_part_id = key % LOG_PART_NUM;
        MockLogPart *part = &log_parts_[log_part_id];
        part->buffer.key[part->buffer.v_num] = key;
        part->buffer.v_num += 1;

        if (part->buffer.v_num < KV_PER_PAGE) {
            return;
        }

        for (uint32_t i = 0; i < part->buffer.v_num; i++) {
            index_.insert(part->buffer.key[i], w_page_id_);
        }

        std::memcpy(
            &zones_[w_page_id_ / PAGE_PER_ZONE].pages[w_page_id_ % PAGE_PER_ZONE],
            &part->buffer,
            sizeof(MockPage)
        );

        part->buffer.v_num = 0;
        part->page_inds.push(w_page_id_);
        w_page_id_ = (w_page_id_ + 1) % LOG_PAGE_CAP;
    }

    bool check_flush() {
        uint64_t w_zone_id = w_page_id_ / PAGE_PER_ZONE;
        uint64_t free_zones;
        if (flush_zone_id_ <= w_zone_id) {
            free_zones = flush_zone_id_ + LOG_ZONE_NUM - w_zone_id;
        } else {
            free_zones = flush_zone_id_ - w_zone_id;
        }
        return free_zones <= 1;
    }

    bool check_obj_newest(uint64_t key, uint64_t page_id) {
        return index_.check_obj_newest(key, page_id);
    }

    std::queue<uint64_t> take_all_log_part(uint64_t key) {
        std::queue<uint64_t> ret;
        ret.swap(log_parts_[key % LOG_PART_NUM].page_inds);
        return ret;
    }

    std::queue<uint64_t> take_log_parts_in_zone(uint64_t key, uint64_t zone_id) {
        std::queue<uint64_t> ret;
        uint64_t log_part_id = key % LOG_PART_NUM;
        // assert(log_parts_[log_part_id].page_inds.size() > 0);
        // assert(log_parts_[log_part_id].page_inds.front() / PAGE_PER_ZONE == zone_id);

        while (!log_parts_[log_part_id].page_inds.empty()) {
            if (log_parts_[log_part_id].page_inds.front() / PAGE_PER_ZONE != zone_id) {
                break;
            }
            ret.push(log_parts_[log_part_id].page_inds.front());
            log_parts_[log_part_id].page_inds.pop();
        }
        return ret;
    }

    std::unordered_map<uint64_t, uint64_t> take_all_set_part(uint64_t key) {
        return index_.take_all_part(key);
    }

    uint64_t flush_zone_id() { return flush_zone_id_; }

    MockZone *get_zone(uint64_t zone_id) { return &zones_[zone_id]; }

    MockPage *get_page(uint64_t page_id) {
        return &zones_[page_id / PAGE_PER_ZONE].pages[page_id % PAGE_PER_ZONE];
    }

    void inc_flush_zone_id() {
        flush_zone_id_ = (flush_zone_id_ + 1) % LOG_ZONE_NUM;
    }

private:
    const uint64_t LOG_PAGE_CAP = LOG_ZONE_NUM * PAGE_PER_ZONE;
    std::vector<MockLogPart> log_parts_;
    MockIndex index_;
    MockZone zones_[LOG_ZONE_NUM];
    uint64_t w_page_id_;
    uint64_t flush_zone_id_;
};

class MockAdSet {
public:
    MockAdSet() = delete;
    MockAdSet(const char *name) : name_(name) {
        for (uint32_t i = 0; i < SET_ZONE_NUM; i++) {
            free_zones_.push(i);
        }

        write_ptr_.zone_id = free_zones_.front();
        free_zones_.pop();
        write_ptr_.page_idx = 0;
    }
    void remove_set(int64_t set_id) {
        auto iter = set_ind_.find(set_id);
        if (iter != set_ind_.end()) {
            zone_infos_[iter->second / PAGE_PER_ZONE].valid_num -= 1;
            set_ind_.erase(iter);
        }
    }
    void insert_set(MockPage *set, uint64_t set_id) {
        set->set_id = set_id;
        zones_[write_ptr_.zone_id]
            .pages[write_ptr_.page_idx]
            .set_id = set_id;
        std::memcpy(&zones_[write_ptr_.zone_id]
                         .pages[write_ptr_.page_idx],
                    set, sizeof(MockPage));
        
        if (set_ind_.find(set_id) != set_ind_.end()) {
            remove_set(set_id);
        }

        zone_infos_[write_ptr_.zone_id].valid_num += 1;
        set_ind_.insert({set_id, write_ptr_.zone_id * PAGE_PER_ZONE + write_ptr_.page_idx});
        
        write_ptr_.page_idx += 1;
        if (write_ptr_.page_idx < PAGE_PER_ZONE) {
            return;
        }

        printf("Write %s Set write zone %lu\n", name_,
                write_ptr_.zone_id);

        zone_infos_[write_ptr_.zone_id].is_full = true;

        if (check_must_gc()) {
            do_must_gc();
        }

        assert(free_zones_.size() > 0);
        write_ptr_.zone_id = free_zones_.front();
        free_zones_.pop();
        write_ptr_.page_idx = 0;
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
        if (SET_ZONE_NUM * SET_OP < 2) {
            return false;
        }
        return free_zones_.size() < SET_ZONE_NUM * SET_OP * 0.5;
    }

    MockZone *get_zone(uint64_t zone_id) { return &zones_[zone_id]; }

    uint64_t valid_num(uint64_t zone_id) { return zone_infos_[zone_id].valid_num; }

    uint64_t prepare_gc_zone() {
        uint64_t gc_zone_id = UINT64_MAX;
        for (uint32_t i = 0; i < SET_ZONE_NUM; i++) {
            if (!zone_infos_[i].is_full) {
                continue;
            }
            if (gc_zone_id == UINT64_MAX) {
                gc_zone_id = i;
            } else if (zone_infos_[i].valid_num < zone_infos_[gc_zone_id].valid_num) {
                gc_zone_id = i;
            }
        }
        assert(gc_zone_id != UINT64_MAX);
        return gc_zone_id;
    }

    void complete_gc_zone(uint64_t gc_zone_id) {
        assert(zone_infos_[gc_zone_id].valid_num == 0);
        printf("Complete %s GC zone %lu\n", name_, gc_zone_id);
        zone_infos_[gc_zone_id].is_full = false;
        zone_infos_[gc_zone_id].valid_num = 0;
        free_zones_.push(gc_zone_id);
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

    bool check_set_exist(uint64_t set_id) {
        return set_ind_.find(set_id) != set_ind_.end();
    }

    static const uint64_t SET_PAGE_CAP = SET_ZONE_NUM * PAGE_PER_ZONE;

private:
    bool check_must_gc() {
        return free_zones_.size() == 0;
    }

    void do_must_gc() {
        uint64_t gc_zone_id = prepare_gc_zone();
        complete_gc_zone(gc_zone_id);
    }

    struct WritePointer {
        uint64_t zone_id{0};
        uint64_t page_idx{0};
    };
    struct ZoneInfo {
        uint64_t valid_num{0};
        bool is_full{false};
    };
    const char *name_{nullptr};
    MockZone zones_[SET_ZONE_NUM];
    std::unordered_map<uint64_t, uint64_t> set_ind_;
    std::queue<uint64_t> free_zones_;
    WritePointer write_ptr_;
    ZoneInfo zone_infos_[SET_ZONE_NUM];
};

class MockK {
public:
    MockK() : cache_(LOG_PART_NUM), set_("Cold") {}
    struct PerfCounter {
        uint64_t set_page_w = 0;
        uint64_t log_page_r = 0;
        uint64_t flush_log_cnt = 0;
        uint64_t collect_cnt = 0;
        uint64_t readmit_cnt = 0;
        std::unordered_map<uint64_t, uint64_t> map;
    };
    void insert(uint64_t key) {
        log_.insert(key);

        if (log_.check_flush()) {
            do_flush_log();
        }
    }

    PerfCounter *get_perf() { return &perf_; }

private:
    const uint64_t FLUSH_PART_CAP = LOG_PART_NUM * 1.5 / LOG_ZONE_NUM;
    void do_move_log_part(uint64_t key, bool trig_gc) {
        for (uint64_t i = 0; i < LOG_SET_DIFF; i++) {
            uint64_t set_id = i * LOG_PART_NUM + key % LOG_PART_NUM;
            if (set_.check_set_exist(set_id)) {
                move_bucket(set_id);
            }
        }
        std::queue<uint64_t> pages = log_.take_all_log_part(key);
        while (!pages.empty()) {
            uint32_t page_id = pages.front(); pages.pop();
            MockPage *page = log_.get_page(page_id);
            for (uint32_t j = 0; j < page->v_num; j++) {
                if (log_.check_obj_newest(page->key[j], page_id)) {
                    move_bucket(page->key[j] % SET_NUM);
                }

                while (set_.check_gc() && trig_gc) {
                    do_gc_locality();
                }
            }
        }
    }

    void do_flush_log() {
        // 按照 Zone 进行 Flush，如何保证一致性
        // 一次性下刷整个 Log Part 对应的所有 Page 吗？
        // 提前聚合所有 Log 区数据会造成 Set 区 GC 压力很高
        // 如果能按照 LogPart 进行 GC，则可以相对聚合最多数据 ！
        // 如果按照实际 Zone 选数据，会造成读放大仍然高！
        uint64_t flush_part = 0;
        uint64_t flush_zone_id = log_.flush_zone_id();
        MockZone *zone = log_.get_zone(flush_zone_id);
        for (uint32_t i = 0; i < PAGE_PER_ZONE; i++) {
            MockPage *page = &zone->pages[i];
            uint64_t page_id = flush_zone_id * PAGE_PER_ZONE + i;
            for (uint32_t j = 0; j < page->v_num; j++) {
                if (!log_.check_obj_newest(page->key[j], page_id)) {
                    continue;
                }
                if (flush_part <= FLUSH_PART_CAP) {
                    do_move_log_part(page->key[j], true);
                    flush_part += 1;
                } else {
                    // readmit
                    log_.take_log_parts_in_zone(page->key[j], flush_zone_id);
                    log_.insert(page->key[j]);
                    perf_.readmit_cnt += 1;
                }
            }
        }

        log_.inc_flush_zone_id();
    }

    void do_move_log_part_reserved(uint64_t key, uint64_t zone_id) {
        std::queue<uint64_t> pages = log_.take_log_parts_in_zone(key, zone_id);
        while (!pages.empty()) {
            uint32_t page_id = pages.front(); pages.pop();
            MockPage *page = log_.get_page(page_id);
            for (uint32_t j = 0; j < page->v_num; j++) {
                if (log_.check_obj_newest(page->key[j], page_id)) {
                    move_bucket(page->key[j] % SET_NUM);
                }

                while (set_.check_gc()) {
                    do_gc();
                }
            }
        }
    }

    void do_flush_log_reserved() {
        uint64_t flush_zone_id = log_.flush_zone_id();
        MockZone *zone = log_.get_zone(flush_zone_id);
        for (uint32_t i = 0; i < PAGE_PER_ZONE; i++) {
            MockPage *page = &zone->pages[i];
            uint64_t page_id = flush_zone_id * PAGE_PER_ZONE + i;
            for (uint32_t j = 0; j < page->v_num; j++) {
                if (!log_.check_obj_newest(page->key[j], page_id)) {
                    continue;
                }
                do_move_log_part_reserved(page->key[j], flush_zone_id);
            }
        }

        log_.inc_flush_zone_id();
    }

    // 面向 GC 的迁移
    void do_gc_locality() {
        std::unordered_set<uint64_t> gc_log_parts;
        uint64_t gc_zone_id = set_.prepare_gc_zone();
        MockZone *zone = set_.get_zone(gc_zone_id);

        uint64_t gc_pages = set_.valid_num(gc_zone_id);

        for (uint64_t i = 0; i < PAGE_PER_ZONE; i++) {
            uint64_t set_id = zone->pages[i].set_id;
            if (set_.check_set_newest(set_id, gc_zone_id * PAGE_PER_ZONE + i)) {
                // assert(gc_log_parts.find(set_id % LOG_PART_NUM) == gc_log_parts.end());
                gc_log_parts.insert(set_id % LOG_PART_NUM);
                do_move_log_part(set_id, false);
            }
        }
        printf("Cold GC %ld pages with log part %ld\n", gc_pages, gc_log_parts.size());
        set_.complete_gc_zone(gc_zone_id);
    }

    // 考虑保留聚合性， 如何实现
    void do_gc() {
        // 为什么局部性会爆炸？？
        uint64_t gc_pages = 0;
        std::unordered_set<uint64_t> gc_log_parts;
        uint64_t gc_zone_id = set_.prepare_gc_zone();
        MockZone *zone = set_.get_zone(gc_zone_id);
        for (uint64_t i = 0; i < PAGE_PER_ZONE; i++) {
            uint64_t set_id = zone->pages[i].set_id;
            if (set_.check_set_newest(set_id, gc_zone_id * PAGE_PER_ZONE + i)) {
                if (gc_log_parts.find(set_id % LOG_PART_NUM) == gc_log_parts.end()) {
                    gc_log_parts.insert(set_id % LOG_PART_NUM);
                }
                gc_pages += 1;
                move_bucket(set_id);
            }
        }
        printf("Cold GC %ld pages with log part %ld\n", gc_pages, gc_log_parts.size());
        set_.complete_gc_zone(gc_zone_id);
    }

    void move_bucket(uint64_t set_id) {
        // printf("Move bucket 0x%lx\n", set_id);
        std::unordered_map<uint64_t, uint64_t> log_part = log_.take_all_set_part(set_id);

        for (auto iter = log_part.begin(); iter != log_part.end(); iter++) {
            if (!cache_.get(iter->second)) {
                perf_.log_page_r += 1;
                cache_.put(iter->second);
            }
        }

        if (log_part.size() > 0) {
            perf_.flush_log_cnt += 1;
            perf_.collect_cnt += log_part.size();
        }
        // calculate page
        MockPage page;
        set_.get_set(&page, set_id);

        for (uint32_t i = 0; i < log_part.size() && page.v_num < KV_PER_PAGE;
             i++) {
            page.key[page.v_num] = log_part[i];
            page.v_num += 1;
        }

        perf_.set_page_w += 1;
        set_.insert_set(&page, set_id);

        perf_.map[set_id] += 1;
    }

    MockMemCache cache_;
    MockSqrtLog log_;
    MockAdSet set_;
    PerfCounter perf_;
};

class MockCursorZ {
public:
    MockCursorZ() : cache_(LOG_PART_NUM), set_("Cold") {}
    struct PerfCounter {
        uint64_t set_page_w = 0;
        uint64_t log_page_r = 0;
        uint64_t flush_log_cnt = 0;
        uint64_t collect_cnt = 0;
        uint64_t readmit_cnt = 0;
        std::unordered_map<uint64_t, uint64_t> map;
    };
    void insert(uint64_t key) {
        log_.insert(key);

        if (log_.check_flush()) {
            do_cursor_flush();
        }
    }

    PerfCounter *get_perf() { return &perf_; }
private:

    void do_move_log_part(uint64_t key, bool trig_gc) {
        for (uint64_t i = 0; i < LOG_SET_DIFF; i++) {
            uint64_t set_id = i * LOG_PART_NUM + key % LOG_PART_NUM;
            if (set_.check_set_exist(set_id)) {
                move_bucket(set_id);
            }
        }
        std::queue<uint64_t> pages = log_.take_all_log_part(key);
        while (!pages.empty()) {
            uint32_t page_id = pages.front(); pages.pop();
            MockPage *page = log_.get_page(page_id);
            for (uint32_t j = 0; j < page->v_num; j++) {
                if (log_.check_obj_newest(page->key[j], page_id)) {
                    move_bucket(page->key[j] % SET_NUM);
                }

                while (set_.check_gc() && trig_gc) {
                    do_cursor_gc();
                }
            }
        }
    }

    void move_bucket(uint64_t set_id) {
        // printf("Move bucket 0x%lx\n", set_id);
        std::unordered_map<uint64_t, uint64_t> log_part = log_.take_all_set_part(set_id);

        for (auto iter = log_part.begin(); iter != log_part.end(); iter++) {
            if (!cache_.get(iter->second)) {
                perf_.log_page_r += 1;
                cache_.put(iter->second);
            }
        }

        if (log_part.size() > 0) {
            perf_.flush_log_cnt += 1;
            perf_.collect_cnt += log_part.size();
        }
        // calculate page
        MockPage page;
        set_.get_set(&page, set_id);

        for (uint32_t i = 0; i < log_part.size() && page.v_num < KV_PER_PAGE;
             i++) {
            page.key[page.v_num] = log_part[i];
            page.v_num += 1;
        }

        perf_.set_page_w += 1;
        set_.insert_set(&page, set_id);

        perf_.map[set_id] += 1;
    }

    void do_cursor_gc() {
        uint64_t gc_zone_id = set_.prepare_gc_zone();
        assert(set_.valid_num(gc_zone_id) == 0);
        set_.complete_gc_zone(gc_zone_id);
    }

    void do_cursor_flush() {
        uint64_t flush_part = 0;
        for (flush_part = 0; flush_part < LOG_PART_NUM * 1.0 / LOG_ZONE_NUM; flush_part++) {
            do_move_log_part(cursor_part_id_, true);
            cache_.clear();
            cursor_part_id_ = (cursor_part_id_ + 1) % LOG_PART_NUM;
        }

        uint64_t flush_zone_id = log_.flush_zone_id();
        MockZone *zone = log_.get_zone(flush_zone_id);
        uint64_t readmit_pages = 0;

        for (uint32_t i = 0; i < PAGE_PER_ZONE; i++) {
            MockPage *page = &zone->pages[i];
            uint64_t page_id = flush_zone_id * PAGE_PER_ZONE + i;
            bool flag = false;
            for (uint32_t j = 0; j < page->v_num; j++) {
                if (!log_.check_obj_newest(page->key[j], page_id)) {
                    continue;
                }
                // readmit
                log_.take_log_parts_in_zone(page->key[j], flush_zone_id);
                log_.insert(page->key[j]);
                perf_.readmit_cnt += 1;
                if (flag == false) {
                    flag = true;
                    readmit_pages += 1;
                }
            }
        }
        printf("Flush Pages Readmit rate: %lf\n", readmit_pages * 1.0 / PAGE_PER_ZONE);
        printf("Flush Cache Miss Rate: %lf\n", perf_.log_page_r * 1.0 / perf_.collect_cnt);
        log_.inc_flush_zone_id();
    }

    uint64_t cursor_part_id_{0};

    MockMemCache cache_;
    MockSqrtLog log_;
    MockAdSet set_;
    PerfCounter perf_;
};

int main() {
    std::mt19937_64 rng(0);
    ZipfBucketRandom *zipf_b = new ZipfBucketRandom(1024, 0.7, SET_NUM);
    MockCursorZ *k = new MockCursorZ();
    MockCursorZ::PerfCounter *perf = k->get_perf();

    for (uint64_t i = 0; true; i++) {
        if (i % (1024 * 1024) == 0  && i > 0) {
            printf("Insert 0x%lx Set Page W 0x%lx(%lf) Log Page R 0x%lx(%lf) Collect Cnt %lf Readmit Rate %lf\n", i, 
                perf->set_page_w, i == 0 ? 0 : perf->set_page_w * 40 / (double)i,
                perf->log_page_r, i == 0 ? 0 : perf->log_page_r * 40 / (double)i,
                perf->collect_cnt * 1.0 / (perf->flush_log_cnt == 0 ? 1 : perf->flush_log_cnt),
                perf->readmit_cnt * 1.0 / i
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
        key = rand64();

        k->insert(key);
    }

    delete zipf_b;
    delete k;
}