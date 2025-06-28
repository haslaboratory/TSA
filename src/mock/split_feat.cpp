#include <unordered_map>
#include <unordered_set>
#include <queue>
#include <list>
#include <algorithm>

#include "common/Rand.h"
////////////////////////// From Leetcode ///////////////////////////
struct DLinkedNode {
    uint64_t key;
    DLinkedNode *prev;
    DLinkedNode *next;
    DLinkedNode() : key(0), prev(nullptr), next(nullptr) {}
    DLinkedNode(uint64_t _key)
        : key(_key), prev(nullptr), next(nullptr) {}
};

class LRUSet {
private:
    std::unordered_map<uint64_t, DLinkedNode *> cache;
    DLinkedNode *head{nullptr};
    DLinkedNode *tail{nullptr};
    uint64_t size;
    uint64_t capacity;

public:
    LRUSet() = delete;
    LRUSet(uint64_t _capacity) : size(0), capacity(_capacity) {
        // 使用伪头部和伪尾部节点
        head = new DLinkedNode();
        tail = new DLinkedNode();
        head->next = tail;
        tail->prev = head;
    }

    // ~LRUSet() {
    //     DLinkedNode *node = head;
    //     while (node != nullptr) {
    //         DLinkedNode *next = node->next;
    //         delete node;
    //         node = next;
    //     }
    // }

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
        removeNode(node);
        return node;
    }
};

class FlashFIFOSet {
private:
    std::unordered_map<uint64_t, DLinkedNode *> cache;
    std::unordered_set<uint64_t> tmp;
    DLinkedNode *head{nullptr};
    DLinkedNode *tail{nullptr};
    uint64_t size;
    uint64_t capacity;

public:
    FlashFIFOSet() = delete;
    FlashFIFOSet(int _capacity) : size(0), capacity(_capacity) {
        // 使用伪头部和伪尾部节点
        head = new DLinkedNode();
        tail = new DLinkedNode();
        head->next = tail;
        tail->prev = head;
    }
    // ~FlashFIFOSet() {
    //     DLinkedNode *node = head;
    //     while (node != nullptr) {
    //         DLinkedNode *next = node->next;
    //         delete node;
    //         node = next;
    //     }
    // }

    bool get(uint64_t key) {
        if (!cache.count(key)) {
            return false;
        }
        if (tmp.count(key) == 0) {
            tmp.insert(key);   
        }
        return true;
    }

    void put(uint64_t key) {
        // 直接冗余数据
        DLinkedNode *node = new DLinkedNode(key);
        cache[key] = node;
        addToTail(node);
        ++size;

        if (!cache.count(key)) {
            cache.insert({key, node});
        } else {
            cache[key] = node;
        }

        while (size > capacity) {
            // 如果超出容量，删除双向链表的尾部节点
            DLinkedNode *removed = removeHead();
            if (cache[removed->key] != removed) {
                // 防止内存泄漏
                delete removed;
                --size;
            } else if (tmp.count(removed->key) > 0) {
                addToTail(removed);
                tmp.erase(removed->key);
            } else {
                // 删除哈希表中对应的项
                cache.erase(removed->key);
                // 防止内存泄漏
                delete removed;
                --size;
            }
        }
    }

private:
    void removeNode(DLinkedNode *node) {
        node->prev->next = node->next;
        node->next->prev = node->prev;
    }

    void addToTail(DLinkedNode *node) {
        node->prev = tail->prev;
        node->next = tail;
        tail->prev->next = node;
        tail->prev = node;
    }

    DLinkedNode *removeHead() {
        DLinkedNode *node = head->next;
        removeNode(node);
        return node;
    }
};

// 存在热点漂移时，才会出现FIFO对LRU的命中率倒挂
class FlashRawFIFOSet {
private:
    std::unordered_map<uint64_t, DLinkedNode *> cache;
    DLinkedNode *head{nullptr};
    DLinkedNode *tail{nullptr};
    uint64_t size;
    uint64_t capacity;

public:
    FlashRawFIFOSet() = delete;
    FlashRawFIFOSet(int _capacity) : size(0), capacity(_capacity) {
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
        return true;
    }

    void put(uint64_t key) {
        // 直接冗余数据
        DLinkedNode *node = new DLinkedNode(key);
        cache[key] = node;
        addToTail(node);
        ++size;

        if (!cache.count(key)) {
            cache.insert({key, node});
        } else {
            cache[key] = node;
        }

        while (size > capacity) {
            // 如果超出容量，删除双向链表的尾部节点
            DLinkedNode *removed = removeHead();
            if (cache[removed->key] == removed) {
                // 删除哈希表中对应的项
                cache.erase(removed->key);
            }
            delete removed;
            --size;
        }
    }

private:
    void removeNode(DLinkedNode *node) {
        node->prev->next = node->next;
        node->next->prev = node->prev;
    }

    void addToTail(DLinkedNode *node) {
        node->prev = tail->prev;
        node->next = tail;
        tail->prev->next = node;
        tail->prev = node;
    }

    DLinkedNode *removeHead() {
        DLinkedNode *node = head->next;
        removeNode(node);
        return node;
    }
};

class MemFIFOSet {
private:
    std::unordered_map<uint64_t, DLinkedNode *> cache;
    DLinkedNode *head{nullptr};
    DLinkedNode *tail{nullptr};
    uint64_t size;
    uint64_t capacity;

public:
    MemFIFOSet() = delete;
    MemFIFOSet(int _capacity) : size(0), capacity(_capacity) {
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
        return true;
    }

    void put(uint64_t key) {
        if (!cache.count(key)) {
            DLinkedNode *node = new DLinkedNode(key);
            addToTail(node);
            ++size;
            cache.insert({key, node});
            cache[key] = node;
        }

        if (size > capacity) {
            // 如果超出容量，删除双向链表的尾部节点
            DLinkedNode *removed = removeHead();
            if (cache[removed->key] == removed) {
                // 删除哈希表中对应的项
                cache.erase(removed->key);
            }
            // 防止内存泄漏
            delete removed;
            --size;
        }
    }

    void remove(uint64_t key) {
        if (cache.count(key)) {
            DLinkedNode *removed = cache[key];
            removeNode(removed);
            cache.erase(key);
            delete removed;
            --size;
        }
    }

private:
    void removeNode(DLinkedNode *node) {
        node->prev->next = node->next;
        node->next->prev = node->prev;
    }

    void addToTail(DLinkedNode *node) {
        node->prev = tail->prev;
        node->next = tail;
        tail->prev->next = node;
        tail->prev = node;
    }

    DLinkedNode *removeHead() {
        DLinkedNode *node = head->next;
        removeNode(node);
        return node;
    }
};

////////////////////////// From Leetcode ///////////////////////////

// const uint64_t RRIP_BITS = 3;
// const uint64_t RRIP_LAST_LEVEL = ((1 << RRIP_BITS) - 1);
// const uint64_t RRIP_LAST_2_LEVEL = ((1 << RRIP_BITS) - 2);

class DLinkedList {
public:
    DLinkedList(uint64_t level_) {
        head = new DLinkedNode();
        tail = new DLinkedNode();
        num = 0;
        level = level_;
        head->next = tail;
        tail->prev = head;
    }

    void removeNode(DLinkedNode *node) {
        node->prev->next = node->next;
        node->next->prev = node->prev;
        num--;
    }

    DLinkedNode *removeHead() {
        DLinkedNode *node = head->next;
        if (node == tail) {
            return nullptr;
        }
        removeNode(node);
        return node;
    }

    void addToTail(DLinkedNode *node) {
        node->prev = tail->prev;
        node->next = tail;
        tail->prev->next = node;
        tail->prev = node;
        num++;
    }

    void clear() {
        for (DLinkedNode *node = head->next; node != tail;) {
            DLinkedNode *next = node->next;
            delete node;
            node = next;
        }
        head->next = tail;
        tail->prev = head;
        num = 0;
    }

    uint64_t get_num() { return num; }

    void set_level(uint64_t level_) {
        level = level_;
    }

    uint64_t get_level() { return level; }

private:
    DLinkedNode *head{nullptr};
    DLinkedNode *tail{nullptr};
    uint64_t level{UINT64_MAX};
    uint64_t num;
};

struct MockRRIPPerfCount {
    MockRRIPPerfCount() = delete;
    MockRRIPPerfCount(uint32_t _rrip_bits) {
        for (uint32_t i = 0; i < (1U << _rrip_bits); i++) {
            hit_levels.push_back(0);
        }
    }
    uint64_t exchange_cnts{0};
    uint64_t top_w_cnts{0};
    uint64_t mid_w_cnts{0};
    std::vector<uint64_t> hit_levels;
};

struct FlashRRIPSet {
private:
    std::unordered_map<uint64_t, std::pair<DLinkedNode *, DLinkedList *>> cache;
    std::unordered_set<uint64_t> tmp;
    std::vector<DLinkedList *> w_levels;

    uint64_t size{0};
    uint64_t capacity{0};
    uint32_t rrip_bits{0};

    // perf bits
    MockRRIPPerfCount *perf_cnts{nullptr};

    void advance_level(uint64_t key) {
        DLinkedNode *node = cache[key].first;
        DLinkedList *list = cache[key].second;
        list->removeNode(node);

        w_levels[0]->addToTail(node);
        cache[key].second = w_levels[0];

        perf_cnts->top_w_cnts ++;
    }

    uint64_t rrip_level_num() {
        return (1 << rrip_bits);
    }

public:
    FlashRRIPSet() = delete;
    FlashRRIPSet(uint64_t _capacity, uint32_t _rrip_bits, MockRRIPPerfCount *_perf_cnts)
        : size(0), capacity(_capacity), rrip_bits(_rrip_bits), perf_cnts(_perf_cnts) {
        // 使用伪头部和伪尾部节点
        for (uint32_t i = 0; i < (1U << rrip_bits); i++) {
            w_levels.emplace_back(new DLinkedList(i));
        }
    }

    bool get(uint64_t key) {
        if (!cache.count(key)) {
            return false;
        }

        // 理论上并不能立即提高 RRIP 等级，而是在一定写入量后批量提高 RRIP 等级
        if (tmp.find(key) == tmp.end()) {
            tmp.insert(key);
        }

        return true;
    }

    void put(uint64_t key) {
        if (!cache.count(key)) {
            if (size >= capacity) {
                DLinkedNode *removed = nullptr;
                while (true) {
                    removed = w_levels[rrip_level_num()-1]->removeHead();
                    if (removed != nullptr) {
                        break;
                    }
                    perf_cnts->exchange_cnts++;
                    for (uint32_t i = rrip_level_num()-1; i > 0; i--) {
                        std::swap(w_levels[i], w_levels[i - 1]);
                    }
                    for (uint32_t i = 0; i < rrip_level_num(); i++) {
                        w_levels[i]->set_level(i);
                    }
                }

                cache.erase(removed->key);
                tmp.erase(removed->key);
                delete removed;
                --size;
            }

            DLinkedNode *node = new DLinkedNode(key);
            w_levels[rrip_level_num()-2]->addToTail(node);
            cache.insert({key, {node, w_levels[rrip_level_num()-2]}});
            
            perf_cnts->mid_w_cnts++;

            ++size;
        } else {
            // 更新？
            // if (tmp.count(key)) {
            //     advance_level(key);
            //     tmp.erase(key);
            // }
        }
    }

    void flush_all_pending() {
        for (auto key: tmp) {
            advance_level(key);
        }
        tmp.clear();
    }

    void redived_hot(FlashRRIPSet *hot) {
        this->flush_all_pending();
        hot->flush_all_pending();

        std::vector<std::pair<uint64_t, DLinkedNode *>> rk_pairs;
        for (auto kv: hot->cache) {
            DLinkedNode *node = kv.second.first;
            DLinkedList *list = kv.second.second;

            if (this->cache.count(node->key) == 0) {
                rk_pairs.emplace_back(list->get_level(), node);
            }
            list->removeNode(node);
        }
        for (auto kv: this->cache) {
            DLinkedNode *node = kv.second.first;
            DLinkedList *list = kv.second.second;
            rk_pairs.emplace_back(list->get_level(), node);
            list->removeNode(node);
        }

        for (uint64_t i = 0; i < (1U << hot->rrip_bits); i++) {
            assert(hot->w_levels[i]->get_num() == 0);
        }
        hot->size = 0;
        hot->cache.clear();

        for (uint64_t i = 0; i < (1U << rrip_bits); i++) {
            assert(w_levels[i]->get_num() == 0);
        }
        this->size = 0;
        this->cache.clear();

        std::sort(rk_pairs.begin(), rk_pairs.end(), [](std::pair<uint64_t, DLinkedNode *> a, std::pair<uint64_t, DLinkedNode *> b) {
            return a.first < b.first;
        });

        uint64_t j = 0;
        for (; hot->size < hot->capacity && j < rk_pairs.size(); j++) {
            uint64_t level = rk_pairs[j].first;
            DLinkedNode *node = rk_pairs[j].second;

            hot->w_levels[level]->addToTail(node);
            hot->cache.insert({node->key, {node, hot->w_levels[level]}});
            hot->size++;
        }

        for (; size < capacity && j < rk_pairs.size(); j++) {
            uint64_t level = rk_pairs[j].first;
            DLinkedNode *node = rk_pairs[j].second;

            w_levels[level]->addToTail(node);
            cache.insert({node->key, {node, w_levels[level]}});
            size++;
        }
    }

    std::vector<DLinkedList *>& get_w_levels() { return w_levels; }
};

// GC 时迁移？
struct LazyRRIPSet {
private:
    std::unordered_map<uint64_t, std::pair<DLinkedNode *, DLinkedList *>> cache;
    std::unordered_set<uint64_t> tmp;
    std::vector<DLinkedList *> w_levels;

    uint64_t size{0};
    uint64_t capacity{0};
    uint32_t rrip_bits{0};

    // perf bits
    MockRRIPPerfCount *perf_cnts{nullptr};

    uint64_t rrip_level_num() {
        return (1 << rrip_bits);
    }

public:
    LazyRRIPSet() = delete;
    LazyRRIPSet(uint64_t _capacity, uint32_t _rrip_bits, MockRRIPPerfCount *_perf_cnts)
        : size(0), capacity(_capacity), rrip_bits(_rrip_bits), perf_cnts(_perf_cnts) {
        // 使用伪头部和伪尾部节点
        for (uint32_t i = 0; i < (1U << rrip_bits); i++) {
            w_levels.emplace_back(new DLinkedList(i));
        }
    }

    bool get(uint64_t key) {
        if (!cache.count(key)) {
            return false;
        }

        perf_cnts->hit_levels[cache[key].second->get_level()]++;

        // 理论上并不能立即提高 RRIP 等级，而是在一定写入量后批量提高 RRIP 等级
        if (tmp.find(key) == tmp.end()) {
            tmp.insert(key);
        }

        return true;
    }

    void put(uint64_t key) {
        if (!cache.count(key)) {
            while (size >= capacity) {
                DLinkedNode *removed = nullptr;
                while (true) {
                    removed = w_levels[rrip_level_num()-1]->removeHead();
                    if (removed != nullptr) {
                        break;
                    }
                    perf_cnts->exchange_cnts++;
                    for (uint32_t i = rrip_level_num()-1; i > 0; i--) {
                        std::swap(w_levels[i], w_levels[i - 1]);
                    }
                    for (uint32_t i = 0; i < rrip_level_num(); i++) {
                        w_levels[i]->set_level(i);
                    }
                }
                if (tmp.count(removed->key) == 0) {
                    cache.erase(removed->key);
                    delete removed;
                    --size;
                } else {
                    w_levels[0]->addToTail(removed);
                    cache[removed->key].second = w_levels[0];
                    perf_cnts->top_w_cnts ++;

                    tmp.erase(removed->key);
                }
            }

            DLinkedNode *node = new DLinkedNode(key);
            w_levels[rrip_level_num()-2]->addToTail(node);
            cache.insert({key, {node, w_levels[rrip_level_num()-2]}});
            
            perf_cnts->mid_w_cnts++;

            ++size;
        } else {
            // 更新？
            // if (tmp.count(key)) {
            //     advance_level(key);
            //     tmp.erase(key);
            // }
        }
    }

    std::vector<DLinkedList *>& get_w_levels() { return w_levels; }
};

struct MemRRIPSet {
private:
    std::unordered_map<uint64_t, std::pair<DLinkedNode *, DLinkedList *>> cache;
    std::vector<DLinkedList *> w_levels;

    uint64_t size{0};
    uint64_t capacity{0};
    uint32_t rrip_bits{0};

    MockRRIPPerfCount *perf_cnts{nullptr};

    void advance_level(uint64_t key) {
        DLinkedNode *node = cache[key].first;
        DLinkedList *list = cache[key].second;

        // uint64_t level = find_level(cnt);
        list->removeNode(node);
        w_levels[0]->addToTail(node);
        cache[key].second = w_levels[0];

        perf_cnts->top_w_cnts ++;
    }

    uint64_t rrip_level_num() {
        return (1 << rrip_bits);
    }

public:
    MemRRIPSet() = delete;
    MemRRIPSet(uint64_t _capacity, uint32_t _rrip_bits, MockRRIPPerfCount *_perf_cnts)
        : size(0), capacity(_capacity), rrip_bits(_rrip_bits), perf_cnts(_perf_cnts) {
        // 使用伪头部和伪尾部节点
        for (uint32_t i = 0; i < (1U << rrip_bits); i++) {
            w_levels.emplace_back(new DLinkedList(i));
        }
    }

    bool get(uint64_t key) {
        if (!cache.count(key)) {
            return false;
        }

        perf_cnts->hit_levels[cache[key].second->get_level()]++;

        advance_level(key);

        return true;
    }

    uint64_t put(uint64_t key) {
        uint64_t removed_key = UINT64_MAX;
        if (!cache.count(key)) {
            if (size >= capacity) {
                DLinkedNode *removed = nullptr;
                while (true) {
                    removed = w_levels[rrip_level_num()-1]->removeHead();
                    if (removed != nullptr) {
                        break;
                    }
                    perf_cnts->exchange_cnts++;
                    for (uint32_t i = rrip_level_num()-1; i > 0; i--) {
                        std::swap(w_levels[i], w_levels[i - 1]);
                    }
                    for (uint32_t i = 0; i < rrip_level_num(); i++) {
                        w_levels[i]->set_level(i);
                    }
                }

                cache.erase(removed->key);
                removed_key = removed->key;
                delete removed;
                --size;
            }

            DLinkedNode *node = new DLinkedNode(key);
            w_levels[rrip_level_num()-2]->addToTail(node);
            cache.insert({key, {node, w_levels[rrip_level_num()-2]}});
            
            perf_cnts->mid_w_cnts ++;
            
            ++size;
        } else {
            // 更新？
            advance_level(key);
        }

        return removed_key;
    }

    std::vector<DLinkedList *>& get_w_levels() { return w_levels; }
};

const uint32_t SIMPLE_RRIP_BITS = 3;
const uint32_t SPLIT_RRIP_BITS = 3;
const double ZIPF_ALPHA = 0.5;
const double WRITE_RATIO = 0.05;
const double CACHE_RATIO = 0.6;

const uint64_t MAX_ITEM_NUM = 128 * 2000;
const uint64_t PART_NUM = 128;

const double SPLIT_RATIO = 0.5;
const double FAIRY_SPLIT_RATIO = 0.25;
const double FAIRY_REDIV_RATIO = 0.2;

const uint64_t READ_LATENCY = 50 * 1000;
const uint64_t WRITE_LATENCY = 500 * 1000;

class MockCache {
public:
    virtual bool get(uint64_t key, uint64_t *r_lat) = 0;
    virtual void put(uint64_t key, uint64_t *w_lat) = 0;
};

class MockSimpleCache: public MockCache {
    std::vector<LazyRRIPSet> sets;
    std::vector<uint64_t> w_cnts;
    MockRRIPPerfCount perf_cnts;
public:
    MockSimpleCache(): perf_cnts(SIMPLE_RRIP_BITS) {
        printf("MockSimpleCache: %f\n", MAX_ITEM_NUM * CACHE_RATIO / (10 * PART_NUM));
        uint64_t part_obj_num = (MAX_ITEM_NUM * CACHE_RATIO) / PART_NUM;
        for (uint32_t i = 0; i < PART_NUM; i++) {
            sets.push_back(LazyRRIPSet(part_obj_num, SIMPLE_RRIP_BITS, &perf_cnts));
            w_cnts.push_back(0);
        }
    }
    virtual bool get(uint64_t key, uint64_t *r_lat) override {
        uint64_t part_id = key % PART_NUM;

        *r_lat += READ_LATENCY;

        return sets[part_id].get(key);
    }

    virtual void put(uint64_t key, uint64_t *w_lat) override {
        uint64_t part_id = key % PART_NUM;
        *w_lat += WRITE_LATENCY;
        sets[part_id].put(key);

        w_cnts[part_id] += 1;
        if (w_cnts[part_id] > MAX_ITEM_NUM * CACHE_RATIO / (1 * PART_NUM)) {
            // sets[part_id].flush_all_pending();
            w_cnts[part_id] = 0;
        }
    }

    void dump_rrip_cnts() {
        std::vector<uint64_t> cnts;
        for (uint32_t j = 0; j < (1 << SIMPLE_RRIP_BITS); j++) {
            cnts.push_back(0);
        }
        for (uint32_t i = 0; i < PART_NUM; i++) {
            std::vector<DLinkedList *>& levels = sets[i].get_w_levels();
            for (uint32_t j = 0; j < (1 << SIMPLE_RRIP_BITS); j++) {
                cnts[j] += levels[j]->get_num();
            }
        }
        for (uint32_t j = 0; j < (1 << SIMPLE_RRIP_BITS); j++) {
            printf("%d %ld\n", j,cnts[j]);
        }
    }

    void dump_hit_levels() {
        for (uint32_t j = 0; j < (1 << SIMPLE_RRIP_BITS); j++) {
            printf("%d %ld\n", j, perf_cnts.hit_levels[j]);
        }
    }

    MockRRIPPerfCount *get_perf_cnts() {
        return &perf_cnts;
    }
};

// 探究： 如何实现 split
// 如何保证直接插入
// fairy 的 log 区热度检测策略是否过于粗略
class MockSplitCache: public MockCache {
    std::vector<MemRRIPSet> hot_sets_;
    std::vector<FlashRawFIFOSet> warm_sets_;
    // std::vector<MemFIFOSet> tmp_sets_;
    uint64_t hot_hit_cnt_{0};
    uint64_t warm_hit_cnt_{0};
    MockRRIPPerfCount perf_cnts;
public:
    MockSplitCache(): perf_cnts(SPLIT_RRIP_BITS) {
        uint64_t part_obj_num = (MAX_ITEM_NUM * CACHE_RATIO) / PART_NUM;
        for (uint32_t i = 0; i < PART_NUM; i++) {
            warm_sets_.push_back(FlashRawFIFOSet(part_obj_num * (1 - SPLIT_RATIO)));
            hot_sets_.push_back(MemRRIPSet(part_obj_num * SPLIT_RATIO, SPLIT_RRIP_BITS, &perf_cnts));
            // tmp_sets_.push_back(MemFIFOSet(part_obj_num * SPLIT_RATIO));
        }
    }
    virtual bool get(uint64_t key, uint64_t *r_lat) override {
        uint64_t part_id = key % PART_NUM;
        
        if (hot_sets_[part_id].get(key)) {
            *r_lat += READ_LATENCY;
            hot_hit_cnt_ += 1;
            return true;
        } else if (warm_sets_[part_id].get(key)) {
            *r_lat += READ_LATENCY * 4;
            warm_hit_cnt_ += 1;
            return true;
        }

        return false;
    }

    virtual void put(uint64_t key, uint64_t *w_lat) override {
        uint64_t part_id = key % PART_NUM;
        
        uint64_t removed_key = hot_sets_[part_id].put(key);
        if (removed_key != UINT64_MAX) {
            warm_sets_[part_id].put(removed_key);
        }

        *w_lat += WRITE_LATENCY / 64;
    }

    void dump_rrip_cnts() {
        std::vector<uint64_t> cnts;
        for (uint32_t j = 0; j < (1 << SPLIT_RRIP_BITS); j++) {
            cnts.push_back(0);
        }
        for (uint32_t i = 0; i < PART_NUM; i++) {
            std::vector<DLinkedList *>& levels = hot_sets_[i].get_w_levels();
            for (uint32_t j = 0; j < (1 << SPLIT_RRIP_BITS); j++) {
                cnts[j] += levels[j]->get_num();
            }
        }
        for (uint32_t j = 0; j < (1 << SPLIT_RRIP_BITS); j++) {
            printf("%d %ld\n", j,cnts[j]);
        }
    }

    void dump_hit_levels() {
        for (uint32_t j = 0; j < (1 << SIMPLE_RRIP_BITS); j++) {
            printf("%d %ld\n", j, perf_cnts.hit_levels[j]);
        }
        printf("hot hit %ld warm hit %ld\n", hot_hit_cnt_, warm_hit_cnt_);
    }

    MockRRIPPerfCount *get_perf_cnts() {
        return &perf_cnts;
    }
};

class MockIdealSplitCache: public MockCache {
private:
    std::vector<std::unordered_set<uint64_t>> hot_sets_;
    std::vector<FlashRawFIFOSet> sets_;
public:
    MockIdealSplitCache() {
        uint64_t part_obj_num = (MAX_ITEM_NUM * CACHE_RATIO) / PART_NUM;
        for (uint32_t i = 0; i < PART_NUM; i++) {
            hot_sets_.push_back(std::unordered_set<uint64_t>());
            sets_.push_back(FlashRawFIFOSet(part_obj_num * (1 - SPLIT_RATIO) * 0.95));
        }
    }
    
    virtual bool get(uint64_t key, uint64_t *r_lat) override {
        uint64_t part_id = key % PART_NUM;
        
        if (hot_sets_[part_id].count(key) > 0) {
            *r_lat += READ_LATENCY;
            return true;
        } else if (sets_[part_id].get(key)) {
            *r_lat += READ_LATENCY * 4;
            return true;
        }
        return false;
    }

    virtual void put(uint64_t key, uint64_t *w_lat) override {
        uint64_t part_id = key % PART_NUM;
        *w_lat += WRITE_LATENCY / 64;

        if (key < MAX_ITEM_NUM * CACHE_RATIO * SPLIT_RATIO) {
            if (hot_sets_[part_id].count(key) == 0) {
                hot_sets_[part_id].insert(key);
            }
        } else {
            sets_[part_id].put(key);
        }
    }
};

class MockFairyCache: public MockCache {
private:
    std::vector<FlashRRIPSet> hot_sets_;
    std::vector<FlashRRIPSet> sets_;
    std::vector<uint64_t> w_cnts_;
    MockRRIPPerfCount perf_cnt_;
public:
    MockFairyCache(): perf_cnt_(SIMPLE_RRIP_BITS) {
        uint64_t part_obj_num = (MAX_ITEM_NUM * CACHE_RATIO) / PART_NUM;
        for (uint32_t i = 0; i < PART_NUM; i++) {
            hot_sets_.push_back(FlashRRIPSet(part_obj_num * (1 - FAIRY_SPLIT_RATIO), SIMPLE_RRIP_BITS, &perf_cnt_));
            sets_.push_back(FlashRRIPSet(part_obj_num * FAIRY_SPLIT_RATIO, SIMPLE_RRIP_BITS, &perf_cnt_));
            
            w_cnts_.push_back(0);
        }
    }
    
    virtual bool get(uint64_t key, uint64_t *r_lat) override {
        uint64_t part_id = key % PART_NUM;

        if (sets_[part_id].get(key)) {
            *r_lat += READ_LATENCY;
            return true;
        } else {
            *r_lat += 2 * READ_LATENCY;
            return hot_sets_[part_id].get(key);
        }
    }

    virtual void put(uint64_t key, uint64_t *w_lat) override {
        uint64_t part_id = key % PART_NUM;
        *w_lat += WRITE_LATENCY;
        sets_[part_id].put(key);

        w_cnts_[part_id] += 1;
        if (w_cnts_[part_id] > MAX_ITEM_NUM * CACHE_RATIO / (10 * PART_NUM)) {
            sets_[part_id].flush_all_pending();
            w_cnts_[part_id] = 0;

            if (rand64() % 100 <= 100 * FAIRY_REDIV_RATIO) {
                sets_[part_id].redived_hot(&hot_sets_[part_id]);
            }
        }
    }
};

// 还需要进一步探究 filter 的影响，是否能减少误报率
// 最高 miss ratio在 50% - 60% 之间
void basic_test(MockCache *cache, const char *cache_name) {
    zipf_table_distribution<uint64_t, double> *zipf = 
        new zipf_table_distribution<uint64_t, double>(MAX_ITEM_NUM, ZIPF_ALPHA);
    std::mt19937_64 rng(0);

    std::unordered_map<uint64_t, uint64_t> r_counts;
    std::unordered_map<uint64_t, uint64_t> m_counts;
    
    uint64_t w_totals = 0;
    uint64_t r_totals = 0;
    uint64_t m_totals = 0;
    
    uint64_t r_lat = 0;
    uint64_t w_lat = 0;

    for (uint64_t operation = 0; operation <= 16 * 1024 * 1024; operation++) {
        bool is_write = (rand64() % 100) <= WRITE_RATIO * 100;
        bool hit = true;
        uint64_t key = (*zipf)(rng);

        if (!is_write) {
            hit = cache->get(key, &r_lat);
            r_counts[key] += 1;
            r_totals += 1;
        }

        if (!hit) {
            m_counts[key] += 1;
            m_totals += 1;
        }
        if (is_write || !hit) {
            cache->put(key, &w_lat);
            w_totals += 1;
        }
    }

    uint64_t best_count = 0;
    for (uint64_t i = 0; i < MAX_ITEM_NUM * CACHE_RATIO; i++) {
        best_count += r_counts[i];
    }

    // printf("%s best miss_rates: %lf\n", cache_name, 1.0 - (best_count * 1.0 / r_totals));
    printf("%s miss_rates: %lf\n", cache_name, m_totals * 1.0 / r_totals);
    printf("%s read latency: %lf\n", cache_name, r_lat * 1.0 / r_totals);
    printf("%s write cnt: %ld\n", cache_name, w_totals);
}

int main() {
    MockSimpleCache lru_cache;
    basic_test(&lru_cache, "LRU Cache");
    printf("Exchange Cnts: %ld\n", lru_cache.get_perf_cnts()->exchange_cnts);
    printf("Top W: %ld, Mid W: %ld\n", lru_cache.get_perf_cnts()->top_w_cnts, lru_cache.get_perf_cnts()->mid_w_cnts);
    lru_cache.dump_rrip_cnts();
    lru_cache.dump_hit_levels();
    MockSplitCache split_cache;
    basic_test(&split_cache, "Split Cache");
    printf("Exchange Cnts: %ld\n", split_cache.get_perf_cnts()->exchange_cnts);
    printf("Top W: %ld, Mid W: %ld\n", split_cache.get_perf_cnts()->top_w_cnts, split_cache.get_perf_cnts()->mid_w_cnts);
    split_cache.dump_rrip_cnts();
    split_cache.dump_hit_levels();
    MockFairyCache fairy_cache;
    basic_test(&fairy_cache, "Fairy Cache");
    MockIdealSplitCache ideal_split_cache;
    basic_test(&ideal_split_cache, "Ideal Split Cache");
    return 0;
}