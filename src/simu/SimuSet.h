#pragma once

#include <cstdint>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <deque>
#include <cassert>
#include <cstdio>
#include <algorithm>

struct DLinkedNode {
    uint64_t key;
    DLinkedNode *prev;
    DLinkedNode *next;
    DLinkedNode() : key(0), prev(nullptr), next(nullptr) {}
    DLinkedNode(uint64_t _key)
        : key(_key), prev(nullptr), next(nullptr) {}
};

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
    uint64_t exchange_cnts{0};
    uint64_t top_w_cnts{0};
    uint64_t mid_w_cnts{0};
};

// 优化 RRIP 比例
class BlockInnerRRIPSet {
private:
    std::unordered_map<uint64_t, std::pair<DLinkedNode *, DLinkedList *>> cache;
    std::unordered_set<uint64_t> tmp;
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
    BlockInnerRRIPSet() = delete;
    BlockInnerRRIPSet(uint64_t _capacity, uint32_t _rrip_bits, MockRRIPPerfCount *_perf_cnts)
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

    // bool check_hit(uint64_t key) {
    //     if (!cache.count(key)) {
    //         return false;
    //     }
    //     return true;
    // }

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
                tmp.erase(removed->key);
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
            // advance_level(key);
        }

        // printf("put size %lu\n", size);

        assert(size == cache.size());
        assert(size <= capacity);

        return removed_key;
    }

    void erase(uint64_t key) {
        if (cache.count(key) > 0) {
            // printf("erase %lu\n", key);
            DLinkedNode *node = cache[key].first;
            DLinkedList *list = cache[key].second;
            list->removeNode(node);
            delete node;
            --size;
            cache.erase(key);
        }
    }

    void redived_hot(BlockInnerRRIPSet* hot) {
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

    void proc_pending() {
        for (auto it = tmp.begin(); it != tmp.end(); it++) {
            advance_level(*it);
        }
        tmp.clear();
    }

    void flush_all_pending() {
        for (auto key: tmp) {
            advance_level(key);
        }
        tmp.clear();
    }
    
    void dump_cache(std::vector<uint64_t> *keys) {
        for (auto it = cache.begin(); it != cache.end(); it++) {
            printf("%lu ", it->first);
            keys->push_back(it->first);
        }
        printf("\n");
    }

    std::vector<DLinkedList *>& get_w_levels() { return w_levels; }

    uint64_t get_size() { return size; }
};

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
        assert(cache[key]->key == key);
        assert(cache[key]->prev != nullptr);
        assert(cache[key]->next != nullptr);
        assert(cache[key]->prev->next == cache[key]);
        assert(cache[key]->next->prev == cache[key]);
        return true;
    }

    void put(uint64_t key) {
        // 直接冗余数据
        DLinkedNode *node = new DLinkedNode(key);
        if (!cache.count(key)) {
            cache.insert({key, node});
        } else {
            cache[key] = node;
        }
        addToTail(node);
        ++size;

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

    uint64_t get_size() { return cache.size(); }

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

class TieredSet {
private:
    std::vector<BlockInnerRRIPSet> sets_;
    std::vector<std::deque<uint64_t>> last_batch_w_;
    uint64_t next_w_{0};

    uint64_t tiered_num_{0};
public:
    TieredSet(
        uint64_t total_num,
        uint64_t rrip_bits,
        uint64_t tiered_num,
        MockRRIPPerfCount *perf_cnts
    ) : tiered_num_(tiered_num) {
        for (uint64_t i = 0; i < tiered_num_; i++) {
            sets_.push_back(BlockInnerRRIPSet(total_num / tiered_num_, rrip_bits, perf_cnts));
            last_batch_w_.push_back(std::deque<uint64_t>());
        }
    }

    bool get(uint64_t key) {
        uint64_t last_r = next_w_;
        for (uint64_t i = (last_r + tiered_num_ - 1) % tiered_num_; true; i = (i + tiered_num_ - 1) % tiered_num_) {

            if (sets_[i].get(key)) {
                return true;
            }

            if (i == last_r) {
                break;
            }
        }

        return false;
    }

    void batched_put(std::deque<uint64_t> *batch_w) {
        sets_[next_w_].proc_pending();
        // 难受啊，重吸收还要管一致性，太丑陋了
        // 删除数据
        for (uint64_t i = 0; i < tiered_num_; i++) {
            if (i == next_w_) {
                continue;
            }

            for (auto pre_key: last_batch_w_[i]) {
                sets_[next_w_].erase(pre_key);
            }
        }

        for (auto pre_key: *batch_w) {
            sets_[next_w_].put(pre_key);
        }
        last_batch_w_[next_w_].clear();
        last_batch_w_[next_w_].swap(*batch_w);

        next_w_ = (next_w_ + 1) % tiered_num_;
    }

    uint64_t get_size() {
        uint64_t total_size = 0;
        for (uint64_t i = 0; i < tiered_num_; i++) {
            total_size += sets_[i].get_size();
        }
        return total_size;
    }
};