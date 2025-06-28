#pragma once

#include <mutex>
#include <shared_mutex>
#include <queue>
#include <optional>

#include "common/Types.h"
#include "common/Device.h"

template <typename K, typename V>
struct DLinkedNode {
    K key;
    V value;
    DLinkedNode *prev;
    DLinkedNode *next;
    DLinkedNode() : key(0), value(0), prev(nullptr), next(nullptr) {}
    DLinkedNode(K _key, V _value)
        : key(_key), value(_value), prev(nullptr), next(nullptr) {}
};

template <typename K, typename V>
class LRUList {
private:
    using DLinkedNodeType = DLinkedNode<K, V>;

    std::unordered_map<K, DLinkedNodeType *> cache;
    std::queue<V> free_q;

    DLinkedNodeType *head{nullptr};
    DLinkedNodeType *tail{nullptr};
    int size;
    int capacity;

public:
    LRUList() = delete;
    LRUList(int _capacity, std::queue<V> *_free_q) : size(0), capacity(_capacity) {
        // 使用伪头部和伪尾部节点
        head = new DLinkedNodeType();
        tail = new DLinkedNodeType();
        head->next = tail;
        tail->prev = head;

        free_q.swap(*_free_q);
    }

    LRUList(LRUList &&rhs) {
        head = rhs.head;
        tail = rhs.tail;
        size = rhs.size;
        capacity = rhs.capacity;
        cache = std::move(rhs.cache);
        free_q = std::move(rhs.free_q);
        rhs.head = nullptr;
        rhs.tail = nullptr;
        rhs.size = 0;
        rhs.capacity = 0;
    }

    ~LRUList() {
        DLinkedNodeType *cur = head;
        while (cur != nullptr) {
            DLinkedNodeType *next = cur->next;
            delete cur;
            cur = next;
        }
    }

    std::optional<V> get(K key) {
        if (!cache.count(key)) {
            return std::nullopt;
        }
        // 如果 key 存在，先通过哈希表定位，再移到头部
        DLinkedNodeType *node = cache[key];
        moveToHead(node);
        return node->value;
    }

    V alloc_or_evict(K key) {
        assert(cache.count(key) == 0);
        DLinkedNodeType *node = nullptr;
        if (size == capacity) {
            // evict
            // 如果超出容量，删除双向链表的尾部节点
            DLinkedNodeType *removed = removeTail();
            // 删除哈希表中对应的项
            cache.erase(removed->key);

            node = removed;
            node->key = key;
        } else {
            // 如果 key 不存在，创建一个新的节点
            assert(free_q.size() > 0);
            node = new DLinkedNodeType(key, free_q.front());
            free_q.pop();
            ++size;
        }
        // 添加进哈希表
        cache[key] = node;
        // 添加至双向链表的头部
        addToHead(node);

        return node->value;
    }

private:

    void addToHead(DLinkedNodeType *node) {
        node->prev = head;
        node->next = head->next;
        head->next->prev = node;
        head->next = node;
    }

    void removeNode(DLinkedNodeType *node) {
        node->prev->next = node->next;
        node->next->prev = node->prev;
    }


    void moveToHead(DLinkedNodeType *node) {
        removeNode(node);
        addToHead(node);
    }

    DLinkedNodeType *removeTail() {
        DLinkedNodeType *node = tail->prev;
        removeNode(node);
        return node;
    }
};

class FlushCache {
public:
    struct Config {
        uint32_t buffer_num{0};
        uint32_t part_num{0};
        uint32_t page_size{0};

        Device *device{nullptr};

        Config &validate();
    };

    FlushCache(Config &&config);
    
    Status read(uint64_t page_offset, uint8_t ver, std::function<Status(Buffer *)> read_cb);

    void dump_stats() const;

private:
    struct CacheSlot {
        Buffer buf;
        uint8_t ver;
    };
    Config config_;
    // Buffer 自动释放内存
    std::vector<CacheSlot> slots_;
    std::vector<LRUList<uint64_t, CacheSlot *>> lru_lists_;
    std::unique_ptr<std::shared_mutex[]> mutexes_;

    uint64_t hit_cnt = 0;
    uint64_t miss_cnt = 0;
};