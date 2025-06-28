#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string>

#include <matplotlibcpp.h>

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

class LRUCache {
private:
    std::unordered_map<int, DLinkedNode *> cache;
    DLinkedNode *head{nullptr};
    DLinkedNode *tail{nullptr};
    int size;
    int capacity;

public:
    LRUCache() = delete;
    LRUCache(int _capacity) : size(0), capacity(_capacity) {
        // 使用伪头部和伪尾部节点
        head = new DLinkedNode();
        tail = new DLinkedNode();
        head->next = tail;
        tail->prev = head;
    }

    bool get(int key) {
        if (!cache.count(key)) {
            return false;
        }
        // 如果 key 存在，先通过哈希表定位，再移到头部
        DLinkedNode *node = cache[key];
        moveToHead(node);
        return true;
    }

    void put(int key) {
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

////////////////////////// From Leetcode ///////////////////////////
#define TOTAL_NUMBER (512 * 16 * 64)
#define PARTITION_NUM (128)

void dump_counts(std::unordered_map<uint64_t, uint64_t> &counts,
                        const char *name, uint64_t num_plots) {
    uint64_t total = 0;
    for (auto &cnts : counts) {
        total += cnts.second;
    }
    printf("%s obj num: %ld, total counts: %ld\n", name, counts.size(), total);

    uint64_t partial = 0;
    // uint64_t last_partial = 0;
    double p_ratio = 1.0 / num_plots;
    for (uint64_t i = 0; i < TOTAL_NUMBER; i++) {
        partial += counts[i];
        if (i >= TOTAL_NUMBER * p_ratio) {
            // printf("%s (%lf) patial ratio: %lf gained: %lf\n", name, p_ratio, partial * 1.0 / total, (partial - last_partial) * 1.0 / total);
            p_ratio += 1.0 / num_plots;
            // last_partial = partial;
        }
    }

    std::vector<std::pair<uint64_t, uint64_t>> totals(counts.begin(),
                                                      counts.end());
    std::sort(totals.begin(), totals.end(),
              [](const std::pair<uint64_t, uint64_t> &a,
                 const std::pair<uint64_t, uint64_t> &b) {
                  return a.second > b.second;
              });

    partial = 0;
    // last_partial = 0;
    p_ratio = 1.0 / num_plots;
    for (uint64_t i = 0; i < totals.size(); i++) {
        partial += totals[i].second;
        if (i >= TOTAL_NUMBER * p_ratio) {
            // printf("%s ordered(%lf) patial ratio: %lf gained: %lf\n", name, p_ratio, partial * 1.0 / total, (partial - last_partial) * 1.0 / total);
            p_ratio += 1.0 / num_plots;
            // last_partial = partial;
        }
    }
}

class DLinkedList {
public:
    DLinkedList() {
        head = new DLinkedNode();
        tail = new DLinkedNode();
        num = new uint64_t(0);
        head->next = tail;
        tail->prev = head;
    }

    static void removeNode(DLinkedNode *node) {
        node->prev->next = node->next;
        node->next->prev = node->prev;
    }

    DLinkedNode *removeHead() {
        DLinkedNode *node = head->next;
        if (node == tail) {
            return nullptr;
        }
        removeNode(node);
        (*num)--;
        return node;
    }

    void addToTail(DLinkedNode *node) {
        node->prev = tail->prev;
        node->next = tail;
        tail->prev->next = node;
        tail->prev = node;
        (*num)++;
    }

    void swap(DLinkedList *other) {
        std::swap(head, other->head);
        std::swap(tail, other->tail);
        std::swap(num, other->num);
    }

    void clear() {
        for (DLinkedNode *node = head->next; node != tail;) {
            DLinkedNode *next = node->next;
            delete node;
            node = next;
        }
        head->next = tail;
        tail->prev = head;
        *num = 0;
    }

    uint64_t * get_num() { return num; }

private:
    DLinkedNode *head{nullptr};
    DLinkedNode *tail{nullptr};
    uint64_t *num{nullptr};
};

struct FlashRRIPCache {
private:
    std::unordered_map<uint64_t, std::pair<DLinkedNode *, uint64_t *>> cache;
    std::unordered_set<uint64_t> tmp;
    std::vector<DLinkedList> w_levels;

    uint64_t size{0};
    uint64_t capacity{0};
    uint32_t rrip_bits{0};

    void advance_level(uint64_t key) {
        DLinkedNode *node = cache[key].first;
        uint64_t *cnt = cache[key].second;
        DLinkedList::removeNode(node);
        (*cnt) --;

        w_levels[0].addToTail(node);
        cache[key].second = w_levels[0].get_num();
    }

    uint64_t rrip_level_num() {
        return (1 << rrip_bits);
    }

public:
    FlashRRIPCache() = delete;
    FlashRRIPCache(uint64_t _capacity, uint32_t _rrip_bits) : size(0), capacity(_capacity), rrip_bits(_rrip_bits) {
        // 使用伪头部和伪尾部节点
        for (uint32_t i = 0; i < (1U << rrip_bits); i++) {
            w_levels.emplace_back(DLinkedList{});
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
                    removed = w_levels[rrip_level_num()-1].removeHead();
                    if (removed != nullptr) {
                        break;
                    }
                    for (uint32_t i = rrip_level_num()-1; i > 0; i--) {
                        w_levels[i].swap(&w_levels[i - 1]);
                    }
                }

                cache.erase(removed->key);
                tmp.erase(key);
                delete removed;
                --size;
            }

            DLinkedNode *node = new DLinkedNode(key);
            w_levels[rrip_level_num()-2].addToTail(node);
            cache.insert({key, {node, w_levels[rrip_level_num()-2].get_num()}});
            ++size;
        } else {
            // 更新？
            if (tmp.count(key)) {
                advance_level(key);
                tmp.erase(key);
            }
        }
    }

    std::vector<DLinkedList>& get_w_levels() { return w_levels; }
};

namespace plt = matplotlibcpp;

void dump_miss_image(
    const char *m_name,
    std::unordered_map<uint64_t, uint64_t> &r_counts,
    std::unordered_map<uint64_t, uint64_t> &m_counts,
    const char *img_name,
    uint64_t num_plots
) {
    std::vector<double> x(num_plots), miss_ratio(num_plots);

    uint64_t r_partial = 0;
    uint64_t m_partial = 0;
    uint64_t last_r_partial = 0;
    uint64_t last_m_partial = 0;
    double p_ratio = 1.0 / num_plots;
    for (uint64_t i = 0, j = 0; i < TOTAL_NUMBER && j < num_plots; i++) {
        r_partial += r_counts[i];
        m_partial += m_counts[i];
        if (i >= TOTAL_NUMBER * p_ratio) {
            x[j] = i * 1.0 / TOTAL_NUMBER;
            miss_ratio[j] = (m_partial - last_m_partial) * 1.0 / (r_partial - last_r_partial);
            last_r_partial = r_partial;
            last_m_partial = m_partial;
            j++;
            p_ratio += 1.0 / num_plots;
        }
    }

    x[num_plots - 1] = 1;
    miss_ratio[num_plots - 1] = (m_partial - last_m_partial) * 1.0 / (r_partial - last_r_partial);

    plt::figure_size(1200, 780);
    plt::named_plot(m_name, x, miss_ratio);
    plt::legend();
    plt::save(std::string("./") + std::string(img_name) + std::string("_seg") + std::string(".pdf"));
}

void dump_rw_image(
    const char *r_name,
    std::unordered_map<uint64_t, uint64_t> &r_counts,
    const char *w_name,
    std::unordered_map<uint64_t, uint64_t> &w_counts,
    const char *img_name,
    uint64_t num_plots
) {
    std::vector<double> x(num_plots), r(num_plots), w(num_plots);
    std::vector<double> r_seg(num_plots), w_seg(num_plots);
    uint64_t r_total = 0;
    uint64_t w_total = 0;
    for (uint64_t i = 0; i < TOTAL_NUMBER; i++) {
        r_total += r_counts[i];
        w_total += w_counts[i];
    }

    uint64_t r_partial = 0;
    uint64_t w_partial = 0;
    uint64_t last_r_partial = 0;
    uint64_t last_w_partial = 0;
    double p_ratio = 1.0 / num_plots;
    for (uint64_t i = 0, j = 0; i < TOTAL_NUMBER && j < num_plots; i++) {
        r_partial += r_counts[i];
        w_partial += w_counts[i];
        if (i >= TOTAL_NUMBER * p_ratio) {
            x[j] = i * 1.0 / TOTAL_NUMBER;
            r[j] = r_partial * 1.0 / r_total;
            w[j] = w_partial * 1.0 / w_total;
            r_seg[j] = (r_partial - last_r_partial) * 1.0 / (r_total * num_plots);
            w_seg[j] = (w_partial - last_w_partial) * 1.0 / (w_total * num_plots);
            last_r_partial = r_partial;
            last_w_partial = w_partial;
            j++;
            p_ratio += 1.0 / num_plots;
        }
    }

    x[num_plots - 1] = 1;
    r[num_plots - 1] = 1;
    w[num_plots - 1] = 1;
    r_seg[num_plots - 1] = (r_partial - last_r_partial) * 1.0 / (r_total * num_plots);
    w_seg[num_plots - 1] = (w_partial - last_w_partial) * 1.0 / (w_total * num_plots);

    plt::figure_size(1200, 780);
    plt::named_plot(r_name, x, r);
    plt::named_plot(w_name, x, w);
    plt::legend();
    plt::save(std::string("./") + std::string(img_name) + std::string(".pdf"));

    plt::figure_size(1200, 780);
    plt::named_plot(r_name, x, r_seg);
    plt::named_plot(w_name, x, w_seg);
    plt::legend();
    plt::save(std::string("./") + std::string(img_name) + std::string("_seg") + std::string(".pdf"));
}

const uint32_t RRIP_BITS = 3;

void policy_write_back(double alpha, double w_ratio, double c_ratio) {
    zipf_table_distribution<uint64_t, double> *zipf =
        new zipf_table_distribution<uint64_t, double>(TOTAL_NUMBER, alpha);
    std::mt19937_64 rng(0);

    uint64_t part_obj_num = (TOTAL_NUMBER * c_ratio) / PARTITION_NUM;

    std::unordered_map<uint64_t, uint64_t> w_counts;
    std::unordered_map<uint64_t, uint64_t> r_counts;
    std::unordered_map<uint64_t, uint64_t> m_counts;
    std::vector<FlashRRIPCache> lrus;

    for (uint32_t i = 0; i < PARTITION_NUM; i++) {
        lrus.push_back(FlashRRIPCache(part_obj_num, RRIP_BITS));
    }

    for (uint32_t operation = 0; operation <= 16 * 1024 * 1024; operation++) {
        bool is_write = (rand64() % 100) <= w_ratio * 100;
        bool hit = true;
        uint64_t key = (*zipf)(rng);
        uint64_t part_id = key % PARTITION_NUM;

        if (!is_write) {
            hit = lrus[part_id].get(key);
            r_counts[key]++;
        } else {
            w_counts[key]++;
        }
        if (!hit) {
            m_counts[key]++;
        }
        if (is_write || !hit) {
            lrus[part_id].put(key);
        }
    }

    dump_counts(r_counts, "read", 20);
    dump_counts(w_counts, "write", 20);
    dump_counts(m_counts, "miss", 20);

    dump_rw_image("Read", r_counts, "Miss", m_counts, "read_miss", 3000);
    dump_miss_image("Read", r_counts, m_counts, "miss_ratio", 3000);

    for (auto &cnt : m_counts) {
        w_counts[cnt.first] += cnt.second;
    }
    dump_counts(w_counts, "hybrid", 20);

    dump_rw_image("Read", r_counts, "Write", w_counts, "read_write", 3000);

    delete zipf;
}

int main() { policy_write_back(0.6, 0.05, 0.6); }