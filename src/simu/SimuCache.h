#pragma once

#include <cstdint>
#include <vector>

#include "simu/SimuSet.h"
#include "common/Rand.h"

class MockCache {
public:
    virtual bool get(uint64_t key) = 0;
    virtual void put(uint64_t key) = 0;
    virtual uint64_t get_valid_size() = 0;
};

class MockSimpleCache : public MockCache {
private:
    void migrate(uint64_t part_id) {
        warm_bkts_[part_id].proc_pending();
        for (auto pre_key: batch_w_[part_id]) {
            if (rand32() % 100 < drop_ratio_ * 100) {
                // drop
                continue;
            }
            warm_bkts_[part_id].put(pre_key);
        }
        batch_w_[part_id].clear();
    }

    std::vector<BlockInnerRRIPSet> warm_bkts_;
    std::vector<std::deque<uint64_t>> batch_w_;
    uint64_t batch_cnt_{0};
    MockRRIPPerfCount perf_cnts_;

    // config
    uint64_t total_size_;
    uint64_t part_num_;
    double batched_ratio_;
    double drop_ratio_;
public:
    MockSimpleCache() = delete;
    MockSimpleCache(
        uint64_t total_size,
        uint64_t part_num,
        double batched_ratio,
        double drop_ratio
    ): total_size_(total_size), part_num_(part_num), batched_ratio_(batched_ratio), drop_ratio_(drop_ratio)
    {
        uint64_t part_obj_num = total_size_ / part_num_;
        for (uint32_t i = 0; i < part_num_; i++) {
            warm_bkts_.push_back(BlockInnerRRIPSet(part_obj_num, 3, &perf_cnts_));
            batch_w_.push_back(std::deque<uint64_t>());
        }
    }

    bool get(uint64_t key) override {
        uint64_t part_id = key % part_num_;
        for (auto pre_key: batch_w_[part_id]) {
            if (pre_key == key) {
                return true;
            }
        }

        if (warm_bkts_[part_id].get(key)) {
            return true;
        } else {
            return false;
        }
    }

    void put(uint64_t key) override {
        uint64_t part_id = key % part_num_;
        batch_w_[part_id].push_back(key);
        batch_cnt_ += 1;

        if (batch_cnt_ >= batched_ratio_* total_size_) {
            for (uint32_t i = 0; i < part_num_; i++) {
                migrate(i);
            }

            batch_cnt_ = 0;
        }
    }

    uint64_t get_valid_size() override {
        uint64_t valid_size = 0;
        for (uint64_t i = 0; i < part_num_; i++) {
            valid_size += warm_bkts_[i].get_size();
        }
        return valid_size;
    }
};

class MockLevelTieredCache: public MockCache {
private:
    void migrate(uint64_t part_id) {
        hot_sets_[part_id].proc_pending();

        std::deque<uint64_t> of_keys;
        for (auto pre_key: batch_w_[part_id]) {
            uint64_t of_key = hot_sets_[part_id].put(pre_key);
            if (of_key != UINT64_MAX) {
                of_keys.push_back(of_key);
            }
        }

        warm_sets_[part_id].batched_put(&of_keys);

        batch_w_[part_id].clear();
    }

    std::vector<BlockInnerRRIPSet> hot_sets_;
    std::vector<TieredSet> warm_sets_;
    std::vector<std::deque<uint64_t>> batch_w_;
    uint64_t batch_cnt_ {0};
    MockRRIPPerfCount perf_cnts_;

    // config
    uint64_t total_size_;
    uint64_t part_num_;
    uint64_t tiered_num_;
    double batched_ratio_;
public:
    MockLevelTieredCache() = delete;
    MockLevelTieredCache(
        uint64_t total_size,
        uint64_t part_num,
        uint64_t tiered_num,
        double batched_ratio
    ): total_size_(total_size),
    part_num_(part_num),
    tiered_num_(tiered_num),
    batched_ratio_(batched_ratio)
    {
        uint64_t part_obj_num = total_size_ / part_num_;
        for (uint64_t i = 0; i < part_num_; i++) {
            batch_w_.push_back(std::deque<uint64_t>());
            hot_sets_.push_back(BlockInnerRRIPSet(part_obj_num * 1.0 / (1 + tiered_num_), 3, &perf_cnts_));
            warm_sets_.push_back(TieredSet(part_obj_num * 1.0 * tiered_num_ / (1 + tiered_num_), 3, tiered_num_, &perf_cnts_));
        }
    }

    void put(uint64_t key) override {
        uint64_t part_id = key % part_num_;
        batch_w_[part_id].push_back(key);
        batch_cnt_ += 1;
        if (batch_cnt_ >= batched_ratio_ * total_size_) {
            for (uint64_t i = 0; i < part_num_; i++) {
                migrate(i);
            }
            batch_cnt_ = 0;
        }
    }

    bool get(uint64_t key) override {
        uint64_t part_id = key % part_num_;
        for (auto pre_key: batch_w_[part_id]) {
            if (pre_key == key) {
                return true;
            }
        }
        if (hot_sets_[part_id].get(key)) {
            return true;
        } else {
            return warm_sets_[part_id].get(key);
        }
    }

    uint64_t get_valid_size() override {
        uint64_t valid_size = 0;
        for (uint64_t i = 0; i < part_num_; i++) {
            valid_size += hot_sets_[i].get_size();
            valid_size += warm_sets_[i].get_size();
        }
        return valid_size;
    }
};

class MockLevelCache: public MockCache {
private:
    void migrate(uint64_t part_id) {
        warm_bkts_[part_id].proc_pending();
        hot_bkts_[part_id].proc_pending();
        for (auto pre_key: batch_w_[part_id]) {
            uint64_t removed = hot_bkts_[part_id].put(pre_key);
            if (rand64() % 100 < 24) {
                continue;
            }
            warm_bkts_[part_id].put(removed);
        }
        batch_w_[part_id].clear();
    }

    std::vector<std::deque<uint64_t>> batch_w_;
    std::vector<BlockInnerRRIPSet> hot_bkts_;
    std::vector<BlockInnerRRIPSet> warm_bkts_;
    uint64_t batch_cnt_ {0};
    MockRRIPPerfCount perf_cnts_;
    // config
    uint64_t total_size_;
    uint64_t part_num_;
    double split_ratio_;
    double batched_ratio_;

public:
    MockLevelCache() = delete;

    MockLevelCache(
        uint64_t total_size,
        uint64_t part_num,
        double split_ratio,
        double batched_ratio
    ): total_size_(total_size),
    part_num_(part_num),
    split_ratio_(split_ratio),
    batched_ratio_(batched_ratio)
    {
        uint64_t part_obj_num = total_size_ / part_num_;
        for (uint64_t i = 0; i < part_num_; i++) {
            batch_w_.push_back(std::deque<uint64_t>());
            hot_bkts_.push_back(BlockInnerRRIPSet(part_obj_num * split_ratio_, 3, &perf_cnts_));
            warm_bkts_.push_back(BlockInnerRRIPSet(part_obj_num - part_obj_num * split_ratio_, 3, &perf_cnts_));
        }
    }

    bool get(uint64_t key) override {
        uint64_t part_id = key % part_num_;
        for (auto pre_key: batch_w_[part_id]) {
            if (pre_key == key) {
                return true;
            }
        }
        if (hot_bkts_[part_id].get(key)) {
            return true;
        } else {
            return warm_bkts_[part_id].get(key);
        }
    }

    void put(uint64_t key) override {
        uint64_t part_id = key % part_num_;
        batch_w_[part_id].push_back(key);
        batch_cnt_ += 1;
        if (batch_cnt_ >= batched_ratio_ * total_size_) {
            for (uint64_t i = 0; i < part_num_; i++) {
                migrate(i);
            }
            batch_cnt_ = 0;
        }
    }

    uint64_t get_valid_size() override {
        uint64_t valid_size = 0;
        for (uint64_t i = 0; i < part_num_; i++) {
            valid_size += hot_bkts_[i].get_size();
            valid_size += warm_bkts_[i].get_size();
        }
        return valid_size;
    }
};

class MockFIFOCache: public MockCache {
private:
    std::vector<std::deque<uint64_t>> batch_w_;
    std::vector<FlashRawFIFOSet> bkts;
    uint64_t batch_cnt_ {0};
    // config
    uint64_t total_size_;
    uint64_t part_num_;
    double batched_ratio_;
public:
    MockFIFOCache() = delete;
    MockFIFOCache(
        uint64_t total_size,
        uint64_t part_num,
        double batched_ratio
    ): total_size_(total_size),
    part_num_(part_num),
    batched_ratio_(batched_ratio) {
        uint64_t part_obj_num = total_size_ / part_num_;
        for (uint64_t i = 0; i < part_num_; i++) {
            batch_w_.push_back(std::deque<uint64_t>());
            bkts.push_back(FlashRawFIFOSet(part_obj_num));
        }
    }

    bool get(uint64_t key) override {
        uint64_t part_id = key % part_num_;
        for (auto pre_key : batch_w_[part_id]) {
            if (pre_key == key) {
                return true;
            }
        }

        return bkts[part_id].get(key);
    }

    void put(uint64_t key) override {
        uint64_t part_id = key % part_num_;
        batch_w_[part_id].push_back(key);
        batch_cnt_ += 1;
        if (batch_cnt_ >= batched_ratio_* total_size_) {
            for (uint32_t i = 0; i < part_num_; i++) {
                for (auto pre_key: batch_w_[i]) {
                    bkts[i].put(pre_key);
                }
                batch_w_[i].clear();
            }
            batch_cnt_ = 0;
        }
    }

    virtual uint64_t get_valid_size() override {
        uint64_t valid_size = 0;
        for (uint64_t i = 0; i < part_num_; i++) {
            valid_size += bkts[i].get_size();
        }
        return valid_size;
    }
};

class MockFairyCache: public MockCache {
private:
    uint64_t batch_cnt_{0};
    std::vector<std::deque<uint64_t>> batch_w_;
    std::vector<BlockInnerRRIPSet> hot_bkts_;
    std::vector<BlockInnerRRIPSet> bkts_;
    std::vector<uint64_t> w_cnts_;
    MockRRIPPerfCount perf_cnts_;
    // config
    uint64_t total_size_;
    uint64_t part_num_;
    double batched_ratio_;

    void migrate(uint64_t part_id) {
        bkts_[part_id].flush_all_pending();
        for (auto pre_key: batch_w_[part_id]) {
            bkts_[part_id].put(pre_key);
        }
        batch_w_[part_id].clear();

        if (rand64() % 5 == 0) {
            bkts_[part_id].redived_hot(&hot_bkts_[part_id]);
        }
    }
public:
    MockFairyCache(
        uint64_t total_size,
        uint64_t part_num,
        double batched_ratio
    ): total_size_(total_size),
    part_num_(part_num),
    batched_ratio_(batched_ratio) {
        uint64_t part_obj_num = total_size_ / part_num_;
        for (uint32_t i = 0; i < part_num_; i++) {
            hot_bkts_.push_back(BlockInnerRRIPSet(part_obj_num * 1 / 2, 3, &perf_cnts_));
            bkts_.push_back(BlockInnerRRIPSet(part_obj_num  * 1 / 2, 3, &perf_cnts_));
            batch_w_.push_back(std::deque<uint64_t>());
            w_cnts_.push_back(0);
        }
    }
    
    bool get(uint64_t key) override {
        uint64_t part_id = key % part_num_;

        for (auto pre_key : batch_w_[part_id]) {
            if (pre_key == key) {
                return true;
            }
        }


        if (bkts_[part_id].get(key)) {
            return true;
        } else {
            return hot_bkts_[part_id].get(key);
        }
    }

    void put(uint64_t key) override {
        uint64_t part_id = key % part_num_;
        batch_w_[part_id].push_back(key);
        batch_cnt_ += 1;
        if (batch_cnt_ >= batched_ratio_ * total_size_) {
            for (uint64_t i = 0; i < part_num_; i++) {
                migrate(i);
            }
            batch_cnt_ = 0;
        }
    }

    uint64_t get_valid_size() override {
        uint64_t valid_size = 0;
        for (uint64_t i = 0; i < part_num_; i++) {
            valid_size += bkts_[i].get_size();
            valid_size += hot_bkts_[i].get_size();
        }
        return valid_size;
    }
};

class MockTieredSACache: public MockCache {
private:
    std::vector<std::deque<uint64_t>> batch_w_;
    std::vector<BlockInnerRRIPSet> middle_bkts_;
    std::vector<BlockInnerRRIPSet> bottom_bkts_;
    uint64_t part_num_ = 0;
public:
    bool get(uint64_t key) override {
        uint64_t part_id = key % part_num_;
        for (auto pre_key: batch_w_[part_id]) {
            if (pre_key == key) {
                return true;
            }
        }
        if (middle_bkts_[part_id].get(key)) {
            return true;
        } else {
            return bottom_bkts_[part_id].get(key);
        }
    }

    void put(uint64_t key) override {
        uint64_t part_id = key % part_num_;
        batch_w_[part_id].push_back(key);
    }

    uint64_t get_valid_size() override {
        uint64_t valid_size = 0;
        for (uint64_t i = 0; i < part_num_; i++) {
            valid_size += middle_bkts_[i].get_size();
            valid_size += bottom_bkts_[i].get_size();
        }
        return valid_size;
    }
};