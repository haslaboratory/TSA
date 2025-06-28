#pragma once

#include <unordered_map>
#include <mutex>
#include <shared_mutex>

#include "common/Types.h"

// 原本应该实现两级索引以减少内存占用，暂时使用 Chained Index

// For Valid Check
// 加锁好了
class PageBit {
public:
    PageBit(uint64_t page_num);
    
    void set_bit(uint64_t page_idx);
    void clear_bit(uint64_t page_idx);
    void set_bits(uint64_t start_idx, uint64_t cnt);
    void clear_bits(uint64_t start_idx, uint64_t cnt);

    Status check_valid_pages(uint64_t start_idx, uint64_t cnt, std::vector<uint64_t> *pages);
private:
    uint64_t page_num_;
    std::unique_ptr<uint8_t[]> bits_;

    std::shared_mutex mutex_;
};