#include "tiered_sa/LogIndex.h"

PageBit::PageBit(uint64_t page_num): page_num_(page_num) {
    assert(page_num % 8 == 0);
    uint64_t byte_num = (page_num + 7) / 8;
    bits_ = std::unique_ptr<uint8_t[]>(new uint8_t[byte_num]);
    set_bits(0, page_num);
}

void PageBit::set_bit(uint64_t page_idx) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    bits_[page_idx / 8] |= 1 << (page_idx % 8);
}

void PageBit::clear_bit(uint64_t page_idx) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    bits_[page_idx / 8] &= ~(1 << (page_idx % 8));
}

void PageBit::set_bits(uint64_t start_idx, uint64_t cnt) {
    assert(start_idx + cnt <= page_num_ && start_idx % 8 == 0 && cnt % 8 == 0);
    std::unique_lock<std::shared_mutex> lock(mutex_);
    memset(bits_.get() + start_idx / 8, 0xff, cnt / 8);
}

void PageBit::clear_bits(uint64_t start_idx, uint64_t cnt) {
    assert(start_idx + cnt <= page_num_ && start_idx % 8 == 0 && cnt % 8 == 0);
    std::unique_lock<std::shared_mutex> lock(mutex_);
    memset(bits_.get() + start_idx / 8, 0, cnt / 8);
}

Status PageBit::check_valid_pages(uint64_t start_idx, uint64_t cnt, std::vector<uint64_t> *pages) {
    assert(start_idx + cnt <= page_num_ && start_idx % 8 == 0 && cnt % 8 == 0);
    std::shared_lock<std::shared_mutex> lock(mutex_);
    for (uint64_t i = start_idx; i < start_idx + cnt; i++) {
        if (bits_[i / 8] & (1 << (i % 8))) {
            pages->push_back(i);
        }
    }
    return Status::Ok;
}