#pragma once

#include <vector>
#include <mutex>
#include <shared_mutex>

#include "common/Types.h"
#include "kangaroo/LogBucket.h"

class LogWriter {
public:
    struct Config {
        uint32_t slice_num{0};
        uint32_t page_size{0};

        Config &validate();
    };

    LogWriter(Config &&config);

    Status lookup(HashedKey hk, Buffer *value, uint32_t slice);

    Status insert(HashedKey hk, BufferView value, uint32_t slice, uint64_t *page_idx);

    Status remove(HashedKey hk, uint32_t slice);

    Status flush_log(uint32_t slice, std::function<void(Buffer, std::vector<HashedKey> &collected)> flush_cb);
private:
    uint64_t calc_page_idx(uint32_t slice);

    Config config_;

    Buffer bucket_buffer_;
    std::unique_ptr<LogBucket*[]> buckets_;
    std::unique_ptr<uint32_t[]> page_cnt_;
};