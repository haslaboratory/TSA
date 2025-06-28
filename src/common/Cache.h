#pragma once
#include <unordered_set>

#include "common/Types.h"

class CacheEngine {
public:
    virtual Status lookup(HashedKey hk, Buffer *value) = 0;
    virtual Status insert(HashedKey hk, BufferView value) = 0;
    virtual Status remove(HashedKey hk) = 0;
    virtual Status prefill(
        std::function<uint64_t ()> k_func,
        std::function<BufferView ()> v_func
    ) = 0;
};
