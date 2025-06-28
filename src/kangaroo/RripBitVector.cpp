#include <cassert>
#include <cstring>

#include "kangaroo/RripBitVector.h"

namespace {
uint16_t bitMask(uint8_t bitIdx) { return 1u << bitIdx; }

bool bitSet(uint8_t &bits, uint8_t bitIdx) {
    bits |= bitMask(bitIdx);
    return true;
}
bool bitGet(uint8_t &bits, uint8_t bitIdx) { return bits & bitMask(bitIdx); }
} // namespace

RripBitVector::RripBitVector(uint32_t numVectors, uint32_t vectorSize)
    : numVectors_{numVectors}, vectorSize_{vectorSize}, 
    bits_{std::make_unique<uint8_t[]>(numVectors_ * vectorSize_)} {

    // Don't have to worry about @bits_ memory:
    // make_unique initialized memory with 0
    return;
}

void RripBitVector::set(uint32_t bucketIdx, uint32_t keyIdx) {
    assert(bucketIdx < numVectors_);
    assert(keyIdx < vectorSize_ * 8);
    bitSet(bits_[bucketIdx * vectorSize_ + keyIdx / vectorSize_], keyIdx % vectorSize_);
}

bool RripBitVector::get(uint32_t bucketIdx, uint32_t keyIdx) {
    assert(bucketIdx < numVectors_);
    if (keyIdx >= vectorSize_ * 8) {
        return 0;
    }
    return bitGet(bits_[bucketIdx * vectorSize_ + keyIdx / vectorSize_], keyIdx % vectorSize_);
}

void RripBitVector::clear(uint32_t bucketIdx) {
    assert(bucketIdx < numVectors_);
    memset(&bits_[bucketIdx * vectorSize_], 0, vectorSize_);
}