#pragma once

#include <mutex>
#include <shared_mutex>

#include "common/Device.h"
#include "common/Types.h"

class Wren {
public:
    class EuIterator {
        // Iterator, does not hold any locks
        // need to guarantee no writes to EU during GC process
    public:
        bool done() { return done_; }
        KangarooBucketId getBucket() { return kbid_; }
        EuIterator() : done_{true}, kbid_{0} {}

    private:
        friend Wren;

        explicit EuIterator(KangarooBucketId kbid) : kbid_{kbid} {}
        bool done_ = false;
        KangarooBucketId kbid_;
        int i_ = 0;
    };

    // Throw std::invalid_argument on bad config
    explicit Wren(Device &device, uint64_t numBuckets, uint64_t bucketSize,
                  uint64_t totalSize, uint64_t setOffset);
    ~Wren();

    Wren(const Wren &) = delete;
    Wren &operator=(const Wren &) = delete;

    Buffer read(KangarooBucketId kbid, bool &newBuffer);
    bool write(KangarooBucketId kbid, Buffer buffer, bool mustErase = false);

    bool shouldClean(double cleaningThreshold);
    bool waitClean(uint32_t euThreshold);
    void mustErase();

    EuIterator getEuIterator();
    EuIterator getNext(EuIterator); // of next zone to erase
    bool erase(); // will throw error if not all buckets are rewritten

    Buffer batchedRead(KangarooBucketId kbid, bool &newBuffer);
    bool batchedWrite(KangarooBucketId kbid, Buffer buffer, bool mustErase = false);

private:
    Device &device_;

    // Erase Unit Id
    class EuId {
    public:
        explicit EuId(uint64_t idx) : idx_{idx} {}

        bool operator==(const EuId &rhs) const noexcept {
            return idx_ == rhs.idx_;
        }
        bool operator!=(const EuId &rhs) const noexcept {
            return !(*this == rhs);
        }

        uint64_t index() const noexcept { return idx_; }

    private:
        uint64_t idx_;
    };

    struct EuIdentifier {
        EuId euid_; // location on device

        EuIdentifier() : euid_{EuId((uint64_t)-1)} {}
    };
    EuId calcEuId(uint32_t erase_unit, uint32_t offset);
    EuId findEuId(KangarooBucketId kbid);
    uint64_t getEuIdLoc(EuId euid);
    uint64_t getEuIdLoc(uint32_t erase_unit, uint32_t offset);

    // page to zone and offset mapping
    EuIdentifier *kbidToEuid_;

    // only implementing FIFO eviction
    std::shared_mutex writeMutex_;
    uint64_t euCap_; // actual erase unit usable capacity
    // zone num
    uint64_t numEus_;
    bool writeEmpty_{true};
    uint64_t writeEraseUnit_{0};
    uint64_t eraseEraseUnit_{0};
    uint32_t writeOffset_{0};
    uint64_t cleaningThreshold_{2};
    uint64_t numBuckets_;
    uint64_t bucketSize_;
    uint64_t setOffset_;
    uint64_t bucketsPerEu_;
};