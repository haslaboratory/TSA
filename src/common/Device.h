#pragma once

#include <memory>
#include <stdint.h>

#include "common/Buffer.h"
#include "common/Utils.h"

class Device {
public:
    // @param size    total size of the device
    explicit Device(uint64_t size)
        : Device{size, 0 /* max device write size */} {}

    // @param size          total size of the device
    // @param encryptor     encryption object
    // @param maxWriteSize  max device write size
    Device(uint64_t size, uint32_t maxWriteSize)
        : Device(size, kDefaultAlignmentSize, maxWriteSize) {}

    // @param size          total size of the device
    // @param encryptor     encryption object
    // @param ioAlignSize   alignment size for IO operations
    // @param maxWriteSize  max device write size
    Device(uint64_t size, uint32_t ioAlignSize, uint32_t maxWriteSize,
           uint32_t ioNoOfZones = 0, uint64_t ioZoneSize = 0,
           uint64_t ioZoneCapSize = 0)
        : size_(size), ioAlignmentSize_{ioAlignSize}, ioZoneSize_{std::move(
                                                          ioZoneSize)},
          ioZoneCapSize_{std::move(ioZoneCapSize)}, ioNoOfZones_{std::move(
                                                        ioNoOfZones)},
          maxWriteSize_(maxWriteSize) {
        if (ioAlignSize == 0) {
            printf("Invalid ioAlignSize %d\n", ioAlignSize);
            exit(-1);
        }
        if (maxWriteSize_ % ioAlignmentSize_ != 0) {
            printf("Invalid max write size %d ioAlignSize %d\n", maxWriteSize_,
                   ioAlignSize);
            exit(-1);
        }
    }
    virtual ~Device() = default;

    // Get the post-alignment size for the size of the data we intend to write
    size_t getIOAlignedSize(size_t size) const {
        return powTwoAlign(size, ioAlignmentSize_);
    }

    // Create an IO buffer of at least @size bytes that can be used for read and
    // write. For example, direct IO device allocates a properly aligned buffer.
    Buffer makeIOBuffer(size_t size) const {
        return Buffer{getIOAlignedSize(size), ioAlignmentSize_};
    }

    // Write buffer to the device. This call takes ownership of the buffer
    // and de-allocates it by end of the call.
    // @param buffer    Data to write to the device. It must be aligned the same
    //                  way as `makeIOBuffer` would return.
    // @param offset    Must be ioAlignmentSize_ aligned
    bool write(uint64_t offset, Buffer buffer);

    // Reads @size bytes from device at @deviceOffset and copys to @value
    // There must be sufficient space allocated already in the mutableView.
    // @offset and @size must be ioAligmentSize_ aligned
    // @offset + @size must be less than or equal to device size_
    // address in @value must be ioAligmentSize_ aligned
    bool read(uint64_t offset, uint32_t size, void *value);

    // Reads @size bytes from device at @deviceOffset into a Buffer allocated
    // If the offset is not aligned or size is not aligned for device IO
    // alignment, they both are aligned to do the read operation successfully
    // from the device and then Buffer is adjusted to return only the size
    // bytes from offset.
    Buffer read(uint64_t offset, uint32_t size);

    // Everything should be on device after this call returns.
    void flush() { flushImpl(); }

    // Returns the size of the device. All IO operations must be from [0, size)
    uint64_t getSize() const { return size_; }

    // Returns the alignment size for device io operations
    uint32_t getIOAlignmentSize() const { return ioAlignmentSize_; }
    bool reset(uint64_t offset, uint32_t size) {
        return resetImpl(offset, size);
    }
    bool finish(uint64_t offset, uint32_t size) {
        return finishImpl(offset, size);
    }
    uint64_t getIOZoneSize() const { return ioZoneSize_; }
    uint64_t getIOZoneCapSize() const { return ioZoneCapSize_; }
    uint64_t getIONrOfZones() const { return ioNoOfZones_; }

protected:
    virtual bool resetImpl(uint64_t offset, uint32_t size) = 0;
    virtual bool finishImpl(uint64_t offset, uint32_t size) = 0;
    virtual bool writeImpl(uint64_t offset, uint32_t size,
                           const void *value) = 0;
    virtual bool readImpl(uint64_t offset, uint32_t size, void *value) = 0;
    virtual void flushImpl() = 0;

private:
    //  mutable AtomicCounter bytesWritten_;
    //  mutable AtomicCounter bytesRead_;
    //  mutable AtomicCounter writeIOErrors_;
    //  mutable AtomicCounter readIOErrors_;
    //  mutable AtomicCounter encryptionErrors_;
    //  mutable AtomicCounter decryptionErrors_;

    //  mutable util::PercentileStats readLatencyEstimator_;
    //  mutable util::PercentileStats writeLatencyEstimator_;

    bool readInternal(uint64_t offset, uint32_t size, void *value);

    // size of the device. All offsets for write/read should be contained
    // below this.
    const uint64_t size_{0};

    // alignment granularity for the offsets and size to read/write calls.
    const uint32_t ioAlignmentSize_{kDefaultAlignmentSize};

    // Zone Size
    const uint64_t ioZoneSize_{0};

    // Zone Cap Size
    const uint64_t ioZoneCapSize_{0};

    // Number of zones
    const uint64_t ioNoOfZones_{0};

    // When write-io is issued, it is broken down into writeImpl calls at
    // this granularity. maxWriteSize_ 0 means no maximum write size.
    // maxWriteSize_ option allows splitting the large writes to smaller
    // writes so that the device read latency is not adversely impacted by
    // large device writes
    const uint32_t maxWriteSize_{0};

    static constexpr uint32_t kDefaultAlignmentSize{1};
};

std::unique_ptr<Device> createDirectIoZNSDevice(std::string fileName,
                                                uint64_t size,
                                                uint32_t ioAlignSize,
                                                uint32_t maxDeviceWriteSize);