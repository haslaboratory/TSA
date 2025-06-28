#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <libzbd/zbd.h>

#include "common/Device.h"

// ZNS Device
class ZNSDevice : public Device {
public:
    explicit ZNSDevice(int dev, struct zbd_info *info, struct zbd_zone *report,
                       uint32_t nr_zones, uint32_t zoneMask, uint64_t size,
                       uint32_t ioAlignSize, uint64_t ioZoneCapSize,
                       uint64_t ioZoneSize, uint32_t maxDeviceWriteSize)
        : Device{size,     ioAlignSize, maxDeviceWriteSize,
                 nr_zones, ioZoneSize,  ioZoneCapSize},
          dev_{std::move(dev)}, info_{info}, report_{std::move(report)},
          nr_zones_{std::move(nr_zones)}, zoneMask_{std::move(zoneMask)},
          ioZoneCapSize_{std::move(ioZoneCapSize)} {}
    ZNSDevice(const ZNSDevice &) = delete;
    ZNSDevice &operator=(const ZNSDevice &) = delete;

    ~ZNSDevice() override = default;

private:
    const int dev_{};
    struct zbd_info *info_;
    struct zbd_zone *report_;
    unsigned int nr_zones_;
    unsigned int zoneMask_;
    uint64_t ioZoneCapSize_;

    bool finishImpl(uint64_t offset, uint32_t len) override {
        // printf("finish zone offset: 0x%lx, len: 0x%x\n", offset, len);
        if (zbd_finish_zones(dev_, offset, len) < 0)
            return false;
        return true;
    }

    bool resetImpl(uint64_t offset, uint32_t len) override {
        if (!finishImpl(offset, len))
            return false;
        if (zbd_reset_zones(dev_, offset, len) < 0)
            return false;
        return true;
    }

    bool writeImpl(uint64_t offset, uint32_t size, const void *value) override {
        ssize_t bytesWritten;

        bytesWritten = ::pwrite(dev_, value, size, offset);
        if (bytesWritten != size) {
            printf("Error Writing to zone! offset: 0x%lx, size: 0x%x, "
                   "bytesWritten: %ld, error: %s\n",
                   offset, size, bytesWritten, strerror(errno));
            // exit(-1);
            return false;
        }
        return bytesWritten == size;
    }

    bool readImpl(uint64_t offset, uint32_t size, void *value) override {
        ssize_t bytesRead;
        bytesRead = ::pread(dev_, value, size, offset);
        return bytesRead == size;
    }
    void flushImpl() override { ::fsync(dev_); }
};

bool Device::write(uint64_t offset, Buffer buffer) {
    // printf("write offset: 0x%lx, size: 0x%lx\n", offset, buffer.size());
    const auto size = buffer.size();
    // TODO: fix to work on non-ZNS drives
    if (unlikely(offset + buffer.size() > ioNoOfZones_ * ioZoneSize_)) {
        printf("offset: 0x%lx buffer_size: %ld, zone_num: %ld, zone_size: 0x%lx\n", offset, buffer.size(), ioNoOfZones_, ioZoneSize_);
        assert(offset + buffer.size() <= ioNoOfZones_ * ioZoneSize_);
    }
    uint8_t *data = reinterpret_cast<uint8_t *>(buffer.data());
    assert(reinterpret_cast<uint64_t>(data) % ioAlignmentSize_ == 0ul);
    if (size < 4096) {
        return false;
    }

    auto remainingSize = size;
    auto maxWriteSize = (maxWriteSize_ == 0) ? remainingSize : maxWriteSize_;
    bool result = true;
    while (remainingSize > 0) {
        auto writeSize = std::min<size_t>(maxWriteSize, remainingSize);
        assert(offset % ioAlignmentSize_ == 0ul);
        assert(writeSize % ioAlignmentSize_ == 0ul);

        auto timeBegin = getSteadyClock();
        result = writeImpl(offset, writeSize, data);

        offset += writeSize;
        data += writeSize;
        remainingSize -= writeSize;
    }
    return result;
}

// reads size number of bytes from the device from the offset into value.
// Both offset and size are expected to be aligned for device IO operations.
// If successful and encryptor_ is defined, size bytes from
// validDataOffsetInValue offset in value are decrypted.
//
// returns true if successful, false otherwise.
bool Device::readInternal(uint64_t offset, uint32_t size, void *value) {
    assert(reinterpret_cast<uint64_t>(value) % ioAlignmentSize_ == 0ul);
    assert(offset % ioAlignmentSize_ == 0ul);
    assert(size % ioAlignmentSize_ == 0ul);
    if (unlikely(offset + size > ioNoOfZones_ * ioZoneSize_)) {
        printf("offset: 0x%lx buffer_size: %d\n", offset, size);
        assert(offset + size <= ioNoOfZones_ * ioZoneSize_);
    }
    auto timeBegin = getSteadyClock();
    bool result = readImpl(offset, size, value);
    if (!result) {
        return result;
    }
    return true;
}

// This API reads size bytes from the Device from offset into a Buffer and
// returns the Buffer. If offset and size are not aligned to device's
// ioAlignmentSize_, IO aligned offset and IO aligned size are determined
// and passed to device read. Upon successful read from the device, the
// buffer is adjusted to return the intended data by trimming the data in
// the front and back.
// An empty buffer is returned in case of error and the caller must check
// the buffer size returned with size passed in to check for errors.
Buffer Device::read(uint64_t offset, uint32_t size) {
    assert(offset + size <= ioNoOfZones_ * ioZoneSize_);
    uint64_t readOffset =
        offset & ~(static_cast<uint64_t>(ioAlignmentSize_) - 1ul);
    uint64_t readPrefixSize =
        offset & (static_cast<uint64_t>(ioAlignmentSize_) - 1ul);
    auto readSize = getIOAlignedSize(readPrefixSize + size);
    auto buffer = makeIOBuffer(readSize);
    bool result = readInternal(readOffset, readSize, buffer.data());
    if (!result) {
        return Buffer{};
    }
    buffer.trimStart(readPrefixSize);
    buffer.shrink(size);
    return buffer;
}

// This API reads size bytes from the Device from the offset into value.
// Both offset and size are expected to be IO aligned.
bool Device::read(uint64_t offset, uint32_t size, void *value) {
    return readInternal(offset, size, value);
}

std::unique_ptr<Device> createDirectIoZNSDevice(std::string fileName,
                                                uint64_t size,
                                                uint32_t ioAlignSize,
                                                uint32_t maxDeviceWriteSize) {

    struct zbd_zone *report;
    struct zbd_info *info;
    int flags{O_RDWR | O_DIRECT};
    int fd, ret;
    uint32_t nr_zones, zoneMask;
    uint64_t ioZoneCapSize, actDevSize;
    uint32_t count;

    // int HACK = 12582912; // TODO: remove;

    info = new zbd_info();
    fd = zbd_open(fileName.c_str(), flags, info);
    if (fd < 0) {
        printf("Unable to open file %s, Exception in zns device: %d \n",
               fileName.c_str(), errno);
        exit(-1);
    }

    ret = zbd_list_zones(fd, 0, size, ZBD_RO_ALL, &report, &nr_zones);
    if (ret < 0 || !nr_zones) {
        printf("report zone %d\n", nr_zones);
        exit(-1);
    }

    for (count = 0, actDevSize = 0; count < nr_zones; count++) {
        // actDevSize += HACK; // += report[count].capacity;
        actDevSize += report[count].capacity;
        // printf("zone id: %d, zone capacity: 0x%llx, zone size: 0x%llx\n", count, report[count].capacity, report[count].len);
    }

    /* minimum zone capacity */
    /* TODO: Done for region size,
    we should be able to map each region to different size */
    for (count = 0, ioZoneCapSize = 0; count < nr_zones; count++) {
        // if (!ioZoneCapSize || ioZoneCapSize > HACK) { //
        // (report[count].capacity / HACK)) {
        if (!ioZoneCapSize || ioZoneCapSize > (report[count].capacity)) {
            // ioZoneCapSize = HACK; //report[count].capacity;
            ioZoneCapSize = report[count].capacity;
        }
    }

    // if (info->zone_size > ioZoneCapSize) {
    //     printf("Zone size is larger than zone capacity: zone size %llx, zone "
    //            "capacity %lx\n",
    //            info->zone_size, ioZoneCapSize);
    //     exit(-1);
    // }

    // modify size
    if (size == 0) {
        size = info->nr_zones * ioZoneCapSize;
    }
    // modify maxDeviceWriteSize
    if (maxDeviceWriteSize == 0) {
        maxDeviceWriteSize = ioZoneCapSize;
    }

    if (size > actDevSize) {
        printf(
            "Size is larger than ZNS drive: drive size %ld MB, size %ld MB\n",
            actDevSize / (1024 * 1024), size / (1024 * 1024));
        exit(-1);
    }

    printf("device setup: 0x%lx size, zone cap 0x%lx\n", size, ioZoneCapSize);
    if (size % ioZoneCapSize) {
        printf(
            "Size should be alligned to zone capacity: capacity %ld MB: Needed "
            "Size: %ld MB\n",
            ioZoneCapSize / (1024 * 1024),
            (((size / ioZoneCapSize) + 1) * ioZoneCapSize) / (1024 * 1024));

        exit(-1);
    }

    if (size < actDevSize) {
        nr_zones = size / ioZoneCapSize;
    }
    zoneMask = info->zone_size - 1;

    printf("Creating ZNS Device with zoneSize 0x%x zoneCap 0x%lx size 0x%lx\n",
           zoneMask + 1, ioZoneCapSize, size);

    return std::make_unique<ZNSDevice>(
        std::move(fd), std::move(info), std::move(report), std::move(nr_zones),
        zoneMask, size, ioAlignSize, ioZoneCapSize, zoneMask + 1,
        maxDeviceWriteSize /* max device write size */);
}