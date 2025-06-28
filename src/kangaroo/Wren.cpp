#include "kangaroo/Wren.h"

Wren::EuIterator Wren::getEuIterator() {
    // printf("WREN: getting iterator: write %ld, erase %ld\n", writeEraseUnit_,
    //        eraseEraseUnit_);
    for (uint64_t i = 0; i < numBuckets_; i++) {
        if (kbidToEuid_[i].euid_.index() / bucketsPerEu_ == eraseEraseUnit_) {
            return EuIterator(KangarooBucketId(i));
        }
    }
    return EuIterator();
}

Wren::EuIterator Wren::getNext(EuIterator euit) {
    for (uint64_t i = euit.getBucket().index() + 1; i < numBuckets_; i++) {
        if (kbidToEuid_[i].euid_.index() / bucketsPerEu_ == eraseEraseUnit_) {
            return EuIterator(KangarooBucketId(i));
        }
    }
    return EuIterator();
}

Wren::Wren(Device &device, uint64_t numBuckets, uint64_t bucketSize,
           uint64_t totalSize, uint64_t setOffset)
    : device_{device}, euCap_{device_.getIOZoneCapSize()},
      numEus_{totalSize / euCap_}, eraseEraseUnit_{0},
      numBuckets_{numBuckets}, bucketSize_{bucketSize}, setOffset_{setOffset},
      bucketsPerEu_{euCap_ / bucketSize_} {
    kbidToEuid_ = new EuIdentifier[numBuckets_];
    printf("Num WREN zones %ld with capacity 0x%lx total Size 0x%lx, "
           "bucketsPerEu_ 0x%lx, setOffset 0x%lx\n",
           numEus_, euCap_, totalSize, bucketsPerEu_, setOffset_);
}

Wren::~Wren() { delete kbidToEuid_; }

Wren::EuId Wren::calcEuId(uint32_t erase_unit, uint32_t offset) {
    uint64_t euOffset = erase_unit * bucketsPerEu_;
    return EuId(euOffset + offset);
}

Wren::EuId Wren::findEuId(KangarooBucketId kbid) {
    assert(kbid.index() < numBuckets_);
    return kbidToEuid_[kbid.index()].euid_;
}

uint64_t Wren::getEuIdLoc(uint32_t erase_unit, uint32_t offset) {
    return getEuIdLoc(calcEuId(erase_unit, offset));
}

uint64_t Wren::getEuIdLoc(EuId euid) {
    uint64_t zone_offset = euid.index() % bucketsPerEu_;
    uint64_t zone = euid.index() / bucketsPerEu_;
    uint64_t offset =
        setOffset_ + zone_offset * bucketSize_ + zone * device_.getIOZoneSize();
    return offset;
}

Buffer Wren::read(KangarooBucketId kbid, bool &newBuffer) {
    EuId euid = findEuId(kbid);
    if (euid.index() >= numEus_ * euCap_) {
        // kbid has not yet been defined
        newBuffer = true;
        return device_.makeIOBuffer(bucketSize_);
    }
    uint64_t loc = getEuIdLoc(euid);

    auto buffer = device_.makeIOBuffer(bucketSize_);
    assert(!buffer.isNull());
    newBuffer = false;

    const bool res = device_.read(loc, buffer.size(), buffer.data());
    if (!res) {
        printf("read failed at %ld euid, %ld.%ld calculated zone + offset, "
               "read zone %ld, loc %ld\n",
               euid.index(), euid.index() / (euCap_ / bucketSize_),
               euid.index() % (euCap_ / bucketSize_), euid.index() / bucketsPerEu_,
               loc);
        return {};
    }

    return buffer;
}

bool Wren::write(KangarooBucketId kbid, Buffer buffer, bool mustWrite) {
    assert(kbid.index() < numBuckets_);
    {
        // TODO: deserialize
        std::unique_lock<std::shared_mutex> lock{writeMutex_};
        if (mustWrite) {
            if ((eraseEraseUnit_ == writeEraseUnit_) && !writeEmpty_) {
                // check bucket
                uint32_t old_bucket_cnt = 0;
                for (uint32_t i = 0; i < numBuckets_; i++) {
                    EuId euid = findEuId(KangarooBucketId(i));
                    if (euid.index() / bucketsPerEu_ == eraseEraseUnit_) {
                        // printf("WREN: bucket %d is not empty: %lu!\n", i, euid.index());
                        old_bucket_cnt += 1;
                    }
                }
                if (old_bucket_cnt > 0) {
                    printf("WREN: bucket %d in EU: %ld is not empty!\n", old_bucket_cnt, eraseEraseUnit_);
                    assert(false);
                }
                erase();
            }
        }
        if (writeEraseUnit_ == eraseEraseUnit_ && !writeEmpty_) {
            // printf("**************WREN Writing caught up to "
            //        "erasing**************\n");
            return false;
        }

        if (writeOffset_ == 0) {
            printf("WREN Write: reseting zone %ld, %ld / %ld\n",
                   getEuIdLoc(writeEraseUnit_, 0) / device_.getIOZoneSize(),
                   writeEraseUnit_, numEus_);
            device_.reset(getEuIdLoc(writeEraseUnit_, 0),
                          device_.getIOZoneSize());
        }

        // TODO: need to update chain before changing and deal with
        // synchronization
        EuId euid = calcEuId(writeEraseUnit_, writeOffset_);
        uint64_t loc = getEuIdLoc(euid);
        // XLOGF(INFO, "WRWrenEN: Writing {} bucket to zone {}, offset {},
        // location
        // {}",
        //    kbid.index(), loc / device_.getIOZoneSize(),
        //    (loc % device_.getIOZoneSize()) / bucketSize_, loc);
        assert(euid.index() < numEus_ * euCap_);

        bool ret = device_.write(loc, std::move(buffer));
        if (!ret) {
            printf(
                "tried to write at %ld euid, %ld.%ld calculated zone + offset, "
                "write zone %ld, loc %ld\n",
                euid.index(), euid.index() / (euCap_ / bucketSize_),
                euid.index() % (euCap_ / bucketSize_), writeEraseUnit_, loc);
            kbidToEuid_[kbid.index()].euid_ = EuId(-1); // fail bucket write
            writeOffset_ =
                euCap_ / bucketSize_; // allow zone to reset for next write
        } else {
            kbidToEuid_[kbid.index()].euid_ = euid;
            writeOffset_++;
        }

        if (writeOffset_ >= euCap_ / bucketSize_) {
            uint64_t offset = getEuIdLoc(writeEraseUnit_, 0);
            uint64_t align_offset = offset - (offset % device_.getIOZoneSize());
            device_.finish(align_offset, device_.getIOZoneSize());
            printf(
                "WREN Write: finishing zone %ld old eu %ld / %ld, euid 0x%lx\n",
                getEuIdLoc(writeEraseUnit_, 0) / device_.getIOZoneSize(),
                writeEraseUnit_, numEus_, calcEuId(writeEraseUnit_, 0).index());
            writeEraseUnit_ = (writeEraseUnit_ + 1) % numEus_;
            // XLOGF(INFO, "WREN Write: new zone {} new eu {} / {}, erase at
            // {}",
            //    getEuIdLoc(writeEraseUnit_, 0)/device_.getIOZoneSize(),
            //    writeEraseUnit_, numEus_, eraseEraseUnit_);
            writeOffset_ = 0;
            writeEmpty_ = false;
        }

        /*if (kbid.index() % 20000 == 10) {
          XLOGF(INFO, "Writing bucket {} in euid zone {} offset {}, loc {}",
              kbid.index(), euid.index() / bucketsPerEu_, euid.index() %
        bucketsPerEu_, loc);
        }*/

        return ret;
    }
}

Buffer Wren::batchedRead(KangarooBucketId kbid, bool &newBuffer)
{

    return Buffer{};
}

bool Wren::batchedWrite(KangarooBucketId kbid, Buffer buffer,
                        bool mustErase)
{
    return false;
}

bool Wren::shouldClean(double cleaningThreshold) {
    if (numEus_ == 0) {
        return false;
    }
    uint32_t freeEus = 0;
    uint32_t writeEu = writeEraseUnit_;
    if (eraseEraseUnit_ == writeEu) {
        if (writeEmpty_) {
            freeEus = numEus_;
        } else {
            freeEus = 0;
        }
    } else if (eraseEraseUnit_ > writeEu) {
        freeEus = eraseEraseUnit_ - writeEu;
    } else {
        freeEus = eraseEraseUnit_ + (numEus_ - writeEu);
    }

    double euThreshold = cleaningThreshold * numEus_;

    if (euThreshold >= 1.0) {
        return freeEus <= euThreshold;
    } 
    // else if (freeEus == 1) {
    //     return writeOffset_ >= (1 - euThreshold) * euCap_ / bucketSize_;
    // }

    return freeEus <= 1;
}

void Wren::mustErase() {
    std::unique_lock<std::shared_mutex> lock{writeMutex_};
    if ((eraseEraseUnit_ == writeEraseUnit_) && !writeEmpty_) {
        erase();
    }
}

bool Wren::waitClean(uint32_t euThreshold) {
    uint32_t freeEus = 0;
    uint32_t writeEu = writeEraseUnit_;
    if (eraseEraseUnit_ == writeEu) {
        if (writeEmpty_) {
            freeEus = numEus_;
        } else {
            freeEus = 0;
        }
    } else if (eraseEraseUnit_ > writeEu) {
        freeEus = eraseEraseUnit_ - writeEu;
    } else {
        freeEus = eraseEraseUnit_ + (numEus_ - writeEu);
    }

    return freeEus <= euThreshold;
}

bool Wren::erase() {
    // EuId euid = calcEuId(eraseEraseUnit_, 0);
    eraseEraseUnit_ = (eraseEraseUnit_ + 1) % numEus_;
    printf("WREN Erase: new zone %ld new eu %ld / %ld\n",
           getEuIdLoc(eraseEraseUnit_, 0) / device_.getIOZoneSize(),
           eraseEraseUnit_, numEus_);
    // return device_.reset(euid.index(), euCap_);
    return true;
}
