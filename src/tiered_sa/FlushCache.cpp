#include "tiered_sa/FlushCache.h"

FlushCache::Config &FlushCache::Config::validate() {
    if (buffer_num == 0 || part_num == 0) {
        printf("buffer_num and part_num cannot be 0\n");
        exit(-1);
    }

    if (page_size != 4096) {
        printf("page_size must be 4096 bytes\n");
        exit(-1);
    }

    if (page_size * buffer_num > 1024 * 1024 * 1024) {
        printf("page_size * buffer_num cannot exceed 1GB\n");
        exit(-1);
    }
    return *this;
}

FlushCache::FlushCache(Config &&config) 
    : config_(std::move(config.validate()))
{
    std::queue<CacheSlot *> free_q;
    const uint32_t buffer_per_part = (config.buffer_num + config.page_size - 1) / config.part_num;

    for (uint32_t i = 0; i < buffer_per_part * config.part_num; i++) {
        slots_.emplace_back(CacheSlot {
            Buffer {config.page_size, config.page_size},
            0
        });
    }

    for (uint32_t i = 0; i < config.part_num; i++) {
        assert(free_q.size() == 0);
        for (uint32_t j = buffer_per_part * i; j < buffer_per_part * (i + 1); j++) {
            free_q.push(&slots_[j]);
        }
        assert(free_q.size() == buffer_per_part);
        lru_lists_.emplace_back(std::move(LRUList<uint64_t, CacheSlot *>(buffer_per_part, &free_q)));
    }

    mutexes_ = std::make_unique<std::shared_mutex[]>(config.part_num);
}

Status FlushCache::read(uint64_t page_offset, uint8_t ver, std::function<Status(Buffer *)> read_cb) {
    uint64_t part_id = (page_offset / config_.page_size) % config_.part_num;
    
    std::unique_lock<std::shared_mutex> lock(mutexes_[part_id]);

    std::optional<CacheSlot *> ret = lru_lists_[part_id].get(page_offset);
    Buffer *buf = nullptr;
    bool flash_read = true;

    if (!ret.has_value()) {
        CacheSlot* slot = lru_lists_[part_id].alloc_or_evict(page_offset);
        slot->ver = ver;
        buf = &slot->buf;
    } else if (ret.value()->ver != ver) {
        ret.value()->ver = ver;
        buf = &ret.value()->buf;
    } else {
        buf = &ret.value()->buf;
        flash_read = false;
    }

    if (flash_read) {
        miss_cnt++;
        config_.device->read(page_offset, config_.page_size, buf->data());
    } else {
        hit_cnt++;
    }
    return read_cb(buf);
}

void FlushCache::dump_stats() const {
    printf("hit_cnt: %lu, miss_cnt: %lu\n", hit_cnt, miss_cnt);
}