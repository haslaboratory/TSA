#include <unistd.h>
#include <string>

#include <matplotlibcpp.h>

#include "common/Cache.h"
#include "common/Device.h"
#include "common/Rand.h"
#include "utils/Bench.h"
#include "utils/Trace.h"
#include "kangaroo/Config.h"
#include "kangaroo/Kangaroo.h"
#include "raw/Config.h"
#include "raw/RawSet.h"
#include "tiered_sa/Config.h"
#include "tiered_sa/TieredSA.h"

std::unique_ptr<Kangaroo> build_kangaroo(Device * device, uint64_t zone_num) {
    uint64_t log_zone_num = zone_num * 0.05;
    std::unique_ptr<KangarooProto> kangarooProto =
        std::make_unique<KangarooProto>();
    kangarooProto->setLayout(device->getIOZoneSize() * log_zone_num,
                             device->getIOZoneCapSize() * zone_num, 4096, 0);
    kangarooProto->setDevice(device);
    kangarooProto->setLog(device->getIOZoneCapSize() * log_zone_num, 0, 2, 8, 256, 128 * 1024,
                          device->getIOZoneCapSize());
    
    kangarooProto->setBloomFilter(3, 30);
    kangarooProto->setBitVector(2);

    kangarooProto->setFairyOptimizations(false);

    return kangarooProto->create();
}

std::unique_ptr<Kangaroo> build_fairy(Device *device, uint64_t zone_num) {
    uint64_t log_zone_num = zone_num * 0.05;
    std::unique_ptr<KangarooProto> kangarooProto =
        std::make_unique<KangarooProto>();
    kangarooProto->setLayout(device->getIOZoneSize() * log_zone_num,
        device->getIOZoneCapSize() * zone_num, 8192, 4096);
    kangarooProto->setDevice(device);
    kangarooProto->setLog(device->getIOZoneCapSize() * log_zone_num, 0, 2, 32, 512, 128 * 1024,
                          device->getIOZoneCapSize());
    
    kangarooProto->setBloomFilter(3, 30);
    kangarooProto->setBitVector(2);

    kangarooProto->setFairyOptimizations(true);

    return kangarooProto->create();
}

std::unique_ptr<RawSet> build_rawset(Device *device, uint64_t zone_num) {
    std::unique_ptr<RawSetProto> rawsetProto =
        std::make_unique<RawSetProto>();
    rawsetProto->set_layout(
        0,
        4096,
        device->getIOZoneCapSize(),
        zone_num
    );
    rawsetProto->set_device(device);

    rawsetProto->set_bloom_filter(3, 30);
    // rawsetProto->set_bit_vector(2);

    return rawsetProto->create();
}

std::unique_ptr<TieredSA> build_tiered_sa(Device *device, uint64_t zone_num) {
    uint64_t log_zone_num = zone_num * 0.05;
    std::unique_ptr<TieredSAProto> tiered_sa_proto = std::make_unique<TieredSAProto>();
    tiered_sa_proto->set_layout(
        0,
        4096,
        device->getIOZoneCapSize(),
        log_zone_num,
        zone_num - log_zone_num
    );
    tiered_sa_proto->set_mode(TieredSA::SetLayerMode::CROSSOVER);
    tiered_sa_proto->set_device(device);

    tiered_sa_proto->set_bloom_filter(3, 30);
    tiered_sa_proto->set_bit_vector(2);

    return tiered_sa_proto->create();
}

std::unique_ptr<RawLog> build_rawlog(Device *device, uint64_t zone_num) {
    std::unique_ptr<RawLogProto> rawlogProto =
        std::make_unique<RawLogProto>();

    rawlogProto->set_layout(
        0,
        4096,
        device->getIOZoneCapSize(),
        zone_num
    );

    rawlogProto->set_device(device);
    return rawlogProto->create();
}

std::unique_ptr<SwapLog> build_swaplog(Device *device, uint64_t zone_num) {
    std::unique_ptr<SwapLogProto> swaplogProto =
        std::make_unique<SwapLogProto>();
    swaplogProto->set_layout(
        4096,
        64 * 4096,
        device->getIOZoneCapSize(),
        zone_num,
        64,
        0
    );
    swaplogProto->set_index(
        4,
        device->getIOZoneSize() * zone_num
    );
    swaplogProto->set_device(device);
    return swaplogProto->create();
}

// 解析后的参数
std::string device_path;
std::string code_type;
std::string workload_type;
std::string population_file_path; // bench 负载的 population 文件路径
std::string size_file_path;       // bench 负载的 size 文件路径
std::string trace_file_path;      // trace 负载的文件路径
std::string trace_type;           // trace 负载类型，twitter 或 meta
double read_ratio = 0.0;
double object_size = 0.0;
bool need_prefill = false;

// 新增参数变量
uint32_t zone_num = 0;
uint32_t thread_num = 16; // 默认线程数为 16
uint32_t run_time = 30 * 60; // 默认运行时间 30 分钟，单位：秒

std::atomic_bool stop_flag = false;

void bench_thread(
    CacheEngine *cache,
    SampleBucketRandom *freq_dist,
    std::discrete_distribution<uint64_t> *size_dist
) {
    std::mt19937_64 rng(rdtsc());
    std::unique_ptr<uint8_t[]> fake_data = std::unique_ptr<uint8_t[]>(new uint8_t[4096]);
    Buffer lookup_buf;
    while (!stop_flag) {
        uint64_t key = (*freq_dist)(rng);
        uint64_t size = (*size_dist)(rng);
        if (cache->lookup(key, &lookup_buf) == Status::Ok) {
            cache->insert(key, BufferView(size - 8, fake_data.get()));
        }
    }
}

// 修改 trace_thread 函数，使其接收请求范围
void trace_thread(
    CacheEngine *cache,
    std::deque<Request> *requests,
    size_t start_idx,
    size_t end_idx
) {
    Buffer lookup_buf;
    std::unique_ptr<uint8_t[]> fake_data = std::unique_ptr<uint8_t[]>(new uint8_t[4096]);
    for (size_t i = start_idx; i < end_idx && !stop_flag; ++i) {
        Request *req = &(*requests)[i];
        switch (req->op) {
        case OpType::OpGet:
            if (cache->lookup(req->key, &lookup_buf) == Status::NotFound) {
                cache->insert(req->key, BufferView(req->key_len + req->value_len - 8, fake_data.get()));
            }
            break;
        case OpType::OpSet:
            cache->insert(req->key, BufferView(req->key_len + req->value_len - 8, fake_data.get()));
            break;
        case OpType::OpDel:
            cache->remove(req->key);
            break;
        default:
            // ignore
            break;
        }
    }
}

void rand_test(
    CacheEngine *cache,
    zipf_table_distribution<uint64_t, double> *key_dist
) {
    std::mt19937_64 rng(rdtsc());
    while (!stop_flag) {
        bool is_read = (rand64() % 100) <= read_ratio * 100;
        bool is_miss = false;
        if (is_read) {
            Status status = cache->lookup((*key_dist)(rng), nullptr);
            if (status == Status::NotFound) {
                is_miss = true;
            }
        }

        if (!is_read || is_miss) {
            cache->insert((*key_dist)(rng), BufferView(0, nullptr));
        }
    }
}

namespace plt = matplotlibcpp;
int main(int argc, char *argv[]) {
    int o;
    // 修改参数选项字符串，添加 trace 文件路径选项 -f
    const char *opt = "D:c:l:p:s:r:o:P:z:t:T:y:f:";

    while ((o = getopt(argc, argv, opt)) != -1) {
        switch (o) {
        case 'D':
            device_path = optarg;
            break;
        case 'c':
            code_type = optarg;
            break;
        case 'l':
            workload_type = optarg;
            break;
        case 'p':
            if (workload_type == "bench") {
                population_file_path = optarg;
            }
            break;
        case 's':
            if (workload_type == "bench") {
                size_file_path = optarg;
            } else {
                object_size = std::stod(optarg);
            }
            break;
        case 'r':
            read_ratio = std::stod(optarg);
            break;
        case 'o':
            object_size = std::stod(optarg);
            break;
        case 'P':
            need_prefill = true;
            break;
        case 'z':
            zone_num = std::stoul(optarg);
            break;
        case 't':
            thread_num = std::stoul(optarg);
            break;
        case 'T':
            run_time = std::stoul(optarg);
            break;
        case 'y':
            trace_type = optarg;
            break;
        case 'f':
            if (workload_type == "trace") {
                trace_file_path = optarg;
            }
            break;
        default:
            // 更新帮助信息中的参数标识
            printf("Usage: %s -D <device_path> -c <code_type> -l <workload_type> "
                   "[-p <population_file_path> -s <size_file_path> (for bench)] "
                   "[-y <trace_type> -f <trace_file_path> (for trace: twitter/meta)] "
                   "[-r <read_ratio> -o <object_size> (for rand)] "
                   "[-P (prefill)] [-z <zone_num>] [-t <thread_num>] [-T <run_time>]\n", argv[0]);
            return 1;
        }
    }

    // 检查必要参数是否提供
    if (device_path.empty() || code_type.empty() || workload_type.empty()) {
        printf("Error: device path, code type and workload type are required.\n");
        printf("Usage: %s -D <device_path> -c <code_type> -l <workload_type> "
               "[-p <population_file_path> -s <size_file_path> (for bench)] "
               "[-y <trace_type> -f <trace_file_path> (for trace: twitter/meta)] "
               "[-r <read_ratio> -o <object_size> (for rand)] "
               "[-P (prefill)] [-z <zone_num>] [-t <thread_num>] [-T <run_time>]\n", argv[0]);
        return 1;
    }

    // 检查负载类型相关参数
    if (workload_type == "bench") {
        if (population_file_path.empty() || size_file_path.empty()) {
            printf("Error: population and size file paths are required for bench workload type.\n");
            printf("Usage: %s -D <device_path> -c <code_type> -l <workload_type> "
                   "[-p <population_file_path> -s <size_file_path> (for bench)] "
                   "[-y <trace_type> -f <trace_file_path> (for trace: twitter/meta)] "
                   "[-r <read_ratio> -o <object_size> (for rand)] "
                   "[-P (prefill)] [-z <zone_num>] [-t <thread_num>] [-T <run_time>]\n", argv[0]);
            return 1;
        }
    } else if (workload_type == "trace") {
        if (trace_type != "twitter" && trace_type != "meta") {
            printf("Error: trace type must be either 'twitter' or 'meta' for trace workload type.\n");
            printf("Usage: %s -D <device_path> -c <code_type> -l <workload_type> "
                   "[-p <population_file_path> -s <size_file_path> (for bench)] "
                   "[-y <trace_type> -f <trace_file_path> (for trace: twitter/meta)] "
                   "[-r <read_ratio> -o <object_size> (for rand)] "
                   "[-P (prefill)] [-z <zone_num>] [-t <thread_num>] [-T <run_time>]\n", argv[0]);
            return 1;
        }
        if (trace_file_path.empty()) {
            printf("Error: trace file path is required for trace workload type.\n");
            printf("Usage: %s -D <device_path> -c <code_type> -l <workload_type> "
                   "[-p <population_file_path> -s <size_file_path> (for bench)] "
                   "[-y <trace_type> -f <trace_file_path> (for trace: twitter/meta)] "
                   "[-r <read_ratio> -o <object_size> (for rand)] "
                   "[-P (prefill)] [-z <zone_num>] [-t <thread_num>] [-T <run_time>]\n", argv[0]);
            return 1;
        }
    } else if (workload_type == "rand") {
        if (read_ratio <= 0.0 || object_size <= 0.0) {
            printf("Error: read ratio and object size are required for rand workload type.\n");
            printf("Usage: %s -D <device_path> -c <code_type> -l <workload_type> "
                   "[-p <population_file_path> -s <size_file_path> (for bench)] "
                   "[-y <trace_type> -f <trace_file_path> (for trace: twitter/meta)] "
                   "[-r <read_ratio> -o <object_size> (for rand)] "
                   "[-P (prefill)] [-z <zone_num>] [-t <thread_num>] [-T <run_time>]\n", argv[0]);
            return 1;
        }
    } else {
        printf("Error: unsupported workload type: %s\n", workload_type.c_str());
        printf("Usage: %s -D <device_path> -c <code_type> -l <workload_type> "
               "[-p <population_file_path> -s <size_file_path> (for bench)] "
               "[-y <trace_type> -f <trace_file_path> (for trace: twitter/meta)] "
               "[-r <read_ratio> -o <object_size> (for rand)] "
               "[-P (prefill)] [-z <zone_num>] [-t <thread_num>] [-T <run_time>]\n", argv[0]);
        return 1;
    }

    std::unique_ptr<Device> dev =
        createDirectIoZNSDevice(device_path.c_str(), 0, 4096, 0);

    std::unique_ptr<CacheEngine> cache;
    if (code_type == "kangaroo") {
        cache = build_kangaroo(dev.get(), zone_num);
    } else if (code_type == "fairy") {
        cache = build_fairy(dev.get(), zone_num);
    } else if (code_type == "tiered_sa") {
        cache = build_tiered_sa(dev.get(), zone_num);
    } else if (code_type == "rawset") {
        cache = build_rawset(dev.get(), zone_num);
    } else if (code_type == "rawlog") {
        cache = build_rawlog(dev.get(), zone_num);
    } else if (code_type == "swaplog") {
        cache = build_swaplog(dev.get(), zone_num);
    } else {
        printf("Error: unsupported code type: %s\n", code_type.c_str());
        return 1;
    }

    // 如果需要 prefill，这里添加 prefill 逻辑
    if (need_prefill) {
        // 定义生成键的函数
        auto key_generator = []() {
            return rand64();
        };

        // 定义生成值的函数，值大小固定为 256 字节
        std::unique_ptr<uint8_t[]> fake_data = std::make_unique<uint8_t[]>(256);
        auto value_generator = [&fake_data]() {
            return BufferView(256, fake_data.get());
        };

        // 调用 prefill 函数
        cache->prefill(key_generator, value_generator);
    }

    std::vector<std::thread> threads;
    std::unique_ptr<SampleBucketRandom> bench_key_dist;
    std::unique_ptr<std::discrete_distribution<uint64_t>> size_dist; // 声明 size_dist

    if (workload_type == "bench") {
        // 加载 bench 负载文件，创建 bench 线程
        std::vector<double> weights;
        std::vector<uint64_t> buckets;
        std::vector<uint64_t> probs;
        std::vector<uint64_t> ranges;
        load_populations(population_file_path.c_str(), &weights, &buckets);
        load_sizes(size_file_path.c_str(), &probs, &ranges);

        // 构建 std::discrete_distribution
        std::vector<double> probabilities;
        for (size_t i = 0; i < probs.size(); ++i) {
            probabilities.push_back(static_cast<double>(probs[i]));
        }
        size_dist = std::make_unique<std::discrete_distribution<uint64_t>>(probabilities.begin(), probabilities.end());

        bench_key_dist = std::make_unique<SampleBucketRandom>(std::move(weights), std::move(buckets));

        // 创建 bench 线程
        for (uint32_t i = 0; i < thread_num; ++i) {
            threads.emplace_back(bench_thread, cache.get(), bench_key_dist.get(), size_dist.get());
        }
    } else if (workload_type == "trace") {
        // 加载 trace 负载文件，创建 trace 线程
        std::deque<Request> old_reqs;
        std::deque<Request> requests;
        // 调用 load_trace 函数，传入 trace 类型和文件路径
        if (trace_type == "twitter") {
            load_twitter(trace_file_path, &old_reqs);
        } else {
            load_meta(trace_file_path, &old_reqs);
        }
        reformat_trace(&old_reqs, &requests);

        size_t total_requests = requests.size();
        size_t requests_per_thread = total_requests / thread_num;
        size_t remainder = total_requests % thread_num;

        size_t start_idx = 0;
        for (uint32_t i = 0; i < thread_num; ++i) {
            size_t end_idx = start_idx + requests_per_thread + (i < remainder ? 1 : 0);
            threads.emplace_back(trace_thread, cache.get(), &requests, start_idx, end_idx);
            start_idx = end_idx;
        }
    } else if (workload_type == "rand") {
        // 创建 rand 线程
        zipf_table_distribution<uint64_t, double> key_dist(1000, 0.99); // 示例参数
        for (uint32_t i = 0; i < thread_num; ++i) {
            threads.emplace_back(rand_test, cache.get(), &key_dist);
        }
    }

    // 使用新的线程数参数
    for (uint32_t i = 0; i < thread_num; i++) {
        // 这里 tiered_sa 和 use_zipf 未定义，需要确认
        // threads.emplace_back(std::thread(work_thread, tiered_sa.get(), use_zipf));
    }

    uint64_t start_ts = RdtscTimer::instance().ms();
    uint64_t last_ts = start_ts;

    // 使用新的运行时间参数，转换为毫秒
    while (true) {
        uint64_t cur_ts = RdtscTimer::instance().ms();
        if (cur_ts - last_ts < 10 * 1000) {
            fflush(stdout);
            last_ts = cur_ts;
        }
        // 使用新的运行时间参数
        if (cur_ts - start_ts > run_time * 1000) {
            stop_flag.store(true);
            break;
        }
    }

    for (auto &t : threads) {
        t.join();
    }

    return 0;
}