#include <cassert>
#include <jsoncpp/json/json.h>
#include <fstream>

#include "utils/Bench.h"

void load_populations(
    std::string name,
    std::vector<double> *weights,
    std::vector<uint64_t> *buckets
) {
    assert(weights != nullptr);
    assert(buckets!= nullptr);
    // 文件读取
    std::ifstream file(name);
    if (!file.is_open()) {
        printf("Failed to open file: %s\n", name.c_str());
        return;
    }
    Json::CharReaderBuilder readerBuilder;
    Json::Value root;
    std::string errs;
    
    // 解析JSON文件
    if (!Json::parseFromStream(readerBuilder, file, &root, &errs)) {
        printf("Failed to parse JSON: %s\n", errs.c_str());
        return;
    }
    // 提取weights，解析List
    const Json::Value j_weights = root["popularityWeights"];
    if (j_weights.isArray()) {
        for (Json::Value::ArrayIndex i = 0; i < j_weights.size(); ++i) {
            weights->push_back(j_weights[i].asUInt64());
        }
    }
    // 提取buckets，解析List
    const Json::Value j_buckets = root["popularityBuckets"];
    if (j_buckets.isArray()) {
        for (Json::Value::ArrayIndex i = 0; i < j_buckets.size(); ++i) {
            buckets->push_back(j_buckets[i].asUInt64());
        }
    }
}

void load_sizes(
    std::string name,
    std::vector<uint64_t> *probs,
    std::vector<uint64_t> *ranges
) {
    assert(probs!= nullptr);
    assert(ranges!= nullptr);

    // 文件读取
    std::ifstream file(name);
    if (!file.is_open()) {
        printf("Failed to open file: %s\n", name.c_str());
        return;
    }
    Json::CharReaderBuilder readerBuilder;
    Json::Value root;
    std::string errs;

    // 解析JSON文件
    if (!Json::parseFromStream(readerBuilder, file, &root, &errs)) {
        printf("Failed to parse JSON: %s\n", errs.c_str());
        return;
    }
    // 提取probs，解析List
    const Json::Value j_probs = root["valSizeRangeProbability"];
    if (j_probs.isArray()) {
        for (Json::Value::ArrayIndex i = 0; i < j_probs.size(); ++i) {
            probs->push_back(j_probs[i].asUInt64());
        }
    }
    // 提取ranges，解析List
    const Json::Value j_ranges = root["valSizeRange"];
    if (j_ranges.isArray()) {
        for (Json::Value::ArrayIndex i = 0; i < j_ranges.size(); ++i) {
            ranges->push_back(j_ranges[i].asUInt64());
        }
    }
}