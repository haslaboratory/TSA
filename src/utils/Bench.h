#pragma once
#include <string>
#include <vector>
#include <cstdint>

void load_populations(
    std::string name,
    std::vector<double> *weights,
    std::vector<uint64_t> *buckets
);

void load_sizes(
    std::string name,
    std::vector<uint64_t> *probs,
    std::vector<uint64_t> *ranges
);