#include <sstream>
#include <cassert>
#include <unordered_map>
#include <vector>
#include <cstring>

#include "Trace.h"

const uint32_t TWITTER_PATERN_NUM = 8;
const uint32_t META_HEADER_NUM = 5;
const uint32_t META_PATERN_NUM = 4;

const char *twitter_op_str[TWITTER_PATERN_NUM] = {
    "get",
    "gets",
    "set",
    "add",
    "delete",
    "cas",
    "prepend",
    "append"
};

OpType twitter_op_map[TWITTER_PATERN_NUM] = {
    OpGet,
    OpGet,
    OpSet,
    OpSet,
    OpDel,
    OpGet,
    OpSet,
    OpSet
};

const char *meta_header[META_HEADER_NUM] = {
    "key",
    "op",
    "size",
    "op_count",
    "key_size"
};

const char *meta_op_str[META_PATERN_NUM] = {
    "GET",
    "GET_LEASE",
    "SET",
    "DELETE"
};

OpType meta_op_map[META_PATERN_NUM] = {
    OpGet,
    OpGet,
    OpSet,
    OpDel
};

const char *op_str[TWITTER_PATERN_NUM] = {
    "INVALID",
    "GET",
    "SET",
    "DELETE",
    "IGNORE"
};

char line[MAX_VAL_LEN + MAX_KEY_LEN];
const char mock_value[MAX_VAL_LEN] = {0};

void load_twitter(std::string name, std::deque<Request> *requests) {
    FILE *fp = fopen(name.c_str(), "r");
    if (fp == NULL) {
        printf("Error: cannot open trace file %s\n", name.c_str());
        exit(-1);
    }

    std::hash<std::string> hash_fn;

    while (fgets(line, MAX_VAL_LEN + MAX_KEY_LEN, fp) != NULL) {
        Request request;
        // 使用std::string按照','分割字符串
        std::stringstream ss(line);
        std::string item;
        std::vector<std::string> items;
        while (std::getline(ss, item, ',')) {
            items.push_back(item);
        }
        if (items.size() < 7) {
            printf("Error: invalid trace file %s: item: %ld\n", name.c_str(), items.size());
            exit(-1);
        }

        request.timestamp = std::stoi(items[0]);
        request.key = hash_fn(items[1]);
        request.value = &mock_value[0];
        request.key_len = std::stoi(items[2]);
        request.value_len = std::stoi(items[3]);
        request.op = OpInvalid;
        for (uint32_t i = 0; i < TWITTER_PATERN_NUM; i++) {
            if (twitter_op_str[i] == items[5]) {
                request.op = twitter_op_map[i];
                break;
            }
        }
        if (request.op == OpInvalid) {
            printf("Error: invalid request: %s", line);
            printf("Requests: %ld\n", requests->size());
            exit(-1);
        }
        if (request.op == OpIgnore) {
            printf("Ignore request: %s", line);
        }
        request.ttl = std::stoi(items[6]);

        
        requests->push_back(request);
    }
    fclose(fp);
    printf("Load trace done!\n");
}

void load_meta(std::string name, std::deque<Request> *requests) {
    FILE *fp = fopen(name.c_str(), "r");
    if (fp == NULL) {
        printf("Error: cannot open trace file %s\n", name.c_str());
        exit(-1);
    }
    
    // 读取第一行表头
    if (fgets(line, MAX_VAL_LEN + MAX_KEY_LEN, fp) == NULL) {
        printf("Error: invalid trace file %s\n", name.c_str());
        exit(-1);
    }

    std::vector<uint32_t> header_idx(META_HEADER_NUM);
    // 解析表头
    line[strlen(line) - 1] = '\0';
    std::stringstream ss(line);
    std::string header_item;
    std::vector<std::string> header_items;
    while (std::getline(ss, header_item, ',')) {
        header_items.push_back(header_item);
    }
    for (uint32_t i = 0; i < META_HEADER_NUM; i++) {
        for (uint32_t j = 0; j < header_items.size(); j++) {
            if (meta_header[i] == header_items[j]) {
                header_idx[i] = j;
            }
        }
    }

    for (uint32_t i = 0; i < META_HEADER_NUM; i++) {
        printf("header_idx[%d]: %d\n", i, header_idx[i]);
    }

    while (fgets(line, MAX_VAL_LEN + MAX_KEY_LEN, fp)!= NULL) {
        Request request;
        // 使用std::string按照','分割字符串
        std::stringstream ss(line);
        std::string item;
        std::vector<std::string> items;
        while (std::getline(ss, item, ',')) {
            items.push_back(item);
        }

        request.timestamp = 0;
        // 16 进制读取 key
        request.key = std::stoul(items[header_idx[0]], nullptr, 16);
        for (uint32_t i = 0; i < META_PATERN_NUM; i++) {
            if (std::string(meta_op_str[i]) == items[header_idx[1]]) {
                request.op = meta_op_map[i];
                break;
            }
        }

        if (request.op == OpInvalid) {
            printf("Error: op = %s, invalid request: %s", items[header_idx[1]].c_str(), line);
            printf("Requests: %ld\n", requests->size());
            exit(-1);
        }
        if (request.op == OpIgnore) {
            printf("Ignore request: %s", line);
        }

        request.value = &mock_value[0];
        request.key_len = std::stoi(items[header_idx[4]]);
        request.value_len = std::stoi(items[header_idx[2]]);

        uint32_t repeat_cnt = 1; // std::stoi(items[header_idx[3]]);
        for (uint32_t i = 0; i < repeat_cnt; i++) {
            requests->push_back(request);
        }
    }
    fclose(fp);
    printf("Load trace done! cnt: %ld\n", requests->size());
}

void dump_meta(std::string name, std::deque<Request> *requests) {
    FILE *fp = fopen(name.c_str(), "w");
    if (fp == NULL) {
        printf("Error: cannot open trace file %s\n", name.c_str());
        exit(-1);
    }
    fprintf(fp, "key,op,size,op_count,key_size\n");
    for (uint32_t i = 0; i < requests->size(); i++) {
        Request request = (*requests)[i];
        if (request.op == OpIgnore) {
            continue;
        }
        fprintf(fp, "%lu,%s,%d,%d,%d\n", request.key, op_str[request.op], request.value_len, 1, request.key_len);
    }
    fclose(fp);
}

void print_request(Request request) {
    printf("timestamp: %d, key: %lu, value: %s, key_len: %d, value_len: %d, ttl: %d, op: %s\n",
           request.timestamp, request.key, request.value, request.key_len, request.value_len,
           request.ttl, op_str[request.op]);
}

// meta 的 trace 需要重新 format
void reformat_trace(std::deque<Request> *old_reqs, std::deque<Request> *new_reqs) {
    uint64_t reformat_cnt = 0;
    std::unordered_map<uint64_t, uint32_t> value_lens;

    while (!old_reqs->empty()) {
        Request cur_req = old_reqs->front();
        old_reqs->pop_front();

        // value len
        if (cur_req.value_len != 0) {
            if (value_lens.find(cur_req.key) == value_lens.end()) {
                value_lens[cur_req.key] = cur_req.value_len;
            } else if (value_lens[cur_req.key] != cur_req.value_len) {
                // 竟然还有不同的 value len，难崩
            }
        }

        if (new_reqs->empty() || cur_req.op != OpGet || cur_req.value_len != 0) {
            new_reqs->push_back(cur_req);
            continue;
        }
        Request pre_req = new_reqs->back();
        if (pre_req.op == OpSet && pre_req.key == cur_req.key) {
            new_reqs->pop_back();
        }
        new_reqs->push_back(cur_req);
    }

    // modify get requests
    for (auto &req : *new_reqs) {
        if (req.value_len == 0 && req.op == OpGet && value_lens.find(req.key) != value_lens.end()) {
            req.value_len = value_lens[req.key];
            if (req.value_len < 2048) {
                reformat_cnt ++;
            }
        }
    }
    // filter large requests
    for (auto &req : *new_reqs) {
        if (req.value_len >= 2048) {
            req.op = OpIgnore;
        }
    }
    printf("Reformat trace done! size: %ld, reformatted: %ld\n", new_reqs->size(), reformat_cnt);
}