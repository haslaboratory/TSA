#pragma once

#include <cstdint>
#include <string>
#include <deque>

#define KEY_LEN 24
#define MAX_KEY_LEN 256
#define MAX_VAL_LEN 8 * 1024 * 1024

enum OpType {
    OpInvalid = 0,
    OpGet,
    OpSet,
    OpDel,
    OpIgnore,
};

struct Request {
    uint32_t timestamp; // trace timestamp
    OpType op;
    uint64_t key;
    const char *value;
    uint32_t key_len;
    uint32_t value_len;
    uint32_t ttl;
};

void load_twitter(std::string name, std::deque<Request> *requests);

void load_meta(std::string name, std::deque<Request> *requests);

void dump_meta(std::string name, std::deque<Request> *requests);

void reformat_trace(std::deque<Request> *old_reqs, std::deque<Request> *new_reqs);

void print_request(Request request);