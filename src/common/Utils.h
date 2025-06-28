#pragma once

#include <cassert>
#include <chrono>
#include <cstdint>
#include <memory>

#include "common/Types.h"

inline std::chrono::nanoseconds getSteadyClock() {
    auto dur = std::chrono::steady_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(dur);
}

inline std::chrono::seconds getSteadyClockSeconds() {
    auto dur = std::chrono::steady_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::seconds>(dur);
}

inline std::chrono::microseconds toMicros(std::chrono::nanoseconds t) {
    return std::chrono::duration_cast<std::chrono::microseconds>(t);
}

inline size_t powTwoAlign(size_t size, size_t boundary) {
    // XDCHECK(folly::isPowTwo(boundary)) << boundary;
    return (size + (boundary - 1)) & ~(boundary - 1);
}

// Estimates actual slot size that malloc(@size) actually allocates. This
// better estimates actual memory used.
inline size_t mallocSlotSize(size_t bytes) {
    if (bytes <= 8) {
        return 8;
    }
    if (bytes <= 128) {
        return powTwoAlign(bytes, 16);
    }
    if (bytes <= 512) {
        return powTwoAlign(bytes, 64);
    }
    if (bytes <= 4096) {
        return powTwoAlign(bytes, 256);
    }
    // Accurate till 4MB
    return powTwoAlign(bytes, 4096);
}

inline const uint8_t *bytePtr(const void *ptr) {
    return reinterpret_cast<const uint8_t *>(ptr);
}

template <typename T> inline bool between(T x, T a, T b) {
    return a <= x && x <= b;
}

template <typename T> inline bool betweenStrict(T x, T a, T b) {
    return a < x && x < b;
}

inline uint64_t hashInt(uint32_t v) {
    uint64_t h = v;
    h = (h << 5u) - h + ((unsigned char)(v >> 8u));
    h = (h << 5u) - h + ((unsigned char)(v >> 16u));
    h = (h << 5u) - h + ((unsigned char)(v >> 24u));
    return h;
}

/// Murmur-inspired hashing.
constexpr uint64_t hash_128_to_64(const uint64_t upper,
                                  const uint64_t lower) noexcept {
    const uint64_t k_mul = 0x9ddfea08eb382d69ULL;
    uint64_t A = (lower ^ upper) * k_mul;
    A ^= (A >> 47);
    uint64_t B = (upper ^ A) * k_mul;
    B ^= (B >> 47);
    B *= k_mul;
    return B;
}

// simple checksum
inline uint32_t simple_checksum(const uint8_t *data, size_t size) {
    uint32_t sum = 0;
    for (size_t i = 0; i < size; i++) {
        sum = (sum << 3) + data[i];
    }
    return sum;
}

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

/// Check a condition at runtime. If the condition is false, throw exception.
inline static void rt_assert(bool condition, const char *throw_str) {
    if (unlikely(!condition)) {
        printf("Assertion failed: %s\n", throw_str);
        exit(-1);
    }
}

/// Return the TSC
inline static size_t rdtsc() {
    uint64_t rax;
    uint64_t rdx;
    asm volatile("rdtsc" : "=a"(rax), "=d"(rdx));
    return static_cast<size_t>((rdx << 32) | rax);
}

inline static void nano_sleep(size_t ns, double freq_ghz) {
    size_t start = rdtsc();
    size_t end = start;
    size_t upp = static_cast<size_t>(freq_ghz * ns);
    while (end - start < upp)
        end = rdtsc();
}

/// Simple time that uses std::chrono
class ChronoTimer {
public:
    ChronoTimer() { reset(); }
    inline void reset() { start_time_ = std::chrono::high_resolution_clock::now(); }

    /// Return seconds elapsed since this timer was created or last reset
    inline double get_sec() const { return get_ns() / 1e9; }

    /// Return milliseconds elapsed since this timer was created or last reset
    inline double get_ms() const { return get_ns() / 1e6; }

    /// Return microseconds elapsed since this timer was created or last reset
    inline double get_us() const { return get_ns() / 1e3; }

    /// Return nanoseconds elapsed since this timer was created or last reset
    inline size_t get_ns() const {
        return static_cast<size_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::high_resolution_clock::now() - start_time_)
                .count());
    }

private:
    std::chrono::time_point<std::chrono::high_resolution_clock> start_time_;
};

inline static double measure_rdtsc_freq() {
    ChronoTimer chrono_timer;
    const uint64_t rdtsc_start = rdtsc();

    // Do not change this loop! The hardcoded value below depends on this loop
    // and prevents it from being optimized out.
    uint64_t sum = 5;
    for (uint64_t i = 0; i < 1000000; i++) {
        sum += i + (sum + i) * (i % sum);
    }
    rt_assert(sum == 13580802877818827968ull,
              "Error in RDTSC freq measurement");

    const uint64_t rdtsc_cycles = rdtsc() - rdtsc_start;
    const double freq_ghz = rdtsc_cycles * 1.0 / chrono_timer.get_ns();
    rt_assert(freq_ghz >= 0.5 && freq_ghz <= 5.0, "Invalid RDTSC frequency");

    return freq_ghz;
}

/// Convert cycles measured by rdtsc with frequence \p freq_ghz to seconds
inline static double to_sec(size_t cycles, double freq_ghz) {
    return (cycles / (freq_ghz * 1000000000));
}

/// Convert cycles measured by rdtsc with frequence \p freq_ghz to msec
inline static double to_msec(size_t cycles, double freq_ghz) {
    return (cycles / (freq_ghz * 1000000));
}

/// Convert cycles measured by rdtsc with frequence \p freq_ghz to usec
inline static double to_usec(size_t cycles, double freq_ghz) {
    return (cycles / (freq_ghz * 1000));
}

inline static size_t ms_to_cycles(double ms, double freq_ghz) {
    return static_cast<size_t>(ms * 1000 * 1000 * freq_ghz);
}

inline static size_t us_to_cycles(double us, double freq_ghz) {
    return static_cast<size_t>(us * 1000 * freq_ghz);
}

inline static size_t ns_to_cycles(double ns, double freq_ghz) {
    return static_cast<size_t>(ns * freq_ghz);
}

/// Convert cycles measured by rdtsc with frequence \p freq_ghz to nsec
inline static double to_nsec(size_t cycles, double freq_ghz) {
    return (cycles / freq_ghz);
}

/// Simple time that uses RDTSC
class TscTimer {
public:
    size_t start_tsc_ = 0;
    size_t tsc_sum_ = 0;
    size_t num_calls_ = 0;

    inline void start() { start_tsc_ = rdtsc(); }
    inline void stop() {
        tsc_sum_ += (rdtsc() - start_tsc_);
        num_calls_++;
    }

    inline void reset() {
        start_tsc_ = 0;
        tsc_sum_ = 0;
        num_calls_ = 0;
    }

    inline size_t avg_cycles() const { return tsc_sum_ / num_calls_; }
    inline double avg_sec(double freq_ghz) const {
        return to_sec(avg_cycles(), freq_ghz);
    }

    inline double avg_usec(double freq_ghz) const {
        return to_usec(avg_cycles(), freq_ghz);
    }

    inline double avg_nsec(double freq_ghz) const {
        return to_nsec(avg_cycles(), freq_ghz);
    }
};

class RdtscTimer {
public:
    inline uint64_t us() const { return to_usec(rdtsc() - start_tsc_, freq_ghz_); }
    inline uint64_t ms() const { return to_msec(rdtsc() - start_tsc_, freq_ghz_); }
    inline uint64_t sec() const { return to_sec(rdtsc() - start_tsc_, freq_ghz_); }
    inline static RdtscTimer &instance() {
        static RdtscTimer timer(measure_rdtsc_freq());
        return timer;
    }

private:
    RdtscTimer(double freq_ghz) :  start_tsc_(rdtsc()), freq_ghz_(freq_ghz) {}
    uint64_t start_tsc_{0};
    double freq_ghz_{0.0};
};

static const int tagSeed = 23;
inline uint32_t createTag(HashedKey hk, uint32_t tagLength) {
    return hash_128_to_64(hk, tagSeed) % (1 << tagLength);
}