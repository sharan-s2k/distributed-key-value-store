#pragma once

#include <cstdint>
#include <random>
#include <stdexcept>

namespace raftkv {

class DeterministicRandom {
public:
    explicit DeterministicRandom(std::uint64_t seed) : engine_(seed) {}

    std::uint64_t nextU64() {
        return engine_();
    }

    std::uint64_t range(std::uint64_t minInclusive, std::uint64_t maxInclusive) {
        if (minInclusive > maxInclusive) {
            throw std::invalid_argument("invalid deterministic random range");
        }
        std::uniform_int_distribution<std::uint64_t> dist(minInclusive, maxInclusive);
        return dist(engine_);
    }

    bool chance(double probability) {
        if (probability <= 0.0) return false;
        if (probability >= 1.0) return true;
        std::bernoulli_distribution dist(probability);
        return dist(engine_);
    }

private:
    std::mt19937_64 engine_;
};

} // namespace raftkv
