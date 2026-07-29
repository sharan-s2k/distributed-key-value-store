#pragma once

#include "types.h"

#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace raftkv {

struct OperationRecord {
    RequestId requestId{0};
    CommandType type{CommandType::Noop};
    std::string key;
    std::string value;
    TimeMs invokedAt{0};
    std::optional<TimeMs> completedAt;
    std::uint32_t attempts{0};
};

struct LinearizabilityResult {
    bool ok{true};
    std::string explanation{"history is write-linearizable"};
    std::size_t completedOperations{0};
    std::size_t pendingOperations{0};
};

LinearizabilityResult checkWriteLinearizability(
    const std::vector<OperationRecord>& history,
    const std::unordered_map<std::string, std::string>& finalState
);

} // namespace raftkv
