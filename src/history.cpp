#include "history.h"

#include <algorithm>
#include <sstream>
#include <unordered_map>

namespace raftkv {

LinearizabilityResult checkWriteLinearizability(
    const std::vector<OperationRecord>& history,
    const std::unordered_map<std::string, std::string>& finalState
) {
    LinearizabilityResult result;
    std::unordered_map<std::string, std::vector<const OperationRecord*>> byKey;

    for (const auto& operation : history) {
        if (!operation.completedAt.has_value()) {
            ++result.pendingOperations;
            continue;
        }
        ++result.completedOperations;
        byKey[operation.key].push_back(&operation);
    }

    for (const auto& [key, operations] : byKey) {
        std::vector<const OperationRecord*> maximal;
        for (const auto* candidate : operations) {
            bool mustPrecedeAnother = false;
            for (const auto* other : operations) {
                if (candidate == other) continue;
                if (*candidate->completedAt < other->invokedAt) {
                    mustPrecedeAnother = true;
                    break;
                }
            }
            if (!mustPrecedeAnother) maximal.push_back(candidate);
        }

        const auto stateIt = finalState.find(key);
        bool explainable = false;
        for (const auto* last : maximal) {
            if (last->type == CommandType::Delete && stateIt == finalState.end()) {
                explainable = true;
            }
            if (
                last->type == CommandType::Put &&
                stateIt != finalState.end() &&
                stateIt->second == last->value
            ) {
                explainable = true;
            }
        }

        if (!explainable) {
            result.ok = false;
            std::ostringstream out;
            out << "final value for key '" << key
                << "' cannot be produced by any operation that may legally be last";
            result.explanation = out.str();
            return result;
        }
    }

    return result;
}

} // namespace raftkv
