#pragma once

#include "types.h"

#include <optional>
#include <stdexcept>
#include <vector>

namespace raftkv {

struct PersistentState {
    Term currentTerm{0};
    std::optional<NodeId> votedFor;
    std::vector<LogEntry> log{LogEntry{0, Command::noop()}};
};

class StableStorage {
public:
    virtual ~StableStorage() = default;
    virtual const PersistentState& load() const = 0;
    virtual void saveTermAndVote(Term term, std::optional<NodeId> votedFor) = 0;
    virtual void saveLog(const std::vector<LogEntry>& log) = 0;
};

class SimulatedStorage final : public StableStorage {
public:
    const PersistentState& load() const override {
        return state_;
    }

    void saveTermAndVote(Term term, std::optional<NodeId> votedFor) override {
        state_.currentTerm = term;
        state_.votedFor = votedFor;
    }

    void saveLog(const std::vector<LogEntry>& log) override {
        if (log.empty() || log.front().term != 0) {
            throw std::logic_error("raft log must retain sentinel entry");
        }
        state_.log = log;
    }

private:
    PersistentState state_;
};

} // namespace raftkv
