#pragma once

#include "types.h"

#include <filesystem>
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
    const PersistentState& load() const override { return state_; }

    void saveTermAndVote(Term term, std::optional<NodeId> votedFor) override {
        state_.currentTerm = term;
        state_.votedFor = votedFor;
    }

    void saveLog(const std::vector<LogEntry>& log) override {
        validateLog(log);
        state_.log = log;
    }

private:
    static void validateLog(const std::vector<LogEntry>& log) {
        if (log.empty() || log.front().term != 0 ||
            log.front().command.type != CommandType::Noop) {
            throw std::logic_error("raft log must retain sentinel entry");
        }
    }

    PersistentState state_;
};

/** Append-only disk representation of Raft's persistent state.
 *
 * Each save appends one checksummed binary record and fsyncs it before returning.
 * On startup, the WAL is replayed from the beginning to rebuild currentTerm,
 * votedFor, and the latest complete log snapshot. An incomplete/corrupted final
 * record is treated as a torn tail write and truncated safely. Corruption in the
 * middle of the file is reported as an error.
 */
class DiskWalStorage final : public StableStorage {
public:
    explicit DiskWalStorage(std::filesystem::path walPath);

    const PersistentState& load() const override { return state_; }
    void saveTermAndVote(Term term, std::optional<NodeId> votedFor) override;
    void saveLog(const std::vector<LogEntry>& log) override;

    const std::filesystem::path& path() const { return walPath_; }
    std::uintmax_t fileSize() const;

private:
    enum class RecordType : std::uint16_t {
        TermAndVote = 1,
        LogSnapshot = 2
    };

    void replay();
    void appendRecord(RecordType type, const std::vector<std::uint8_t>& payload);
    static void validateLog(const std::vector<LogEntry>& log);

    std::filesystem::path walPath_;
    PersistentState state_;
};

} // namespace raftkv