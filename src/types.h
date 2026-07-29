#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace raftkv {

using NodeId = std::uint32_t;
using Term = std::uint64_t;
using LogIndex = std::uint64_t;
using TimeMs = std::uint64_t;
using RequestId = std::uint64_t;

enum class Role { Follower, Candidate, Leader };

inline const char* toString(Role role) {
    switch (role) {
        case Role::Follower: return "follower";
        case Role::Candidate: return "candidate";
        case Role::Leader: return "leader";
    }
    return "unknown";
}

enum class CommandType { Noop, Put, Delete };

inline const char* toString(CommandType type) {
    switch (type) {
        case CommandType::Noop: return "noop";
        case CommandType::Put: return "put";
        case CommandType::Delete: return "delete";
    }
    return "unknown";
}

struct Command {
    CommandType type{CommandType::Noop};
    RequestId requestId{0};
    std::string key;
    std::string value;

    static Command noop() { return {}; }

    static Command put(RequestId requestId, std::string key, std::string value) {
        return Command{CommandType::Put, requestId, std::move(key), std::move(value)};
    }

    static Command erase(RequestId requestId, std::string key) {
        return Command{CommandType::Delete, requestId, std::move(key), {}};
    }

    bool operator==(const Command&) const = default;
};

struct LogEntry {
    Term term{0};
    Command command{};
    bool operator==(const LogEntry&) const = default;
};

struct RequestVote {
    Term term{0};
    NodeId candidateId{0};
    LogIndex lastLogIndex{0};
    Term lastLogTerm{0};
};

struct RequestVoteResponse {
    Term term{0};
    bool voteGranted{false};
};

struct AppendEntries {
    Term term{0};
    NodeId leaderId{0};
    LogIndex prevLogIndex{0};
    Term prevLogTerm{0};
    std::vector<LogEntry> entries;
    LogIndex leaderCommit{0};
};

struct AppendEntriesResponse {
    Term term{0};
    bool success{false};
    LogIndex matchIndex{0};
    LogIndex conflictIndex{1};
};

using MessagePayload = std::variant<RequestVote, RequestVoteResponse, AppendEntries, AppendEntriesResponse>;

struct Envelope {
    std::uint64_t messageId{0};
    NodeId from{0};
    NodeId to{0};
    MessagePayload payload;
};

inline const char* messageName(const MessagePayload& payload) {
    return std::visit([](const auto& msg) -> const char* {
        using T = std::decay_t<decltype(msg)>;
        if constexpr (std::is_same_v<T, RequestVote>) return "RequestVote";
        if constexpr (std::is_same_v<T, RequestVoteResponse>) return "RequestVoteResponse";
        if constexpr (std::is_same_v<T, AppendEntries>) return "AppendEntries";
        if constexpr (std::is_same_v<T, AppendEntriesResponse>) return "AppendEntriesResponse";
        return "Unknown";
    }, payload);
}

} // namespace raftkv
