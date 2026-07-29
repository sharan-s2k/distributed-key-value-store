#pragma once

#include "state_machine.h"
#include "storage.h"
#include "types.h"

#include <functional>
#include <optional>
#include <set>
#include <unordered_map>
#include <vector>

namespace raftkv {

struct RaftConfig {
    TimeMs electionTimeoutMinMs{150};
    TimeMs electionTimeoutMaxMs{300};
    TimeMs heartbeatIntervalMs{50};
};

class RaftNode {
public:
    using SendFn = std::function<void(Envelope)>;
    using ScheduleTimerFn = std::function<void(NodeId, TimeMs, std::uint64_t)>;
    using ApplyFn = std::function<void(NodeId, LogIndex, const Command&)>;

    RaftNode(
        NodeId id,
        std::vector<NodeId> peers,
        StableStorage& storage,
        RaftConfig config,
        SendFn send,
        ScheduleTimerFn scheduleElectionTimer,
        ScheduleTimerFn scheduleHeartbeatTimer,
        std::function<std::uint64_t(std::uint64_t, std::uint64_t)> randomRange,
        std::function<void(const std::string&)> trace,
        ApplyFn onApplied
    );

    void start(TimeMs now);
    void onElectionTimeout(TimeMs now, std::uint64_t timerGeneration);
    void onHeartbeatTimeout(TimeMs now, std::uint64_t timerGeneration);
    void onMessage(TimeMs now, const Envelope& envelope);
    bool submit(TimeMs now, Command command);

    NodeId id() const { return id_; }
    Role role() const { return role_; }
    Term currentTerm() const { return currentTerm_; }
    std::optional<NodeId> leaderId() const { return leaderId_; }
    LogIndex commitIndex() const { return commitIndex_; }
    LogIndex lastApplied() const { return lastApplied_; }
    const std::vector<LogEntry>& log() const { return log_; }
    const KeyValueStateMachine& stateMachine() const { return stateMachine_; }
    std::uint64_t electionGeneration() const { return electionGeneration_; }
    std::uint64_t heartbeatGeneration() const { return heartbeatGeneration_; }

private:
    void becomeFollower(TimeMs now, Term term, std::optional<NodeId> leader);
    void becomeCandidate(TimeMs now);
    void becomeLeader(TimeMs now);
    void resetElectionTimer(TimeMs now);
    void resetHeartbeatTimer(TimeMs now);
    void broadcastAppendEntries();
    void sendAppendEntries(NodeId peer);
    void advanceCommitIndex();
    void applyCommittedEntries();
    void handleRequestVote(TimeMs, const Envelope&, const RequestVote&);
    void handleRequestVoteResponse(TimeMs, const Envelope&, const RequestVoteResponse&);
    void handleAppendEntries(TimeMs, const Envelope&, const AppendEntries&);
    void handleAppendEntriesResponse(TimeMs, const Envelope&, const AppendEntriesResponse&);
    bool candidateLogAtLeastAsUpToDate(LogIndex, Term) const;
    std::size_t majority() const;
    LogIndex lastLogIndex() const;
    Term lastLogTerm() const;
    void persistTermAndVote();
    void persistLog();
    void trace(const std::string& message) const;

    NodeId id_;
    std::vector<NodeId> peers_;
    StableStorage& storage_;
    RaftConfig config_;
    SendFn send_;
    ScheduleTimerFn scheduleElectionTimer_;
    ScheduleTimerFn scheduleHeartbeatTimer_;
    std::function<std::uint64_t(std::uint64_t, std::uint64_t)> randomRange_;
    std::function<void(const std::string&)> trace_;
    ApplyFn onApplied_;

    Role role_{Role::Follower};
    Term currentTerm_{0};
    std::optional<NodeId> votedFor_;
    std::optional<NodeId> leaderId_;
    std::vector<LogEntry> log_;
    LogIndex commitIndex_{0};
    LogIndex lastApplied_{0};
    std::set<NodeId> votesReceived_;
    std::unordered_map<NodeId, LogIndex> nextIndex_;
    std::unordered_map<NodeId, LogIndex> matchIndex_;
    std::uint64_t electionGeneration_{0};
    std::uint64_t heartbeatGeneration_{0};
    KeyValueStateMachine stateMachine_;
};

} // namespace raftkv
