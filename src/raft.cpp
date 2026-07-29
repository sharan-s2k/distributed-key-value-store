#include "raft.h"

#include <algorithm>
#include <sstream>
#include <stdexcept>
#include <type_traits>

namespace raftkv {

RaftNode::RaftNode(
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
)
    : id_(id),
      peers_(std::move(peers)),
      storage_(storage),
      config_(config),
      send_(std::move(send)),
      scheduleElectionTimer_(std::move(scheduleElectionTimer)),
      scheduleHeartbeatTimer_(std::move(scheduleHeartbeatTimer)),
      randomRange_(std::move(randomRange)),
      trace_(std::move(trace)),
      onApplied_(std::move(onApplied)) {
    const auto& persisted = storage_.load();
    currentTerm_ = persisted.currentTerm;
    votedFor_ = persisted.votedFor;
    log_ = persisted.log;
    if (log_.empty()) {
        log_.push_back(LogEntry{0, Command::noop()});
    }
}

void RaftNode::start(TimeMs now) {
    role_ = Role::Follower;
    leaderId_.reset();
    resetElectionTimer(now);
    trace("started role=follower term=" + std::to_string(currentTerm_));
}

void RaftNode::onElectionTimeout(TimeMs now, std::uint64_t timerGeneration) {
    if (timerGeneration != electionGeneration_ || role_ == Role::Leader) return;
    becomeCandidate(now);
}

void RaftNode::onHeartbeatTimeout(TimeMs now, std::uint64_t timerGeneration) {
    if (timerGeneration != heartbeatGeneration_ || role_ != Role::Leader) return;
    broadcastAppendEntries();
    resetHeartbeatTimer(now);
}

void RaftNode::onMessage(TimeMs now, const Envelope& envelope) {
    std::visit([&](const auto& message) {
        using T = std::decay_t<decltype(message)>;
        if constexpr (std::is_same_v<T, RequestVote>) {
            handleRequestVote(now, envelope, message);
        } else if constexpr (std::is_same_v<T, RequestVoteResponse>) {
            handleRequestVoteResponse(now, envelope, message);
        } else if constexpr (std::is_same_v<T, AppendEntries>) {
            handleAppendEntries(now, envelope, message);
        } else if constexpr (std::is_same_v<T, AppendEntriesResponse>) {
            handleAppendEntriesResponse(now, envelope, message);
        }
    }, envelope.payload);
}

bool RaftNode::submit(TimeMs /*now*/, Command command) {
    if (role_ != Role::Leader) return false;

    log_.push_back(LogEntry{currentTerm_, std::move(command)});
    persistLog();
    matchIndex_[id_] = lastLogIndex();
    trace("client_entry_appended index=" + std::to_string(lastLogIndex()));
    broadcastAppendEntries();

    if (majority() == 1) {
        commitIndex_ = lastLogIndex();
        applyCommittedEntries();
    }
    return true;
}

void RaftNode::becomeFollower(TimeMs now, Term term, std::optional<NodeId> leader) {
    if (term > currentTerm_) {
        currentTerm_ = term;
        votedFor_.reset();
        persistTermAndVote();
    }

    const Role oldRole = role_;
    role_ = Role::Follower;
    leaderId_ = leader;
    votesReceived_.clear();
    nextIndex_.clear();
    matchIndex_.clear();
    ++heartbeatGeneration_;

    trace(
        std::string("state=") + toString(oldRole) + "->follower term=" +
        std::to_string(currentTerm_)
    );
    resetElectionTimer(now);
}

void RaftNode::becomeCandidate(TimeMs now) {
    role_ = Role::Candidate;
    leaderId_.reset();
    ++currentTerm_;
    votedFor_ = id_;
    persistTermAndVote();

    votesReceived_.clear();
    votesReceived_.insert(id_);

    trace("state=follower_or_candidate->candidate term=" + std::to_string(currentTerm_));
    resetElectionTimer(now);

    RequestVote request{
        currentTerm_,
        id_,
        lastLogIndex(),
        lastLogTerm()
    };

    for (NodeId peer : peers_) {
        send_(Envelope{0, id_, peer, request});
    }

    if (votesReceived_.size() >= majority()) {
        becomeLeader(now);
    }
}

void RaftNode::becomeLeader(TimeMs now) {
    role_ = Role::Leader;
    leaderId_ = id_;
    ++electionGeneration_;

    const LogIndex next = lastLogIndex() + 1;
    nextIndex_.clear();
    matchIndex_.clear();
    matchIndex_[id_] = lastLogIndex();

    for (NodeId peer : peers_) {
        nextIndex_[peer] = next;
        matchIndex_[peer] = 0;
    }

    log_.push_back(LogEntry{currentTerm_, Command::noop()});
    persistLog();
    matchIndex_[id_] = lastLogIndex();

    trace("state=candidate->leader term=" + std::to_string(currentTerm_));
    broadcastAppendEntries();
    resetHeartbeatTimer(now);
}

void RaftNode::resetElectionTimer(TimeMs now) {
    ++electionGeneration_;
    const auto timeout = randomRange_(
        config_.electionTimeoutMinMs,
        config_.electionTimeoutMaxMs
    );
    scheduleElectionTimer_(id_, now + timeout, electionGeneration_);
}

void RaftNode::resetHeartbeatTimer(TimeMs now) {
    ++heartbeatGeneration_;
    scheduleHeartbeatTimer_(
        id_,
        now + config_.heartbeatIntervalMs,
        heartbeatGeneration_
    );
}

void RaftNode::broadcastAppendEntries() {
    for (NodeId peer : peers_) {
        sendAppendEntries(peer);
    }
}

void RaftNode::sendAppendEntries(NodeId peer) {
    const LogIndex next = std::max<LogIndex>(1, nextIndex_[peer]);
    const LogIndex prevIndex = next - 1;
    const Term prevTerm = log_.at(static_cast<std::size_t>(prevIndex)).term;

    std::vector<LogEntry> entries;
    for (LogIndex i = next; i <= lastLogIndex(); ++i) {
        entries.push_back(log_.at(static_cast<std::size_t>(i)));
    }

    AppendEntries append{
        currentTerm_,
        id_,
        prevIndex,
        prevTerm,
        std::move(entries),
        commitIndex_
    };
    send_(Envelope{0, id_, peer, std::move(append)});
}

void RaftNode::advanceCommitIndex() {
    for (LogIndex candidate = lastLogIndex(); candidate > commitIndex_; --candidate) {
        if (log_.at(static_cast<std::size_t>(candidate)).term != currentTerm_) {
            continue;
        }

        std::size_t replicated = 1;
        for (NodeId peer : peers_) {
            if (matchIndex_[peer] >= candidate) {
                ++replicated;
            }
        }

        if (replicated >= majority()) {
            commitIndex_ = candidate;
            trace("commit_index=" + std::to_string(commitIndex_));
            applyCommittedEntries();
            broadcastAppendEntries();
            return;
        }
    }
}

void RaftNode::applyCommittedEntries() {
    while (lastApplied_ < commitIndex_) {
        ++lastApplied_;
        const auto& command = log_.at(static_cast<std::size_t>(lastApplied_)).command;
        stateMachine_.apply(command);
        if (onApplied_) onApplied_(id_, lastApplied_, command);
        trace("applied index=" + std::to_string(lastApplied_));
    }
}

void RaftNode::handleRequestVote(
    TimeMs now,
    const Envelope& envelope,
    const RequestVote& request
) {
    if (request.term > currentTerm_) {
        becomeFollower(now, request.term, std::nullopt);
    }

    bool grant = false;
    if (
        request.term == currentTerm_ &&
        (!votedFor_.has_value() || votedFor_ == request.candidateId) &&
        candidateLogAtLeastAsUpToDate(request.lastLogIndex, request.lastLogTerm)
    ) {
        votedFor_ = request.candidateId;
        persistTermAndVote();
        resetElectionTimer(now);
        grant = true;
    }

    send_(Envelope{
        0,
        id_,
        envelope.from,
        RequestVoteResponse{currentTerm_, grant}
    });
}

void RaftNode::handleRequestVoteResponse(
    TimeMs now,
    const Envelope& envelope,
    const RequestVoteResponse& response
) {
    if (response.term > currentTerm_) {
        becomeFollower(now, response.term, std::nullopt);
        return;
    }

    if (
        role_ != Role::Candidate ||
        response.term != currentTerm_ ||
        !response.voteGranted
    ) {
        return;
    }

    votesReceived_.insert(envelope.from);
    if (votesReceived_.size() >= majority()) {
        becomeLeader(now);
    }
}

void RaftNode::handleAppendEntries(
    TimeMs now,
    const Envelope& envelope,
    const AppendEntries& append
) {
    if (append.term < currentTerm_) {
        send_(Envelope{
            0,
            id_,
            envelope.from,
            AppendEntriesResponse{currentTerm_, false, 0, lastLogIndex() + 1}
        });
        return;
    }

    if (append.term > currentTerm_ || role_ != Role::Follower) {
        becomeFollower(now, append.term, append.leaderId);
    } else {
        leaderId_ = append.leaderId;
        resetElectionTimer(now);
    }

    if (append.prevLogIndex > lastLogIndex()) {
        send_(Envelope{
            0,
            id_,
            envelope.from,
            AppendEntriesResponse{currentTerm_, false, 0, lastLogIndex() + 1}
        });
        return;
    }

    if (
        log_.at(static_cast<std::size_t>(append.prevLogIndex)).term !=
        append.prevLogTerm
    ) {
        const Term conflictTerm =
            log_.at(static_cast<std::size_t>(append.prevLogIndex)).term;
        LogIndex firstIndex = append.prevLogIndex;
        while (
            firstIndex > 1 &&
            log_.at(static_cast<std::size_t>(firstIndex - 1)).term == conflictTerm
        ) {
            --firstIndex;
        }

        send_(Envelope{
            0,
            id_,
            envelope.from,
            AppendEntriesResponse{currentTerm_, false, 0, firstIndex}
        });
        return;
    }

    LogIndex index = append.prevLogIndex + 1;
    std::size_t incoming = 0;

    while (incoming < append.entries.size() && index <= lastLogIndex()) {
        if (log_.at(static_cast<std::size_t>(index)).term != append.entries[incoming].term) {
            log_.resize(static_cast<std::size_t>(index));
            break;
        }
        ++index;
        ++incoming;
    }

    for (; incoming < append.entries.size(); ++incoming) {
        log_.push_back(append.entries[incoming]);
    }
    persistLog();

    if (append.leaderCommit > commitIndex_) {
        commitIndex_ = std::min(append.leaderCommit, lastLogIndex());
        applyCommittedEntries();
    }

    send_(Envelope{
        0,
        id_,
        envelope.from,
        AppendEntriesResponse{currentTerm_, true, lastLogIndex(), lastLogIndex() + 1}
    });
}

void RaftNode::handleAppendEntriesResponse(
    TimeMs now,
    const Envelope& envelope,
    const AppendEntriesResponse& response
) {
    if (response.term > currentTerm_) {
        becomeFollower(now, response.term, std::nullopt);
        return;
    }

    if (role_ != Role::Leader || response.term != currentTerm_) return;

    if (response.success) {
        matchIndex_[envelope.from] = response.matchIndex;
        nextIndex_[envelope.from] = response.matchIndex + 1;
        advanceCommitIndex();
    } else {
        nextIndex_[envelope.from] = std::max<LogIndex>(1, response.conflictIndex);
        sendAppendEntries(envelope.from);
    }
}

bool RaftNode::candidateLogAtLeastAsUpToDate(
    LogIndex candidateLastIndex,
    Term candidateLastTerm
) const {
    if (candidateLastTerm != lastLogTerm()) {
        return candidateLastTerm > lastLogTerm();
    }
    return candidateLastIndex >= lastLogIndex();
}

std::size_t RaftNode::majority() const {
    const std::size_t clusterSize = peers_.size() + 1;
    return clusterSize / 2 + 1;
}

LogIndex RaftNode::lastLogIndex() const {
    return static_cast<LogIndex>(log_.size() - 1);
}

Term RaftNode::lastLogTerm() const {
    return log_.back().term;
}

void RaftNode::persistTermAndVote() {
    storage_.saveTermAndVote(currentTerm_, votedFor_);
}

void RaftNode::persistLog() {
    storage_.saveLog(log_);
}

void RaftNode::trace(const std::string& message) const {
    trace_("node=" + std::to_string(id_) + " " + message);
}

} // namespace raftkv
