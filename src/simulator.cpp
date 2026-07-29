#include "simulator.h"

#include <algorithm>
#include <iomanip>
#include <map>
#include <sstream>
#include <stdexcept>
#include <type_traits>

namespace raftkv {

namespace {
std::uint64_t fnv1a64(const std::string& text) {
    std::uint64_t hash = 1469598103934665603ULL;
    for (unsigned char c : text) {
        hash ^= c;
        hash *= 1099511628211ULL;
    }
    return hash;
}
} // namespace

Simulator::Simulator(SimulatorConfig config)
    : config_(std::move(config)), random_(config_.seed) {
    if (config_.nodeCount == 0) throw std::invalid_argument("nodeCount must be positive");
    if (config_.minNetworkDelayMs > config_.maxNetworkDelayMs) {
        throw std::invalid_argument("minimum network delay cannot exceed maximum delay");
    }
}

void Simulator::initialize() {
    for (NodeId id = 1; id <= config_.nodeCount; ++id) nodes_.emplace(id, NodeRuntime{});
    for (NodeId id = 1; id <= config_.nodeCount; ++id) {
        createNode(id);
        nodes_.at(id).raft->start(now_);
    }
    trace("simulator_started seed=" + std::to_string(config_.seed) +
          " nodes=" + std::to_string(config_.nodeCount));
}

void Simulator::run(std::uint64_t maxEvents) {
    std::uint64_t processed = 0;
    while (!events_.empty() && processed < maxEvents) {
        Event event = events_.top();
        events_.pop();
        now_ = event.time;
        processEvent(event);
        assertInvariants();
        ++processed;
    }
    trace("simulation_finished processed=" + std::to_string(processed));
}

RequestId Simulator::scheduleClientPut(TimeMs time, std::string key, std::string value) {
    const RequestId id = nextRequestId_++;
    historyIndex_[id] = history_.size();
    history_.push_back(OperationRecord{id, CommandType::Put, key, value, time, std::nullopt, 0});
    schedule(Event{time, 0, EventType::ClientPut, ClientPutEvent{id, std::move(key), std::move(value)}});
    return id;
}

RequestId Simulator::scheduleClientDelete(TimeMs time, std::string key) {
    const RequestId id = nextRequestId_++;
    historyIndex_[id] = history_.size();
    history_.push_back(OperationRecord{id, CommandType::Delete, key, {}, time, std::nullopt, 0});
    schedule(Event{time, 0, EventType::ClientDelete, ClientDeleteEvent{id, std::move(key)}});
    return id;
}

void Simulator::scheduleClientPutRetry(TimeMs time, const ClientPutEvent& request) {
    schedule(Event{time, 0, EventType::ClientPut, request});
}

void Simulator::scheduleClientDeleteRetry(TimeMs time, const ClientDeleteEvent& request) {
    schedule(Event{time, 0, EventType::ClientDelete, request});
}

void Simulator::scheduleCrash(TimeMs time, NodeId nodeId) {
    schedule(Event{time, 0, EventType::CrashNode, NodeEvent{nodeId}});
}
void Simulator::scheduleCrashLeader(TimeMs time) {
    schedule(Event{time, 0, EventType::CrashLeader, std::monostate{}});
}
void Simulator::scheduleRestart(TimeMs time, NodeId nodeId) {
    schedule(Event{time, 0, EventType::RestartNode, NodeEvent{nodeId}});
}
void Simulator::scheduleRestartLastCrashedLeader(TimeMs time) {
    schedule(Event{time, 0, EventType::RestartLastCrashedLeader, std::monostate{}});
}
void Simulator::schedulePartition(TimeMs time, std::set<NodeId> left, std::set<NodeId> right) {
    schedule(Event{time, 0, EventType::Partition, PartitionEvent{std::move(left), std::move(right)}});
}
void Simulator::scheduleHeal(TimeMs time) {
    schedule(Event{time, 0, EventType::Heal, std::monostate{}});
}

std::optional<NodeId> Simulator::currentLeader() const {
    std::optional<NodeId> leader;
    Term highestTerm = 0;
    for (const auto& [id, runtime] : nodes_) {
        if (runtime.alive && runtime.raft && runtime.raft->role() == Role::Leader &&
            runtime.raft->currentTerm() >= highestTerm) {
            leader = id;
            highestTerm = runtime.raft->currentTerm();
        }
    }
    return leader;
}

const RaftNode& Simulator::node(NodeId id) const { return *nodes_.at(id).raft; }
bool Simulator::isAlive(NodeId id) const { return nodes_.at(id).alive; }

std::string Simulator::traceHash() const {
    std::string joined;
    for (const auto& line : traceLines_) { joined += line; joined.push_back('\n'); }
    std::ostringstream out;
    out << std::hex << std::setfill('0') << std::setw(16) << fnv1a64(joined);
    return out.str();
}

LinearizabilityResult Simulator::checkLinearizability() const {
    const auto ids = allNodeIds();
    if (ids.empty()) return {false, "cluster has no nodes", 0, 0};

    const auto& expected = canonicalFinalState();
    for (NodeId id : ids) {
        if (node(id).stateMachine().data() != expected) {
            return {false, "replicas have different final key-value states", 0, 0};
        }
    }
    return checkWriteLinearizability(history_, expected);
}

void Simulator::assertInvariants() {
    std::map<Term, NodeId> leaderByTerm;

    for (const auto& [id, runtime] : nodes_) {
        if (!runtime.raft) continue;
        const auto& raft = *runtime.raft;
        if (raft.log().empty()) throw std::logic_error("raft log lost sentinel entry");
        if (raft.commitIndex() > raft.log().size() - 1) throw std::logic_error("commit index beyond log");
        if (raft.lastApplied() > raft.commitIndex()) throw std::logic_error("applied index beyond commit index");

        if (runtime.alive && raft.role() == Role::Leader) {
            const auto [it, inserted] = leaderByTerm.emplace(raft.currentTerm(), id);
            if (!inserted && it->second != id) {
                throw std::logic_error("election safety violated: two leaders in same term");
            }
        }

        for (LogIndex index = 1; index <= raft.commitIndex(); ++index) {
            const auto& entry = raft.log().at(static_cast<std::size_t>(index));
            const auto [it, inserted] = committedEntries_.emplace(index, entry);
            if (!inserted && !(it->second == entry)) {
                throw std::logic_error("state machine safety violated: conflicting committed entries");
            }
        }
    }

    const auto ids = allNodeIds();
    for (std::size_t a = 0; a < ids.size(); ++a) {
        for (std::size_t b = a + 1; b < ids.size(); ++b) {
            const auto& logA = node(ids[a]).log();
            const auto& logB = node(ids[b]).log();
            const auto common = std::min(logA.size(), logB.size());
            for (std::size_t i = 1; i < common; ++i) {
                if (logA[i].term == logB[i].term) {
                    for (std::size_t j = 1; j <= i; ++j) {
                        if (!(logA[j] == logB[j])) throw std::logic_error("log matching invariant violated");
                    }
                }
            }
        }
    }

    for (const auto& [_, leaderId] : leaderByTerm) {
        const auto& leaderLog = node(leaderId).log();
        for (const auto& [index, entry] : committedEntries_) {
            if (index >= leaderLog.size() || !(leaderLog[static_cast<std::size_t>(index)] == entry)) {
                throw std::logic_error("leader completeness violated");
            }
        }
    }
}

void Simulator::createNode(NodeId id) {
    std::vector<NodeId> peers;
    for (NodeId peer = 1; peer <= config_.nodeCount; ++peer) if (peer != id) peers.push_back(peer);

    auto& runtime = nodes_.at(id);
    runtime.raft = std::make_unique<RaftNode>(
        id,
        std::move(peers),
        runtime.storage,
        config_.raft,
        [this](Envelope envelope) { send(std::move(envelope)); },
        [this](NodeId nodeId, TimeMs deadline, std::uint64_t generation) {
            schedule(Event{deadline, 0, EventType::ElectionTimeout, TimerEvent{nodeId, generation}});
        },
        [this](NodeId nodeId, TimeMs deadline, std::uint64_t generation) {
            schedule(Event{deadline, 0, EventType::HeartbeatTimeout, TimerEvent{nodeId, generation}});
        },
        [this](std::uint64_t min, std::uint64_t max) { return randomRange(min, max); },
        [this](const std::string& message) { trace(message); },
        [this](NodeId nodeId, LogIndex index, const Command& command) { onApplied(nodeId, index, command); }
    );
}

void Simulator::processEvent(const Event& event) {
    switch (event.type) {
        case EventType::DeliverMessage:
            deliver(std::get<DeliverMessageEvent>(event.payload).envelope);
            break;
        case EventType::ElectionTimeout: {
            const auto timer = std::get<TimerEvent>(event.payload);
            if (nodes_.at(timer.nodeId).alive) nodes_.at(timer.nodeId).raft->onElectionTimeout(now_, timer.generation);
            break;
        }
        case EventType::HeartbeatTimeout: {
            const auto timer = std::get<TimerEvent>(event.payload);
            if (nodes_.at(timer.nodeId).alive) nodes_.at(timer.nodeId).raft->onHeartbeatTimeout(now_, timer.generation);
            break;
        }
        case EventType::ClientPut: {
            const auto request = std::get<ClientPutEvent>(event.payload);
            auto& record = historyRecord(request.requestId);
            ++record.attempts;
            const auto target = chooseClientTarget();
            bool accepted = false;
            if (target.has_value()) {
                accepted = nodes_.at(*target).raft->submit(now_, Command::put(request.requestId, request.key, request.value));
            }
            trace("client PUT request=" + std::to_string(request.requestId) + " key=" + request.key +
                  " target=" + (target ? std::to_string(*target) : "none") +
                  " accepted=" + (accepted ? "true" : "false"));
            if (!accepted && !record.completedAt.has_value()) {
                scheduleClientPutRetry(now_ + 100, request);
                trace("client PUT request=" + std::to_string(request.requestId) + " retry_scheduled=100");
            }
            break;
        }
        case EventType::ClientDelete: {
            const auto request = std::get<ClientDeleteEvent>(event.payload);
            auto& record = historyRecord(request.requestId);
            ++record.attempts;
            const auto target = chooseClientTarget();
            bool accepted = false;
            if (target.has_value()) {
                accepted = nodes_.at(*target).raft->submit(now_, Command::erase(request.requestId, request.key));
            }
            trace("client DELETE request=" + std::to_string(request.requestId) + " key=" + request.key +
                  " target=" + (target ? std::to_string(*target) : "none") +
                  " accepted=" + (accepted ? "true" : "false"));
            if (!accepted && !record.completedAt.has_value()) {
                scheduleClientDeleteRetry(now_ + 100, request);
                trace("client DELETE request=" + std::to_string(request.requestId) + " retry_scheduled=100");
            }
            break;
        }
        case EventType::CrashNode: {
            const NodeId id = std::get<NodeEvent>(event.payload).nodeId;
            nodes_.at(id).alive = false;
            trace("node=" + std::to_string(id) + " crashed");
            break;
        }
        case EventType::CrashLeader: {
            const auto leader = currentLeader();
            if (!leader) { trace("leader_crash_skipped reason=no_leader"); break; }
            lastCrashedLeader_ = *leader;
            nodes_.at(*leader).alive = false;
            trace("node=" + std::to_string(*leader) + " leader_crashed");
            break;
        }
        case EventType::RestartNode: {
            const NodeId id = std::get<NodeEvent>(event.payload).nodeId;
            auto& runtime = nodes_.at(id);
            if (!runtime.alive) {
                runtime.alive = true;
                createNode(id);
                runtime.raft->start(now_);
                trace("node=" + std::to_string(id) + " restarted");
            }
            break;
        }
        case EventType::RestartLastCrashedLeader: {
            if (!lastCrashedLeader_) { trace("leader_restart_skipped reason=no_crashed_leader"); break; }
            const NodeId id = *lastCrashedLeader_;
            auto& runtime = nodes_.at(id);
            if (!runtime.alive) {
                runtime.alive = true;
                createNode(id);
                runtime.raft->start(now_);
                trace("node=" + std::to_string(id) + " former_leader_restarted");
            }
            break;
        }
        case EventType::Partition: {
            const auto& partition = std::get<PartitionEvent>(event.payload);
            for (NodeId left : partition.left) for (NodeId right : partition.right) {
                blockedLinks_.insert({left, right});
                blockedLinks_.insert({right, left});
            }
            trace("network_partitioned");
            break;
        }
        case EventType::Heal:
            blockedLinks_.clear();
            trace("network_healed");
            break;
    }
}

void Simulator::schedule(Event event) {
    event.sequence = nextSequence_++;
    events_.push(std::move(event));
}

void Simulator::send(Envelope envelope) {
    envelope.messageId = nextMessageId_++;
    if (!nodes_.at(envelope.from).alive || !nodes_.at(envelope.to).alive) {
        trace("message_dropped id=" + std::to_string(envelope.messageId) + " reason=node_down");
        return;
    }
    if (!connected(envelope.from, envelope.to)) {
        trace("message_dropped id=" + std::to_string(envelope.messageId) + " type=" +
              messageName(envelope.payload) + " reason=partition");
        return;
    }
    if (random_.chance(config_.dropRate)) {
        trace("message_dropped id=" + std::to_string(envelope.messageId) + " type=" +
              messageName(envelope.payload) + " reason=random");
        return;
    }

    const TimeMs delay = randomRange(config_.minNetworkDelayMs, config_.maxNetworkDelayMs);
    trace("message_scheduled id=" + std::to_string(envelope.messageId) + " type=" +
          messageName(envelope.payload) + " from=" + std::to_string(envelope.from) +
          " to=" + std::to_string(envelope.to) + " delay=" + std::to_string(delay));
    schedule(Event{now_ + delay, 0, EventType::DeliverMessage, DeliverMessageEvent{envelope}});

    if (random_.chance(config_.duplicateRate)) {
        Envelope duplicate = envelope;
        duplicate.messageId = nextMessageId_++;
        const TimeMs duplicateDelay = delay + randomRange(1, 5);
        trace("message_duplicated original=" + std::to_string(envelope.messageId) +
              " duplicate=" + std::to_string(duplicate.messageId));
        schedule(Event{now_ + duplicateDelay, 0, EventType::DeliverMessage, DeliverMessageEvent{std::move(duplicate)}});
    }
}

void Simulator::deliver(const Envelope& envelope) {
    if (!nodes_.at(envelope.to).alive) {
        trace("message_dropped id=" + std::to_string(envelope.messageId) + " reason=destination_down");
        return;
    }
    if (!connected(envelope.from, envelope.to)) {
        trace("message_dropped id=" + std::to_string(envelope.messageId) + " reason=partition_at_delivery");
        return;
    }
    trace("message_delivered id=" + std::to_string(envelope.messageId) + " type=" +
          messageName(envelope.payload) + " from=" + std::to_string(envelope.from) +
          " to=" + std::to_string(envelope.to));
    nodes_.at(envelope.to).raft->onMessage(now_, envelope);
}

void Simulator::onApplied(NodeId nodeId, LogIndex index, const Command& command) {
    if (command.requestId == 0) return;
    auto& record = historyRecord(command.requestId);
    if (!record.completedAt.has_value()) {
        record.completedAt = now_;
        trace("client_completed request=" + std::to_string(command.requestId) +
              " node=" + std::to_string(nodeId) + " index=" + std::to_string(index));
    }
}

void Simulator::trace(const std::string& line) {
    traceLines_.push_back("[t=" + std::to_string(now_) + "] " + line);
}

bool Simulator::connected(NodeId from, NodeId to) const {
    return !blockedLinks_.contains({from, to});
}

std::vector<NodeId> Simulator::allNodeIds() const {
    std::vector<NodeId> ids;
    ids.reserve(nodes_.size());
    for (const auto& [id, _] : nodes_) ids.push_back(id);
    std::sort(ids.begin(), ids.end());
    return ids;
}

std::optional<NodeId> Simulator::chooseClientTarget() const {
    if (const auto leader = currentLeader(); leader.has_value()) return leader;
    for (NodeId id : allNodeIds()) if (nodes_.at(id).alive) return id;
    return std::nullopt;
}

std::uint64_t Simulator::randomRange(std::uint64_t min, std::uint64_t max) {
    return random_.range(min, max);
}

OperationRecord& Simulator::historyRecord(RequestId requestId) {
    return history_.at(historyIndex_.at(requestId));
}

const std::unordered_map<std::string, std::string>& Simulator::canonicalFinalState() const {
    const auto ids = allNodeIds();
    if (ids.empty()) throw std::logic_error("cluster has no nodes");
    return node(ids.front()).stateMachine().data();
}

} // namespace raftkv
