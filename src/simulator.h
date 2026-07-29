#pragma once

#include "history.h"
#include "raft.h"
#include "random.h"
#include "storage.h"
#include "types.h"

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <queue>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace raftkv {

enum class EventType {
    DeliverMessage,
    ElectionTimeout,
    HeartbeatTimeout,
    ClientPut,
    ClientDelete,
    CrashNode,
    CrashLeader,
    RestartNode,
    RestartLastCrashedLeader,
    Partition,
    Heal
};

struct DeliverMessageEvent { Envelope envelope; };
struct TimerEvent { NodeId nodeId{0}; std::uint64_t generation{0}; };
struct ClientPutEvent { RequestId requestId{0}; std::string key; std::string value; };
struct ClientDeleteEvent { RequestId requestId{0}; std::string key; };
struct NodeEvent { NodeId nodeId{0}; };
struct PartitionEvent { std::set<NodeId> left; std::set<NodeId> right; };

using EventPayload = std::variant<
    DeliverMessageEvent,
    TimerEvent,
    ClientPutEvent,
    ClientDeleteEvent,
    NodeEvent,
    PartitionEvent,
    std::monostate
>;

struct Event {
    TimeMs time{0};
    std::uint64_t sequence{0};
    EventType type{EventType::DeliverMessage};
    EventPayload payload;
};

struct EventCompare {
    bool operator()(const Event& a, const Event& b) const {
        if (a.time != b.time) return a.time > b.time;
        return a.sequence > b.sequence;
    }
};

struct SimulatorConfig {
    std::size_t nodeCount{3};
    std::uint64_t seed{42};
    TimeMs minNetworkDelayMs{5};
    TimeMs maxNetworkDelayMs{20};
    double dropRate{0.0};
    double duplicateRate{0.0};
    RaftConfig raft{};
    std::optional<std::filesystem::path> walDirectory;
    bool resetWalOnInitialize{true};
};

class Simulator {
public:
    explicit Simulator(SimulatorConfig config);

    void initialize();
    void run(std::uint64_t maxEvents);

    RequestId scheduleClientPut(TimeMs time, std::string key, std::string value);
    RequestId scheduleClientDelete(TimeMs time, std::string key);
    void scheduleCrash(TimeMs time, NodeId nodeId);
    void scheduleCrashLeader(TimeMs time);
    void scheduleRestart(TimeMs time, NodeId nodeId);
    void scheduleRestartLastCrashedLeader(TimeMs time);
    void schedulePartition(TimeMs time, std::set<NodeId> left, std::set<NodeId> right);
    void scheduleHeal(TimeMs time);

    std::optional<NodeId> currentLeader() const;
    const RaftNode& node(NodeId id) const;
    bool isAlive(NodeId id) const;
    TimeMs now() const { return now_; }
    const std::vector<std::string>& traceLines() const { return traceLines_; }
    const std::vector<OperationRecord>& operationHistory() const { return history_; }
    std::string traceHash() const;
    LinearizabilityResult checkLinearizability() const;
    void assertInvariants();

private:
    struct NodeRuntime {
        std::unique_ptr<StableStorage> storage;
        std::unique_ptr<RaftNode> raft;
        bool alive{true};
    };

    void createNode(NodeId id);
    void processEvent(const Event& event);
    void schedule(Event event);
    void scheduleClientPutRetry(TimeMs time, const ClientPutEvent& request);
    void scheduleClientDeleteRetry(TimeMs time, const ClientDeleteEvent& request);
    void send(Envelope envelope);
    void deliver(const Envelope& envelope);
    void onApplied(NodeId nodeId, LogIndex index, const Command& command);
    void trace(const std::string& line);
    bool connected(NodeId from, NodeId to) const;
    std::vector<NodeId> allNodeIds() const;
    std::optional<NodeId> chooseClientTarget() const;
    std::uint64_t randomRange(std::uint64_t min, std::uint64_t max);
    OperationRecord& historyRecord(RequestId requestId);
    const std::unordered_map<std::string, std::string>& canonicalFinalState() const;

    SimulatorConfig config_;
    DeterministicRandom random_;
    TimeMs now_{0};
    std::uint64_t nextSequence_{1};
    std::uint64_t nextMessageId_{1};
    RequestId nextRequestId_{1};
    std::priority_queue<Event, std::vector<Event>, EventCompare> events_;
    std::unordered_map<NodeId, NodeRuntime> nodes_;
    std::set<std::pair<NodeId, NodeId>> blockedLinks_;
    std::vector<std::string> traceLines_;
    std::vector<OperationRecord> history_;
    std::unordered_map<RequestId, std::size_t> historyIndex_;
    std::unordered_map<LogIndex, LogEntry> committedEntries_;
    std::optional<NodeId> lastCrashedLeader_;
};

} // namespace raftkv