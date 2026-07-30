#include "grpc/node_runtime.h"

#include <chrono>
#include <iostream>
#include <stdexcept>

namespace raftkv {

NodeRuntime::NodeRuntime(
    NodeId id,
    std::unordered_map<NodeId, std::string> cluster,
    std::filesystem::path walPath,
    RaftConfig config
)
    : id_(id),
      cluster_(std::move(cluster)),
      storage_(std::move(walPath)),
      transport_([&] {
          auto peers = cluster_;
          peers.erase(id_);
          return peers;
      }()),
      config_(config),
      random_(std::random_device{}()) {
    if (!cluster_.contains(id_)) throw std::invalid_argument("cluster configuration does not contain this node");
}

NodeRuntime::~NodeRuntime() {
    stop();
}

void NodeRuntime::start() {
    if (started_) return;
    loop_.start();

    std::vector<NodeId> peers;
    peers.reserve(cluster_.size() - 1);
    for (const auto& [peerId, _] : cluster_) if (peerId != id_) peers.push_back(peerId);

    node_ = std::make_unique<RaftNode>(
        id_,
        std::move(peers),
        storage_,
        config_,
        [this](Envelope envelope) { transport_.send(std::move(envelope)); },
        [this](NodeId, TimeMs deadline, std::uint64_t generation) {
            loop_.scheduleAt(deadline, [this, generation] {
                node_->onElectionTimeout(EventLoop::nowMs(), generation);
            });
        },
        [this](NodeId, TimeMs deadline, std::uint64_t generation) {
            loop_.scheduleAt(deadline, [this, generation] {
                node_->onHeartbeatTimeout(EventLoop::nowMs(), generation);
            });
        },
        [this](std::uint64_t min, std::uint64_t max) { return randomRange(min, max); },
        [this](const std::string& message) {
            std::cout << "node=" << id_ << " " << message << '\n';
        },
        [this](NodeId nodeId, LogIndex index, const Command& command) {
            onApplied(nodeId, index, command);
        }
    );

    loop_.invoke([this] { node_->start(EventLoop::nowMs()); });
    started_ = true;
}

void NodeRuntime::stop() {
    if (!started_) return;
    loop_.stop();
    node_.reset();
    started_ = false;
}

void NodeRuntime::deliver(Envelope envelope) {
    if (!started_ || envelope.to != id_) return;
    loop_.post([this, envelope = std::move(envelope)] {
        node_->onMessage(EventLoop::nowMs(), envelope);
    });
}

WriteResult NodeRuntime::put(
    RequestId requestId,
    std::string key,
    std::string value,
    TimeMs timeoutMs
) {
    return submitAndWait(Command::put(requestId, std::move(key), std::move(value)), timeoutMs);
}

WriteResult NodeRuntime::erase(
    RequestId requestId,
    std::string key,
    TimeMs timeoutMs
) {
    return submitAndWait(Command::erase(requestId, std::move(key)), timeoutMs);
}

WriteResult NodeRuntime::submitAndWait(Command command, TimeMs timeoutMs) {
    if (command.requestId == 0) return {false, false, std::nullopt, "request_id must be non-zero"};

    const RequestId requestId = command.requestId;
    const auto initial = loop_.invoke([this, command = std::move(command)]() mutable {
        WriteResult result;
        result.isLeader = node_->role() == Role::Leader;
        result.leaderId = node_->leaderId();
        if (!result.isLeader) {
            result.error = "NOT_LEADER";
            return result;
        }
        if (!node_->submit(EventLoop::nowMs(), std::move(command))) {
            result.error = "SUBMIT_REJECTED";
            return result;
        }
        return result;
    });

    if (!initial.isLeader || !initial.error.empty()) return initial;

    std::unique_lock lock(appliedMutex_);
    const bool committed = appliedCv_.wait_for(
        lock,
        std::chrono::milliseconds(timeoutMs == 0 ? 3000 : timeoutMs),
        [this, requestId] { return appliedRequests_.contains(requestId); }
    );
    if (!committed) return {false, true, id_, "COMMIT_TIMEOUT"};
    appliedRequests_.erase(requestId);
    return {true, true, id_, {}};
}

std::optional<std::string> NodeRuntime::get(const std::string& key) {
    return loop_.invoke([this, key] { return node_->stateMachine().get(key); });
}

NodeStatus NodeRuntime::status() {
    return loop_.invoke([this] {
        return NodeStatus{
            id_,
            node_->role(),
            node_->currentTerm(),
            node_->commitIndex(),
            node_->lastApplied(),
            node_->leaderId()
        };
    });
}

void NodeRuntime::onApplied(NodeId, LogIndex, const Command& command) {
    if (command.requestId == 0) return;
    {
        std::lock_guard lock(appliedMutex_);
        appliedRequests_[command.requestId] = true;
    }
    appliedCv_.notify_all();
}

std::uint64_t NodeRuntime::randomRange(std::uint64_t min, std::uint64_t max) {
    std::uniform_int_distribution<std::uint64_t> distribution(min, max);
    return distribution(random_);
}

} // namespace raftkv
