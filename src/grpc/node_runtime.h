#pragma once

#include "grpc/event_loop.h"
#include "grpc/grpc_transport.h"
#include "raft.h"
#include "storage.h"

#include <condition_variable>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

namespace raftkv {

struct NodeStatus {
    NodeId nodeId{0};
    Role role{Role::Follower};
    Term term{0};
    LogIndex commitIndex{0};
    LogIndex lastApplied{0};
    std::optional<NodeId> leaderId;
};

struct WriteResult {
    bool committed{false};
    bool isLeader{false};
    std::optional<NodeId> leaderId;
    std::string error;
};

class NodeRuntime {
public:
    NodeRuntime(
        NodeId id,
        std::unordered_map<NodeId, std::string> cluster,
        std::filesystem::path walPath,
        RaftConfig config = {}
    );
    ~NodeRuntime();

    void start();
    void stop();
    void deliver(Envelope envelope);

    WriteResult put(RequestId requestId, std::string key, std::string value, TimeMs timeoutMs);
    WriteResult erase(RequestId requestId, std::string key, TimeMs timeoutMs);
    std::optional<std::string> get(const std::string& key);
    NodeStatus status();

private:
    WriteResult submitAndWait(Command command, TimeMs timeoutMs);
    void onApplied(NodeId nodeId, LogIndex index, const Command& command);
    std::uint64_t randomRange(std::uint64_t min, std::uint64_t max);

    NodeId id_;
    std::unordered_map<NodeId, std::string> cluster_;
    EventLoop loop_;
    DiskWalStorage storage_;
    GrpcTransport transport_;
    RaftConfig config_;
    std::mt19937_64 random_;
    std::unique_ptr<RaftNode> node_;

    std::mutex appliedMutex_;
    std::condition_variable appliedCv_;
    std::unordered_map<RequestId, bool> appliedRequests_;
    bool started_{false};
};

} // namespace raftkv
