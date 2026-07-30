#pragma once

#include "raft.grpc.pb.h"
#include "types.h"

#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace raftkv {

class GrpcTransport {
public:
    explicit GrpcTransport(std::unordered_map<NodeId, std::string> peerAddresses, std::size_t workers = 2);
    ~GrpcTransport();

    GrpcTransport(const GrpcTransport&) = delete;
    GrpcTransport& operator=(const GrpcTransport&) = delete;

    void send(Envelope envelope);

private:
    void workerLoop();
    void deliver(const Envelope& envelope);

    std::unordered_map<NodeId, std::unique_ptr<rpc::RaftTransport::Stub>> stubs_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::queue<Envelope> queue_;
    std::vector<std::thread> workers_;
    bool stopping_{false};
};

} // namespace raftkv
