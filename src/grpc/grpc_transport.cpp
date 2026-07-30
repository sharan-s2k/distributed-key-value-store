#include "grpc/grpc_transport.h"

#include "grpc/proto_converter.h"

#include <chrono>
#include <grpcpp/grpcpp.h>

namespace raftkv {

GrpcTransport::GrpcTransport(
    std::unordered_map<NodeId, std::string> peerAddresses,
    std::size_t workers
) {
    for (const auto& [id, address] : peerAddresses) {
        stubs_.emplace(id, rpc::RaftTransport::NewStub(
            grpc::CreateChannel(address, grpc::InsecureChannelCredentials())
        ));
    }
    workers = std::max<std::size_t>(1, workers);
    for (std::size_t i = 0; i < workers; ++i) workers_.emplace_back([this] { workerLoop(); });
}

GrpcTransport::~GrpcTransport() {
    {
        std::lock_guard lock(mutex_);
        stopping_ = true;
    }
    cv_.notify_all();
    for (auto& worker : workers_) if (worker.joinable()) worker.join();
}

void GrpcTransport::send(Envelope envelope) {
    {
        std::lock_guard lock(mutex_);
        if (stopping_) return;
        queue_.push(std::move(envelope));
    }
    cv_.notify_one();
}

void GrpcTransport::workerLoop() {
    while (true) {
        Envelope envelope;
        {
            std::unique_lock lock(mutex_);
            cv_.wait(lock, [this] { return stopping_ || !queue_.empty(); });
            if (stopping_ && queue_.empty()) return;
            envelope = std::move(queue_.front());
            queue_.pop();
        }
        deliver(envelope);
    }
}

void GrpcTransport::deliver(const Envelope& envelope) {
    const auto found = stubs_.find(envelope.to);
    if (found == stubs_.end()) return;

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(300));
    rpc::DeliveryAck response;
    const auto request = toProto(envelope);
    (void)found->second->Deliver(&context, request, &response);
}

} // namespace raftkv
