#pragma once

#include "grpc/node_runtime.h"
#include "raft.grpc.pb.h"

namespace raftkv {

class RaftRpcService final : public rpc::RaftTransport::Service {
public:
    explicit RaftRpcService(NodeRuntime& runtime) : runtime_(runtime) {}

    grpc::Status Deliver(
        grpc::ServerContext* context,
        const rpc::Envelope* request,
        rpc::DeliveryAck* response
    ) override;

private:
    NodeRuntime& runtime_;
};

} // namespace raftkv
