#include "grpc/raft_rpc_service.h"

#include "grpc/proto_converter.h"

#include <exception>

namespace raftkv {

grpc::Status RaftRpcService::Deliver(
    grpc::ServerContext*,
    const rpc::Envelope* request,
    rpc::DeliveryAck* response
) {
    try {
        runtime_.deliver(fromProto(*request));
        response->set_accepted(true);
        return grpc::Status::OK;
    } catch (const std::exception& error) {
        response->set_accepted(false);
        response->set_error(error.what());
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, error.what());
    }
}

} // namespace raftkv
