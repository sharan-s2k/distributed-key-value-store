#include "grpc/kv_rpc_service.h"

namespace raftkv {

void KvRpcService::fillWriteResponse(const WriteResult& result, rpc::WriteResponse* response) {
    response->set_committed(result.committed);
    response->set_is_leader(result.isLeader);
    response->set_leader_id(result.leaderId.value_or(0));
    response->set_error(result.error);
}

grpc::Status KvRpcService::Put(
    grpc::ServerContext*,
    const rpc::PutRequest* request,
    rpc::WriteResponse* response
) {
    fillWriteResponse(
        runtime_.put(request->request_id(), request->key(), request->value(), request->timeout_ms()),
        response
    );
    return grpc::Status::OK;
}

grpc::Status KvRpcService::Delete(
    grpc::ServerContext*,
    const rpc::DeleteRequest* request,
    rpc::WriteResponse* response
) {
    fillWriteResponse(
        runtime_.erase(request->request_id(), request->key(), request->timeout_ms()),
        response
    );
    return grpc::Status::OK;
}

grpc::Status KvRpcService::Get(
    grpc::ServerContext*,
    const rpc::GetRequest* request,
    rpc::GetResponse* response
) {
    const auto status = runtime_.status();
    response->set_is_leader(status.role == Role::Leader);
    response->set_leader_id(status.leaderId.value_or(0));
    if (status.role != Role::Leader) {
        response->set_error("NOT_LEADER");
        return grpc::Status::OK;
    }

    const auto value = runtime_.get(request->key());
    response->set_found(value.has_value());
    if (value) response->set_value(*value);
    return grpc::Status::OK;
}

grpc::Status KvRpcService::Status(
    grpc::ServerContext*,
    const rpc::StatusRequest*,
    rpc::StatusResponse* response
) {
    const auto status = runtime_.status();
    response->set_node_id(status.nodeId);
    response->set_role(toString(status.role));
    response->set_term(status.term);
    response->set_commit_index(status.commitIndex);
    response->set_last_applied(status.lastApplied);
    response->set_leader_id(status.leaderId.value_or(0));
    return grpc::Status::OK;
}

} // namespace raftkv
