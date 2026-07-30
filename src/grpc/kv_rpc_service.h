#pragma once

#include "grpc/node_runtime.h"
#include "kv.grpc.pb.h"

namespace raftkv {

class KvRpcService final : public rpc::KeyValue::Service {
public:
    explicit KvRpcService(NodeRuntime& runtime) : runtime_(runtime) {}

    grpc::Status Put(grpc::ServerContext*, const rpc::PutRequest*, rpc::WriteResponse*) override;
    grpc::Status Delete(grpc::ServerContext*, const rpc::DeleteRequest*, rpc::WriteResponse*) override;
    grpc::Status Get(grpc::ServerContext*, const rpc::GetRequest*, rpc::GetResponse*) override;
    grpc::Status Status(grpc::ServerContext*, const rpc::StatusRequest*, rpc::StatusResponse*) override;

private:
    static void fillWriteResponse(const WriteResult& result, rpc::WriteResponse* response);
    NodeRuntime& runtime_;
};

} // namespace raftkv
