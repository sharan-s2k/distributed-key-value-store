#include "kv.grpc.pb.h"

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <iostream>
#include <random>
#include <stdexcept>
#include <string>

namespace rpc = raftkv::rpc;

namespace {

std::uint64_t makeRequestId() {
    const auto now = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    return static_cast<std::uint64_t>(now) ^ std::random_device{}();
}

void printWrite(const grpc::Status& status, const rpc::WriteResponse& response) {
    if (!status.ok()) {
        std::cout << "rpc_error=" << status.error_message() << '\n';
        return;
    }
    std::cout << "committed=" << (response.committed() ? "true" : "false")
              << " is_leader=" << (response.is_leader() ? "true" : "false")
              << " leader_id=" << response.leader_id()
              << " error=" << response.error() << '\n';
}

} // namespace

int main(int argc, char** argv) {
    try {
        if (argc < 3) {
            throw std::invalid_argument(
                "usage: kv-client HOST:PORT put KEY VALUE | get KEY | delete KEY | status"
            );
        }

        const std::string address = argv[1];
        const std::string command = argv[2];
        auto stub = rpc::KeyValue::NewStub(
            grpc::CreateChannel(address, grpc::InsecureChannelCredentials())
        );
        grpc::ClientContext context;

        if (command == "put") {
            if (argc != 5) throw std::invalid_argument("put requires KEY VALUE");
            rpc::PutRequest request;
            request.set_key(argv[3]);
            request.set_value(argv[4]);
            request.set_request_id(makeRequestId());
            request.set_timeout_ms(3000);
            rpc::WriteResponse response;
            printWrite(stub->Put(&context, request, &response), response);
        } else if (command == "delete") {
            if (argc != 4) throw std::invalid_argument("delete requires KEY");
            rpc::DeleteRequest request;
            request.set_key(argv[3]);
            request.set_request_id(makeRequestId());
            request.set_timeout_ms(3000);
            rpc::WriteResponse response;
            printWrite(stub->Delete(&context, request, &response), response);
        } else if (command == "get") {
            if (argc != 4) throw std::invalid_argument("get requires KEY");
            rpc::GetRequest request;
            request.set_key(argv[3]);
            rpc::GetResponse response;
            const auto status = stub->Get(&context, request, &response);
            if (!status.ok()) std::cout << "rpc_error=" << status.error_message() << '\n';
            else std::cout << "found=" << (response.found() ? "true" : "false")
                           << " value=" << response.value()
                           << " leader_id=" << response.leader_id()
                           << " error=" << response.error() << '\n';
        } else if (command == "status") {
            rpc::StatusRequest request;
            rpc::StatusResponse response;
            const auto status = stub->Status(&context, request, &response);
            if (!status.ok()) std::cout << "rpc_error=" << status.error_message() << '\n';
            else std::cout << "node=" << response.node_id()
                           << " role=" << response.role()
                           << " term=" << response.term()
                           << " commit=" << response.commit_index()
                           << " applied=" << response.last_applied()
                           << " leader_id=" << response.leader_id() << '\n';
        } else {
            throw std::invalid_argument("unknown command: " + command);
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "kv-client error: " << error.what() << '\n';
        return 1;
    }
}
