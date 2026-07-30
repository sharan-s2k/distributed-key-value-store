#include "grpc/kv_rpc_service.h"
#include "grpc/node_runtime.h"
#include "grpc/raft_rpc_service.h"

#include <grpcpp/grpcpp.h>

#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <unordered_map>

using namespace raftkv;

namespace {

std::pair<NodeId, std::string> parsePeer(const std::string& value) {
    const auto separator = value.find('=');
    if (separator == std::string::npos) throw std::invalid_argument("--peer must use ID=HOST:PORT");
    return {
        static_cast<NodeId>(std::stoul(value.substr(0, separator))),
        value.substr(separator + 1)
    };
}

} // namespace

int main(int argc, char** argv) {
    try {
        NodeId id = 0;
        std::string listen;
        std::filesystem::path walPath;
        std::unordered_map<NodeId, std::string> cluster;

        for (int i = 1; i < argc; ++i) {
            const std::string arg = argv[i];
            auto next = [&](const std::string& option) {
                if (++i >= argc) throw std::invalid_argument("missing value for " + option);
                return std::string(argv[i]);
            };

            if (arg == "--id") id = static_cast<NodeId>(std::stoul(next(arg)));
            else if (arg == "--listen") listen = next(arg);
            else if (arg == "--wal") walPath = next(arg);
            else if (arg == "--peer") {
                auto [peerId, address] = parsePeer(next(arg));
                cluster[peerId] = std::move(address);
            } else {
                throw std::invalid_argument("unknown argument: " + arg);
            }
        }

        if (id == 0 || listen.empty()) {
            throw std::invalid_argument("usage: kv-server --id N --listen HOST:PORT --wal FILE --peer 1=HOST:PORT ...");
        }
        cluster[id] = listen;
        if (walPath.empty()) walPath = "wal-data/node-" + std::to_string(id) + ".wal";

        NodeRuntime runtime(id, cluster, walPath);
        runtime.start();
        RaftRpcService raftService(runtime);
        KvRpcService kvService(runtime);

        grpc::ServerBuilder builder;
        builder.AddListeningPort(listen, grpc::InsecureServerCredentials());
        builder.RegisterService(&raftService);
        builder.RegisterService(&kvService);
        auto server = builder.BuildAndStart();
        if (!server) throw std::runtime_error("failed to start gRPC server");

        std::cout << "node " << id << " listening on " << listen << " wal=" << walPath << '\n';
        server->Wait();
        runtime.stop();
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "kv-server error: " << error.what() << '\n';
        return 1;
    }
}
