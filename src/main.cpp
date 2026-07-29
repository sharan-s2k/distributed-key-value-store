#include "random.h"
#include "simulator.h"

#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

using namespace raftkv;

namespace {

struct RunOptions {
    SimulatorConfig config;
    std::uint64_t events{3000};
    std::uint64_t users{25};
    std::uint64_t operations{100};
    std::uint64_t keys{10};
    std::string scenario{"failover"};
    std::string traceOut;
};

std::uint64_t parseU64(const std::string& value, const std::string& name) {
    try { return std::stoull(value); }
    catch (...) { throw std::invalid_argument("invalid value for " + name); }
}

double parseDouble(const std::string& value, const std::string& name) {
    try { return std::stod(value); }
    catch (...) { throw std::invalid_argument("invalid value for " + name); }
}

void printUsage() {
    std::cout
        << "Usage: raft-simulator [options]\n"
        << "  --nodes N\n"
        << "  --seed N\n"
        << "  --events N\n"
        << "  --users N\n"
        << "  --operations N            Operations in random campaign\n"
        << "  --keys N                  Key-space size in random campaign\n"
        << "  --drop-rate P\n"
        << "  --duplicate-rate P\n"
        << "  --scenario basic|failover|partition|random\n"
        << "  --trace-out FILE          Save metadata and complete trace\n"
        << "  --replay FILE             Re-run saved trace configuration and verify hash\n";
}

void scheduleGeneratedUsers(Simulator& simulator, std::uint64_t first, std::uint64_t count,
                            TimeMs start, TimeMs interval) {
    for (std::uint64_t offset = 0; offset < count; ++offset) {
        const auto number = first + offset;
        simulator.scheduleClientPut(start + offset * interval,
            "user:" + std::to_string(number), "value:" + std::to_string(number));
    }
}

void scheduleScenario(Simulator& simulator, const RunOptions& options) {
    constexpr TimeMs firstWriteTime = 500;
    constexpr TimeMs interval = 40;

    if (options.scenario == "basic") {
        scheduleGeneratedUsers(simulator, 1, options.users, firstWriteTime, interval);
        return;
    }

    if (options.scenario == "failover") {
        const auto before = (options.users + 1) / 2;
        const auto after = options.users - before;
        scheduleGeneratedUsers(simulator, 1, before, firstWriteTime, interval);
        const TimeMs crashTime = firstWriteTime + before * interval + 120;
        simulator.scheduleCrashLeader(crashTime);
        const TimeMs secondStart = crashTime + 450;
        scheduleGeneratedUsers(simulator, before + 1, after, secondStart, interval);
        simulator.scheduleRestartLastCrashedLeader(secondStart + after * interval + 150);
        return;
    }

    if (options.scenario == "partition") {
        if (options.config.nodeCount < 3) throw std::invalid_argument("partition scenario needs at least 3 nodes");
        const auto before = (options.users + 1) / 2;
        const auto after = options.users - before;
        scheduleGeneratedUsers(simulator, 1, before, firstWriteTime, interval);
        const TimeMs partitionTime = firstWriteTime + before * interval + 100;
        std::set<NodeId> majority;
        for (NodeId id = 2; id <= options.config.nodeCount; ++id) majority.insert(id);
        simulator.schedulePartition(partitionTime, {1}, majority);
        const TimeMs healTime = partitionTime + 500;
        simulator.scheduleHeal(healTime);
        scheduleGeneratedUsers(simulator, before + 1, after, healTime + 250, interval);
        return;
    }

    if (options.scenario == "random") {
        if (options.keys == 0) throw std::invalid_argument("--keys must be positive");
        DeterministicRandom workload(options.config.seed ^ 0x9e3779b97f4a7c15ULL);
        TimeMs time = 500;
        TimeMs crashUntil = 0;
        TimeMs partitionUntil = 0;

        for (std::uint64_t i = 1; i <= options.operations; ++i) {
            time += workload.range(20, 80);
            const auto choice = workload.range(0, 99);
            const auto keyNumber = workload.range(1, options.keys);
            const std::string key = "key:" + std::to_string(keyNumber);

            if (choice < 62) {
                simulator.scheduleClientPut(time, key, "value:" + std::to_string(i));
            } else if (choice < 75) {
                simulator.scheduleClientDelete(time, key);
            } else if (choice < 84 && time >= crashUntil) {
                const NodeId nodeId = static_cast<NodeId>(workload.range(1, options.config.nodeCount));
                const TimeMs restartTime = time + workload.range(250, 450);
                simulator.scheduleCrash(time, nodeId);
                simulator.scheduleRestart(restartTime, nodeId);
                crashUntil = restartTime;
            } else if (choice < 94 && time >= partitionUntil && options.config.nodeCount >= 3) {
                std::set<NodeId> majority;
                for (NodeId id = 2; id <= options.config.nodeCount; ++id) majority.insert(id);
                const TimeMs healTime = time + workload.range(200, 400);
                simulator.schedulePartition(time, {1}, majority);
                simulator.scheduleHeal(healTime);
                partitionUntil = healTime;
            } else {
                simulator.scheduleClientPut(time, key, "value:" + std::to_string(i));
            }
        }

        const TimeMs recoveryTime = time + 600;
        simulator.scheduleHeal(recoveryTime);
        for (NodeId id = 1; id <= options.config.nodeCount; ++id) {
            simulator.scheduleRestart(recoveryTime + id * 10, id);
        }
        return;
    }

    throw std::invalid_argument("unknown scenario: " + options.scenario);
}

void writeTraceFile(const std::string& path, const RunOptions& options,
                    const Simulator& simulator) {
    std::ofstream out(path);
    if (!out) throw std::runtime_error("cannot open trace output: " + path);
    out << "RAFTKV_TRACE_V1\n"
        << "seed=" << options.config.seed << '\n'
        << "nodes=" << options.config.nodeCount << '\n'
        << "events=" << options.events << '\n'
        << "users=" << options.users << '\n'
        << "operations=" << options.operations << '\n'
        << "keys=" << options.keys << '\n'
        << "drop_rate=" << options.config.dropRate << '\n'
        << "duplicate_rate=" << options.config.duplicateRate << '\n'
        << "scenario=" << options.scenario << '\n'
        << "trace_hash=" << simulator.traceHash() << '\n'
        << "---TRACE---\n";
    for (const auto& line : simulator.traceLines()) out << line << '\n';
}

RunOptions readReplayOptions(const std::string& path, std::string& expectedHash) {
    std::ifstream in(path);
    if (!in) throw std::runtime_error("cannot open replay file: " + path);
    std::string line;
    std::getline(in, line);
    if (line != "RAFTKV_TRACE_V1") throw std::runtime_error("unsupported trace format");

    std::map<std::string, std::string> metadata;
    while (std::getline(in, line) && line != "---TRACE---") {
        const auto position = line.find('=');
        if (position != std::string::npos) metadata[line.substr(0, position)] = line.substr(position + 1);
    }

    RunOptions options;
    options.config.seed = parseU64(metadata.at("seed"), "seed");
    options.config.nodeCount = parseU64(metadata.at("nodes"), "nodes");
    options.events = parseU64(metadata.at("events"), "events");
    options.users = parseU64(metadata.at("users"), "users");
    options.operations = parseU64(metadata.at("operations"), "operations");
    options.keys = parseU64(metadata.at("keys"), "keys");
    options.config.dropRate = parseDouble(metadata.at("drop_rate"), "drop_rate");
    options.config.duplicateRate = parseDouble(metadata.at("duplicate_rate"), "duplicate_rate");
    options.scenario = metadata.at("scenario");
    expectedHash = metadata.at("trace_hash");
    return options;
}

int execute(const RunOptions& options, const std::string& expectedHash = {}) {
    Simulator simulator(options.config);
    simulator.initialize();
    scheduleScenario(simulator, options);
    simulator.run(options.events);

    for (const auto& line : simulator.traceLines()) std::cout << line << '\n';
    std::cout << "Trace hash: " << simulator.traceHash() << '\n';

    if (!expectedHash.empty()) {
        if (simulator.traceHash() != expectedHash) {
            std::cerr << "Replay mismatch: expected " << expectedHash
                      << " but got " << simulator.traceHash() << '\n';
            return 2;
        }
        std::cout << "Replay verification: PASS\n";
    }

    const auto linear = simulator.checkLinearizability();
    std::cout << "Write linearizability: " << (linear.ok ? "PASS" : "FAIL")
              << " completed=" << linear.completedOperations
              << " pending=" << linear.pendingOperations
              << " explanation=" << linear.explanation << '\n';

    for (NodeId id = 1; id <= options.config.nodeCount; ++id) {
        const auto& node = simulator.node(id);
        std::cout << "Node " << id
                  << " role=" << toString(node.role())
                  << " term=" << node.currentTerm()
                  << " commit=" << node.commitIndex()
                  << " applied=" << node.lastApplied()
                  << " alive=" << (simulator.isAlive(id) ? "true" : "false") << '\n';

        std::vector<std::pair<std::string, std::string>> values(
            node.stateMachine().data().begin(), node.stateMachine().data().end());
        std::sort(values.begin(), values.end());
        for (const auto& [key, value] : values) std::cout << "  " << key << '=' << value << '\n';
    }

    if (!options.traceOut.empty()) {
        writeTraceFile(options.traceOut, options, simulator);
        std::cout << "Trace saved: " << options.traceOut << '\n';
    }

    return linear.ok ? 0 : 3;
}

} // namespace

int main(int argc, char** argv) {
    try {
        RunOptions options;
        std::string replayFile;

        for (int i = 1; i < argc; ++i) {
            const std::string arg = argv[i];
            auto value = [&](const std::string& name) -> std::string {
                if (i + 1 >= argc) throw std::invalid_argument("missing value for " + name);
                return argv[++i];
            };

            if (arg == "--nodes") options.config.nodeCount = parseU64(value(arg), arg);
            else if (arg == "--seed") options.config.seed = parseU64(value(arg), arg);
            else if (arg == "--events") options.events = parseU64(value(arg), arg);
            else if (arg == "--users") options.users = parseU64(value(arg), arg);
            else if (arg == "--operations") options.operations = parseU64(value(arg), arg);
            else if (arg == "--keys") options.keys = parseU64(value(arg), arg);
            else if (arg == "--drop-rate") options.config.dropRate = parseDouble(value(arg), arg);
            else if (arg == "--duplicate-rate") options.config.duplicateRate = parseDouble(value(arg), arg);
            else if (arg == "--scenario") options.scenario = value(arg);
            else if (arg == "--trace-out") options.traceOut = value(arg);
            else if (arg == "--replay") replayFile = value(arg);
            else if (arg == "--help") { printUsage(); return 0; }
            else throw std::invalid_argument("unknown argument: " + arg);
        }

        if (!replayFile.empty()) {
            std::string expected;
            const auto replayOptions = readReplayOptions(replayFile, expected);
            return execute(replayOptions, expected);
        }

        if (options.users == 0) throw std::invalid_argument("--users must be at least 1");
        return execute(options);
    } catch (const std::exception& error) {
        std::cerr << "error: " << error.what() << '\n';
        return 1;
    }
}
