#include "simulator.h"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>

using namespace raftkv;

namespace {
void require(bool condition, const std::string& message) {
    if (!condition) throw std::runtime_error(message);
}

void testDeterministicTrace() {
    SimulatorConfig config;
    config.seed = 12345;
    config.dropRate = 0.03;
    config.duplicateRate = 0.02;

    Simulator a(config);
    a.initialize();
    a.scheduleClientPut(500, "a", "1");
    a.schedulePartition(800, {1}, {2, 3});
    a.scheduleHeal(1400);
    a.scheduleClientPut(1700, "b", "2");
    a.run(1200);

    Simulator b(config);
    b.initialize();
    b.scheduleClientPut(500, "a", "1");
    b.schedulePartition(800, {1}, {2, 3});
    b.scheduleHeal(1400);
    b.scheduleClientPut(1700, "b", "2");
    b.run(1200);

    require(a.traceHash() == b.traceHash(), "same seed must produce identical trace");
}

void testGeneratedWorkloadReplicationAndLinearizability() {
    SimulatorConfig config;
    config.seed = 7;

    Simulator sim(config);
    sim.initialize();
    for (std::uint64_t i = 1; i <= 25; ++i) {
        sim.scheduleClientPut(500 + i * 30, "user:" + std::to_string(i), "value:" + std::to_string(i));
    }
    sim.run(3500);

    for (NodeId id = 2; id <= 3; ++id) {
        require(sim.node(id).stateMachine().data() == sim.node(1).stateMachine().data(),
                "replicas must converge");
    }
    require(sim.checkLinearizability().ok, "generated history must be write-linearizable");
}

void testLeaderFailover() {
    SimulatorConfig config;
    config.seed = 99;

    Simulator sim(config);
    sim.initialize();
    sim.run(250);
    const auto leader = sim.currentLeader();
    require(leader.has_value(), "leader should be elected");

    sim.scheduleCrashLeader(sim.now() + 10);
    sim.scheduleClientPut(sim.now() + 700, "after", "failover");
    sim.scheduleRestartLastCrashedLeader(sim.now() + 1000);
    sim.run(1800);

    const auto newLeader = sim.currentLeader();
    require(newLeader.has_value(), "new leader should be elected");
    require(*newLeader != *leader, "new leader must differ from crashed leader");
    require(sim.checkLinearizability().ok, "failover history must be write-linearizable");
}

void testDeleteHistory() {
    SimulatorConfig config;
    config.seed = 555;
    Simulator sim(config);
    sim.initialize();
    sim.scheduleClientPut(500, "x", "1");
    sim.scheduleClientPut(800, "x", "2");
    sim.scheduleClientDelete(1100, "x");
    sim.run(1800);
    require(!sim.node(1).stateMachine().get("x").has_value(), "delete must be replicated");
    require(sim.checkLinearizability().ok, "put/delete history must be write-linearizable");
}

std::filesystem::path temporaryWalPath(const std::string& name) {
    const auto directory = std::filesystem::temp_directory_path() / "raftkv-wal-tests";
    std::filesystem::create_directories(directory);
    const auto path = directory / name;
    std::filesystem::remove(path);
    return path;
}

void testDiskWalRecovery() {
    const auto path = temporaryWalPath("recovery.wal");
    {
        DiskWalStorage storage(path);
        storage.saveTermAndVote(7, NodeId{2});
        std::vector<LogEntry> log{
            LogEntry{0, Command::noop()},
            LogEntry{6, Command::put(101, "user:1", "value:1")},
            LogEntry{7, Command::erase(102, "user:2")}
        };
        storage.saveLog(log);
        require(storage.fileSize() > 0, "WAL file must contain durable records");
    }

    DiskWalStorage recovered(path);
    require(recovered.load().currentTerm == 7, "WAL must recover current term");
    require(recovered.load().votedFor == NodeId{2}, "WAL must recover votedFor");
    require(recovered.load().log.size() == 3, "WAL must recover full Raft log");
    require(recovered.load().log[1].command.key == "user:1", "WAL must recover commands");
    std::filesystem::remove(path);
}

void testDiskWalTruncatedTailRecovery() {
    const auto path = temporaryWalPath("truncated-tail.wal");
    std::uintmax_t validSize = 0;
    {
        DiskWalStorage storage(path);
        storage.saveTermAndVote(3, NodeId{1});
        storage.saveLog({
            LogEntry{0, Command::noop()},
            LogEntry{3, Command::put(1, "x", "10")}
        });
        validSize = storage.fileSize();
    }

    {
        std::ofstream out(path, std::ios::binary | std::ios::app);
        const char tornRecord[] = {0x57, 0x56, 0x4b};
        out.write(tornRecord, sizeof(tornRecord));
    }
    require(std::filesystem::file_size(path) > validSize, "test must append torn tail bytes");

    DiskWalStorage recovered(path);
    require(recovered.load().currentTerm == 3, "valid records before torn tail must survive");
    require(recovered.load().log.size() == 2, "valid log must survive torn tail");
    require(std::filesystem::file_size(path) == validSize, "torn WAL tail must be truncated");
    std::filesystem::remove(path);
}

void testSimulatorWithDiskWal() {
    const auto directory = std::filesystem::temp_directory_path() / "raftkv-simulator-wal";
    std::filesystem::remove_all(directory);

    SimulatorConfig config;
    config.seed = 909;
    config.walDirectory = directory;
    config.resetWalOnInitialize = true;

    Simulator sim(config);
    sim.initialize();
    sim.scheduleClientPut(500, "durable", "value");
    sim.run(700);

    for (NodeId id = 1; id <= 3; ++id) {
        require(std::filesystem::exists(directory / ("node-" + std::to_string(id) + ".wal")),
                "each node must have a WAL file");
    }
    require(sim.checkLinearizability().ok, "disk-WAL simulation must remain linearizable");
    std::filesystem::remove_all(directory);
}

} // namespace

int main() {
    try {
        testDeterministicTrace();
        testGeneratedWorkloadReplicationAndLinearizability();
        testLeaderFailover();
        testDeleteHistory();
        testDiskWalRecovery();
        testDiskWalTruncatedTailRecovery();
        testSimulatorWithDiskWal();
        std::cout << "all tests passed\n";
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr << "test failure: " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}