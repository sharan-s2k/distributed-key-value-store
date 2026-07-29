#include "simulator.h"

#include <cstdlib>
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
} // namespace

int main() {
    try {
        testDeterministicTrace();
        testGeneratedWorkloadReplicationAndLinearizability();
        testLeaderFailover();
        testDeleteHistory();
        std::cout << "all tests passed\n";
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr << "test failure: " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
