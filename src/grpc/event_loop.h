#pragma once

#include "types.h"

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <thread>
#include <type_traits>
#include <utility>

namespace raftkv {

class EventLoop {
public:
    EventLoop() = default;
    ~EventLoop();

    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    void start();
    void stop();
    void post(std::function<void()> task);
    void scheduleAt(TimeMs deadlineMs, std::function<void()> task);

    static TimeMs nowMs();

    template <typename Fn>
    auto invoke(Fn&& fn) -> std::invoke_result_t<Fn> {
        using Result = std::invoke_result_t<Fn>;
        auto promise = std::make_shared<std::promise<Result>>();
        auto future = promise->get_future();
        post([promise, fn = std::forward<Fn>(fn)]() mutable {
            try {
                if constexpr (std::is_void_v<Result>) {
                    fn();
                    promise->set_value();
                } else {
                    promise->set_value(fn());
                }
            } catch (...) {
                promise->set_exception(std::current_exception());
            }
        });
        return future.get();
    }

private:
    struct ScheduledTask {
        TimeMs deadlineMs{0};
        std::uint64_t sequence{0};
        std::function<void()> task;
    };

    struct LaterFirst {
        bool operator()(const ScheduledTask& left, const ScheduledTask& right) const {
            if (left.deadlineMs != right.deadlineMs) return left.deadlineMs > right.deadlineMs;
            return left.sequence > right.sequence;
        }
    };

    void run();

    std::mutex mutex_;
    std::condition_variable cv_;
    std::priority_queue<ScheduledTask, std::vector<ScheduledTask>, LaterFirst> tasks_;
    std::thread thread_;
    bool running_{false};
    bool stopping_{false};
    std::uint64_t nextSequence_{1};
};

} // namespace raftkv
