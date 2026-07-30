#include "grpc/event_loop.h"

#include <chrono>
#include <stdexcept>

namespace raftkv {

EventLoop::~EventLoop() {
    stop();
}

void EventLoop::start() {
    std::lock_guard lock(mutex_);
    if (running_) return;
    stopping_ = false;
    running_ = true;
    thread_ = std::thread([this] { run(); });
}

void EventLoop::stop() {
    {
        std::lock_guard lock(mutex_);
        if (!running_) return;
        stopping_ = true;
    }
    cv_.notify_all();
    if (thread_.joinable()) thread_.join();
    std::lock_guard lock(mutex_);
    running_ = false;
}

void EventLoop::post(std::function<void()> task) {
    scheduleAt(nowMs(), std::move(task));
}

void EventLoop::scheduleAt(TimeMs deadlineMs, std::function<void()> task) {
    {
        std::lock_guard lock(mutex_);
        if (!running_ || stopping_) throw std::runtime_error("event loop is not running");
        tasks_.push(ScheduledTask{deadlineMs, nextSequence_++, std::move(task)});
    }
    cv_.notify_all();
}

TimeMs EventLoop::nowMs() {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return static_cast<TimeMs>(
        std::chrono::duration_cast<std::chrono::milliseconds>(now).count()
    );
}

void EventLoop::run() {
    std::unique_lock lock(mutex_);
    while (!stopping_) {
        if (tasks_.empty()) {
            cv_.wait(lock, [this] { return stopping_ || !tasks_.empty(); });
            continue;
        }

        const TimeMs now = nowMs();
        const TimeMs deadline = tasks_.top().deadlineMs;
        if (deadline > now) {
            cv_.wait_for(lock, std::chrono::milliseconds(deadline - now));
            continue;
        }

        auto task = std::move(const_cast<ScheduledTask&>(tasks_.top()).task);
        tasks_.pop();
        lock.unlock();
        try {
            task();
        } catch (...) {
            // Runtime tasks are isolated so one malformed RPC does not terminate the node.
        }
        lock.lock();
    }

    while (!tasks_.empty()) tasks_.pop();
}

} // namespace raftkv
