#pragma once

#include "types.h"

#include <optional>
#include <string>
#include <unordered_map>

namespace raftkv {

class KeyValueStateMachine {
public:
    void apply(const Command& command) {
        switch (command.type) {
            case CommandType::Noop:
                break;
            case CommandType::Put:
                data_[command.key] = command.value;
                break;
            case CommandType::Delete:
                data_.erase(command.key);
                break;
        }
    }

    std::optional<std::string> get(const std::string& key) const {
        const auto it = data_.find(key);
        if (it == data_.end()) return std::nullopt;
        return it->second;
    }

    const std::unordered_map<std::string, std::string>& data() const {
        return data_;
    }

private:
    std::unordered_map<std::string, std::string> data_;
};

} // namespace raftkv
