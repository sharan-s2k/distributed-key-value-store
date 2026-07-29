#include "storage.h"

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <string>
#include <system_error>
#include <unistd.h>

namespace raftkv {
namespace {

constexpr std::uint32_t kMagic = 0x524B5657U; // "RKVW"
constexpr std::uint16_t kVersion = 1;
constexpr std::size_t kHeaderSize = 16;
constexpr std::uint32_t kMaxPayloadBytes = 64U * 1024U * 1024U;

struct RecordHeader {
    std::uint32_t magic{0};
    std::uint16_t version{0};
    std::uint16_t type{0};
    std::uint32_t payloadLength{0};
    std::uint32_t checksum{0};
};

[[noreturn]] void throwSystemError(const std::string& operation) {
    throw std::system_error(errno, std::generic_category(), operation);
}

void writeAll(int fd, const std::uint8_t* data, std::size_t size) {
    std::size_t written = 0;
    while (written < size) {
        const ssize_t result = ::write(fd, data + written, size - written);
        if (result < 0) {
            if (errno == EINTR) continue;
            throwSystemError("write WAL");
        }
        if (result == 0) throw std::runtime_error("write WAL returned zero bytes");
        written += static_cast<std::size_t>(result);
    }
}

std::uint32_t checksum32(const std::vector<std::uint8_t>& payload) {
    std::uint32_t hash = 2166136261U;
    for (const std::uint8_t byte : payload) {
        hash ^= byte;
        hash *= 16777619U;
    }
    return hash;
}

template <typename T>
void appendInteger(std::vector<std::uint8_t>& output, T value) {
    static_assert(std::is_unsigned_v<T>);
    for (std::size_t i = 0; i < sizeof(T); ++i) {
        output.push_back(static_cast<std::uint8_t>((value >> (i * 8U)) & 0xFFU));
    }
}

template <typename T>
T readInteger(const std::vector<std::uint8_t>& input, std::size_t& offset) {
    static_assert(std::is_unsigned_v<T>);
    if (offset + sizeof(T) > input.size()) throw std::runtime_error("truncated WAL payload");
    T value = 0;
    for (std::size_t i = 0; i < sizeof(T); ++i) {
        value |= static_cast<T>(input[offset + i]) << (i * 8U);
    }
    offset += sizeof(T);
    return value;
}

void appendString(std::vector<std::uint8_t>& output, const std::string& value) {
    if (value.size() > std::numeric_limits<std::uint32_t>::max()) {
        throw std::length_error("WAL string is too large");
    }
    appendInteger<std::uint32_t>(output, static_cast<std::uint32_t>(value.size()));
    output.insert(output.end(), value.begin(), value.end());
}

std::string readString(const std::vector<std::uint8_t>& input, std::size_t& offset) {
    const auto length = readInteger<std::uint32_t>(input, offset);
    if (offset + length > input.size()) throw std::runtime_error("truncated WAL string");
    std::string value(
        reinterpret_cast<const char*>(input.data() + offset),
        static_cast<std::size_t>(length)
    );
    offset += length;
    return value;
}

std::vector<std::uint8_t> encodeHeader(const RecordHeader& header) {
    std::vector<std::uint8_t> bytes;
    bytes.reserve(kHeaderSize);
    appendInteger<std::uint32_t>(bytes, header.magic);
    appendInteger<std::uint16_t>(bytes, header.version);
    appendInteger<std::uint16_t>(bytes, header.type);
    appendInteger<std::uint32_t>(bytes, header.payloadLength);
    appendInteger<std::uint32_t>(bytes, header.checksum);
    return bytes;
}

RecordHeader decodeHeader(const std::vector<std::uint8_t>& bytes, std::size_t offset) {
    if (offset + kHeaderSize > bytes.size()) throw std::runtime_error("truncated WAL header");
    std::vector<std::uint8_t> headerBytes(
        bytes.begin() + static_cast<std::ptrdiff_t>(offset),
        bytes.begin() + static_cast<std::ptrdiff_t>(offset + kHeaderSize)
    );
    std::size_t cursor = 0;
    return RecordHeader{
        readInteger<std::uint32_t>(headerBytes, cursor),
        readInteger<std::uint16_t>(headerBytes, cursor),
        readInteger<std::uint16_t>(headerBytes, cursor),
        readInteger<std::uint32_t>(headerBytes, cursor),
        readInteger<std::uint32_t>(headerBytes, cursor)
    };
}

std::vector<std::uint8_t> encodeTermAndVote(
    Term term,
    std::optional<NodeId> votedFor
) {
    std::vector<std::uint8_t> payload;
    appendInteger<std::uint64_t>(payload, term);
    appendInteger<std::uint8_t>(payload, votedFor.has_value() ? 1U : 0U);
    appendInteger<std::uint64_t>(payload, votedFor.value_or(0));
    return payload;
}

std::vector<std::uint8_t> encodeLog(const std::vector<LogEntry>& log) {
    std::vector<std::uint8_t> payload;
    appendInteger<std::uint64_t>(payload, static_cast<std::uint64_t>(log.size()));
    for (const auto& entry : log) {
        appendInteger<std::uint64_t>(payload, entry.term);
        appendInteger<std::uint8_t>(payload, static_cast<std::uint8_t>(entry.command.type));
        appendInteger<std::uint64_t>(payload, entry.command.requestId);
        appendString(payload, entry.command.key);
        appendString(payload, entry.command.value);
    }
    return payload;
}

PersistentState applyTermAndVote(
    PersistentState state,
    const std::vector<std::uint8_t>& payload
) {
    std::size_t offset = 0;
    state.currentTerm = readInteger<std::uint64_t>(payload, offset);
    const bool hasVote = readInteger<std::uint8_t>(payload, offset) != 0;
    const NodeId vote = readInteger<std::uint64_t>(payload, offset);
    if (offset != payload.size()) throw std::runtime_error("unexpected bytes in term/vote WAL record");
    state.votedFor = hasVote ? std::optional<NodeId>{vote} : std::nullopt;
    return state;
}

std::vector<LogEntry> decodeLog(const std::vector<std::uint8_t>& payload) {
    std::size_t offset = 0;
    const auto count = readInteger<std::uint64_t>(payload, offset);
    if (count > 10'000'000ULL) throw std::runtime_error("unreasonable WAL log entry count");

    std::vector<LogEntry> log;
    log.reserve(static_cast<std::size_t>(count));
    for (std::uint64_t i = 0; i < count; ++i) {
        const Term term = readInteger<std::uint64_t>(payload, offset);
        const auto rawType = readInteger<std::uint8_t>(payload, offset);
        if (rawType > static_cast<std::uint8_t>(CommandType::Delete)) {
            throw std::runtime_error("unknown command type in WAL");
        }
        Command command;
        command.type = static_cast<CommandType>(rawType);
        command.requestId = readInteger<std::uint64_t>(payload, offset);
        command.key = readString(payload, offset);
        command.value = readString(payload, offset);
        log.push_back(LogEntry{term, std::move(command)});
    }
    if (offset != payload.size()) throw std::runtime_error("unexpected bytes in log WAL record");
    return log;
}

} // namespace

DiskWalStorage::DiskWalStorage(std::filesystem::path walPath)
    : walPath_(std::move(walPath)) {
    if (walPath_.empty()) throw std::invalid_argument("WAL path cannot be empty");
    if (walPath_.has_parent_path()) {
        std::filesystem::create_directories(walPath_.parent_path());
    }
    replay();
}

void DiskWalStorage::saveTermAndVote(Term term, std::optional<NodeId> votedFor) {
    appendRecord(RecordType::TermAndVote, encodeTermAndVote(term, votedFor));
    state_.currentTerm = term;
    state_.votedFor = votedFor;
}

void DiskWalStorage::saveLog(const std::vector<LogEntry>& log) {
    validateLog(log);
    appendRecord(RecordType::LogSnapshot, encodeLog(log));
    state_.log = log;
}

std::uintmax_t DiskWalStorage::fileSize() const {
    if (!std::filesystem::exists(walPath_)) return 0;
    return std::filesystem::file_size(walPath_);
}

void DiskWalStorage::appendRecord(
    RecordType type,
    const std::vector<std::uint8_t>& payload
) {
    if (payload.size() > kMaxPayloadBytes) throw std::length_error("WAL record is too large");

    const RecordHeader header{
        kMagic,
        kVersion,
        static_cast<std::uint16_t>(type),
        static_cast<std::uint32_t>(payload.size()),
        checksum32(payload)
    };
    const auto headerBytes = encodeHeader(header);

    const int fd = ::open(walPath_.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0644);
    if (fd < 0) throwSystemError("open WAL");

    try {
        writeAll(fd, headerBytes.data(), headerBytes.size());
        if (!payload.empty()) writeAll(fd, payload.data(), payload.size());
        if (::fsync(fd) != 0) throwSystemError("fsync WAL");
        if (::close(fd) != 0) throwSystemError("close WAL");
    } catch (...) {
        const int savedErrno = errno;
        ::close(fd);
        errno = savedErrno;
        throw;
    }
}

void DiskWalStorage::replay() {
    state_ = PersistentState{};
    if (!std::filesystem::exists(walPath_)) return;

    std::ifstream input(walPath_, std::ios::binary);
    if (!input) throw std::runtime_error("cannot open WAL for replay: " + walPath_.string());
    const std::vector<std::uint8_t> bytes{
        std::istreambuf_iterator<char>(input),
        std::istreambuf_iterator<char>()
    };

    std::size_t offset = 0;
    std::size_t validOffset = 0;
    while (offset < bytes.size()) {
        const std::size_t recordStart = offset;
        if (bytes.size() - offset < kHeaderSize) {
            std::filesystem::resize_file(walPath_, validOffset);
            return;
        }

        const RecordHeader header = decodeHeader(bytes, offset);
        if (header.magic != kMagic || header.version != kVersion) {
            throw std::runtime_error("invalid WAL header at byte " + std::to_string(recordStart));
        }
        if (header.payloadLength > kMaxPayloadBytes) {
            throw std::runtime_error("invalid WAL payload length at byte " + std::to_string(recordStart));
        }
        offset += kHeaderSize;

        const std::size_t recordEnd = offset + header.payloadLength;
        if (recordEnd > bytes.size()) {
            std::filesystem::resize_file(walPath_, validOffset);
            return;
        }

        std::vector<std::uint8_t> payload(
            bytes.begin() + static_cast<std::ptrdiff_t>(offset),
            bytes.begin() + static_cast<std::ptrdiff_t>(recordEnd)
        );
        if (checksum32(payload) != header.checksum) {
            if (recordEnd == bytes.size()) {
                std::filesystem::resize_file(walPath_, validOffset);
                return;
            }
            throw std::runtime_error("WAL checksum failure at byte " + std::to_string(recordStart));
        }

        switch (static_cast<RecordType>(header.type)) {
            case RecordType::TermAndVote:
                state_ = applyTermAndVote(std::move(state_), payload);
                break;
            case RecordType::LogSnapshot:
                state_.log = decodeLog(payload);
                validateLog(state_.log);
                break;
            default:
                throw std::runtime_error("unknown WAL record type at byte " + std::to_string(recordStart));
        }

        offset = recordEnd;
        validOffset = offset;
    }
}

void DiskWalStorage::validateLog(const std::vector<LogEntry>& log) {
    if (log.empty() || log.front().term != 0 ||
        log.front().command.type != CommandType::Noop) {
        throw std::logic_error("raft log must retain sentinel entry");
    }
}

} // namespace raftkv