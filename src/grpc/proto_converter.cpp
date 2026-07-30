#include "grpc/proto_converter.h"

#include <stdexcept>
#include <type_traits>

namespace raftkv {
namespace {

rpc::Command toProtoCommand(const Command& command) {
    rpc::Command output;
    output.set_type(static_cast<rpc::Command::Type>(command.type));
    output.set_request_id(command.requestId);
    output.set_key(command.key);
    output.set_value(command.value);
    return output;
}

Command fromProtoCommand(const rpc::Command& command) {
    if (command.type() < rpc::Command::NOOP || command.type() > rpc::Command::DELETE) {
        throw std::invalid_argument("unknown protobuf command type");
    }
    return Command{
        static_cast<CommandType>(command.type()),
        command.request_id(),
        command.key(),
        command.value()
    };
}

rpc::LogEntry toProtoLogEntry(const LogEntry& entry) {
    rpc::LogEntry output;
    output.set_term(entry.term);
    *output.mutable_command() = toProtoCommand(entry.command);
    return output;
}

LogEntry fromProtoLogEntry(const rpc::LogEntry& entry) {
    return LogEntry{entry.term(), fromProtoCommand(entry.command())};
}

} // namespace

rpc::Envelope toProto(const Envelope& envelope) {
    rpc::Envelope output;
    output.set_message_id(envelope.messageId);
    output.set_from(envelope.from);
    output.set_to(envelope.to);

    std::visit([&](const auto& message) {
        using T = std::decay_t<decltype(message)>;
        if constexpr (std::is_same_v<T, RequestVote>) {
            auto* target = output.mutable_request_vote();
            target->set_term(message.term);
            target->set_candidate_id(message.candidateId);
            target->set_last_log_index(message.lastLogIndex);
            target->set_last_log_term(message.lastLogTerm);
        } else if constexpr (std::is_same_v<T, RequestVoteResponse>) {
            auto* target = output.mutable_request_vote_response();
            target->set_term(message.term);
            target->set_vote_granted(message.voteGranted);
        } else if constexpr (std::is_same_v<T, AppendEntries>) {
            auto* target = output.mutable_append_entries();
            target->set_term(message.term);
            target->set_leader_id(message.leaderId);
            target->set_prev_log_index(message.prevLogIndex);
            target->set_prev_log_term(message.prevLogTerm);
            target->set_leader_commit(message.leaderCommit);
            for (const auto& entry : message.entries) {
                *target->add_entries() = toProtoLogEntry(entry);
            }
        } else if constexpr (std::is_same_v<T, AppendEntriesResponse>) {
            auto* target = output.mutable_append_entries_response();
            target->set_term(message.term);
            target->set_success(message.success);
            target->set_match_index(message.matchIndex);
            target->set_conflict_index(message.conflictIndex);
        }
    }, envelope.payload);
    return output;
}

Envelope fromProto(const rpc::Envelope& envelope) {
    Envelope output;
    output.messageId = envelope.message_id();
    output.from = envelope.from();
    output.to = envelope.to();

    switch (envelope.payload_case()) {
        case rpc::Envelope::kRequestVote: {
            const auto& value = envelope.request_vote();
            output.payload = RequestVote{
                value.term(), value.candidate_id(), value.last_log_index(), value.last_log_term()
            };
            break;
        }
        case rpc::Envelope::kRequestVoteResponse: {
            const auto& value = envelope.request_vote_response();
            output.payload = RequestVoteResponse{value.term(), value.vote_granted()};
            break;
        }
        case rpc::Envelope::kAppendEntries: {
            const auto& value = envelope.append_entries();
            AppendEntries message;
            message.term = value.term();
            message.leaderId = value.leader_id();
            message.prevLogIndex = value.prev_log_index();
            message.prevLogTerm = value.prev_log_term();
            message.leaderCommit = value.leader_commit();
            message.entries.reserve(static_cast<std::size_t>(value.entries_size()));
            for (const auto& entry : value.entries()) message.entries.push_back(fromProtoLogEntry(entry));
            output.payload = std::move(message);
            break;
        }
        case rpc::Envelope::kAppendEntriesResponse: {
            const auto& value = envelope.append_entries_response();
            output.payload = AppendEntriesResponse{
                value.term(), value.success(), value.match_index(), value.conflict_index()
            };
            break;
        }
        case rpc::Envelope::PAYLOAD_NOT_SET:
            throw std::invalid_argument("protobuf envelope has no payload");
    }
    return output;
}

} // namespace raftkv
