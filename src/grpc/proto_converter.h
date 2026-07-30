#pragma once

#include "raft.pb.h"
#include "types.h"

namespace raftkv {

rpc::Envelope toProto(const Envelope& envelope);
Envelope fromProto(const rpc::Envelope& envelope);

} // namespace raftkv
