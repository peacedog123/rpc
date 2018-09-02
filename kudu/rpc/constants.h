#ifndef KUDU_RPC_RPC_CONSTANTS_H
#define KUDU_RPC_RPC_CONSTANTS_H

#include <cstdint>
#include <set>

namespace kudu {
namespace rpc {

// Magic number bytes sent at connection setup time.
extern const char* const kMagicNumber;

// Current version of the RPC protocol.
static const uint32_t kCurrentRpcVersion = 9;

// From Hadoop.
static const int32_t kInvalidCallId = -2;
static const int32_t kConnectionContextCallId = -3;
static const int32_t kNegotiateCallId = -33;

static const uint8_t kMagicNumberLength = 4;
static const uint8_t kHeaderFlagsLength = 3;

// There is a 4-byte length prefix before any packet.
static const uint8_t kMsgLengthPrefixLength = 4;

}
}
#endif
