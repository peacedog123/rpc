#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest_prod.h>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/rpc/connection_id.h"
#include "kudu/rpc/constants.h"
#include "kudu/rpc/remote_method.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/rpc/transfer.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "rpc_header.pb.h"


namespace google {
namespace protobuf {
class Message;
} // namespace protobuf
} // namespace google

namespace kudu {
namespace rpc {

class CallResponse;
class RpcController;
class RpcSidecar;
class RequestHeader;

class OutboundCall {
 public:

  // Phases of an outbound RPC. Making an outbound RPC might involve establishing
  // a connection to the remote server first, and the actual call is made only
  // once the connection to the server is established.
  enum class Phase {
    // The phase of connection negotiation between the caller and the callee.
    CONNECTION_NEGOTIATION,

    // The phase of sending a call over already established connection.
    REMOTE_CALL,
  };

  OutboundCall(const ConnectionId& conn_id, const RemoteMethod& remote_method,
               google::protobuf::Message* response_storage,
               RpcController* controller, ResponseCallback callback);

  ~OutboundCall();

  // Serialize the given request PB into this call's internal storage, and assume
  // ownership of any sidecars that should accompany this request.
  //
  // Because the request data is fully serialized by this call, 'req' may be subsequently
  // mutated with no ill effects.
  void SetRequestPayload(const google::protobuf::Message& req);

  // Assign the call ID for this call. This is called from the reactor
  // thread once a connection has been assigned. Must only be called once.
  void set_call_id(int32_t call_id) {
    DCHECK_EQ(header_.call_id(), kInvalidCallId) << "Already has a call ID";
    header_.set_call_id(call_id);
  }

  // Serialize the call for the wire. Requires that SetRequestPayload()
  // is called first. This is called from the Reactor thread.
  // Returns the number of slices in the serialized call.
  size_t SerializeTo(TransferPayload* slices);

  // Callback after the call has been put on the outbound connection queue.
  void SetQueued();

  // Update the call state to show that the request has started being sent
  // on the socket.
  void SetSending();

  // Update the call state to show that the request has been sent.
  void SetSent();

  // Mark the call as failed. This also triggers the callback to notify
  // the caller. If the call failed due to a remote error, then err_pb
  // should be set to the error returned by the remote server.
  void SetFailed(Status status,
                 Phase phase = Phase::REMOTE_CALL,
                 std::unique_ptr<ErrorStatusPB> err_pb = nullptr);

  ////////////////////////////////////////////////////////////
  // Getters
  ////////////////////////////////////////////////////////////

  RpcController* controller() { return controller_; }
  const RpcController* controller() const { return controller_; }

  const ConnectionId& conn_id() const { return conn_id_; }

  // Return true if a call ID has been assigned to this call.
  bool call_id_assigned() const {
    return header_.call_id() != kInvalidCallId;
  }

  int32_t call_id() const {
    DCHECK(call_id_assigned());
    return header_.call_id();
  }

  // Mark the call as timed out. This also triggers the callback to notify
  // the caller.
  void SetTimedOut(Phase phase);
  bool IsTimedOut() const;

  bool IsCancelled() const;

  // Is the call finished?
  bool IsFinished() const;

  // Fill in the call response.
  void SetResponse(gscoped_ptr<CallResponse> resp);

  std::string ToString() const;

   // Returns true if cancellation has been requested. Must be called from
  // reactor thread.
  bool cancellation_requested() const {
    return cancellation_requested_;
  }

 private:
  friend class RpcController;

  enum State {
    READY = 0,
    ON_OUTBOUND_QUEUE,
    SENDING,
    SENT,
    NEGOTIATION_TIMED_OUT,
    TIMED_OUT,
    CANCELLED,
    FINISHED_NEGOTIATION_ERROR,
    FINISHED_ERROR,
    FINISHED_SUCCESS
  };

  static std::string StateName(State state);

  // Mark the call as cancelled. This also invokes the callback to notify the caller.
  void SetCancelled();

  void set_state(State new_state);

  State state() const;

  // Same as set_state, but requires that the caller already holds
  // lock_
  void set_state_unlocked(State new_state);

  // return current status
  Status status() const;

  // Time when the call was first initiatied.
  MonoTime start_time_;

  // Return the error protobuf, if a remote error occurred.
  // This will only be non-NULL if status().IsRemoteError().
  const ErrorStatusPB* error_pb() const;

  // Lock for state_ status_, error_pb_ fields, since they
  // may be mutated by the reactor thread while the client thread
  // reads them.
  mutable simple_spinlock lock_;
  State state_;
  Status status_;
  std::unique_ptr<ErrorStatusPB> error_pb_;

  // Call the user-provided callback. Note that entries in 'sidecars_' are cleared
  // prior to invoking the callback so the client can assume that the call doesn't
  // hold references to outbound sidecars.
  void CallCallback();

  RequestHeader header_;

  RemoteMethod remote_method_;
  const ConnectionId conn_id_;
  ResponseCallback callback_;
  RpcController* controller_;

  google::protobuf::Message* response_;

  // Buffers for storing segments of the wire-format request.
  faststring header_buf_;
  faststring request_buf_;

  // Once a response has been received for this call, contains that response.
  // Otherwise NULL.
  gscoped_ptr<CallResponse> call_response_;

  // True if cancellation was requested on this call.
  bool cancellation_requested_;

  DISALLOW_COPY_AND_ASSIGN(OutboundCall);
};

// A response to a call, on the client side.
// Upon receiving a response, this is allocated in the reactor thread and filled
// into the OutboundCall instance via OutboundCall::SetResponse.
//
// This may either be a success or error response.
//
// This class takes care of separating out the distinct payload slices sent
// over.
class CallResponse {
 public:
  CallResponse();

  // Parse the response received from a call. This must be called before any
  // other methods on this object.
  Status ParseFrom(gscoped_ptr<InboundTransfer> transfer);

  // Return true if the call succeeded.
  bool is_success() const {
    DCHECK(parsed_);
    return !header_.is_error();
  }

  // Return the call ID that this response is related to.
  int32_t call_id() const {
    DCHECK(parsed_);
    return header_.call_id();
  }

  // Return the serialized response data. This is just the response "body" --
  // either a serialized ErrorStatusPB, or the serialized user response protobuf.
  const Slice &serialized_response() const {
    DCHECK(parsed_);
    return serialized_response_;
  }

 private:
  // True once ParseFrom() is called.
  bool parsed_;

  // The parsed header.
  ResponseHeader header_;

  // The slice of data for the encoded protobuf response.
  // This slice refers to memory allocated by transfer_
  Slice serialized_response_;

  // Slices of data for rpc sidecars. They point into memory owned by transfer_.
  Slice sidecar_slices_[TransferLimits::kMaxSidecars];

  // The incoming transfer data - retained because serialized_response_
  // and sidecar_slices_ refer into its data.
  gscoped_ptr<InboundTransfer> transfer_;

  DISALLOW_COPY_AND_ASSIGN(CallResponse);
};

}}
