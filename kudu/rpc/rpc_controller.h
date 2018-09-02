#pragma once

#include <cstdint>
#include <memory>
#include <unordered_set>
#include <vector>

#include "kudu/gutil/macros.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

namespace google {
namespace protobuf {
class Message;
}
}

namespace kudu {

class Slice;

namespace rpc {

class ErrorStatusPB;
class Messenger;
class OutboundCall;
class RequestIdPB;
class RpcSidecar;

class RpcController {
 public:
  // Set the outbound call_'s request parameter, and transfer ownership of
  // outbound_sidecars_ to call_ in preparation for serialization.
  void SetRequestParam(const google::protobuf::Message& req);

  // Set the messenger which contains the reactor thread handling the outbound call.
  void SetMessenger(Messenger* messenger) { messenger_ = messenger; }

  // Return the current status of a call.
  //
  // A call is "OK" status until it finishes, at which point it may
  // either remain in "OK" status (if the call was successful), or
  // change to an error status. Error status indicates that there was
  // some RPC-layer issue with making the call, for example, one of:
  //
  // * failed to establish a connection to the server
  // * the server was too busy to handle the request
  // * the server was unable to interpret the request (eg due to a version
  //   mismatch)
  // * a network error occurred which caused the connection to be torn
  //   down
  // * the call timed out
  Status status() const;

  // Set the timeout for the call to be made with this RPC controller.
  //
  // The configured timeout applies to the entire time period between
  // the AsyncRequest() method call and getting a response. For example,
  // if it takes too long to establish a connection to the remote host,
  // or to DNS-resolve the remote host, those will be accounted as part
  // of the timeout period.
  //
  // Timeouts must be set prior to making the request -- the timeout may
  // not currently be adjusted for an already-sent call.
  //
  // Using an uninitialized timeout will result in a call which never
  // times out (not recommended!)
  void set_timeout(const MonoDelta& timeout);

  // Allows setting the request id for the next request sent to the server.
  // A request id allows the server to identify each request sent by the client uniquely,
  // in some cases even when sent to multiple servers, enabling exactly once semantics.
  void SetRequestIdPB(std::unique_ptr<RequestIdPB> request_id);

  // Returns whether a request id has been set on RPC header.
  bool has_request_id() const;

  // Returns the currently set request id.
  // When the request is sent to the server, it gets "moved" from RpcController
  // so an absence of a request after send doesn't mean one wasn't sent.
  // REQUIRES: the controller has a request ID set.
  const RequestIdPB& request_id() const;

  // Return the configured timeout.
  MonoDelta timeout() const;

 private:
  friend class OutboundCall;
  friend class Proxy;

  MonoDelta timeout_;

  mutable simple_spinlock lock_;

  // The id of this request.
  // Ownership is transferred to OutboundCall once the call is sent.
  std::unique_ptr<RequestIdPB> request_id_;

  // The messenger which contains the reactor thread for 'call_'.
  // Set only when 'call_' is set.
  Messenger* messenger_;

  // Once the call is sent, it is tracked here.
  std::shared_ptr<OutboundCall> call_;
};

}}
