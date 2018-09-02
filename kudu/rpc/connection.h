#pragma once

#include <memory>
#include <unordered_map>

#include <boost/intrusive/list.hpp>
#include <boost/optional/optional.hpp>
#include <ev++.h>
#include <glog/logging.h>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/connection_id.h"
#include "kudu/rpc/transfer.h"
#include "kudu/util/monotime.h"
#include "kudu/util/object_pool.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/status.h"
#include "rpc_header.pb.h"

namespace kudu {
namespace rpc {

class ReactorThread;
class OutboundCall;

class Connection : public RefCountedThreadSafe<Connection> {
 public:
  enum Direction {
    // This host is sending calls via this connection.
    CLIENT,
    // This host is receiving calls via this connection.
    SERVER
  };

  // Create a new Connection.
  // reactor_thread: the reactor that owns us.
  // remote: the address of the remote end
  // socket: the socket to take ownership of.
  // direction: whether we are the client or server side
  Connection(ReactorThread *reactor_thread,
             Sockaddr remote,
             std::unique_ptr<Socket> socket,
             Direction direction);

  // Register our socket with an epoll loop.  We will only ever be registered in
  // one epoll loop at a time.
  void EpollRegister(ev::loop_ref& loop);

  // Set the user credentials for an outbound connection.
  void set_outbound_connection_id(ConnectionId conn_id) {
    DCHECK_EQ(direction_, CLIENT);
    DCHECK(!outbound_connection_id_);
    outbound_connection_id_ = std::move(conn_id);
  }

  // Get the user credentials which will be used to log in.
  const ConnectionId& outbound_connection_id() const {
    DCHECK_EQ(direction_, CLIENT);
    DCHECK(outbound_connection_id_);
    return *outbound_connection_id_;
  }

  MonoTime last_activity_time() const {
    return last_activity_time_;
  }

  // Returns true if we are not in the process of receiving or sending a
  // message, and we have no outstanding calls.
  bool Idle() const;

  // Fail any calls which are currently queued or awaiting response.
  // Prohibits any future calls (they will be failed immediately with this
  // same Status).
  void Shutdown(const Status& status,
                std::unique_ptr<ErrorStatusPB> rpc_error = {});

  // Queue a new call to be made. If the queueing fails, the call will be
  // marked failed. The caller is expected to check if 'call' has been cancelled
  // before making the call.
  // Takes ownership of the 'call' object regardless of whether it succeeds or fails.
  void QueueOutboundCall(std::shared_ptr<OutboundCall> call);

  // libev callback when data is available to read.
  void ReadHandler(ev::io &watcher, int revents);

  // libev callback when we may write to the socket.
  void WriteHandler(ev::io &watcher, int revents);

  // Safe to be called from other threads.
  std::string ToString() const;

  Direction direction() const { return direction_; }

  Socket* socket() { return socket_.get(); }

  // Go through the process of transferring control of the underlying socket back to the Reactor.
  void CompleteNegotiation(Status negotiation_status,
                           std::unique_ptr<ErrorStatusPB> rpc_error);

  // Indicate that negotiation is complete and that the Reactor is now in control of the socket.
  void MarkNegotiationComplete();

  ReactorThread* reactor_thread() const { return reactor_thread_; }

 private:
  friend struct CallAwaitingResponse;
  friend struct CallTransferCallbacks;

  // A call which has been fully sent to the server, which we're waiting for
  // the server to process. This is used on the client side only.
  struct CallAwaitingResponse {
    ~CallAwaitingResponse();

    // Notification from libev that the call has timed out.
    void HandleTimeout(ev::timer &watcher, int revents);

    Connection *conn;
    std::shared_ptr<OutboundCall> call;
    ev::timer timeout_timer;

    // We time out RPC calls in two stages. This is set to the amount of timeout
    // remaining after the next timeout fires. See Connection::QueueOutboundCall().
    double remaining_timeout;
  };

  typedef std::unordered_map<uint64_t, CallAwaitingResponse*> car_map_t;

  // Returns the next valid (positive) sequential call ID by incrementing a counter
  // and ensuring we roll over from INT32_MAX to 0.
  // Negative numbers are reserved for special purposes.
  int32_t GetNextCallId() {
    int32_t call_id = next_call_id_;
    if (PREDICT_FALSE(next_call_id_ == std::numeric_limits<int32_t>::max())) {
      next_call_id_ = 0;
    } else {
      next_call_id_++;
    }
    return call_id;
  }

  // An incoming packet has completed on the client side. This parses the
  // call response, looks up the CallAwaitingResponse, and calls the
  // client callback.
  void HandleCallResponse(gscoped_ptr<InboundTransfer> transfer);

  // The given CallAwaitingResponse has elapsed its user-defined timeout.
  // Set it to Failed.
  void HandleOutboundCallTimeout(CallAwaitingResponse *car);

  // Queue a transfer for sending on this connection.
  // We will take ownership of the transfer.
  // This must be called from the reactor thread.
  void QueueOutbound(gscoped_ptr<OutboundTransfer> transfer);

  // Internal test function for injecting cancellation request when 'call'
  // reaches state specified in 'FLAGS_rpc_inject_cancellation_state'.
  void MaybeInjectCancellation(const std::shared_ptr<OutboundCall> &call);

 private:

  // The reactor thread that created this connection.
  ReactorThread* const reactor_thread_;

  // The remote address we're talking to.
  const Sockaddr remote_;

  // The socket we're communicating on.
  std::unique_ptr<Socket> socket_;

  // The ConnectionId that serves as a key into the client connection map
  // within this reactor. Only set in the case of outbound connections.
  boost::optional<ConnectionId> outbound_connection_id_;

  // whether we are client or server
  Direction direction_;

  // The last time we read or wrote from the socket
  MonoTime last_activity_time_;

  // the inbound transfer, if any
  gscoped_ptr<InboundTransfer> inbound_;

  // notifies us when our socket is writable.
  ev::io write_io_;

  // notifies us when our socket is readable.
  ev::io read_io_;

  // Set to true when the connection is registered on a loop.
  // This is used for a sanity check in the destructor that we are properly
  // un-registered before shutting down.
  bool is_epoll_registered_;

  // 2 status a call in
  // P1: waiting to be sent
  boost::intrusive::list<OutboundTransfer> outbound_transfers_;

  // P2: Calls which have been sent and are now waiting for a response.
  car_map_t awaiting_response_;

  // the next call ID to use
  int32_t next_call_id_;

  // Whether we completed connection negotiation.
  bool negotiation_complete_;

  // Pool from which CallAwaitingResponse objects are allocated.
  // Also a funny name.
  ObjectPool<CallAwaitingResponse> car_pool_;
  typedef ObjectPool<CallAwaitingResponse>::scoped_ptr scoped_car;
};

}}
