#include "kudu/rpc/connection.h"

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/outbound_call.h"
#include "kudu/rpc/reactor.h"
#include "kudu/rpc/rpc_controller.h"

namespace kudu {
namespace rpc {

typedef OutboundCall::Phase Phase;

using std::shared_ptr;
using std::unique_ptr;

Connection::Connection(ReactorThread *reactor_thread,
                       Sockaddr remote,
                       std::unique_ptr<Socket> socket,
                       Direction direction)
    : reactor_thread_(reactor_thread),
      remote_(remote),
      socket_(std::move(socket)),
      direction_(direction),
      last_activity_time_(MonoTime::Now()),
      is_epoll_registered_(false),
      next_call_id_(1),
      negotiation_complete_(false) {

}

void Connection::EpollRegister(ev::loop_ref& loop) {
  DCHECK(reactor_thread_->IsCurrentThread());

  DVLOG(4) << "Registering connection for epoll: " << ToString();
  write_io_.set(loop);
  write_io_.set(socket_->GetFd(), ev::WRITE);
  write_io_.set<Connection, &Connection::WriteHandler>(this);
  if (direction_ == CLIENT && negotiation_complete_) {
    write_io_.start();
  }

  read_io_.set(loop);
  read_io_.set(socket_->GetFd(), ev::READ);
  read_io_.set<Connection, &Connection::ReadHandler>(this);
  read_io_.start();
  is_epoll_registered_ = true;
}

void Connection::QueueOutbound(gscoped_ptr<OutboundTransfer> transfer) {
  DCHECK(reactor_thread_->IsCurrentThread());

  /* TODO
  if (!shutdown_status_.ok()) {
    // If we've already shut down, then we just need to abort the
    // transfer rather than bothering to queue it.
    transfer->Abort(shutdown_status_);
    return;
  }*/

  DVLOG(3) << "Queueing transfer: " << transfer->HexDump();

  outbound_transfers_.push_back(*transfer.release());

  if (negotiation_complete_ && !write_io_.is_active()) {
    // If we weren't currently in the middle of sending anything,
    // then our write_io_ interest is stopped. Need to re-start it.
    // Only do this after connection negotiation is done doing its work.
    write_io_.start();
  }
}

bool Connection::Idle() const {
  DCHECK(reactor_thread_->IsCurrentThread());

  // check if we're in the middle of receiving something
  InboundTransfer *transfer = inbound_.get();
  if (transfer && (transfer->TransferStarted())) {
    return false;
  }
  // check if we still need to send something
  if (!outbound_transfers_.empty()) {
    return false;
  }
  // can't kill a connection if calls are waiting response
  if (!awaiting_response_.empty()) {
    return false;
  }

  // We are not idle if we are in the middle of connection negotiation.
  if (!negotiation_complete_) {
    return false;
  }

  return true;
}

void Connection::Shutdown(const Status &status,
                          unique_ptr<ErrorStatusPB> rpc_error) {
  DCHECK(reactor_thread_->IsCurrentThread());
  shutdown_status_ = status.CloneAndPrepend("RPC connection failed");

  // Clear any calls which have been sent and were awaiting a response
  for (const car_map_t::value_type& v: awaiting_response_) {
    CallAwaitingResponse* c = v.second;
    if (c->call) {
      unique_ptr<ErrorStatusPB> error;
      if (rpc_error) {
        error.reset(new ErrorStatusPB(*rpc_error));
      }
      c->call->SetFailed(status,
                         negotiation_complete_ ? Phase::REMOTE_CALL
                                               : Phase::CONNECTION_NEGOTIATION,
                         std::move(error));
    }
    //And we must return the CallAwaitingResponse to the pool
    car_pool_.Destroy(c);
  }

  awaiting_response_.clear();

  // Clear any outbound transfers.
  while (!outbound_transfers_.empty()) {
    OutboundTransfer *t = &outbound_transfers_.front();
    outbound_transfers_.pop_front();
    delete t;
  }

  read_io_.stop();
  write_io_.stop();
  is_epoll_registered_ = false;
  if (socket_) {
    WARN_NOT_OK(socket_->Close(), "Error closing socket");
  }
}

Connection::CallAwaitingResponse::~CallAwaitingResponse() {
  DCHECK(conn->reactor_thread_->IsCurrentThread());
}

void Connection::CallAwaitingResponse::HandleTimeout(ev::timer &watcher, int revents) {
  // 两阶段timeout
  if (remaining_timeout > 0) {
    if (watcher.remaining() < -1.0) {
      LOG(WARNING) << "RPC call timeout handler was delayed by "
                   << -watcher.remaining() << "s! This may be due to a process-wide "
                   << "pause such as swapping, logging-related delays, or allocator lock "
                   << "contention. Will allow an additional "
                   << remaining_timeout << "s for a response.";
    }

    watcher.set(remaining_timeout, 0);
    watcher.start();
    remaining_timeout = 0;
    return;
  }

  conn->HandleOutboundCallTimeout(this);
}

// Callbacks after sending a call on the wire.
// This notifies the OutboundCall object to change its state to SENT once it
// has been fully transmitted.
struct CallTransferCallbacks : public TransferCallbacks {
 public:
  explicit CallTransferCallbacks(shared_ptr<OutboundCall> call,
                                 Connection *conn)
      : call_(std::move(call)), conn_(conn) {}

  virtual void NotifyTransferFinished() OVERRIDE {
    // TODO: would be better to cancel the transfer while it is still on the queue if we
    // timed out before the transfer started, but there is still a race in the case of
    // a partial send that we have to handle here
    if (call_->IsFinished()) {
      DCHECK(call_->IsTimedOut() || call_->IsCancelled());
    } else {
      call_->SetSent();
      // Test cancellation when 'call_' is in 'SENT' state.
      conn_->MaybeInjectCancellation(call_);
    }
    delete this;
  }

  virtual void NotifyTransferAborted(const Status &status) OVERRIDE {
    VLOG(1) << "Transfer of RPC call " << call_->ToString() << " aborted: "
            << status.ToString();
    delete this;
  }

 private:
  shared_ptr<OutboundCall> call_;
  Connection* conn_;
};

void Connection::QueueOutboundCall(shared_ptr<OutboundCall> call) {
  DCHECK(call);
  DCHECK_EQ(direction_, CLIENT);
  DCHECK(reactor_thread_->IsCurrentThread());

  // TODO shutdown
  /*if (PREDICT_FALSE(!shutdown_status_.ok())) {
    // Already shutdown
    call->SetFailed(shutdown_status_,
                    negotiation_complete_ ? Phase::REMOTE_CALL
                                          : Phase::CONNECTION_NEGOTIATION);
    return;
  } */

  // At this point the call has a serialized request, but no call header, since we haven't
  // yet assigned a call ID.
  DCHECK(!call->call_id_assigned());

  // We shouldn't reach this point if 'call' was requested to be cancelled.
  DCHECK(!call->cancellation_requested());

  // Assign the call ID.
  int32_t call_id = GetNextCallId();
  call->set_call_id(call_id);

  // Serialize the actual bytes to be put on the wire.
  TransferPayload tmp_slices;
  size_t n_slices = call->SerializeTo(&tmp_slices);

  call->SetQueued();

  // Test cancellation when 'call_' is in 'ON_OUTBOUND_QUEUE' state.
  MaybeInjectCancellation(call);

  // allocate the car
  scoped_car car(car_pool_.make_scoped_ptr(car_pool_.Construct()));
  car->conn = this;
  car->call = call;

  // TODO deal with the timeout timer -
  // 这里用来处理定时器(有可能超时的时候还没发出去, 或者已经发出去，但是server还没返回
  const MonoDelta& timeout = call->controller()->timeout();
  if (timeout.Initialized()) {
    reactor_thread_->RegisterTimeout(&car->timeout_timer);
    car->timeout_timer.set<CallAwaitingResponse,
                            &CallAwaitingResponse::HandleTimeout>(car.get());

    // 2 phase timeout
    double time = timeout.ToSeconds();
    if (time >= 0.5) {
      car->remaining_timeout = time * 0.1;
      time -= car->remaining_timeout;
    } else {
      car->remaining_timeout = 0;
    }

    car->timeout_timer.set(time, 0);
    car->timeout_timer.start();
  }

  TransferCallbacks* cb = new CallTransferCallbacks(std::move(call), this);
  awaiting_response_[call_id] = car.release();
  QueueOutbound(gscoped_ptr<OutboundTransfer>(
    OutboundTransfer::CreateForCallRequest(call_id, tmp_slices, n_slices, cb)));
}

std::string Connection::ToString() const {
  // This may be called from other threads, so we cannot
  // include anything in the output about the current state,
  // which might concurrently change from another thread.
  return strings::Substitute(
    "$0 $1",
    direction_ == SERVER ? "server connection from" : "client connection to",
    remote_.ToString());
}

// Reactor task that transitions this Connection from connection negotiation to
// regular RPC handling. Destroys Connection on negotiation error.
class NegotiationCompletedTask : public ReactorTask {
 public:
  NegotiationCompletedTask(Connection* conn,
                           Status negotiation_status,
                           std::unique_ptr<ErrorStatusPB> rpc_error)
    : conn_(conn),
      negotiation_status_(std::move(negotiation_status)),
      rpc_error_(std::move(rpc_error)) {
  }

  virtual void Run(ReactorThread *rthread) OVERRIDE {
    rthread->CompleteConnectionNegotiation(conn_,
                                           negotiation_status_,
                                           std::move(rpc_error_));
    delete this;
  }

  virtual void Abort(const Status &status) OVERRIDE {
    DCHECK(conn_->reactor_thread()->reactor()->closing());
    VLOG(1) << "Failed connection negotiation due to shut down reactor thread: "
            << status.ToString();
    delete this;
  }

 private:
  scoped_refptr<Connection> conn_;
  const Status negotiation_status_;
  std::unique_ptr<ErrorStatusPB> rpc_error_;
};

void Connection::CompleteNegotiation(Status negotiation_status,
                                     unique_ptr<ErrorStatusPB> rpc_error) {
  auto task = new NegotiationCompletedTask(
      this, std::move(negotiation_status), std::move(rpc_error));
  reactor_thread_->reactor()->ScheduleReactorTask(task);
}

void Connection::MarkNegotiationComplete() {
  DCHECK(reactor_thread_->IsCurrentThread());
  negotiation_complete_ = true;
}

// Inject a cancellation when 'call' is in state 'FLAGS_rpc_inject_cancellation_state'.
void inline Connection::MaybeInjectCancellation(const shared_ptr<OutboundCall> &call) {
  /* TODO
  if (PREDICT_FALSE(call->ShouldInjectCancellation())) {
    reactor_thread_->reactor()->messenger()->QueueCancellation(call);
  }*/
}

// libev的回调，表明socket可以写入。
// 此时如果有pending transfer:则写入；否则可以把write_io_停止，等有新的请求时再打开
void Connection::WriteHandler(ev::io& watcher, int revents) {
  DCHECK(reactor_thread_->IsCurrentThread());
  if (revents & EV_ERROR) {
    reactor_thread_->DestroyConnection(this, Status::NetworkError(ToString() +
          ": writeHandler encountered an error"));
    return;
  }
  DVLOG(3) << ToString() << ": writeHandler: revents = " << revents;

  OutboundTransfer* transfer;
  if (outbound_transfers_.empty()) {
    LOG(WARNING) << ToString() << " got a ready-to-write callback, but there is "
      "nothing to write.";
    write_io_.stop();
    return;
  }

  // begin transfer
  while (!outbound_transfers_.empty()) {
    transfer = &(outbound_transfers_.front());
    // 当前请求没发送过，则可以检查是否要发送(比如Timeout或者Cancel)
    if (!transfer->TransferStarted()) {
      if (transfer->is_for_outbound_call()) {
        CallAwaitingResponse* car = FindOrDie(awaiting_response_, transfer->call_id());
        if (!car->call) {
          // If the call has already timed out or has already been cancelled, the 'call'
          // field would be set to NULL. In that case, don't bother sending it.
          outbound_transfers_.pop_front();
          transfer->Abort(Status::Aborted("already timed out or cancelled"));
          delete transfer;
          continue;
        }

        car->call->SetSending();

        // Test cancellation when 'call_' is in 'SENDING' state.
        MaybeInjectCancellation(car->call);
      }
    }

    last_activity_time_ = reactor_thread_->cur_time();
    Status status = transfer->SendBuffer(*socket_);
    if (PREDICT_FALSE(!status.ok())) {
      LOG(WARNING) << ToString() << " send error: " << status.ToString();
      reactor_thread_->DestroyConnection(this, status);
      return;
    }

    if (!transfer->TransferFinished()) {
      DVLOG(3) << ToString() << ": writeHandler: xfer not finished.";
      return;
    }

    outbound_transfers_.pop_front();
    delete transfer;
  }

  // If we were able to write all of our outbound transfers,
  // we don't have any more to write.
  write_io_.stop();
}

void Connection::ReadHandler(ev::io& watcher, int revents) {
  DCHECK(reactor_thread_->IsCurrentThread());

  DVLOG(3) << ToString() << " ReadHandler(revents=" << revents << ")";
  if (revents & EV_ERROR) {
    reactor_thread_->DestroyConnection(this, Status::NetworkError(ToString() +
                                     ": ReadHandler encountered an error"));
    return;
  }
  last_activity_time_ = reactor_thread_->cur_time();

  while (true) {
    if (!inbound_) {
      inbound_.reset(new InboundTransfer());
    }
    Status status = inbound_->ReceiveBuffer(*socket_);
    if (PREDICT_FALSE(!status.ok())) {
      if (status.posix_code() == ESHUTDOWN) {
        VLOG(1) << ToString() << " shut down by remote end.";
      } else {
        LOG(WARNING) << ToString() << " recv error: " << status.ToString();
      }
      reactor_thread_->DestroyConnection(this, status);
      return;
    }
    if (!inbound_->TransferFinished()) {
      DVLOG(3) << ToString() << ": read is not yet finished yet.";
      return;
    }

    DVLOG(3) << ToString() << ": finished reading " << inbound_->data().size() << " bytes";

    if (direction_ == CLIENT) {
      HandleCallResponse(std::move(inbound_));
    } else {
      LOG(FATAL) << "Invalid direction: " << direction_;
    }

    // TODO: it would seem that it would be good to loop around and see if
    // there is more data on the socket by trying another recv(), but it turns
    // out that it really hurts throughput to do so. A better approach
    // might be for each InboundTransfer to actually try to read an extra byte,
    // and if it succeeds, then we'd copy that byte into a new InboundTransfer
    // and loop around, since it's likely the next call also arrived at the
    // same time.
    break;
  }
}

void Connection::HandleCallResponse(gscoped_ptr<InboundTransfer> transfer) {
  DCHECK(reactor_thread_->IsCurrentThread());
  gscoped_ptr<CallResponse> resp(new CallResponse);
  CHECK_OK(resp->ParseFrom(std::move(transfer)));

  CallAwaitingResponse* car_ptr =
    EraseKeyReturnValuePtr(&awaiting_response_, resp->call_id());
  if (PREDICT_FALSE(car_ptr == nullptr)) {
    LOG(WARNING) << ToString() << ": Got a response for call id " << resp->call_id() << " which "
                 << "was not pending! Ignoring.";
    return;
  }

  // The car->timeout_timer ev::timer will be stopped automatically by its destructor.
  scoped_car car(car_pool_.make_scoped_ptr(car_ptr));

  if (PREDICT_FALSE(!car->call)) {
    // The call already failed due to a timeout.
    VLOG(1) << "Got response to call id " << resp->call_id() << " after client "
            << "already timed out or cancelled";
    return;
  }

  car->call->SetResponse(std::move(resp));

  // Test cancellation when 'car->call' is in 'FINISHED_SUCCESS' or 'FINISHED_ERROR' state.
  MaybeInjectCancellation(car->call);
}

void Connection::HandleOutboundCallTimeout(CallAwaitingResponse *car) {
  DCHECK(reactor_thread_->IsCurrentThread());
  DCHECK(car->call);
  // The timeout timer is stopped by the car destructor exiting Connection::HandleCallResponse()
  DCHECK(!car->call->IsFinished());

  // Mark the call object as failed.
  car->call->SetTimedOut(negotiation_complete_ ? Phase::REMOTE_CALL
                                               : Phase::CONNECTION_NEGOTIATION);

  // Test cancellation when 'car->call' is in 'TIMED_OUT' state
  MaybeInjectCancellation(car->call);

  // Drop the reference to the call. If the original caller has moved on after
  // seeing the timeout, we no longer need to hold onto the allocated memory
  // from the request.
  car->call.reset();

  // We still leave the CallAwaitingResponse in the map -- this is because we may still
  // receive a response from the server, and we don't want a spurious log message
  // when we do finally receive the response. The fact that CallAwaitingResponse::call
  // is a NULL pointer indicates to the response processing code that the call
  // already timed out.*/
}

}}
