#include "kudu/rpc/outbound_call.h"

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/function.hpp>
#include <gflags/gflags.h>
#include <google/protobuf/message.h>

#include "kudu/gutil/endian.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/rpc/transfer.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_sidecar.h"

// 100M cycles should be about 50ms on a 2Ghz box. This should be high
// enough that involuntary context switches don't trigger it, but low enough
// that any serious blocking behavior on the reactor would.
DEFINE_int64(rpc_callback_max_cycles, 100 * 1000 * 1000,
             "The maximum number of cycles for which an RPC callback "
             "should be allowed to run without emitting a warning."
             " (Advanced debugging option)");

using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace rpc {

using google::protobuf::Message;
using strings::Substitute;

static const double kMicrosPerSecond = 1000000.0;

OutboundCall::OutboundCall(const ConnectionId& conn_id,
                           const RemoteMethod& remote_method,
                           google::protobuf::Message* response_storage,
                           RpcController* controller,
                           ResponseCallback callback)
    : state_(READY),
      remote_method_(remote_method),
      conn_id_(conn_id),
      callback_(std::move(callback)),
      controller_(DCHECK_NOTNULL(controller)),
      response_(DCHECK_NOTNULL(response_storage)),
      cancellation_requested_(false) {
  DVLOG(4) << "OutboundCall " << this << " constructed with state_: " << StateName(state_)
           << " and RPC timeout: "
           << (controller->timeout().Initialized() ? controller->timeout().ToString() : "none");

  header_.set_call_id(kInvalidCallId);
  remote_method.ToPB(header_.mutable_remote_method());
  start_time_ = MonoTime::Now();
}

OutboundCall::~OutboundCall() {
  DCHECK(IsFinished());
  DVLOG(4) << "OutboundCall " << this << " destroyed with state_: " << StateName(state_);
}

size_t OutboundCall::SerializeTo(TransferPayload* slices) {
  DCHECK_LT(0, request_buf_.size())
      << "Must call SetRequestPayload() before SerializeTo()";

  const MonoDelta &timeout = controller_->timeout();
  if (timeout.Initialized()) {
    header_.set_timeout_millis(timeout.ToMilliseconds());
  }

  uint32_t header_pb_len = header_.ByteSize();
  uint32_t header_total_len = kMsgLengthPrefixLength + sizeof(header_pb_len) + header_pb_len;
  size_t total_size = header_total_len + request_buf_.size();

  header_buf_.resize(header_total_len);
  uint8_t* dst = header_buf_.data();

  // 1. The length for the whole request, not including the 4-byte
  // length prefix.
  NetworkByteOrder::Store32(dst, total_size - kMsgLengthPrefixLength);
  dst += kMsgLengthPrefixLength;

  // 2. The length and body of the header
  NetworkByteOrder::Store32(dst, header_pb_len);
  dst += kMsgLengthPrefixLength;
  header_.SerializeToArray(dst, header_.ByteSize());

  size_t n_slices = 2;
  DCHECK_LE(n_slices, slices->size());
  auto slice_iter = slices->begin();
  *slice_iter++ = Slice(header_buf_);
  *slice_iter++ = Slice(request_buf_);
  DCHECK_EQ(slice_iter - slices->begin(), n_slices);
  return n_slices;
}

void OutboundCall::SetRequestPayload(const Message& req) {
  // 4 byte header + message body
  request_buf_.resize(req.ByteSize() + kMsgLengthPrefixLength);
  uint8_t* dst = request_buf_.data();
  NetworkByteOrder::Store32(dst, req.ByteSize());
  req.SerializeToArray(dst + kMsgLengthPrefixLength, req.ByteSize());
}

void OutboundCall::SetQueued() {
  set_state(ON_OUTBOUND_QUEUE);
}

void OutboundCall::SetSending() {
  set_state(SENDING);
}

void OutboundCall::SetSent() {
  set_state(SENT);

  // This method is called in the reactor thread, so free the header buf,
  // which was also allocated from this thread. tcmalloc's thread caching
  // behavior is a lot more efficient if memory is freed from the same thread
  // which allocated it -- this lets it keep to thread-local operations instead
  // of taking a mutex to put memory back on the global freelist.
  delete [] header_buf_.release();

  // request_buf_ is also done being used here, but since it was allocated by
  // the caller thread, we would rather let that thread free it whenever it
  // deletes the RpcController.

  // If cancellation was requested, it's now a good time to do the actual cancellation.
  if (cancellation_requested()) {
    SetCancelled();
  }
}

void OutboundCall::SetFailed(Status status,
                             Phase phase,
                             unique_ptr<ErrorStatusPB> err_pb) {
  DCHECK(!status.ok());
  DCHECK(phase == Phase::CONNECTION_NEGOTIATION || phase == Phase::REMOTE_CALL);
  {
    std::lock_guard<simple_spinlock> l(lock_);
    status_ = std::move(status);
    error_pb_ = std::move(err_pb);
    set_state_unlocked(phase == Phase::CONNECTION_NEGOTIATION
        ? FINISHED_NEGOTIATION_ERROR
        : FINISHED_ERROR);
  }
  CallCallback();
}


Status OutboundCall::status() const {
  std::lock_guard<simple_spinlock> l(lock_);
  return status_;
}

const ErrorStatusPB* OutboundCall::error_pb() const {
  std::lock_guard<simple_spinlock> l(lock_);
  return error_pb_.get();
}

string OutboundCall::StateName(State state) {
  switch (state) {
    case READY:
      return "READY";
    case ON_OUTBOUND_QUEUE:
      return "ON_OUTBOUND_QUEUE";
    case SENDING:
      return "SENDING";
    case SENT:
      return "SENT";
    case NEGOTIATION_TIMED_OUT:
      return "NEGOTIATION_TIMED_OUT";
    case TIMED_OUT:
      return "TIMED_OUT";
    case CANCELLED:
      return "CANCELLED";
    case FINISHED_NEGOTIATION_ERROR:
      return "FINISHED_NEGOTIATION_ERROR";
    case FINISHED_ERROR:
      return "FINISHED_ERROR";
    case FINISHED_SUCCESS:
      return "FINISHED_SUCCESS";
    default:
      LOG(DFATAL) << "Unknown state in OutboundCall: " << state;
      return StringPrintf("UNKNOWN(%d)", state);
  }
}

void OutboundCall::set_state(State new_state) {
  std::lock_guard<simple_spinlock> l(lock_);
  set_state_unlocked(new_state);
}

OutboundCall::State OutboundCall::state() const {
  std::lock_guard<simple_spinlock> l(lock_);
  return state_;
}

void OutboundCall::set_state_unlocked(State new_state) {
  // Sanity check state transitions.
  DVLOG(3) << "OutboundCall " << this << " (" << ToString() << ") switching from " <<
    StateName(state_) << " to " << StateName(new_state);
  switch(new_state) {
    case ON_OUTBOUND_QUEUE:
      DCHECK_EQ(state_, READY);
      break;
    case SENDING:
      // Allow SENDING to be set idempotently so we don't have to specifically check
      // whether the state is transitioning in the RPC code.
      DCHECK(state_ == ON_OUTBOUND_QUEUE || state_ == SENDING);
      break;
    case SENT:
      DCHECK_EQ(state_, SENDING);
      break;
    case NEGOTIATION_TIMED_OUT:
      DCHECK(state_ == ON_OUTBOUND_QUEUE);
      break;
    case TIMED_OUT:
      DCHECK(state_ == SENT || state_ == ON_OUTBOUND_QUEUE || state_ == SENDING);
      break;
    case CANCELLED:
      DCHECK(state_ == READY || state_ == ON_OUTBOUND_QUEUE || state_ == SENT);
      break;
    case FINISHED_SUCCESS:
      DCHECK_EQ(state_, SENT);
      break;
    default:
      // No sanity checks for others.
      break;
  }

  state_ = new_state;
}

void OutboundCall::CallCallback() {

  int64_t start_cycles = CycleClock::Now();
  {
    // TODO SCOPED_WATCH_STACK(100);
    callback_();
    // Clear the callback, since it may be holding onto reference counts
    // via bound parameters. We do this inside the timer because it's possible
    // the user has naughty destructors that block, and we want to account for that
    // time here if they happen to run on this thread.
    callback_ = NULL;
  }
  int64_t end_cycles = CycleClock::Now();
  int64_t wait_cycles = end_cycles - start_cycles;
  if (PREDICT_FALSE(wait_cycles > FLAGS_rpc_callback_max_cycles)) {
    double micros = static_cast<double>(wait_cycles) / base::CyclesPerSecond()
      * kMicrosPerSecond;

    LOG(WARNING) << "RPC callback for " << ToString() << " blocked reactor thread for "
                 << micros << "us";
  }
}

void OutboundCall::SetResponse(gscoped_ptr<CallResponse> resp) {
  call_response_ = std::move(resp);
  Slice r(call_response_->serialized_response());

  if (call_response_->is_success()) {
    // TODO: here we're deserializing the call response within the reactor thread,
    // which isn't great, since it would block processing of other RPCs in parallel.
    // Should look into a way to avoid this.
    if (!response_->ParseFromArray(r.data(), r.size())) {
      SetFailed(Status::IOError("invalid RPC response, missing fields",
                                response_->InitializationErrorString()));
      return;
    }
    set_state(FINISHED_SUCCESS);
    CallCallback();
  } else {
    // Error
    unique_ptr<ErrorStatusPB> err(new ErrorStatusPB());
    if (!err->ParseFromArray(r.data(), r.size())) {
      SetFailed(Status::IOError("Was an RPC error but could not parse error response",
                                err->InitializationErrorString()));
      return;
    }
    Status s = Status::RemoteError(err->message());
    SetFailed(std::move(s), Phase::REMOTE_CALL, std::move(err));
  }
}

void OutboundCall::SetCancelled() {
  DCHECK(!IsFinished());
  {
    std::lock_guard<simple_spinlock> l(lock_);
    status_ = Status::Aborted(
        Substitute("$0 RPC to $1 is cancelled in state $2",
                   remote_method_.method_name(),
                   conn_id_.remote().ToString(),
                   StateName(state_)));
    set_state_unlocked(CANCELLED);
  }
  CallCallback();
}

void OutboundCall::SetTimedOut(Phase phase) {
  static const char* kErrMsgNegotiation =
      "connection negotiation to $1 for RPC $0 timed out after $2 ($3)";
  static const char* kErrMsgCall = "$0 RPC to $1 timed out after $2 ($3)";

  DCHECK(phase == Phase::CONNECTION_NEGOTIATION || phase == Phase::REMOTE_CALL);

  // We have to fetch timeout outside the lock to avoid a lock
  // order inversion between this class and RpcController.
  const MonoDelta timeout = controller_->timeout();
  {
    std::lock_guard<simple_spinlock> l(lock_);
    status_ = Status::TimedOut(
        Substitute((phase == Phase::REMOTE_CALL) ? kErrMsgCall : kErrMsgNegotiation,
                   remote_method_.method_name(),
                   conn_id_.remote().ToString(),
                   timeout.ToString(),
                   StateName(state_)));
    set_state_unlocked((phase == Phase::REMOTE_CALL) ? TIMED_OUT : NEGOTIATION_TIMED_OUT);
  }

  CallCallback();
}

bool OutboundCall::IsTimedOut() const {
  std::lock_guard<simple_spinlock> l(lock_);
  switch (state_) {
    case NEGOTIATION_TIMED_OUT:       // fall-through
    case TIMED_OUT:
      return true;
    default:
      return false;
  }
}

bool OutboundCall::IsCancelled() const {
  std::lock_guard<simple_spinlock> l(lock_);
  return state_ == CANCELLED;
}

bool OutboundCall::IsFinished() const {
  std::lock_guard<simple_spinlock> l(lock_);
  switch (state_) {
    case READY:
    case SENDING:
    case ON_OUTBOUND_QUEUE:
    case SENT:
      return false;
    case NEGOTIATION_TIMED_OUT:
    case TIMED_OUT:
    case CANCELLED:
    case FINISHED_NEGOTIATION_ERROR:
    case FINISHED_ERROR:
    case FINISHED_SUCCESS:
      return true;
    default:
      LOG(FATAL) << "Unknown call state: " << state_;
      return false;
  }
}

std::string OutboundCall::ToString() const {
  return Substitute("RPC call $0 -> $1", remote_method_.ToString(), conn_id_.ToString());
}

///
/// CallResponse
///

CallResponse::CallResponse()
 : parsed_(false) {
}

Status CallResponse::ParseFrom(gscoped_ptr<InboundTransfer> transfer) {
  CHECK(!parsed_);

  Slice slice = transfer->data();
  uint32_t total_length = NetworkByteOrder::Load32(slice.data());
  uint32_t header_length = NetworkByteOrder::Load32(slice.data() + kMsgLengthPrefixLength);

  bool flag = header_.ParseFromArray(slice.data() + kMsgLengthPrefixLength * 2, header_length);

  uint32_t body_length = NetworkByteOrder::Load32(slice.data() + kMsgLengthPrefixLength * 2 + header_length);
  serialized_response_ = Slice(slice.data() + kMsgLengthPrefixLength * 2 + header_length + kMsgLengthPrefixLength,
                               body_length);

  transfer_.swap(transfer);
  parsed_ = true;
  return Status::OK();
}

}}
