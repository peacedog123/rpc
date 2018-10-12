#include "kudu/rpc/rpc_controller.h"

#include <memory>
#include <mutex>
#include <ostream>
#include <utility>

#include <glog/logging.h>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/outbound_call.h"
#include "kudu/rpc/rpc_sidecar.h"

namespace kudu {
namespace rpc {

void RpcController::SetRequestParam(const google::protobuf::Message& req) {
  DCHECK(call_ != nullptr);
  call_->SetRequestPayload(req);
}

Status RpcController::status() const {
  if (call_) {
    return call_->status();
  }
  return Status::OK();
}

void RpcController::set_timeout(const MonoDelta& timeout) {
  std::lock_guard<simple_spinlock> l(lock_);
  DCHECK(!call_ || call_->state() == OutboundCall::READY);
  timeout_ = timeout;
}

void RpcController::SetRequestIdPB(std::unique_ptr<RequestIdPB> request_id) {
  request_id_ = std::move(request_id);
}

bool RpcController::has_request_id() const {
  return request_id_ != nullptr;
}

const RequestIdPB& RpcController::request_id() const {
  DCHECK(has_request_id());
  return *request_id_;
}

MonoDelta RpcController::timeout() const {
  std::lock_guard<simple_spinlock> l(lock_);
  return timeout_;
}

}}
