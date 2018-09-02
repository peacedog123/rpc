#include "kudu/rpc/proxy.h"

#include <iostream>
#include <memory>
#include <utility>

#include <boost/bind.hpp> // IWYU pragma: keep
#include <boost/core/ref.hpp>
#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/outbound_call.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/remote_method.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/status.h"

namespace kudu {
namespace rpc {

using google::protobuf::Message;
using std::string;
using std::shared_ptr;

Proxy::Proxy(std::shared_ptr<Messenger> messenger,
             const Sockaddr& remote,
             string hostname,
             string service_name)
    : service_name_(std::move(service_name)),
      messenger_(std::move(messenger)),
      is_started_(false) {
  CHECK(messenger_ != nullptr);
  DCHECK(!service_name_.empty()) << "Proxy service name must not be empty.";

  conn_id_ = ConnectionId(remote, std::move(hostname));
}

Proxy::~Proxy() {
}

void Proxy::AsyncRequest(const string& method,
                         const google::protobuf::Message& req,
                         google::protobuf::Message* response,
                         RpcController* controller,
                         const ResponseCallback& callback) const {
  CHECK(!controller->call_) << "Controller should be reset";
  base::subtle::NoBarrier_Store(&is_started_, true);
  RemoteMethod remote_method(service_name_, method);
  controller->call_.reset(
      new OutboundCall(conn_id_, remote_method, response, controller, callback));
  controller->SetRequestParam(req);
  controller->SetMessenger(messenger_.get());

  // If this fails to queue, the callback will get called immediately
  // and the controller will be in an ERROR state.
  messenger_->QueueOutboundCall(controller->call_);
}

Status Proxy::SyncRequest(const string& method,
                          const google::protobuf::Message& req,
                          google::protobuf::Message* response,
                          RpcController* controller) const {
  CountDownLatch latch(1);
  AsyncRequest(method, req, response, controller,
    boost::bind(&CountDownLatch::CountDown, boost::ref(latch)));

  latch.Wait();
  return controller->status();
}

std::string Proxy::ToString() const {
  return strings::Substitute("$0@$1", service_name_, conn_id_.ToString());
}

}
}
