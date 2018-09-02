#pragma once

#include <memory>
#include <string>

#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/macros.h"
#include "kudu/rpc/connection_id.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/util/status.h"

namespace google {
namespace protobuf {
class Message;
}
}

namespace kudu {

class Sockaddr;

namespace rpc {

class Messenger;
class RpcController;

class Proxy {
 public:
  Proxy(std::shared_ptr<Messenger> messenger,
        const Sockaddr& remote,
        std::string hostname,
        std::string service_name);

  ~Proxy();

  void AsyncRequest(const std::string& method,
                    const google::protobuf::Message& req,
                    google::protobuf::Message* res,
                    RpcController* controller,
                    const ResponseCallback& callback) const;

  Status SyncRequest(const std::string& method,
                    const google::protobuf::Message& req,
                    google::protobuf::Message* res,
                    RpcController* controller) const;

  std::string ToString() const;

 private:
  const std::string service_name_;
  std::shared_ptr<Messenger> messenger_;
  ConnectionId conn_id_;
  mutable Atomic32 is_started_;

  DISALLOW_COPY_AND_ASSIGN(Proxy);
};

}
}
