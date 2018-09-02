#include <string>
#include <memory>

#include <glog/logging.h>

#include "kudu/util/net/sockaddr.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/proxy.h"
#include "kudu/rpc/rpc_controller.h"
#include "myproto.pb.h"

using namespace std;
using namespace kudu;
using namespace kudu::rpc;

std::atomic<int> g_id;

Status CreateMessenger(const std::string& name,
                       std::shared_ptr<Messenger>* messenger,
                       int n_reactors = 1) {
  MessengerBuilder bld(name);

  bld.set_num_reactors(n_reactors);

  int32_t keepalive_time_ms = 500;

  bld.set_rpc_negotiation_timeout_ms(5000);
  bld.set_connection_keep_alive_time(keepalive_time_ms);
  if (keepalive_time_ms >= 0) {
    // In order for the keepalive timing to be accurate, we need to scan connections
    // significantly more frequently than the keepalive time. This "coarse timer"
    // granularity determines this.
    bld.set_coarse_timer_granularity(
        MonoDelta::FromMilliseconds(std::min(keepalive_time_ms / 5, 100)));
  }
  return bld.Build(messenger);
}

Status DoTestSyncCall(const Proxy &proxy, const char *method) {
  myproto::Ping req;
  req.set_address("localhost");
  req.set_port(8080);
  req.set_id(++g_id);
  myproto::PingRes res;
  RpcController controller;
  controller.set_timeout(MonoDelta::FromMilliseconds(10000));

  Status status = proxy.SyncRequest(method, req, &res, &controller);
  if (!status.ok()) {
    LOG(WARNING) << "Rpc request failed.";
    return status;
  }

  LOG(WARNING) << "Req id is: " << req.id() << ", res is [" << res.mess() << "]";
  return Status::OK();
}

int main() {
  Sockaddr server_addr;
  Status status = server_addr.ParseString("127.0.0.1", 9211);
  if (!status.ok()) {
    LOG(WARNING) << "Bad address: " << server_addr.ToString();
    return 1;
  }
  LOG(INFO) << "Connecting to " << server_addr.ToString();
  shared_ptr<Messenger> client_messenger;
  status = CreateMessenger("Client", &client_messenger, 1);
  if (!status.ok()) {
    LOG(WARNING) << "Create messenger failed.";
    return 1;
  }

  Proxy proxy(client_messenger, server_addr, server_addr.host(), "test");
  while(true) {
    DoTestSyncCall(proxy, "hello");
    sleep(1);
  }

  return 0;
}
