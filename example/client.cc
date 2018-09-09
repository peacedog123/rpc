#include <fcntl.h>
#include <string>
#include <memory>

#include <glog/logging.h>

#include <google/protobuf/text_format.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include "kudu/util/net/sockaddr.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/proxy.h"
#include "kudu/rpc/rpc_controller.h"
#include "myproto.pb.h"

#include "zk/subscriber.h"
#include "zk/util.h"

using namespace std;
using namespace kudu;
using namespace kudu::rpc;
using namespace zk;

DEFINE_string(cluster_path, "/data/service/zk.conf", "");

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

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  LOG(WARNING) << "Client starting...";

  /*Sockaddr server_addr;
  Status status = server_addr.ParseString("127.0.0.1", 9211);
  if (!status.ok()) {
    LOG(WARNING) << "Bad address: " << server_addr.ToString();
    return 1;
  }
  LOG(INFO) << "Connecting to " << server_addr.ToString();*/

  // open the cluster conf
  int32_t fd = open(FLAGS_cluster_path.c_str(), O_RDONLY);
  if (fd < 0) {
    LOG(ERROR) << "failed to open cluster config. path: " << FLAGS_cluster_path;
    exit(1);
  }

  google::protobuf::io::FileInputStream conf(fd);
  conf.SetCloseOnDelete(true);
  ClusterInfo cluster_info;
  if (!google::protobuf::TextFormat::Parse(&conf, &cluster_info)) {
    LOG(ERROR) << "fail to parse cluster config file;";
    exit(1);
  }

  // Get the service list
  Subscriber sub(cluster_info);
  if (!sub.Init()) {
    LOG(ERROR) << "Subscriber init error";
    exit(1);
  }


  // create the messenger
  shared_ptr<Messenger> client_messenger;
  Status status = CreateMessenger("Client", &client_messenger, 1);
  if (!status.ok()) {
    LOG(WARNING) << "Create messenger failed.";
    return 1;
  }

  /*Proxy proxy(client_messenger, server_addr, server_addr.host(), "test");*/
  while(true) {
    //DoTestSyncCall(proxy, "hello");
    //LOG(WARNING) << "In main thread...";
    LOG(WARNING) << "Remote is: " << sub.ToString();
    sleep(2);
  }

  return 0;
}
