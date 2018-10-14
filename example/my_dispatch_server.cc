#include <stdio.h>
#include <signal.h>
#include <unistd.h>
#include <fcntl.h>

#include <atomic>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <google/protobuf/text_format.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include "slash/include/xdebug.h"
#include "pink/include/pink_thread.h"
#include "pink/include/server_thread.h"

#include "my_conn.h"
#include "zk/register.h"
#include "zk/util.h"

using namespace pink;
using namespace zk;

DEFINE_string(cluster_path, "/data/service/zk.conf", "");
DEFINE_int32(port, 34952, "");

class MyConnFactory : public ConnFactory {
 public:
  virtual PinkConn *NewPinkConn(int connfd, const std::string &ip_port,
                                ServerThread *thread, void* worker_private_data) const {
    return new MyConn(connfd, ip_port, thread);
  }
};

static std::atomic<bool> running(false);

static void IntSigHandle(const int sig) {
  printf("Catch Signal %d, cleanup...\n", sig);
  running.store(false);
  printf("server Exit");
}

static void SignalSetup() {
  signal(SIGHUP, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, &IntSigHandle);
  signal(SIGQUIT, &IntSigHandle);
  signal(SIGTERM, &IntSigHandle);
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  // get the ip info
  std::string ip = GetLocalIP();
  if (ip.empty()) {
    LOG(ERROR) << "Can't get the local ip";
    exit(1);
  }

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

  // rpc
  SignalSetup();
  ConnFactory *my_conn_factory = new MyConnFactory();
  ServerThread *st = NewDispatchThread(FLAGS_port, 10, my_conn_factory, 1000);
  if (st->StartThread() != 0) {
    printf("StartThread error happened!\n");
    exit(-1);
  }

  // service register
  ServiceRegister reg(cluster_info);
  if (!reg.Init()) {
    LOG(ERROR) << "service register init error";
    exit(1);
  }

  RegisterInfo reg_info;
  reg_info.set_ip(ip);
  reg_info.set_port(FLAGS_port);
  if (!reg.Register(reg_info)) {
    LOG(ERROR) << "service register error";
    exit(1);
  }

  running.store(true);
  while (running.load()) {
    sleep(1);
  }
  st->StopThread();

  delete st;
  delete my_conn_factory;

  return 0;
}
