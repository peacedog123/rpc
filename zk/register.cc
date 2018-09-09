#include "zk/register.h"

#include <glog/logging.h>

namespace zk {

using std::string;

static void watcher(zhandle_t* zh,
                    int type, int state,
                    const char* path,
                    void* context) {
  if (!context) {
    return;
  }
  ServiceRegister* ptr = reinterpret_cast<ServiceRegister*>(context);
  (void)ptr;
}

ServiceRegister::ServiceRegister(const zk::ClusterInfo& cluster_info) {
  cluster_info_.CopyFrom(cluster_info);
}

bool ServiceRegister::Init() {
  zoo_set_debug_level(ZOO_LOG_LEVEL_INFO);
  handle_ = zookeeper_init(cluster_info_.url().c_str(),
                           watcher,
                           cluster_info_.timeout(),
                           nullptr, this, 0);
  if (handle_ == nullptr) {
    LOG(ERROR) << "Error when connecting to zookeeper servers...";
    return false;
  }

  // check parent node
  Stat stat;
  int rc = zoo_exists(handle_, cluster_info_.service_prefix().c_str(), 0, &stat);
  if (rc != ZOK) {
    LOG(ERROR) << "Error happend. rc: " << rc;
    return false;
  }

  LOG(INFO) << "The parent node exists. Ok to go on.";
  return true;
}

bool ServiceRegister::Register(const zk::RegisterInfo& register_info) {

  std::string key;
  if (register_info.has_ip() && register_info.has_port()) {
    key = register_info.ip() + "_" + std::to_string(register_info.port());
  }

  if (key.empty()) {
    LOG(WARNING) << "key is empty when creating node";
    return false;
  }

  // TODO
  string path = cluster_info_.service_prefix() + "/" + key;
  string value;
  register_info.SerializeToString(&value);
  char real_path[1024];
  int rc = zoo_create(handle_, path.c_str(), value.c_str(),
                      value.length(),
                      &ZOO_OPEN_ACL_UNSAFE,
                      ZOO_EPHEMERAL,
                      real_path, 1024);
  if (rc != ZOK) {
    LOG(ERROR) << "zoo_create failed. rc: " << rc;
    return false;
  }

  return true;
}

}
