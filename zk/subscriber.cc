#include "zk/subscriber.h"

#include <string>

#include <glog/logging.h>

using namespace std;

namespace zk {

static void watcher(zhandle_t* zh,
                    int type, int state,
                    const char* path,
                    void* context) {
  LOG(WARNING) << "Something happened. type: " << type
            << ", state: " << state;
  if (!context) {
    return;
  }

  Subscriber* ptr = reinterpret_cast<Subscriber*>(context);
  if (type == ZOO_CHILD_EVENT) {
    ptr->OnEvent();
  }
}

Subscriber::Subscriber(const zk::ClusterInfo& cluster_info) {
  cluster_info_.CopyFrom(cluster_info);
}

bool Subscriber::OnEvent() {
  String_vector str_vec;
  int32_t rc = zoo_get_children(handle_, cluster_info_.service_prefix().c_str(), 1, &str_vec);
  if (rc != ZOK) {
    LOG(ERROR) << "zoo_get_children failed. rc: " << rc;
    return false;
  }

  vector<RegisterInfo> temp_vec;
  for (int32_t i = 0; i < str_vec.count; i++) {
    string content(str_vec.data[i]);
    string child_path = cluster_info_.service_prefix() + "/" + content;
    char buf[1024];
    int32_t size = 1024;
    struct Stat stat;
    rc = zoo_get(handle_, child_path.c_str(), 0, buf, &size, &stat);
    if (rc != ZOK) {
      LOG(ERROR) << "zoo_get error. path: " << child_path;
      continue;
    }
    RegisterInfo register_info;
    if (!register_info.ParseFromString(string(buf,size))) {
      LOG(ERROR) << "registerinfo parse error";
      continue;
    }
    LOG(WARNING) << "child content: " << register_info.DebugString();
    temp_vec.emplace_back(register_info);
  }

  std::lock_guard<std::mutex> guard(mutex_);
  register_info_vec_ = temp_vec;
}

bool Subscriber::Init() {
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

  char buf[1024];
  int32_t size = 1024;
  rc = zoo_get(handle_, cluster_info_.service_prefix().c_str(), 0, buf, &size, &stat);
  if (rc != ZOK) {
    LOG(ERROR) << "zoo_get failed. rc: " << rc;
    return false;
  }
  LOG(WARNING) << "Prefix dataLength is: " << stat.dataLength;
  LOG(WARNING) << "Prefix value is: " << string(buf, size);

  // watch the children
  String_vector str_vec;
  rc = zoo_get_children(handle_, cluster_info_.service_prefix().c_str(), 1, &str_vec);
  if (rc != ZOK) {
    LOG(ERROR) << "zoo_get_children failed. rc: " << rc;
    return false;
  }

  for (int32_t i = 0; i < str_vec.count; i++) {
    string content(str_vec.data[i]);
    string child_path = cluster_info_.service_prefix() + "/" + content;
    char buf[1024];
    int32_t size = 1024;
    struct Stat stat;
    rc = zoo_get(handle_, child_path.c_str(), 0, buf, &size, &stat);
    if (rc != ZOK) {
      LOG(ERROR) << "zoo_get error. path: " << child_path;
      continue;
    }
    RegisterInfo register_info;
    if (!register_info.ParseFromString(string(buf,size))) {
      LOG(ERROR) << "registerinfo parse error";
      continue;
    }
    LOG(WARNING) << "child content: " << register_info.DebugString();
    register_info_vec_.emplace_back(register_info);
  }

  return true;
}

}
