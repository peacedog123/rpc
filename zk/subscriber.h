#pragma once

#include <vector>
#include <mutex>
#include <string>
#include <sstream>

#include <zookeeper/zookeeper.h>
#include <zookeeper/zookeeper_log.h>

#include "zk.pb.h"

namespace zk {

class Subscriber {
 public:
  Subscriber(const zk::ClusterInfo& cluster_info);

  bool Init();

  bool OnEvent();

  const std::vector<RegisterInfo> GetRemote() {
    std::vector<RegisterInfo> temp;
    {
      std::lock_guard<std::mutex> guard(mutex_);
      temp = register_info_vec_;
    }
    return temp;
  }

  std::string ToString() {
    std::stringstream ss;
    auto vec = GetRemote();
    for (const auto& item : vec) {
      ss << item.DebugString() << "\n";
    }
    return ss.str();
  }

 private:
  ClusterInfo cluster_info_;
  zhandle_t* handle_;

  std::mutex mutex_;
  std::vector<RegisterInfo> register_info_vec_;
};

}
