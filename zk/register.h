#pragma once

#include <zookeeper/zookeeper.h>
#include <zookeeper/zookeeper_log.h>

#include "zk.pb.h"

namespace zk {

class ServiceRegister {
 public:
  ServiceRegister(const zk::ClusterInfo& cluster_info);

  bool Init();

  bool Register(const zk::RegisterInfo& register_info);

 private:
  ClusterInfo cluster_info_;
  zhandle_t* handle_;

};

}
