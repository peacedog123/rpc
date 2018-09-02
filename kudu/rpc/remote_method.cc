#include "kudu/rpc/remote_method.h"

#include <ostream>
#include <utility>

#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"
#include "rpc_header.pb.h"

namespace kudu {
namespace rpc {

using strings::Substitute;

RemoteMethod::RemoteMethod(std::string service_name,
                           const std::string method_name)
    : service_name_(std::move(service_name)), method_name_(method_name) {}

void RemoteMethod::FromPB(const RemoteMethodPB& pb) {
  DCHECK(pb.IsInitialized()) << "PB is uninitialized: " << pb.InitializationErrorString();
  service_name_ = pb.service_name();
  method_name_ = pb.method_name();
}

void RemoteMethod::ToPB(RemoteMethodPB* pb) const {
  pb->set_service_name(service_name_);
  pb->set_method_name(method_name_);
}

std::string RemoteMethod::ToString() const {
  return Substitute("$0.$1", service_name_, method_name_);
}

} // namespace rpc
} // namespace kudu

