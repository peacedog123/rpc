#pragma once

#include <boost/function.hpp>

namespace kudu {
namespace rpc {

typedef boost::function<void ()> ResponseCallback;

}
}

