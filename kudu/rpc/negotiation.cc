#include "kudu/rpc/negotiation.h"

#include <poll.h>
#include <sys/socket.h>

#include <cerrno>
#include <ctime>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/connection.h"
#include "kudu/rpc/connection_id.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/reactor.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/errno.h"
#include "rpc_header.pb.h"

namespace kudu {
namespace rpc {

using std::string;
using std::unique_ptr;
using strings::Substitute;

static Status WaitForClientConnect(Socket* socket, const MonoTime& deadline) {
  int fd = socket->GetFd();
  struct pollfd poll_fd;
  poll_fd.fd = fd;
  poll_fd.events = POLLOUT;
  poll_fd.revents = 0;

  MonoTime now;
  MonoDelta remaining;
  while (true) {
    now = MonoTime::Now();
    remaining = deadline - now;
    DVLOG(4) << "Client waiting to connect for negotiation, time remaining until timeout deadline: "
             << remaining.ToString();
    if (PREDICT_FALSE(remaining.ToNanoseconds() <= 0)) {
      return Status::TimedOut("Timeout exceeded waiting to connect");
    }
#if defined(__linux__)
    struct timespec ts;
    remaining.ToTimeSpec(&ts);
    int ready = ppoll(&poll_fd, 1, &ts, NULL);
#else
    int ready = poll(&poll_fd, 1, remaining.ToMilliseconds());
#endif
    if (ready == -1) {
      int err = errno;
      if (err == EINTR) {
        // We were interrupted by a signal, let's go again.
        continue;
      } else {
        return Status::NetworkError("Error from ppoll() while waiting to connect",
            ErrnoToString(err), err);
      }
    } else if (ready == 0) {
      // Timeout exceeded. Loop back to the top to our impending doom.
      continue;
    } else {
      // Success.
      break;
    }
  }

  // Connect finished, but this doesn't mean that we connected successfully.
  // Check the socket for an error.
  int so_error = 0;
  socklen_t socklen = sizeof(so_error);
  int rc = getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_error, &socklen);
  if (rc != 0) {
    return Status::NetworkError("Unable to check connected socket for errors",
                                ErrnoToString(errno),
                                errno);
  }
  if (so_error != 0) {
    return Status::NetworkError("connect", ErrnoToString(so_error), so_error);
  }

  return Status::OK();

}

static Status DoClientNegotiation(Connection* conn,
                                  MonoTime deadline,
                                  unique_ptr<ErrorStatusPB>* rpc_error) {

  RETURN_NOT_OK(WaitForClientConnect(conn->socket(), deadline));
  return Status::OK();
}

void Negotiation::RunNegotiation(const scoped_refptr<Connection>& conn,
                                 MonoTime deadline) {

  Status status;
  unique_ptr<ErrorStatusPB> rpc_error;
  if (conn->direction() == Connection::CLIENT) {
    status = DoClientNegotiation(conn.get(), deadline, &rpc_error);
  }
  if (PREDICT_FALSE(!status.ok())) {
    string msg = Substitute("$0 connection negotiation failed: $1",
                            conn->direction() == Connection::SERVER ? "Server" : "Client",
                            conn->ToString());
    status = status.CloneAndPrepend(msg);
  }

  bool is_bad = !status.ok() && !(
      (status.IsNetworkError() && status.posix_code() == ECONNREFUSED));
  if (is_bad) {
    string msg = "bad";
    if (is_bad) {
      LOG(WARNING) << "Failed RPC negotiation. Trace:\n" << msg;
    } else {
      LOG(INFO) << "RPC negotiation tracing enabled. Trace:\n" << msg;
    }
  }
  conn->CompleteNegotiation(std::move(status), std::move(rpc_error));
}

}}
