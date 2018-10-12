#pragma once

#include <memory>
#include <thread>

#include <cstdint>
#include <list>
#include <memory>
#include <string>
#include <unordered_map>

#include <boost/function.hpp> // IWYU pragma: keep
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/list_hook.hpp>

#include <ev++.h>

#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/connection.h"
#include "kudu/rpc/connection_id.h"
#include "kudu/rpc/messenger.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "rpc_header.pb.h"

namespace kudu {

class Sockaddr;
class Socket;

namespace rpc {

class Connection;
class Messenger;
class MessengerBuilder;
class OutboundCall;
class Reactor;

// The libev thread
class ReactorThread {
 public:
  friend class Connection;

  // Client-side connection map. Multiple connections could be open to a remote
  // server if multiple credential policies are used for individual RPCs.
  typedef std::unordered_multimap<ConnectionId, scoped_refptr<Connection>,
                                  ConnectionIdHash, ConnectionIdEqual>
      conn_multimap_t;

  ReactorThread(Reactor *reactor, const MessengerBuilder& builder);

  // This may be called from another thread.
  Status Init();

  // Shuts down a reactor thread, optionally waiting for it to exit.
  // Reactor::Shutdown() must have been called already.
  //
  // If mode == SYNC, may not be called from the reactor thread itself.
  void Shutdown(Messenger::ShutdownMode mode);

  // called from
  void WakeThread();

  // libev callback for handling async notifications in our epoll thread.
  void AsyncHandler(ev::async &watcher, int revents);

  // libev callback for handling timer events in our epoll thread.
  void TimerHandler(ev::timer &watcher, int revents);

  // Register an epoll timer watcher with our event loop.
  // Does not set a timeout or start it.
  void RegisterTimeout(ev::timer *watcher);

  // This may be called from another thread.
  const std::string &name() const;

  MonoTime cur_time() const {
    return cur_time_;
  }

  Reactor* reactor() const {
    return reactor_;
  }

  // Return true if this reactor thread is the thread currently
  // running. Should be used in DCHECK assertions.
  bool IsCurrentThread() const;

  // Begin the process of connection negotiation.
  // Must be called from the reactor thread.
  // HERE - we only do make sure the connection established
  Status StartConnectionNegotiation(const scoped_refptr<Connection>& conn);

  // Transition back from negotiating to processing requests.
  // Must be called from the reactor thread.
  void CompleteConnectionNegotiation(const scoped_refptr<Connection>& conn,
                                     const Status& status,
                                     std::unique_ptr<ErrorStatusPB> rpc_error);
 private:
  friend class AssignOutboundCallTask;

  // Deal the outgoint rpc
  void AssignOutboundCall(std::shared_ptr<OutboundCall> call);

  // Run the main event loop of the reactor
  void Run();

  // Find a connection to the given remote and returns it in 'conn'.
  // Returns true if a connection is found. Returns false otherwise.
  bool FindConnection(const ConnectionId& conn_id,
                      scoped_refptr<Connection>* conn);

  // Find or create a new connection to the given remote.
  // If such a connection already exists, returns that, otherwise creates a new one.
  // May return a bad Status if the connect() call fails.
  // The resulting connection object is managed internally by the reactor thread.
  Status FindOrStartConnection(const ConnectionId& conn_id,
                               scoped_refptr<Connection>* conn);

  // Shut down the given connection, removing it from the connection tracking
  // structures of this reactor.
  //
  // The connection is not explicitly deleted -- shared_ptr reference counting
  // may hold on to the object after this, but callers should assume that it
  // _may_ be deleted by this call.
  void DestroyConnection(Connection *conn, const Status &conn_status,
                         std::unique_ptr<ErrorStatusPB> rpc_error = {});

  // Create a new client socket (non-blocking, NODELAY)
  static Status CreateClientSocket(Socket *sock);

  // Initiate a new connection on the given socket.
  static Status StartConnect(Socket *sock, const Sockaddr &remote);

  // Actually perform shutdown of the thread, tearing down any connections,
  // etc. This is called from within the thread.
  void ShutdownInternal();

 private:
  // the eventloop thread
  std::shared_ptr<std::thread> thread_ptr_;

  // Scan for idle connections on this granularity.
  const MonoDelta coarse_timer_granularity_;

  // the event_loop
  ev::dynamic_loop loop_;

  // async notify
  ev::async async_;

  ev::timer timer_;

  // The current monotonic time.  Updated every coarse_timer_granularity_secs_.
  MonoTime cur_time_;

  // Map of sockaddrs to Connection objects for outbound (client) connections.
  conn_multimap_t client_conns_;

  // Total number of client connections opened during Reactor's lifetime.
  uint64_t total_client_conns_cnt_;

  // The parent pointer
  Reactor* reactor_;
};

class ReactorTask : public boost::intrusive::list_base_hook<> {
 public:
  ReactorTask();

  virtual void Run(ReactorThread* reactor_thread) = 0;

  virtual void Abort(const Status& abort_status) {}

  virtual ~ReactorTask();

 private:
  DISALLOW_COPY_AND_ASSIGN(ReactorTask);
};

class Reactor {
 public:
  Reactor(std::shared_ptr<Messenger> messenger,
          int index,
          const MessengerBuilder &builder);

  Status Init();

  // Shuts down the reactor and its corresponding thread, optionally waiting
  // until the thread has exited.
  void Shutdown(Messenger::ShutdownMode mode);

  // Schedule the given task's Run() method to be called on the
  // reactor thread.
  // If the reactor shuts down before it is run, the Abort method will be
  // called.
  // Does _not_ take ownership of 'task' -- the task should take care of
  // deleting itself after running if it is allocated on the heap.
  void ScheduleReactorTask(ReactorTask* task);

  // queue the outbound call
  void QueueOutboundCall(const std::shared_ptr<OutboundCall>& call);

  // If the Reactor is closing, returns false.
  // Otherwise, drains the pending_tasks_ queue into the provided list.
  bool DrainTaskQueue(boost::intrusive::list<ReactorTask> *tasks);

  bool closing() const {
    return closing_;
  }

  Messenger *messenger() const {
    return messenger_.get();
  }

  const std::string &name() const;

 private:
  friend class ReactorThread;

  typedef simple_spinlock LockType;
  mutable LockType lock_;

  // guarded by lock_
  boost::intrusive::list<ReactorTask> pending_tasks_;

  // hold the messenger lifetime
  std::shared_ptr<Messenger> messenger_;

  bool closing_ { false };
  const std::string name_;
  ReactorThread thread_;
};

}
}
