#pragma once

#include <memory>
#include <string>
#include <vector>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/rpc/connection.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/locks.h"
#include "kudu/util/thread_pool.h"

namespace kudu {

namespace rpc {

class MessengerBuilder;
class OutboundCall;
class Reactor;
class ReactorThread;

class Messenger {
 public:
  friend class MessengerBuilder;
  friend class Proxy;
  friend class Reactor;
  friend class ReactorThread;

  static const uint64_t UNKNOWN_CALL_ID = 0;

  ~Messenger();

  // Stops all communication and prevents further use. If called explicitly,
  // also waits for outstanding tasks running on reactor threads to finish,
  // which means it may  not be called from a reactor task.
  //
  // It's not required to call this -- dropping the shared_ptr provided
  // from MessengerBuilder::Build will automatically call this method.
  void Shutdown();

  // Queue a call for transmission. This will pick the appropriate reactor,
  // and enqueue a task on that reactor to assign and send the call.
  void QueueOutboundCall(const std::shared_ptr<OutboundCall> &call);

  const int64_t rpc_negotiation_timeout_ms() const {
    return rpc_negotiation_timeout_ms_;
  }

  ThreadPool* negotiation_pool(Connection::Direction dir);

 private:
  explicit Messenger(const MessengerBuilder &builder);
  Reactor* RemoteToReactor(const Sockaddr &remote);
  Status Init();

  // Shuts down the messenger.
  //
  // Depending on 'mode', may or may not wait on any outstanding reactor tasks.
  enum class ShutdownMode {
    SYNC,
    ASYNC,
  };
  void ShutdownInternal(ShutdownMode mode);

  // Called by external-facing shared_ptr when the user no longer holds
  // any references. See 'retain_self_' for more info.
  void AllExternalReferencesDropped();

  const std::string name_;

  //Timeout in milliseconds after which an incomplete connection negotiation will timeout.
  const int64_t rpc_negotiation_timeout_ms_;

  gscoped_ptr<ThreadPool> client_negotiation_pool_;

  // Protects closing_
  mutable percpu_rwlock lock_;

  bool closing_;

  std::vector<Reactor*> reactors_;

  std::shared_ptr<Messenger> retain_self_;

  DISALLOW_COPY_AND_ASSIGN(Messenger);
};

class MessengerBuilder {
 public:
  friend class Messenger;
  friend class ReactorThread;

  explicit MessengerBuilder(std::string name);

  // Set the number of reactor threads that will be used for sending and
  // receiving.
  MessengerBuilder &set_num_reactors(int num_reactors);

  // Set the timeout for negotiating an RPC connection.
  MessengerBuilder &set_rpc_negotiation_timeout_ms(int64_t time_in_ms);

  // Set the granularity with which connections are checked for keepalive.
  MessengerBuilder &set_coarse_timer_granularity(const MonoDelta &granularity);

  // Set the time in milliseconds after which an idle connection from a client will be
  // disconnected by the server.
  MessengerBuilder &set_connection_keep_alive_time(int32_t time_in_ms);

  Status Build(std::shared_ptr<Messenger> *msgr);

 private:
  const std::string name_;
  int num_reactors_;
  int64_t rpc_negotiation_timeout_ms_;
  MonoDelta connection_keepalive_time_;
  MonoDelta coarse_timer_granularity_;
};

}}
