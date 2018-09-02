#include "kudu/rpc/messenger.h"

#include <glog/logging.h>

#include "kudu/rpc/outbound_call.h"
#include "kudu/rpc/reactor.h"
#include "kudu/util/status.h"

namespace kudu {
namespace rpc {

using std::shared_ptr;

MessengerBuilder::MessengerBuilder(std::string name)
  : name_(std::move(name)),
    num_reactors_(4) {

}

MessengerBuilder& MessengerBuilder::set_num_reactors(int num_reactors) {
  num_reactors_ = num_reactors;
  return *this;
}

MessengerBuilder &MessengerBuilder::set_rpc_negotiation_timeout_ms(int64_t time_in_ms) {
  rpc_negotiation_timeout_ms_ = time_in_ms;
  return *this;
}

MessengerBuilder& MessengerBuilder::set_coarse_timer_granularity(const MonoDelta &granularity) {
  coarse_timer_granularity_ = granularity;
  return *this;
}

MessengerBuilder &MessengerBuilder::set_connection_keep_alive_time(int32_t time_in_ms) {
  connection_keepalive_time_ = MonoDelta::FromMilliseconds(time_in_ms);
  return *this;
}

Status MessengerBuilder::Build(std::shared_ptr<Messenger> *msgr) {

  Messenger* messenger(new Messenger(*this));
  RETURN_NOT_OK(messenger->Init());

  // See docs on Messenger::retain_self_ for info about this odd hack.
  *msgr = shared_ptr<Messenger>(messenger, std::mem_fun(&Messenger::AllExternalReferencesDropped));
  return Status::OK();
}


//////// Messenger ////////
Messenger::Messenger(const MessengerBuilder& builder)
  : name_(builder.name_),
    rpc_negotiation_timeout_ms_(builder.rpc_negotiation_timeout_ms_),
    closing_(false),
    retain_self_(this) {
  for (int i = 0; i < builder.num_reactors_; i++) {
    reactors_.push_back(new Reactor(retain_self_, i, builder));
  }

  client_negotiation_pool_.reset(new ThreadPool());
}

Messenger::~Messenger() {
  CHECK(closing_) << "Should have already shut down";
}

// TODO
void Messenger::ShutdownInternal(ShutdownMode mode) {

}

void Messenger::AllExternalReferencesDropped() {
  ShutdownInternal(ShutdownMode::ASYNC);
  CHECK(retain_self_.get());
  // If we have no more external references, then we no longer
  // need to retain ourself. We'll destruct as soon as all our
  // internal-facing references are dropped (ie those from reactor
  // threads).
  retain_self_.reset();
}

Status Messenger::Init() {
  for (Reactor* reactor : reactors_) {
    RETURN_NOT_OK(reactor->Init());
  }
  return Status::OK();
}

void Messenger::QueueOutboundCall(const std::shared_ptr<OutboundCall> &call) {
  Reactor* reactor = RemoteToReactor(call->conn_id().remote());
  reactor->QueueOutboundCall(call);
}

Reactor* Messenger::RemoteToReactor(const Sockaddr &remote) {
  uint32_t hashCode = remote.HashCode();
  int reactor_idx = hashCode % reactors_.size();
  // This is just a static partitioning; we could get a lot
  // fancier with assigning Sockaddrs to Reactors.
  return reactors_[reactor_idx];
}

ThreadPool* Messenger::negotiation_pool(Connection::Direction dir) {
  switch (dir) {
    case Connection::CLIENT: return client_negotiation_pool_.get();
  }
  DCHECK(false) << "Unknown Connection::Direction value: " << dir;
  return nullptr;
}

}}
