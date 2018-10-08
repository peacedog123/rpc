#include "kudu/rpc/reactor.h"

#include <memory>

#include "kudu/rpc/messenger.h"
#include "kudu/rpc/negotiation.h"
#include "kudu/rpc/outbound_call.h"
#include "kudu/util/net/socket.h"

namespace kudu {
namespace rpc {

using std::string;
using std::shared_ptr;
using std::unique_ptr;

namespace {

// Callback for libev fatal errors
void LibevSysErr(const char* msg) throw() {
  PLOG(FATAL) << "LibEV fatal error: " << msg;
}

void DoInitLibEv() {
  ev::set_syserr_cb(LibevSysErr);
}

}

ReactorTask::ReactorTask() {
}

ReactorTask::~ReactorTask() {
}

class AssignOutboundCallTask : public ReactorTask {
 public:
  explicit AssignOutboundCallTask(shared_ptr<OutboundCall> call)
    : call_(std::move(call)) {
  }

  void Run(ReactorThread* reactor_thread) override {
    reactor_thread->AssignOutboundCall(std::move(call_));
    delete this;
  }

  void Abort(const Status& status) override {
    // It doesn't matter what is the actual phase of the OutboundCall: just set
    // it to Phase::REMOTE_CALL to finalize the state of the call.
    call_->SetFailed(status, OutboundCall::Phase::REMOTE_CALL);
    delete this;
  }

 private:
  shared_ptr<OutboundCall> call_;
};

Reactor::Reactor(std::shared_ptr<Messenger> messenger,
                 int index, const MessengerBuilder &builder)
  : messenger_(std::move(messenger)),
    thread_(this, builder)  {
  static std::once_flag libev_once;
  std::call_once(libev_once, DoInitLibEv);
}

Status Reactor::Init() {
  DVLOG(6) << "Called Reactor::Init()";
  return thread_.Init();
}

const std::string& Reactor::name() const {
  return name_;
}

void Reactor::QueueOutboundCall(const std::shared_ptr<OutboundCall>& call) {
  DVLOG(3) << name_ << ": queueing outbound call "
           << call->ToString() << " to remote " << call->conn_id().remote().ToString();

  ScheduleReactorTask(new AssignOutboundCallTask(call));
}

void Reactor::ScheduleReactorTask(ReactorTask* reactor_task) {
  {
    std::unique_lock<LockType> l(lock_);
    if (closing_) {
      // TODO
    }
    pending_tasks_.push_back(*reactor_task);
  }
  thread_.WakeThread();
}

bool Reactor::DrainTaskQueue(boost::intrusive::list<ReactorTask> *tasks) {
  std::lock_guard<LockType> l(lock_);
  if (closing_) {
    return false;
  }
  tasks->swap(pending_tasks_);
  return true;
}

////// ReactorThread //////
ReactorThread::ReactorThread(Reactor *reactor, const MessengerBuilder& builder)
  : cur_time_(MonoTime::Now()),
    reactor_(reactor),
    coarse_timer_granularity_(builder.coarse_timer_granularity_) {
}

const std::string& ReactorThread::name() const {
  return reactor_->name();
}

Status ReactorThread::Init() {
  DCHECK(thread_ptr_.get() == nullptr) << "Already stated";
  DVLOG(6) << "Called ReactorThread::Init()";

  // Register to get async notifications in our epoll loop.
  async_.set(loop_);
  async_.set<ReactorThread, &ReactorThread::AsyncHandler>(this);
  async_.start();

  // Register the timer watcher
  // The timer is used for closing old TCP connections and applying
  // backpressure
  timer_.set(loop_);
  timer_.set<ReactorThread, &ReactorThread::TimerHandler>(this);
  timer_.start(coarse_timer_granularity_.ToSeconds(),
               coarse_timer_granularity_.ToSeconds());

  // TODO Register our other callbackes.


  thread_ptr_.reset(new std::thread(&ReactorThread::Run, this));
  return Status::OK();
}

bool ReactorThread::IsCurrentThread() const {
  return thread_ptr_ && thread_ptr_->get_id() == std::this_thread::get_id();
}

void ReactorThread::Run() {
  DVLOG(6) << "Calling ReactorThread::Run()...";
  loop_.run(0);
  VLOG(1) << name() << " thread exiting.";
  // No longer need the messenger. This cause the messenger to
  // get deleted when all the reactors exit.
  reactor_->messenger_.reset();
}

void ReactorThread::WakeThread() {
  // TODO some log
  async_.send();
}

// do the real work
void ReactorThread::AssignOutboundCall(shared_ptr<OutboundCall> call) {
  DCHECK(IsCurrentThread());

  // Skip if the outbound has been cancelled already.
  // 此时还没有建立连接，且没有数据放到socket上，可以取消
  if (PREDICT_FALSE(call->IsCancelled())) {
    return;
  }

  // 此时会去找到一条已经建立好的连接或者INPROGRESS的连接
  // INPROGRESS的连接会在client_negotiation_pool_里面等待直到连接建立
  scoped_refptr<Connection> conn;
  Status s = FindOrStartConnection(call->conn_id(), &conn);

  if (PREDICT_FALSE(!s.ok())) {
    call->SetFailed(std::move(s), OutboundCall::Phase::CONNECTION_NEGOTIATION);
    return;
  }

  conn->QueueOutboundCall(std::move(call));
}

bool ReactorThread::FindConnection(const ConnectionId& conn_id,
                                   scoped_refptr<Connection>* conn) {
  DCHECK(IsCurrentThread());

  const auto range = client_conns_.equal_range(conn_id);
  scoped_refptr<Connection> found_conn;
  for (auto it = range.first; it != range.second; ) {
    const auto& c = it->second.get();
    // TODO deal with shutdown
    found_conn = c;
    ++it;
  }

  if (found_conn) {
    conn->swap(found_conn);
    return true;
  }
  return false;
}

Status ReactorThread::FindOrStartConnection(const ConnectionId& conn_id,
                                            scoped_refptr<Connection>* conn) {
  DCHECK(IsCurrentThread());
  if (FindConnection(conn_id, conn)) {
    return Status::OK();
  }

  // no available connection, so create one
  VLOG(2) << name() << " FindOrStartConnection: creating "
          << "new connection for " << conn_id.remote().ToString();

  // Create a new socket and start connecting to the remote async
  Socket socket;
  RETURN_NOT_OK(CreateClientSocket(&socket));
  RETURN_NOT_OK(StartConnect(&socket, conn_id.remote()));

  // acquire the socket
  std::unique_ptr<Socket> new_socket(new Socket(socket.Release()));

  // wait for the connected state
  *conn = new Connection(
      this, conn_id.remote(), std::move(new_socket), Connection::CLIENT);
  (*conn)->set_outbound_connection_id(conn_id);

  // Kick off blocking client connection negotiation.
  Status s = StartConnectionNegotiation(*conn);
  if (s.IsIllegalState()) {
    // Return a nicer error message to the user indicating -- if we just
    // forward the status we'd get something generic like "ThreadPool is closing".
    return Status::ServiceUnavailable("Client RPC Messenger shutting down");
  }
  // Propagate any other errors as-is.
  RETURN_NOT_OK_PREPEND(s, "Unable to start connection negotiation thread");

  // Insert into the client connection map to avoid duplicate connection requests.
  client_conns_.emplace(conn_id, *conn);
  ++total_client_conns_cnt_;

  return Status::OK();
}

Status ReactorThread::StartConnectionNegotiation(const scoped_refptr<Connection>& conn) {
  DCHECK(IsCurrentThread());

  // Set a limit on how long the server will negotiate with a new client.
  MonoTime deadline = MonoTime::Now() + MonoDelta::FromMilliseconds(reactor()->messenger()->rpc_negotiation_timeout_ms());

  // submit the task
  ThreadPool* pool =
      reactor()->messenger()->negotiation_pool(conn->direction());
  pool->AddJob(std::bind(&Negotiation::RunNegotiation, conn, deadline));
  return Status::OK();
}

void ReactorThread::CompleteConnectionNegotiation(
    const scoped_refptr<Connection>& conn,
    const Status& status,
    unique_ptr<ErrorStatusPB> rpc_error) {
  DCHECK(IsCurrentThread());
  if (PREDICT_FALSE(!status.ok())) {
    DestroyConnection(conn.get(), status, std::move(rpc_error));
    return;
  }

  conn->MarkNegotiationComplete();
  conn->EpollRegister(loop_);
}

Status ReactorThread::CreateClientSocket(Socket* socket) {
  Status ret = socket->Init(Socket::FLAG_NONBLOCKING);
  if (ret.ok()) {
    ret = socket->SetNoDelay(true);
  }
  LOG_IF(WARNING, !ret.ok())
      << "failed to create an outbound connection because a new socket could not be created: "
      << ret.ToString();
  return ret;
}

// Non-blocking connect - Return OK when connected or INPROGRESS
Status ReactorThread::StartConnect(Socket* socket, const Sockaddr& remote) {
  const Status ret = socket->Connect(remote);
  if (ret.ok()) {
    VLOG(3) << "StartConnect: connect finished immediately for " << remote.ToString();
    return Status::OK();
  }

  int posix_code = ret.posix_code();
  if (Socket::IsTemporarySocketError(posix_code) || posix_code == EINPROGRESS) {
    VLOG(3) << "StartConnect: connect in progress for " << remote.ToString();
    return Status::OK();
  }

  LOG(WARNING) << "Failed to create an outbound connection to " << remote.ToString()
               << " because connect() failed: " << ret.ToString();
  return ret;
}

// Remove from conn_map
void ReactorThread::DestroyConnection(Connection* conn, const Status& conn_status,
                                      unique_ptr<ErrorStatusPB> rpc_error) {
  DCHECK(IsCurrentThread());

  conn->Shutdown(conn_status, std::move(rpc_error));

  // Unlink connection from lists.
  if (conn->direction() == Connection::CLIENT) {
    const auto range = client_conns_.equal_range(conn->outbound_connection_id());
    CHECK(range.first != range.second) << "Couldn't find connection " << conn->ToString();
    // The client_conns_ container is a multi-map.
    for (auto it = range.first; it != range.second;) {
      if (it->second.get() == conn) {
        it = client_conns_.erase(it);
        break;
      }
      ++it;
    }
  }
}

void ReactorThread::AsyncHandler(ev::async &watcher, int revents) {
  DCHECK(IsCurrentThread());

  if (PREDICT_FALSE(reactor_->closing())) {
    // TODO shutdown

  }

  boost::intrusive::list<ReactorTask> tasks;
  reactor_->DrainTaskQueue(&tasks);

  while (!tasks.empty()) {
    ReactorTask& task = tasks.front();
    tasks.pop_front();
    task.Run(this);
  }
}

//
//// Handles timer events.  The periodic timer:
////
//// 1. updates Reactor::cur_time_
//// 2. every tcp_conn_timeo_ seconds, close down connections older than
////    tcp_conn_timeo_ seconds.
////
void ReactorThread::TimerHandler(ev::timer &watcher, int revents) {
  DCHECK(IsCurrentThread());
  if (EV_ERROR & revents) {
    LOG(WARNING) << "Reactor " << name() << " got an error in "
      "the timer handler.";
    return;
  }
  cur_time_ = MonoTime::Now();

  // 2. TODO - Scan Idle Connections
}

void ReactorThread::RegisterTimeout(ev::timer *watcher) {
  watcher->set(loop_);
}

}}
