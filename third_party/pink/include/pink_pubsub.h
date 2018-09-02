// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PINK_INCLUDE_PUBSUB_H_
#define PINK_INCLUDE_PUBSUB_H_

#include <sys/epoll.h>

#include <string>
#include <functional>
#include <queue>
#include <map>
#include <utility>
#include <atomic>
#include <vector>
#include <set>
#include <fcntl.h>

#include "slash/include/xdebug.h"
#include "slash/include/slash_mutex.h"
#include "slash/include/slash_string.h"

#include "pink/src/pink_epoll.h"
#include "pink/include/pink_thread.h"
#include "pink/include/pink_define.h"

namespace pink {

class PinkEpoll;
class PinkFiredEvent;
class PinkConn;

class PubSubThread : public Thread {
 public:
  PubSubThread();

  virtual ~PubSubThread();

  // PubSub

  int Publish(const std::string& channel, const std::string& msg);

  void Subscribe(PinkConn* conn,
                 const std::vector<std::string>& channels,
                 const bool pattern,
                 std::vector<std::pair<std::string, int>>* result);

  int UnSubscribe(PinkConn* conn,
                  const std::vector<std::string>& channels,
                  const bool pattern,
                  std::vector<std::pair<std::string, int>>* result);

  void PubSubChannels(const std::string& pattern,
                      std::vector<std::string >* result);

  void PubSubNumSub(const std::vector<std::string>& channels,
                    std::vector<std::pair<std::string, int>>* result);

  int PubSubNumPat();

 private:
  void RemoveConn(PinkConn* conn);

  int ClientChannelSize(PinkConn* conn);

  int msg_pfd_[2];
  int notify_pfd_[2];
  bool should_exit_;

  mutable slash::RWMutex rwlock_; /* For external statistics */
  std::map<int, PinkConn*> conns_;

  slash::Mutex pub_mutex_;
  slash::CondVar receiver_rsignal_;
  slash::Mutex receiver_mutex_;

  /*
   * receive fd from worker thread
   */
  slash::Mutex mutex_;
  std::queue<int > fd_queue_;

  std::string channel_;
  std::string message_;
  int receivers_;

  /*
   * The epoll handler
   */
  PinkEpoll *pink_epoll_;

  virtual void *ThreadMain();

  // clean conns
  void Cleanup();

  // PubSub
  slash::Mutex channel_mutex_;
  slash::Mutex pattern_mutex_;

  std::map<std::string, std::vector<PinkConn* >> pubsub_channel_;    // channel <---> conns
  std::map<std::string, std::vector<PinkConn* >> pubsub_pattern_;    // channel <---> conns

  // No copying allowed
  PubSubThread(const PubSubThread&);
  void operator=(const PubSubThread&);
};  // class PubSubThread

}  // namespace pink
#endif  // THIRD_PINK_PINK_INCLUDE_PINK_PUBSUB_H_
