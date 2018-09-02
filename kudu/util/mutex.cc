// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// Portions (c) 2011 The Chromium Authors.

#include "kudu/util/mutex.h"

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <ostream>
#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
//#include "kudu/util/debug-util.h"
#include "kudu/util/env.h"
#include "kudu/util/flag_tags.h"
//#include "kudu/util/trace.h"

using std::string;
using strings::Substitute;
using strings::SubstituteAndAppend;

namespace kudu {

Mutex::Mutex()
{
  // In release, go with the default lock attributes.
  pthread_mutex_init(&native_handle_, NULL);
}

Mutex::~Mutex() {
  int rv = pthread_mutex_destroy(&native_handle_);
  DCHECK_EQ(0, rv) << ". " << strerror(rv);
}

bool Mutex::TryAcquire() {
  int rv = pthread_mutex_trylock(&native_handle_);
  return rv == 0;
}

void Mutex::Acquire() {
  // Optimize for the case when mutexes are uncontended. If they
  // are contended, we'll have to go to sleep anyway, so the extra
  // cost of branch mispredictions is moot.
  //
  // TryAcquire() is implemented as a simple CompareAndSwap inside
  // pthreads so this does not require a system call.
  if (PREDICT_TRUE(TryAcquire())) {
    return;
  }

  // If we weren't able to acquire the mutex immediately, then it's
  // worth gathering timing information about the mutex acquisition.
  MicrosecondsInt64 start_time = GetMonoTimeMicros();
  int rv = pthread_mutex_lock(&native_handle_);
  DCHECK_EQ(0, rv) << ". " << strerror(rv)
  ; // NOLINT(whitespace/semicolon)
  MicrosecondsInt64 end_time = GetMonoTimeMicros();

  int64_t wait_time = end_time - start_time;
  if (wait_time > 0) {
    //TRACE_COUNTER_INCREMENT("mutex_wait_us", wait_time);
  }

}

void Mutex::Release() {
  int rv = pthread_mutex_unlock(&native_handle_);
  DCHECK_EQ(0, rv) << ". " << strerror(rv);
}

} // namespace kudu
