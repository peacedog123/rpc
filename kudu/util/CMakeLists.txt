# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


#######################################
# kudu_util
#######################################
set(SEMAPHORE_CC "semaphore.cc")

set(UTIL_SRCS
  atomic.cc
  debug/unwind_safeness.cc
  condition_variable.cc
  net/net_util.cc
  net/sockaddr.cc
  net/socket.cc
  env.cc
  errno.cc
  faststring.cc
  flag_tags.cc
  locks.cc
  malloc.cc
  monotime.cc
  mutex.cc
  os-util.cc
  path_util.cc
  random_util.cc
  rw_mutex.cc
  semaphore.cc
  signal.cc
  slice.cc
  subprocess.cc
  status.cc
)

# overwrite.cc contains a single function which would be a hot spot in
# debug builds. It's separated into a separate file so it can be
# optimized regardless of the default optimization options.
#set_source_files_properties(memory/overwrite.cc PROPERTIES COMPILE_FLAGS "-O3")

set(UTIL_LIBS
  gflags
  glog
  gutil
  ev
  z)

set(UTIL_LIBS
    ${UTIL_LIBS}
    dl
    rt)

ADD_EXPORTABLE_LIBRARY(kudu_util
  SRCS ${UTIL_SRCS}
  DEPS ${UTIL_LIBS}
  EXPORTED_DEPS ${EXPORTED_UTIL_LIBS})
