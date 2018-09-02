#pragma once

#include <stdio.h>
#include <signal.h>
#include <unistd.h>
#include <atomic>

#include "slash/include/xdebug.h"
#include "slash/include/slash_status.h"
#include "pink/include/pink_thread.h"
#include "pink/include/server_thread.h"

#include "rpc_header.pb.h"
#include "myproto.pb.h"
#include "pink/include/pink_conn.h"

#include <google/protobuf/message.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

using namespace pink;
using slash::Status;

class MyConn : public PinkConn {
 public:
  MyConn(const int fd, const std::string &ip_port, ServerThread *thread);
  ~MyConn();

  ReadStatus GetRequest() override;
  WriteStatus SendReply() override;

  uint32_t header_len_;
  uint8_t* rbuf_;
  uint32_t cur_pos_;
  uint32_t rbuf_len_;
  int32_t remain_packet_len_;

  ConnStatus connStatus_;

 protected:
  virtual int DealMessage();
  virtual Status BuildObuf();

 private:
  uint8_t* wbuf_;
  uint32_t wbuf_len_;
  uint32_t wbuf_pos_;

  kudu::rpc::RequestHeader request_header_;
  myproto::Ping ping_;
  myproto::PingRes ping_res_;
};
