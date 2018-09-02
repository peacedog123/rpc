#include "my_conn.h"

#include <arpa/inet.h>

#include <string>

#include "kudu/gutil/endian.h"

MyConn::MyConn(const int fd, const std::string &ip_port, ServerThread *thread)
  : PinkConn(fd, ip_port, thread),
    header_len_(-1),
    cur_pos_(0),
    rbuf_len_(0),
    remain_packet_len_(0),
    connStatus_(kHeader),
    wbuf_len_(0),
    wbuf_pos_(0) {
  rbuf_ = reinterpret_cast<uint8_t *>(malloc(sizeof(uint8_t) * kProtoMaxMessage));
  wbuf_ = reinterpret_cast<uint8_t *>(malloc(sizeof(uint8_t) * kProtoMaxMessage));
}

MyConn::~MyConn() {
  free(rbuf_);
  free(wbuf_);
}

ReadStatus MyConn::GetRequest() {
  while (true) {
    switch (connStatus_) {
      case kHeader: {
        ssize_t nread = read(fd(), rbuf_ + rbuf_len_, COMMAND_HEADER_LENGTH - rbuf_len_);
        if (nread == -1) {
          if (errno == EAGAIN) {
            return kReadHalf;
          } else {
            return kReadError;
          }
        } else if (nread == 0) {
          return kReadClose;
        } else {
          rbuf_len_ += nread;
          if (rbuf_len_ - cur_pos_ == COMMAND_HEADER_LENGTH) {
            header_len_ = NetworkByteOrder::Load32(rbuf_ + cur_pos_);
            remain_packet_len_ = header_len_;
            cur_pos_ += COMMAND_HEADER_LENGTH;
            connStatus_ = kPacket;
            continue;
          }
          return kReadHalf;
        }
      }
      case kPacket: {
        if (header_len_ >= kProtoMaxMessage - COMMAND_HEADER_LENGTH) {
          return kFullError;
        } else {
          // read msg body
          ssize_t nread = read(fd(), rbuf_ + rbuf_len_, remain_packet_len_);
          if (nread == -1) {
            if (errno == EAGAIN) {
              return kReadHalf;
            } else {
              return kReadError;
            }
          } else if (nread == 0) {
            return kReadClose;
          }

          rbuf_len_ += nread;
          remain_packet_len_ -= nread;
          if (remain_packet_len_ == 0) {
            cur_pos_ = rbuf_len_;
            connStatus_ = kComplete;
            continue;
          }
          return kReadHalf;
        }
      }
      case kComplete: {
        if (DealMessage() != 0) {
          return kDealError;
        }
        connStatus_ = kHeader;
        cur_pos_ = 0;
        rbuf_len_ = 0;
        return kReadAll;
      }
      // Add this switch case just for delete compile warning
      case kBuildObuf:
        break;

      case kWriteObuf:
        break;
    }
  }

  return kReadHalf;
}

WriteStatus MyConn::SendReply() {
  if (!BuildObuf().ok()) {
    return kWriteError;
  }
  ssize_t nwritten = 0;
  while (wbuf_len_ > 0) {
    nwritten = write(fd(), wbuf_ + wbuf_pos_, wbuf_len_ - wbuf_pos_);
    if (nwritten <= 0) {
      break;
    }
    wbuf_pos_ += nwritten;
    if (wbuf_pos_ == wbuf_len_) {
      wbuf_len_ = 0;
      wbuf_pos_ = 0;
    }
  }
  if (nwritten == -1) {
    if (errno == EAGAIN) {
      return kWriteHalf;
    } else {
      // Here we should close the connection
      return kWriteError;
    }
  }
  if (wbuf_len_ == 0) {
    return kWriteAll;
  } else {
    return kWriteHalf;
  }
}

int MyConn::DealMessage() {
  // 1. Parse RprcHeader
  uint32_t request_header_length = NetworkByteOrder::Load32(rbuf_ + COMMAND_HEADER_LENGTH);
  bool success = request_header_.ParseFromArray(rbuf_ + COMMAND_HEADER_LENGTH + COMMAND_HEADER_LENGTH, request_header_length);

  // 2. Parse Body
  uint32_t body_start = COMMAND_HEADER_LENGTH + COMMAND_HEADER_LENGTH + request_header_length;
  uint32_t body_length = NetworkByteOrder::Load32(rbuf_ + body_start);
  success = ping_.ParseFromArray(rbuf_ + body_start + COMMAND_HEADER_LENGTH, body_length);

  // populate response
  uint32_t call_id = request_header_.call_id();
  ping_res_.set_res(0);
  ping_res_.set_mess("Hello, call_id: " + std::to_string(call_id));

  set_is_reply(true);

  return 0;
}

Status MyConn::BuildObuf() {
  kudu::rpc::ResponseHeader response_header;
  response_header.set_call_id(request_header_.call_id());

  uint32_t total_length = COMMAND_HEADER_LENGTH
                        + response_header.ByteSize() + COMMAND_HEADER_LENGTH
                        + ping_res_.ByteSize();

  NetworkByteOrder::Store32(wbuf_, total_length);

  uint32_t header_length = response_header.ByteSize();
  NetworkByteOrder::Store32(wbuf_ + COMMAND_HEADER_LENGTH, header_length);

  response_header.SerializeToArray(wbuf_ + COMMAND_HEADER_LENGTH + COMMAND_HEADER_LENGTH,
                                   response_header.ByteSize());

  uint32_t body_length = ping_res_.ByteSize();
  NetworkByteOrder::Store32(wbuf_ + COMMAND_HEADER_LENGTH + COMMAND_HEADER_LENGTH
         + response_header.ByteSize(), body_length);

  ping_res_.SerializeToArray(wbuf_ + COMMAND_HEADER_LENGTH + COMMAND_HEADER_LENGTH
                             + response_header.ByteSize() + COMMAND_HEADER_LENGTH, ping_res_.ByteSize());

  wbuf_len_ = total_length + COMMAND_HEADER_LENGTH;
  return Status::OK();
}

