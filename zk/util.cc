#include "zk/util.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <netdb.h>
#include <ifaddrs.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <glog/logging.h>

namespace zk {

using std::string;

std::string GetLocalIP() {
  string result;
  struct ifaddrs *ifaddr, *ifa;
  int family, s;
  char host[NI_MAXHOST];

  if (getifaddrs(&ifaddr) == -1) {
    LOG(ERROR) << "getifaddrs error";
    return "";
  }

  /* Walk through linked list, maintaining head pointer so we can free list later */
  for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
    if (ifa->ifa_addr == NULL) {
      continue;
    }

    family = ifa->ifa_addr->sa_family;
    // only interest in IPV4
    if (family != AF_INET) {
      continue;
    }

    s = getnameinfo(ifa->ifa_addr, sizeof(struct sockaddr_in),
                    host, NI_MAXHOST, NULL, 0, NI_NUMERICHOST);
    if (s != 0) {
      LOG(ERROR) << "getnameinfo() failed: " << gai_strerror(s);
      continue;
    }

    string temp(host);
    if (temp != "127.0.0.1") {
      result = temp;
    }
  }

  freeifaddrs(ifaddr);
  return result;
}


}
