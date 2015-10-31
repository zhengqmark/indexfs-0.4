// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "common/network.h"

#include <net/if.h>
#include <arpa/inet.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>

namespace indexfs {

namespace {

class SoConf {
 public:

  SoConf() : fd_(-1) {
    ifconf_.ifc_len = sizeof(ifr_);
    ifconf_.ifc_buf = reinterpret_cast<char*>(ifr_);
  }

  ~SoConf() {
    if (fd_ >= 0) {
      close(fd_);
    }
  }

  Status OpenSocket() {
    Status s;
    fd_ = socket(AF_INET, SOCK_STREAM, 0);
    return fd_ >= 0 ? s : Status::IOError("Cannot open socket");
  }

  Status LoadSocketConfig();
  Status GetHostIPAddresses(std::vector<std::string>* ips);

 private:

  int fd_;
  struct ifconf ifconf_;
  struct ifreq ifr_[64];
};

Status SoConf::LoadSocketConfig() {
  Status s;
  if (ioctl(fd_, SIOCGIFCONF, &ifconf_) < 0) {
    return Status::IOError("Cannot get socket configurations");
  }
  return s;
}

Status SoConf::GetHostIPAddresses(std::vector<std::string>* ips) {
  Status s;
  char ip[INET_ADDRSTRLEN];
  int num_ips = ifconf_.ifc_len / sizeof(ifr_[0]);
  for (int i = 0; i < num_ips; i++) {
    struct sockaddr_in* s_in = (struct sockaddr_in *) &ifr_[i].ifr_addr;
    if (inet_ntop(AF_INET, &s_in->sin_addr, ip, INET_ADDRSTRLEN) == NULL) {
      return Status::IOError("Cannot get IP address");
    }
    ips->push_back(ip);
  }
  return s;
}

} /* anonymous namespace */

Status GetHostIPAddrs(std::vector<std::string>* ips) {
  SoConf conf;
  Status s = conf.OpenSocket();
  if (!s.ok()) {
    return s;
  }
  s = conf.LoadSocketConfig();
  return s.ok() ? conf.GetHostIPAddresses(ips) : s;
}

} /* namespace indexfs */
