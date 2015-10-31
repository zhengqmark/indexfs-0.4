// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_COMMON_NETWORK_H_
#define _INDEXFS_COMMON_NETWORK_H_

#include "common/common.h"
#include "common/options.h"

#include <vector>
#include <unistd.h>

namespace indexfs {

inline Status FetchHostname(std::string* hostname) {
  Status s;
  char buffer[HOST_NAME_MAX];
  if (gethostname(buffer, HOST_NAME_MAX) < 0) {
    return Status::IOError("Cannot get local host name");
  }
  *hostname = buffer;
  return s;
}

extern Status GetHostIPAddrs(std::vector<std::string>* ips);

} /* namespace indexfs */

#endif /* _INDEXFS_COMMON_NETWORK_H_ */
