// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdlib.h>

#include "common/network.h"

using ::indexfs::Status;
using ::indexfs::FetchHostname;
using ::indexfs::GetHostIPAddrs;

static
void CheckErrors(const Status &status) {
  if (!status.ok()) {
    fprintf(stderr, "%s\n", status.ToString().c_str());
    exit(EXIT_FAILURE);
  }
}

int main(int argc, char* argv[]) {
  std::string hostname;
  CheckErrors(FetchHostname(&hostname));
  fprintf(stdout, "hostname=%s\n", hostname.c_str());
  std::vector<std::string> ips;
  CheckErrors(GetHostIPAddrs(&ips));
  std::vector<std::string>::iterator it = ips.begin();
  for (; it != ips.end(); it++) {
    fprintf(stdout, "ip=%s\n", it->c_str());
  }
  return 0;
}
