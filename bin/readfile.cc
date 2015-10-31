// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <iostream>

#include "client/libclient_helper.h"

int main(int argc, char* argv[]) {
  SetUsageMessage("IndexFS Client Toolkit - read");
  ParseCommandLineFlags(&argc, &argv, true);
  if (argc != 3) {
    std::cerr << "== Usage: " << argv[0] << " <path> <size> " << std::endl;
    return -1;
  }
  int fd;
  struct conf_t* config = NULL;
  if (IDX_Init(config) == 0) {
    const char* p = argv[1];
    if (IDX_Open(p, O_RDONLY, &fd) == 0) {
      int size = atoi(argv[2]);
      char data[size];
      int read_size = IDX_Pread(fd, data, 0, size);
      std::cout << "Read Size: " << read_size << std::endl;
      for (int i = 0; i < read_size; ++i) {
        std::cout << data[i];
      }
      std::cout << std::endl;
      IDX_Close(fd);
    }
  }
  IDX_Destroy();
  return 0;
}
