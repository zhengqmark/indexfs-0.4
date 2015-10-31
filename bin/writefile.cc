// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <iostream>

#include "client/libclient_helper.h"

int main(int argc, char* argv[]) {
  SetUsageMessage("IndexFS Client Toolkit - write");
  ParseCommandLineFlags(&argc, &argv, true);
  if (argc != 3) {
    std::cerr << "== Usage: " << argv[0] << " <path> <size> " << std::endl;
    return -1;
  }
  int fd;
  struct stat buf;
  struct conf_t* config = NULL;
  if (IDX_Init(config) == 0) {
    const char* p = argv[1];
    if (IDX_Mknod(p, (S_IRWXU | S_IRWXG | S_IRWXO)) == 0) {
      if (IDX_Open(p, O_WRONLY, &fd) == 0) {
        int size = atoi(argv[2]);
        char data[size];
        for (int i = 0; i < size; ++i) {
          data[i] = static_cast<char>('a' + i % 26);
        }
        IDX_Pwrite(fd, data, 0, size);
        IDX_Close(fd);
        if (IDX_GetAttr(p, &buf) == 0) {
          std::cout << p << ": ";
          if (S_ISDIR(buf.st_mode)) {
            std::cout << "(dir)" << std::endl;
            std::cout << "  inode=" << buf.st_ino << std::endl;
            std::cout << "  zserv=" << buf.st_dev << std::endl;
          } else {
            std::cout << "(file)" << std::endl;
            std::cout << "  size=" << buf.st_size << std::endl;
          }
          std::cout << "  mode=" << std::oct << buf.st_mode << std::endl;
        }
      }
    }
  }
  IDX_Destroy();
  return 0;
}
