// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <iostream>

#include "c/cli.h"

int main(int argc, char* argv[]) {
  idxfs_log_init();
  idxfs_parse_arguments(&argc, &argv);
  if (argc != 2) {
    std::cerr << "== Usage: " << argv[0] << " <path> " << std::endl;
    return -1;
  }
  cli_t* cli = NULL;
  info_t info;
  if (idxfs_create_client(NULL, &cli) == 0) {
    if (idxfs_getinfo(cli, argv[1], &info) == 0) {
      if (idxfs_delete(cli, argv[1]) == 0) {
        std::cout << "== File deleted" << std::endl;
      }
    }
  }
  return idxfs_destroy_client(cli);
}
