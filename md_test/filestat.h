// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_FILESTAT_H_
#define _INDEXFS_FILESTAT_H_

#include <stdint.h>
#include <sys/stat.h>

struct filestat {
  uint64_t fs_ino;
  uint64_t fs_size;
  struct timespec fs_ctim;
  struct timespec fs_mtim;
  uint32_t fs_mode;
};

#endif /* FILESTAT_H_ */
