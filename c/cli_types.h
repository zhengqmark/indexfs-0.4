// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_C_CLI_TYPES_H_
#define _INDEXFS_C_CLI_TYPES_H_

#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>

typedef struct {
  int mode;
  int uid;
  int gid;
  int size;
  int mtime;
  int ctime;
  int is_dir;
} info_t;

typedef struct {
  const char* config_file;
  const char* server_list;
} conf_t;

typedef struct {
  void* rep;
} cli_t;
// User callback for readdir operation.
typedef void (*readdir_handler_t)(const char* name, void* arg);

#endif /* _INDEXFS_C_CLI_TYPES_H_ */
