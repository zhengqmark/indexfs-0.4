// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "c/libclient.h"
#include "c/cli.h"

extern "C" {

static cli_t* cli = NULL;

int IDX_Init(conf_t* conf) {
  idxfs_log_init();
  return idxfs_create_client(conf, &cli);
}

int IDX_Destroy() {
  int r = idxfs_destroy_client(cli);
  cli = NULL;
  idxfs_log_close();
  return r;
}

//////////////////////////////////////////////////////////////////////////////////
// METADATA OPERATIONS
//

int IDX_Mknod(const char* path, mode_t mode) {
  return idxfs_mknod(cli, path, mode);
}

int IDX_Mkdir(const char* path, mode_t mode) {
  return idxfs_mkdir(cli, path, mode);
}

int IDX_Unlink(const char* path) {
  return idxfs_delete(cli, path);
}

int IDX_Rmdir(const char* path) {
  return idxfs_rmdir(cli, path);
}

int IDX_Chmod(const char* path, mode_t mode, info_t* info) {
  bool is_dir;
  int r = idxfs_chmod(cli, path, mode, &is_dir);
  if (info != NULL) {
    info->is_dir = is_dir;
  }
  return r;
}

int IDX_Chown(const char* path, uid_t owner, gid_t group, info_t* info) {
  bool is_dir;
  int r = idxfs_chown(cli, path, owner, group, &is_dir);
  if (info != NULL) {
    info->is_dir = is_dir;
  }
  return r;
}

int IDX_Access(const char* path) {
  return idxfs_exists(cli, path);
}

int IDX_AccessDir(const char* path) {
  return idxfs_dir_exists(cli, path);
}

int IDX_Getattr(const char* path, info_t* buf) {
  return idxfs_getinfo(cli, path, buf);
}

int IDX_Readdir(const char* path, readdir_handler_t handler, void* arg) {
  return idxfs_readdir(cli, path, handler, arg);
}

//////////////////////////////////////////////////////////////////////////////////
// IO OPERATIONS
//

int IDX_Create(const char* path, mode_t mode) {
  return 0;
}

int IDX_Fsync(int fd) {
  return 0;
}

int IDX_Close(int fd) {
  return 0;
}

int IDX_Open(const char* path, int flags, int* fd) {
  return 0;
}

int IDX_Pread(int fd, void* buf, off_t offset, size_t size) {
  return 0;
}

int IDX_Pwrite(int fd, const void* buf, off_t offset, size_t size) {
  return 0;
}

} /* end extern "C" */
