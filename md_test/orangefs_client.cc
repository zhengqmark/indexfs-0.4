// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

extern "C" {
#include <orange.h>
}

#include "client/libclient.h"

extern "C" {

int IDX_Init() {
  return 0;
}

void IDX_Destroy() {
  // empty
}

//////////////////////////////////////////////////////////////////////////////////
// METADATA OPERATIONS
//

int IDX_Mknod(const char *path, mode_t mode) {
  return pvfs_creat(path, mode);
}

int IDX_Mkdir(const char *path, mode_t mode) {
  return pvfs_mkdir(path, mode);
}

int IDX_Unlink(const char *path) {
  return pvfs_unlink(path);
}

int IDX_Chmod(const char *path, mode_t mode) {
  return pvfs_chmod(path, mode);
}

int IDX_GetAttr(const char *path, struct stat *buf) {
  return pvfs_stat(path, buf);
}

int IDX_Access(const char* path) {
  return pvfs_access(path, F_OK);
}

int IDX_Create(const char *path, mode_t mode) {
  return pvfs_creat(path, mode);
}

int IDX_Rmdir(const char *path) {
  return pvfs_rmdir(path);
}

int IDX_RecMknod(const char *path, mode_t mode) {
  return IDX_Mknod(path, mode); // fall back to mknod
}

int IDX_RecMkdir(const char *path, mode_t mode) {
  return IDX_Mkdir(path, mode); // fall back to mkdir
}

//////////////////////////////////////////////////////////////////////////////////
// IO OPERATIONS
//

int IDX_Fsync(int fd) {
  return pvfs_fsync(fd);
}

int IDX_Close(int fd) {
  return pvfs_close(fd);
}

int IDX_Open(const char *path, int flags, int *fd) {
  return -1;
}

int IDX_Read(int fd, char *buf, size_t size) {
  return -1;
}

int IDX_Write(int fd, const char *buf, size_t size) {
  return -1;
}

int IDX_Pread(int fd, void *buf, off_t offset, size_t size) {
  return -1;
}

int IDX_Pwrite(int fd, const void *buf, off_t offset, size_t size) {
  return -1;
}

}  /* end extern "C" */
