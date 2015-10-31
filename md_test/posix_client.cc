// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>

#include "client/libclient.h"

extern "C" {

void IDX_Destroy() {
  // empty
}

int IDX_Init(struct conf_t* config) {
  return 0;
}

//////////////////////////////////////////////////////////////////////////////////
// METADATA OPERATIONS
//

int IDX_Mknod(const char *path, mode_t mode) {
  return creat(path, mode);
}

int IDX_Mkdir(const char *path, mode_t mode) {
#ifdef IDXFS_VERBOSE
  fprintf(stderr, "mkdir %s\n", path);
#endif
  return mkdir(path, mode);
}

int IDX_Unlink(const char *path) {
#ifdef IDXFS_VERBOSE
  fprintf(stderr, "unlink %s\n", path);
#endif
  return unlink(path);
}

int IDX_Chmod(const char *path, mode_t mode) {
  return chmod(path, mode);
}

int IDX_GetAttr(const char *path, struct stat *buf) {
#ifdef IDXFS_VERBOSE
  fprintf(stderr, "getattr %s\n", path);
#endif
  return stat(path, buf);
}

int IDX_Access(const char* path) {
#ifdef IDXFS_VERBOSE
  fprintf(stderr, "access %s\n", path);
#endif
  return access(path, F_OK);
}

int IDX_Create(const char *path, mode_t mode) {
#ifdef IDXFS_VERBOSE
  fprintf(stderr, "create %s\n", path);
#endif
  return creat(path, mode);
}

int IDX_Rmdir(const char *path) {
#ifdef IDXFS_VERBOSE
  fprintf(stderr, "rmdir %s\n", path);
#endif
  return rmdir(path);
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
#ifdef IDXFS_VERBOSE
  fprintf(stderr, "fsync %s\n", path);
#endif
  return fsync(fd);
}

int IDX_Close(int fd) {
#ifdef IDXFS_VERBOSE
  fprintf(stderr, "close %s\n", path);
#endif
  return close(fd);
}

int IDX_Open(const char *path, int flags, int *fd) {
  errno = EPERM;
  return -1; // Not supported
}

int IDX_Read(int fd, char *buf, size_t size) {
  errno = EPERM;
  return -1; // Not supported
}

int IDX_Write(int fd, const char *buf, size_t size) {
  errno = EPERM;
  return -1; // Not supported
}

int IDX_Pread(int fd, void *buf, off_t offset, size_t size) {
  errno = EPERM;
  return -1; // Not supported
}

int IDX_Pwrite(int fd, const void *buf, off_t offset, size_t size) {
  errno = EPERM;
  return -1; // Not supported
}

}  /* end extern "C" */
