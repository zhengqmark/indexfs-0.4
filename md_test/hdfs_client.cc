// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <map>
#include <hdfs.h>
#include <stdio.h>

#include "client/libclient.h"

extern "C" {

static hdfsFS fs; // HDFS handler

static int fd_counter = 0;
static std::map<int, hdfsFile> files;

int IDX_Init(struct conf_t* config) {
  struct hdfsBuilder* bld = hdfsNewBuilder();
  hdfsBuilderSetNameNode(bld, "default");
  hdfsBuilderSetForceNewInstance(bld);
  fs = hdfsBuilderConnect(bld);
  if (fs != NULL) {
    return 0;
  }
  fprintf(stderr, "cannot connect to hdfs\n");
  return -1;
}

void IDX_Destroy() {
  if (fs != NULL) {
    hdfsDisconnect(fs);
    fs = NULL;
  }
}

static inline
void RetrieveStatus(hdfsFileInfo* info, struct stat* buf) {
  buf->st_ino = 0;
  buf->st_size = info->mSize;
  buf->st_blksize = info->mBlockSize;
  buf->st_mode = info->mPermissions;
  if (info->mKind == kObjectKindFile)
    buf->st_mode |= S_IFREG;
  if (info->mKind == kObjectKindDirectory)
    buf->st_mode |= S_IFDIR;
  buf->st_mtime = info->mLastMod;
  buf->st_ctime = info->mLastMod;
  buf->st_atime = info->mLastAccess;
}

static inline
hdfsFile GetFile(int fd) {
  return files[fd];
}

static inline
void RemoveFileDescriptor(int fd) {
  files.erase(fd);
}

static inline
int FetchFileDescriptor(hdfsFile file) {
  int fd = ++fd_counter;
  files.insert(std::make_pair(fd, file));
  return fd;
}

//////////////////////////////////////////////////////////////////////////////////
// METADATA OPERATIONS
//

int IDX_Mknod(const char *path, mode_t mode) {
  hdfsFile f = hdfsOpenFile(fs, path, O_WRONLY, 0, 0, 0);
  if (f == NULL) {
    return -1;
  }
  hdfsCloseFile(fs, f);
  return 0;
}

int IDX_Mkdir(const char *path, mode_t mode) {
  return hdfsCreateDirectory(fs, path);
}

int IDX_Unlink(const char *path) {
  return hdfsDelete(fs, path, false);
}

int IDX_Chmod(const char *path, mode_t mode) {
  return hdfsChmod(fs, path, mode);
}

int IDX_GetAttr(const char *path, struct stat *buf) {
  hdfsFileInfo* info = hdfsGetPathInfo(fs, path);
  if (info == NULL) {
    return -1;
  }
  RetrieveStatus(info, buf);
  delete info;
  return 0;
}

int IDX_Access(const char* path) {
  return hdfsExists(fs, path);
}

int IDX_Create(const char *path, mode_t mode) {
  hdfsFile f = hdfsOpenFile(fs, path, O_WRONLY, 0, 0, 0);
  if (f == NULL) {
    return -1;
  }
  return FetchFileDescriptor(f);
}

int IDX_Rmdir(const char *path) {
  return hdfsDelete(fs, path, true);
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
  hdfsFile f = GetFile(fd);
  if (f == NULL) {
    return -1;
  }
  return hdfsHSync(fs, f);
}

int IDX_Close(int fd) {
  hdfsFile f = GetFile(fd);
  if (f == NULL) {
    return -1;
  }
  RemoveFileDescriptor(fd);
  return hdfsCloseFile(fs, f);
}

int IDX_Open(const char *path, int flags, int *fd) {
  hdfsFile f = hdfsOpenFile(fs, path, O_RDONLY, 0, 0, 0);
  if (f == NULL) {
    return -1;
  }
  (*fd) = FetchFileDescriptor(f);
  return 0;
}

int IDX_Read(int fd, void *buf, size_t size) {
  hdfsFile f = GetFile(fd);
  if (f == NULL) {
    return -1;
  }
  return hdfsRead(fs, f, buf, size);
}

int IDX_Write(int fd, const void *buf, size_t size) {
  hdfsFile f = GetFile(fd);
  if (f == NULL) {
    return -1;
  }
  return hdfsWrite(fs, f, buf, size);
}

int IDX_Pread(int fd, void *buf, off_t offset, size_t size) {
  hdfsFile f = GetFile(fd);
  if (f == NULL) {
    return -1;
  }
  return hdfsPread(fs, f, offset, buf, size);
}

int IDX_Pwrite(int fd, const void *buf, off_t offset, size_t size) {
  return IDX_Write(fd, buf, size); // fall back to write
}

}  /* end extern "C" */
