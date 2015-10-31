// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <string.h>

#include "fuse_helper.h"

using indexfs::Status;
using indexfs::Client;
using indexfs::StatInfo;
using indexfs::IDXClientManager;

//////////////////////////////////////////////////////////////////////////////////
// FUSE HELPER
//

static Client* GetClient() {
  IDXClientManager* manager =
      (IDXClientManager*) fuse_get_context()->private_data;
  Client* client;
  Status s = manager->GetThreadLoadClient(&client);
  if (s.ok()) {
    return client;
  }
  std::string err = s.ToString();
  fprintf(stderr, "%s\n", err.c_str());
  exit(EXIT_FAILURE);
}

static int LogErrorAndReturn(const Status &st,
                             const char* op = NULL,
                             const char* path = NULL,
                             int return_code = 0) {
  if (st.ok()) {
    return return_code;
  }
  std::string err = st.ToString();
  if (op == NULL || path == NULL) {
    fprintf(stderr, "%s\n", err.c_str());
  } else {
    fprintf(stderr, "Cannot %s at %s - %s\n", op, path, err.c_str());
  }
  if (st.IsNotFound()) {
    return -ENOENT;
  }
  if (st.IsIOError()) {
    return -EEXIST;
  }
  return st.IsCorruption() ? -EIO : -EPERM;
}

//////////////////////////////////////////////////////////////////////////////////
// FUSE INTERFACE
//

static
int Rmdir(const char* path) {
  std::string p = path;
  Status s = GetClient()->Remove(p);
  return LogErrorAndReturn(s, "rmdir", path);
}

static
int Unlink(const char *path) {
  std::string p = path;
  Status s = GetClient()->Remove(p);
  return LogErrorAndReturn(s, "unlink", path);
}

static
int Mkdir(const char *path, mode_t mode) {
  std::string p = path;
  Status s = GetClient()->Mkdir(p, mode | S_IFDIR);
  return LogErrorAndReturn(s, "mkdir", path);
}

static
int Mknod(const char *path, mode_t mode, dev_t dev) {
  std::string p = path;
  Status s = GetClient()->Mknod(p, mode | S_IFREG);
  return LogErrorAndReturn(s, "mknod", path);
}

static
int Chmod(const char *path, mode_t mode) {
  std::string p = path;
  Status s = GetClient()->Chmod(p, mode);
  return LogErrorAndReturn(s, "chmod", path);
}

static
int Chown(const char *path, uid_t uid, gid_t gid) {
  return 0; // Do Nothing
}

static
int Utimens(const char *path, const struct timespec tv[2]) {
  return 0; // Do Nothing
}

static
int GetAttr(const char *path, struct stat *buffer) {
  std::string p = path;
  StatInfo info;
  Status s = GetClient()->Getattr(p, &info);
  if (s.ok()) {
    buffer->st_ino = info.id;
    buffer->st_mode = info.mode;
    buffer->st_uid = info.uid;
    buffer->st_gid = info.gid;
    buffer->st_size = info.size;
    buffer->st_dev = info.zeroth_server;
    buffer->st_mtime = info.mtime;
    buffer->st_ctime = info.ctime;
    buffer->st_atime = time(NULL);
  }
  return LogErrorAndReturn(s, "getattr", path);
}

static
int Open(const char *path, struct fuse_file_info *file) {
  std::string p = path;
  int fd;
  Status s = GetClient()->Open(p, file->flags, &fd);
  if (s.ok()) {
    file->fh = fd;
  }
  return LogErrorAndReturn(s, "open", path);
}

static
int Flush(const char *path, struct fuse_file_info *file) {
  return 0; // Do Nothing
}

static
int Fsync(const char *path, int mode, struct fuse_file_info *file) {
  return 0; // Do Nothing
}

static
int Release(const char *path, struct fuse_file_info *file) {
  Status s = GetClient()->Close(file->fh);
  return LogErrorAndReturn(s, "close", path);
}

static
int Read(const char *path, char *buf,
         size_t size, off_t off, struct fuse_file_info *file) {
  int fd = (int) file->fh;
  int read_size;
  Status s = GetClient()->Read(fd, off, size, buf, &read_size);
  return LogErrorAndReturn(s, "read", path, read_size);
}

static
int Write(const char *path, const char *buf,
          size_t size, off_t off, struct fuse_file_info *file) {
  int fd = (int) file->fh;
  Status s = GetClient()->Write(fd, off, size, buf);
  return LogErrorAndReturn(s, "write", path, size);
}

static
int OpenDir(const char *path, struct fuse_file_info *file) {
  return 0; // Do Nothing
}

static
int ReadDir(const char *path, void *handle,
            fuse_fill_dir_t filler, off_t off, struct fuse_file_info *file) {
  std::string p = path;
  std::vector<std::string> results;
  Status s = GetClient()->Readdir(p, &results);

  if (s.ok()) {
    std::vector<std::string>::iterator iter = results.begin();
    while (iter != results.end()) {
      if (filler(handle, iter->c_str(), NULL, 0) != 0) {
        return -ENOMEM;
      }
      iter++;
    }
  }

  return LogErrorAndReturn(s, "readdir", path);
}

static
int FsyncDir(const char *path, int mode, struct fuse_file_info *file) {
  return 0; // Do Nothing
}

static
int ReleaseDir(const char *path, struct fuse_file_info *file) {
  return 0; // Do Nothing
}

//////////////////////////////////////////////////////////////////////////////////
// OPERATION MAPPING
//

static
void InitOpMapping(struct fuse_operations *opers) {
  opers->mkdir       =     Mkdir;
  opers->mknod       =     Mknod;
  opers->chmod       =     Chmod;
  opers->chown       =     Chown;
  opers->utimens     =     Utimens;
  opers->rmdir       =     Rmdir;
  opers->unlink      =     Unlink;
  opers->getattr     =     GetAttr;
  opers->open        =     Open;
  opers->flush       =     Flush;
  opers->fsync       =     Fsync;
  opers->release     =     Release;
  opers->read        =     Read;
  opers->write       =     Write;
  opers->opendir     =     OpenDir;
  opers->readdir     =     ReadDir;
  opers->fsyncdir    =     FsyncDir;
  opers->releasedir  =     ReleaseDir;
}

//////////////////////////////////////////////////////////////////////////////////
// MAIN
//

using google::SetUsageMessage;
using google::ParseCommandLineFlags;

int main(int argc, char* argv[]) {
  SetUsageMessage("IndexFS FUSE");
  ParseCommandLineFlags(&argc, &argv, true);

  struct fuse_operations opers;
  memset(&opers, 0, sizeof(opers));
  InitOpMapping(&opers);

  IDXClientManager* manager = new IDXClientManager();
  int ret = fuse_main(argc, argv, &opers, manager);
  delete manager;

  return ret;
}
