// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

#include <sstream>
#include <sys/stat.h>
#include "io_client.h"

namespace indexfs { namespace mpi {

DEFINE_string(mount_point, "/tmp/localfs", "FS mount point (localfs only)");

namespace {

static mode_t FULL_PERMS = (S_IRWXU | S_IRWXG | S_IRWXO);

static
Status CreateErrorStatus(const char* msg, Path &path) {
  std::stringstream ss;
  ss << msg << ": " << path << ", " << strerror(errno);
  return Status::IOError(ss.str());
}

static
Status CreateDirIfMissing(const char* path) {
  if (access(path, F_OK) == 0) {
    return Status::OK();
  }
  if (mkdir(path, FULL_PERMS) == 0) {
    return Status::OK();
  } else if (errno != EEXIST) {
    return CreateErrorStatus(
       "cannot make mount point", FLAGS_mount_point);
  }
  return Status::OK();
}

// An IO client implementation that uses local FS as its backend file system.
// Here we use POSIX API to access the local FS. In the future, we might support
// using MPI_IO API to access the local FS.
//
class LocalFSClient: public IOClient {
 public:

  LocalFSClient()
    : IOClient(), mp_size_(0), buffer_() {
  }

  virtual ~LocalFSClient() {
  }

  virtual Status Init() {
    const char* mp = FLAGS_mount_point.c_str();
    Status s = CreateDirIfMissing(mp);
    if (!s.ok()) {
      return s;
    }
    buffer_.reserve(256);
    buffer_.append(FLAGS_mount_point);
    mp_size_ = buffer_.size();
    return Status::OK();
  }

  virtual Status Dispose() {
    return Status::OK();
  }

  virtual Status NewFile(Path &path);
  virtual Status MakeDirectory(Path &path);
  virtual Status MakeDirectories(Path &path);
  virtual Status SyncDirectory(Path &path);
  virtual Status ResetMode(Path &path);
  virtual Status Rename(Path &src, Path &des);
  virtual Status GetAttr(Path &path);
  virtual Status ListDirectory(Path &path);
  virtual Status Remove(Path &path);

 protected:
  size_t mp_size_; // Length of the mount point path
  std::string buffer_; // A buffered path prefixed with the mount point

 private:
  // No copying allowed
  LocalFSClient(const LocalFSClient&);
  LocalFSClient& operator=(const LocalFSClient&);
};

Status LocalFSClient::NewFile
  (Path &path) {
  buffer_.resize(mp_size_);
  buffer_.append(path);
  const char* filename = buffer_.c_str();
# ifndef NDEBUG
  if (FLAGS_print_ops) {
    printf("mknod %s ... ", filename);
  }
# endif
  Status s = mknod(filename, FULL_PERMS, S_IFREG) != 0
    ? CreateErrorStatus("cannot create file", buffer_)
    : Status::OK();
# ifndef NDEBUG
  if (FLAGS_print_ops) {
    printf("%s\n", s.ToString().c_str());
  }
# endif
  return s;
}

Status LocalFSClient::MakeDirectory
  (Path &path) {
  buffer_.resize(mp_size_);
  buffer_.append(path);
  const char* dirname = buffer_.c_str();
# ifndef NDEBUG
  if (FLAGS_print_ops) {
    printf("mkdir %s ... ", dirname);
  }
# endif
  Status s = mkdir(dirname, FULL_PERMS) != 0
    ? CreateErrorStatus("cannot create directory", buffer_)
    : Status::OK();
# ifndef NDEBUG
  if (FLAGS_print_ops) {
    printf("%s\n", s.ToString().c_str());
  }
# endif
  return s;
}

Status LocalFSClient::MakeDirectories
  (Path &path) {
# ifndef NDEBUG
  if (FLAGS_print_ops) {
    printf("mkdirs %s ... OK\n", path.c_str());
  }
# endif
  return Status::OK(); // Not implemented
}

Status LocalFSClient::SyncDirectory
  (Path &path) {
  return Status::OK(); // Not implemented
}

Status LocalFSClient::ResetMode
  (Path &path) {
# ifndef NDEBUG
  if (FLAGS_print_ops) {
    printf("chmod %s ...OK\n", path.c_str());
  }
# endif
  return Status::OK(); // Not implemented
}

Status LocalFSClient::Rename
  (Path &src, Path &des) {
  return Status::OK(); // Not implemented
}

Status LocalFSClient::GetAttr
  (Path &path) {
# ifndef NDEBUG
  if (FLAGS_print_ops) {
    printf("getattr %s ...OK\n", path.c_str());
  }
# endif
  return Status::OK(); // Not implemented
}

Status LocalFSClient::Remove
  (Path &path) {
  return Status::OK(); // Not implemented
}

Status LocalFSClient::ListDirectory
  (Path &path) {
  return Status::OK(); // Not implemented
}

} /* anonymous namepsace */

IOClient* IOClient::NewLocalFSClient() {
  return new LocalFSClient();
}

} /* namespace mpi */ } /* namespace indexfs */
