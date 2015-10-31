// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "io_client.h"
#include "client/client.h"

#include <sys/stat.h>
#include "common/config.h"
#include "common/logging.h"

namespace indexfs {

namespace mpi {

static const uint16_t BULK_BIT
    = (S_ISVTX); // --------t or --------T
static const uint16_t DEFAULT_FILE_PERMISSION
    = (S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH); // rw-r--r--
static const uint16_t DEFAULT_DIR_PERMISSION
    = (S_IRWXU | S_IRGRP | S_IROTH | S_IXGRP | S_IXOTH); // rwxr-x-r-x

DEFINE_bool(bulk_insert,
    false, "Set to turn on the client-side bulk_insert feature of indexfs");
DEFINE_bool(batch_creates,
    false, "Set to true to enable client-side mknod batching");

namespace {

// An IO Client implementation that uses IndexFS as its backend file system.
// Here we assume that such client will only be initialized once. Use --bulk_insert
// to enable the bulk_insert feature of IndexFS.
//
class IndexFSClient: virtual public IOClient {
 public:

  virtual ~IndexFSClient() {
    delete cli_;
    Logger::Shutdown();
  }

  IndexFSClient(int my_rank, int comm_sz):
      IOClient(),
      my_rank_(my_rank),
      comm_sz_(comm_sz),
      mknod_pending_(false) {
    Logger::Initialize(NULL);
    Config* config = FLAGS_bulk_insert ?
        LoadBatchClientConfig(my_rank_, comm_sz_) :
        LoadClientConfig(my_rank_, comm_sz_);
    cli_ = FLAGS_bulk_insert ?
        ClientFactory::GetBatchClient(config) : ClientFactory::GetClient(config);
  }

  Status Init() {
    return cli_->Init();
  }

  Status Dispose() {
    Status s;
    s = FlushWriteBuffer();
    if (!s.ok()) {
      return s;
    }
    return cli_->Dispose();
  }

  void Noop() {
    cli_->Noop();
  }

  Status FlushMknod() {
    Status s;
    if (FLAGS_batch_creates && mknod_pending_) {
      s = cli_->Mknod_Flush();
      if (s.ok()) {
        mknod_pending_ = false;
      }
    }
    return s;
  }

  Status FlushWriteBuffer() {
    Status s;
    s = FlushMknod();
    if (s.ok()) {
      s = cli_->FlushWriteBuffer();
    }
    return s;
  }

  void PrintMeasurements(FILE* output) {
    cli_->PrintMeasurements(output);
  }

  Status NewFile          (Path &path);
  Status MakeDirectory    (Path &path);
  Status MakeDirectories  (Path &path);
  Status SyncDirectory    (Path &path);
  Status ResetMode        (Path &path);
  Status GetAttr          (Path &path);
  Status ListDirectory    (Path &path);
  Status Remove           (Path &path);

  Status Rename           (Path &source, Path &destination);

 private:
  int my_rank_;
  int comm_sz_;
  Client* cli_;
  bool mknod_pending_;

  // No copying allowed
  IndexFSClient(const IndexFSClient&);
  IndexFSClient& operator=(const IndexFSClient&);
};

Status IndexFSClient::NewFile(Path &path) {
  Status s;
# ifndef NDEBUG
  if (FLAGS_print_ops) {
    printf("mknod %s ... ", path.c_str());
  }
# endif
  s = FLAGS_batch_creates ?
    cli_->Mknod_Buffered(path, DEFAULT_FILE_PERMISSION) :
    cli_->Mknod(path, DEFAULT_FILE_PERMISSION);
  mknod_pending_ = FLAGS_batch_creates;
# ifndef NDEBUG
  if (FLAGS_print_ops) {
    printf("%s\n", s.ToString().c_str());
  }
# endif
  return s;
}

Status IndexFSClient::MakeDirectory(Path &path) {
  Status s;
# ifndef NDEBUG
  if (FLAGS_print_ops) {
    printf("mkdir %s ... ", path.c_str());
  }
# endif
  if (!FLAGS_bulk_insert) {
    s = FLAGS_batch_creates ?
      cli_->Mkdir_Presplit(path, DEFAULT_DIR_PERMISSION) :
      cli_->Mkdir(path, DEFAULT_DIR_PERMISSION);
  } else if (my_rank_ == 0) {
    // TODO Obtain lease from the server
    s = cli_->Mkdir(path, (BULK_BIT | DEFAULT_DIR_PERMISSION));
  } else {
    // TODO Obtain lease from the zeroth client
    s = cli_->Mkdir(path, (BULK_BIT | DEFAULT_DIR_PERMISSION));
  }
# ifndef NDEBUG
  if (FLAGS_print_ops) {
    printf("%s\n", s.ToString().c_str());
  }
# endif
  return s;
}

namespace {
static inline
size_t NextEntry(Path &path, size_t start) {
  size_t pos = path.find('/', start);
  return pos == std::string::npos ? path.size() : pos;
}
}

Status IndexFSClient::MakeDirectories(Path &path) {
  Status s;
# ifndef NDEBUG
  if (FLAGS_print_ops) {
    printf("mkdirs %s ... \n", path.c_str());
  }
# endif
  StatInfo info;
  std::string buffer;
  buffer.reserve(path.size());
  size_t entry = path.rfind('/');
  size_t parent = 0;
  while ((parent = NextEntry(path, parent + 1)) <= entry ) {
    buffer = path.substr(0, parent);
    s = cli_->Getattr(buffer, &info);
    if (s.IsNotFound()) {
#     ifndef NDEBUG
      if (FLAGS_print_ops) {
        printf("\tmkdir %s ... ", buffer.c_str());
      }
#     endif
      s = !FLAGS_bulk_insert ?
        cli_->Mkdir(buffer, DEFAULT_DIR_PERMISSION) :
        Status::NotSupported(Slice());
#     ifndef NDEBUG
      if (FLAGS_print_ops) {
        printf("%s\n", s.ToString().c_str());
      }
#     endif
      if (s.IsAlreadyExists()) {
#       ifndef NDEBUG
        if (FLAGS_print_ops) {
          fprintf(stderr, "[WARN]: dir %s already exists\n", buffer.c_str());
        }
#       endif
      } else if (!s.ok()) return s;
    } else if (!s.ok()) return s;
  }
# ifndef NDEBUG
  if (FLAGS_print_ops) {
    printf("\tmkdir %s ... ", path.c_str());
  }
# endif
  s = !FLAGS_bulk_insert ?
    cli_->Mkdir(path, DEFAULT_DIR_PERMISSION) :
    Status::NotSupported(Slice());
# ifndef NDEBUG
  if (FLAGS_print_ops) {
    printf("%s\n", s.ToString().c_str());
    printf("mkdirs done\n");
  }
# endif
  if (s.IsAlreadyExists()) {
#   ifndef NDEBUG
    if (FLAGS_print_ops) {
      fprintf(stderr, "[WARN]: dir %s already exists\n", buffer.c_str());
    }
#   endif
    return Status::OK();
  }
  return s;
}

Status IndexFSClient::SyncDirectory(Path &path) {
  Status s;
# ifndef NDEBUG
  if (FLAGS_print_ops) {
    printf("fsyncdir %s ... ", path.c_str());
  }
# endif
  s = Status::NotSupported(Slice());
# ifndef NDEBUG
  if (FLAGS_print_ops) {
    printf("%s\n", s.ToString().c_str());
  }
# endif
  return s;
}

Status IndexFSClient::Rename(Path &src, Path &des) {
  Status s;
  s = FlushMknod();
  if (!s.ok()) {
    return s;
  }
# ifndef NDEBUG
  if (FLAGS_print_ops) {
    printf("rename %s -> %s ... ", src.c_str(), des.c_str());
  }
# endif
  s = Status::NotSupported(Slice());
# ifndef NDEBUG
  if (FLAGS_print_ops) {
    printf("%s\n", s.ToString().c_str());
  }
# endif
  return s;
}

Status IndexFSClient::GetAttr(Path &path) {
  Status s;
  s = FlushMknod();
  if (!s.ok()) {
    return s;
  }
# ifndef NDEBUG
  if (FLAGS_print_ops) {
    printf("getattr %s ... ", path.c_str());
  }
# endif
  StatInfo info;
  s = cli_->Getattr(path, &info);
# ifndef NDEBUG
  if (FLAGS_print_ops) {
    printf("%s\n", s.ToString().c_str());
  }
# endif
  return s;
}

Status IndexFSClient::Remove(Path &path) {
  Status s;
  s = FlushMknod();
  if (!s.ok()) {
    return s;
  }
# ifndef NDEBUG
  if (FLAGS_print_ops) {
    printf("remove %s ... ", path.c_str());
  }
# endif
  s = Status::NotSupported(Slice());
# ifndef NDEBUG
  if (FLAGS_print_ops) {
    printf("%s\n", s.ToString().c_str());
  }
# endif
  return s;
}

Status IndexFSClient::ListDirectory(Path &path) {
  Status s;
  s = FlushMknod();
  if (!s.ok()) {
    return s;
  }
# ifndef NDEBUG
  if (FLAGS_print_ops) {
    printf("readdir %s ... ", path.c_str());
  }
# endif
  std::vector<std::string> list;
  s = Status::NotSupported(Slice());
# ifndef NDEBUG
  if (FLAGS_print_ops) {
    printf("%s\n", s.ToString().c_str());
  }
# endif
  return s;
}

Status IndexFSClient::ResetMode(Path &path) {
  Status s;
  s = FlushMknod();
  if (!s.ok()) {
    return s;
  }
# ifndef NDEBUG
  if (FLAGS_print_ops) {
    printf("chmod %s ... ", path.c_str());
  }
# endif
  s = cli_->Chmod(path, (S_IRWXU | S_IRWXG | S_IRWXO), NULL);
# ifndef NDEBUG
  if (FLAGS_print_ops) {
    printf("%s\n", s.ToString().c_str());
  }
# endif
  return s;
}

} /* anonymous namespace */

IOClient* IOClient::NewIndexFSClient(int my_rank, int comm_sz) {
  return new IndexFSClient(my_rank, comm_sz);
}

} /* namespace mpi */
} /* namespace indexfs */
