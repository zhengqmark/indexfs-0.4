// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "env/obj_io.h"
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

namespace leveldb {
namespace {

class PosixIO: public virtual IO {
 private:
  Env* posix_env_;

 public:
  virtual ~PosixIO() {
    fprintf(stderr, "Destroying global POSIX IO instance\n");
    abort();
  }

  PosixIO()
    : posix_env_(Env::Default()) {
  }

  bool Exists(const std::string& oname) {
    return posix_env_->FileExists(oname);
  }

  Status PutObject(const std::string& oname, const Slice& data) {
    return Status::NotSupported(Slice());
  }

  Status DeleteObject(const std::string& oname) {
    return posix_env_->DeleteFile(oname);
  }

  Status CopyObject(const std::string& src,
      const std::string& target) {
    return posix_env_->CopyFile(src, target);
  }

  Status NewWritableObject(const std::string& oname,
      WritableFile** result) {
    return posix_env_->NewWritableFile(oname, result);
  }

  Status NewSequentialObject(const std::string& oname,
      SequentialFile** result) {
    return posix_env_->NewSequentialFile(oname, result);
  }

  Status NewRandomAccessObject(const std::string& oname,
      RandomAccessFile** result) {
    return posix_env_->NewRandomAccessFile(oname, result);
  }

  Status GetObjectSize(const std::string& oname, uint64_t* size) {
    return posix_env_->GetFileSize(oname, size);
  }
};

} // namespace

static pthread_once_t once = PTHREAD_ONCE_INIT;
static IO* default_io = NULL;
static void InitDefaultIO() { default_io = new PosixIO; }

IO* IO::PosixIO() {
  pthread_once(&once, InitDefaultIO);
  return default_io;
}

} // namespace leveldb
