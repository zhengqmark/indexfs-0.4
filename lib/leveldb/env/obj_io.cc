// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "env/obj_io.h"

namespace leveldb {
namespace {

class PrefixedIOWrapper: virtual public IO {
 private:
  IO* io_;
  std::string prefix_;

 public:
  virtual ~PrefixedIOWrapper() { }
  PrefixedIOWrapper(IO* io, const std::string& prefix)
    : io_(io), prefix_(prefix) {
  }

  bool Exists(const std::string& oname) {
    return io_->Exists(prefix_ + oname);
  }

  Status GetObjectSize(const std::string& oname, uint64_t* size) {
    return io_->GetObjectSize(prefix_ + oname, size);
  }

  Status PutObject(const std::string& oname, const Slice& data) {
    return io_->PutObject(prefix_ + oname, data);
  }

  Status DeleteObject(const std::string& oname) {
    return io_->DeleteObject(prefix_ + oname);
  }

  Status NewWritableObject(const std::string& oname,
      WritableFile** object) {
    return io_->NewWritableObject(prefix_ + oname, object);
  }

  Status NewSequentialObject(const std::string& oname,
      SequentialFile** object) {
    return io_->NewSequentialObject(prefix_ + oname, object);
  }

  Status NewRandomAccessObject(const std::string& oname,
      RandomAccessFile** object) {
    return io_->NewRandomAccessObject(prefix_ + oname, object);
  }

  Status CopyObject(const std::string& src, const std::string& target) {
    return io_->CopyObject(prefix_ + src, prefix_ + target);
  }
};

} // namespace

IO* IO::PrefixedIO(IO* io, const std::string& prefix) {
  return new PrefixedIOWrapper(io, prefix);
}

} // namespace leveldb
