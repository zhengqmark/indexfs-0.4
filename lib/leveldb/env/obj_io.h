// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_OBJ_IO_H_
#define STORAGE_OBJ_IO_H_

#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

class IO {
 public:
  static IO* PosixIO();
  static IO* PrefixedIO(IO* io, const std::string& prefix);
#ifdef RADOS
  static IO* RadosIO(const char* conf_path, const char* pool_name);
#endif
  virtual ~IO() { }
  virtual bool Exists(const std::string& oname) = 0;
  virtual Status PutObject(const std::string& oname, const Slice& data) = 0;
  virtual Status DeleteObject(const std::string& oname) = 0;
  virtual Status CopyObject(const std::string& src,
      const std::string& target) = 0;
  virtual Status NewWritableObject(const std::string& oname,
      WritableFile** object) = 0;
  virtual Status NewSequentialObject(const std::string& oname,
      SequentialFile** object) = 0;
  virtual Status NewRandomAccessObject(const std::string& oname,
      RandomAccessFile** object) = 0;
  virtual Status GetObjectSize(const std::string& oname, uint64_t* size) = 0;
};

} // namespace leveldb

#endif /* STORAGE_OBJ_IO_H_ */
