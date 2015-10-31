// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_MEMBUF_H_
#define STORAGE_LEVELDB_UTIL_MEMBUF_H_

namespace leveldb {

class MemBuffer {

public:
  MemBuffer(size_t write_buffer_size) :
    buffer_size(write_buffer_size), free_buffer_size(write_buffer_size) {
    buffer = new char[write_buffer_size];
  }

  ~MemBuffer() {
    delete [] buffer;
  }

  bool HasEnough(size_t bytes) {
    return free_buffer_size >= bytes;
  }

  Status Append(const Slice& data, size_t &location) {
    if (data.size() > free_buffer_size) {
      return Status::BufferFull("Appending data to a nearly full buffer");
    }
    location = buffer_size - free_buffer_size;
    memcpy(buffer + location, data.data(), data.size());
    free_buffer_size -= data.size();
    return Status::OK();
  }

  Status Get(size_t offset, size_t size, Slice* result, char* scratch) {
    if (offset >= buffer_size)
      return Status::IOError("Exceeding memory buffer size");
    if (size + offset > buffer_size) {
      size = buffer_size - offset;
    }
    memcpy(scratch, buffer+offset, size);
    *result = Slice(scratch, size);
    return Status::OK();
  }

  void Truncate() {
    free_buffer_size = buffer_size;
  }

private:
  char* buffer;
  size_t buffer_size;
  size_t free_buffer_size;

};

} //namespace leveldb

#endif
