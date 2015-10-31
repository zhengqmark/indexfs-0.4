// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "dbtypes.h"

namespace indexfs {

namespace mdb {

// Initializes the internal representation of the value structure using the specified
// file name, underlying storage path, and embedded file data, if they are provided.
// Note that file status is not initialized here.
//
void MDBValue::Init(const Slice &name,
                    const Slice &path,
                    const Slice &data) {

  size_t real_total_size = kFileStatSize;
  size_t estimated_total_size = kFileStatSize;

  name_size_ = static_cast<uint32_t>(name.size());
  path_size_ = static_cast<uint32_t>(path.size());
  data_size_ = static_cast<uint32_t>(data.size());

  estimated_total_size += 5 + name_size_ + 1;
  estimated_total_size += 5 + path_size_ + 1;
  estimated_total_size += 5 + data_size_ + 1;

  char* buffer = new char[estimated_total_size]; // more than enough
  char* start = buffer;
  char* end = buffer + kFileStatSize;

  start = end;
  end = EncodeVarint32(start, name_size_);
  uint32_t namelen_size = end - start;
  start = end;
  end = EncodeVarint32(start, path_size_);
  uint32_t pathlen_size = end - start;
  start = end;
  end = EncodeVarint32(start, data_size_);
  uint32_t datalen_size = end - start;

  real_total_size += namelen_size + name_size_ + 1;
  real_total_size += pathlen_size + path_size_ + 1;
  real_total_size += datalen_size + data_size_ + 1;

  name_ = end;
  strncpy(name_, name.data(), name_size_ + 1);
  path_ = name_ + (name_size_ + 1);
  strncpy(path_, path.data(), path_size_ + 1);
  data_ = path_ + (path_size_ + 1);

  memcpy(data_, data.data(), data_size_);

  data_[data_size_] = 0;
  size_ = real_total_size;
  rep_ = reinterpret_cast<unsigned char*>(buffer); // file status remain uninitialized
  DLOG_ASSERT(rep_[size_ - 1] == 0);
}

// Reconstruct the in-memory representation of an MDBValue.
//
void MDBValue::Unmarshall(const Slice &raw_bytes) {
  size_ = raw_bytes.size();
  rep_ = new unsigned char[size_];
  memcpy(rep_, raw_bytes.data(), size_);

  char* ptr = reinterpret_cast<char*>(rep_ + kFileStatSize);

  ptr = const_cast<char*>(GetVarint32Ptr(ptr, ptr + 5, &name_size_));
  CHECK(ptr != NULL) << "Fail to parse name length from bytes";
  ptr = const_cast<char*>(GetVarint32Ptr(ptr, ptr + 5, &path_size_));
  CHECK(ptr != NULL) << "Fail to parse path length from bytes";
  ptr = const_cast<char*>(GetVarint32Ptr(ptr, ptr + 5, &data_size_));
  CHECK(ptr != NULL) << "Fail to parse data length from bytes";

  name_ = ptr;
  path_ = name_ + name_size_ + 1;
  data_ = path_ + path_size_ + 1;
}

// Reconstruct the in-memory representation of an MDBValue.
//
void MDBValueRef::Unmarshall() {
  const char* ptr = rep_ + sizeof(FileStat);

  ptr = const_cast<char*>(GetVarint32Ptr(ptr, ptr + 5, &name_size_));
  CHECK(ptr != NULL) << "Fail to parse name length from bytes";
  ptr = const_cast<char*>(GetVarint32Ptr(ptr, ptr + 5, &path_size_));
  CHECK(ptr != NULL) << "Fail to parse path length from bytes";
  ptr = const_cast<char*>(GetVarint32Ptr(ptr, ptr + 5, &data_size_));
  CHECK(ptr != NULL) << "Fail to parse data length from bytes";

  name_ = const_cast<char*>(ptr);
  path_ = const_cast<char*>(name_ + name_size_ + 1);
  data_ = const_cast<char*>(path_ + path_size_ + 1);
}

// Create a new MDBValue instance by inserting a new data range into
// an existing MDBValue instance.
//
MDBValue::MDBValue(const MDBValueRef &base, size_t offset, size_t size, const char *data) {
  size_t old_data_size = base.data_size_;
  size_t new_data_size = std::max(old_data_size, offset + size);

  size_t real_total_size = kFileStatSize;
  size_t estimated_total_size = kFileStatSize;

  name_size_ = base.name_size_;
  path_size_ = base.path_size_;
  data_size_ = static_cast<uint32_t>(new_data_size);

  estimated_total_size += 5 + name_size_ + 1;
  estimated_total_size += 5 + path_size_ + 1;
  estimated_total_size += 5 + data_size_ + 1;

  char* buffer = new char[estimated_total_size]; // more than enough
  char* start = buffer;
  char* end = buffer + kFileStatSize;

  memcpy(start, base.rep_, kFileStatSize);

  start = end;
  end = EncodeVarint32(start, name_size_);
  uint32_t namelen_size = end - start;
  start = end;
  end = EncodeVarint32(start, path_size_);
  uint32_t pathlen_size = end - start;
  start = end;
  end = EncodeVarint32(start, data_size_);
  uint32_t datalen_size = end - start;

  real_total_size += namelen_size + name_size_ + 1;
  real_total_size += pathlen_size + path_size_ + 1;
  real_total_size += datalen_size + data_size_ + 1;

  name_ = end;
  strncpy(name_, base.name_, name_size_ + 1);
  path_ = name_ + (name_size_ + 1);
  strncpy(path_, base.path_, path_size_ + 1);
  data_ = path_ + (path_size_ + 1);

  memcpy(data_, base.data_, old_data_size);
  if (offset > old_data_size) {
    memset(data_ + old_data_size, 0, offset - old_data_size);
  }
  memcpy(data_ + offset, data, size);

  data_[data_size_] = 0;
  size_ = real_total_size;
  rep_ = reinterpret_cast<unsigned char*>(buffer);
  DLOG_ASSERT(rep_[size_ - 1] == 0);
}

} /* namespace mdb */
} /* namespace indexfs */
