// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_METADB_TYPES_H_
#define _INDEXFS_METADB_TYPES_H_

#include <string.h>
#include <endian.h>
#include <stdint.h>

#include "metadb/fstat.h"
#include "common/gigaidx.h"
#include "common/common.h"
#include "common/logging.h"
#include "common/murmurhash3.h"

namespace indexfs {

// IndexFS features an unique metadata representation where each file system object
// (such as file, directory, as well as other special entities) is represented by a
// key-value pair stored in an underlying key-value store (such as LevelDB) using
// log-structured and indexed data structures (SSTables).
//
namespace mdb {

class MDBKey;
class MDBValue;
class MDBValueRef;

// In IndexFS, each file system object is represented by a key-value pair. The key
// is able to uniquely identify an object inside a global file system namespace.
//
// A typical key consists of the following three fields:
//   1) parent_id, 2) partition_id, 3) name_hash.
//
// Special keys do not have a partition_id.
//
// Since the size of the key will have significant effects on overall system
// performance, keys are designed to be as small as possible. Currently, each key
// consists of a 6-byte parent id, a 2-byte partition id, and an 8-byte name hash.
//
struct MDBKey {

  int64_t GetParent() const; // Returns the parent id

  int16_t GetPartition() const; // Returns the partition id

  // Special key constructor.
  // Creates a special key featuring a negative id.
  //
  MDBKey(int64_t special_id) {
    DLOG_ASSERT(special_id < 0);
    InitSpecial(special_id);
    memset(GetNameHash(), 0, kHashSize);
  }

  // Normal key constructor.
  // Creates an ordinary key consisting of a positive parent id,
  // a partition id and an empty hash.
  //
  MDBKey(int64_t parent_id, int16_t partition_id) {
    DLOG_ASSERT(parent_id >= 0);
    Init(parent_id, partition_id);
    memset(GetNameHash(), 0, kHashSize);
  }

  // Normal key constructor.
  // Creates an ordinary key consisting of a positive parent id,
  // a partition id and a hash deduced from the specified file name.
  //
  MDBKey(int64_t parent_id,
         int16_t partition_id,
         const std::string &name) {
    DLOG_ASSERT(parent_id >= 0);
    Init(parent_id, partition_id);
    MurmurHash3_x64_128(name.data(), name.length(), 0, GetNameHash());
  }

  // Returns the overall size of the key, which currently is a
  // fixed value.
  //
  inline size_t GetKeySize() const {
    return kTotalSize;
  }

  // Returns the size of the name hash inside the key, which currently is
  // a fixed value.
  //
  inline size_t GetHashSize() const {
    return kHashSize;
  }

  // Returns the size of the prefix (directory id + partition id) inside
  // the key, which currently is a fixed value.
  //
  inline size_t GetPrefixSize() const {
    return kPrefixSize;
  }

  // Returns true iff the key has a special id.
  //
  inline bool IsSpecial() const {
    return (rep_[0] & 0x80) == 0x80;
  }

  // Returns true iff the key has a special partition id.
  //
  inline bool IsSystemPartition() const {
    return (!IsSpecial()) && (rep_[6] & 0x80) == 0x80;
  }

  // Returns a pointer to the name hash inside the key.
  //
  inline char* GetNameHash() {
    return reinterpret_cast<char*>(rep_ + kPrefixSize);
  }

  // Returns a constant pointer to the name hash inside the key.
  //
  inline const char* GetNameHash() const {
    return reinterpret_cast<const char*>(rep_ + kPrefixSize);
  }

  // Returns a Slice object backed by the raw memory of the key.
  //
  inline Slice ToSlice() {
    return Slice(reinterpret_cast<char*>(rep_), kTotalSize);
  }

  // Returns the length (in bytes) of the raw key representation.
  //
  size_t size() const { return kTotalSize; }

  // Returns a pointer to the beginning of the raw key representation.
  //
  const char* data() const { return reinterpret_cast<const char*>(rep_); }

 private:

  enum {
    kPrefixSize = 8, kHashSize = 8, kTotalSize = kPrefixSize + kHashSize,
  };

  // --------------------------------------------------
  // Internal Representation (in bit)
  // --------------------------------------------------
  // [0, 0] -> type bit: 0=normal_key, 1=special_key
  // [1, 47] -> parent_id (6 bytes)
  // [48, 48] -> type bit: 0=normal_partition, 1=system_partition
  // [49, 63] -> partition_id (2 bytes)
  // [64, 191] -> name_hash (as many as 16 bytes)
  // --------------------------------------------------
  unsigned char rep_[24]; // Capable of holding a 16-byte name hash.

  void Init(int64_t parent_id, int16_t partition_id); // Initializes a normal key.

  void InitSpecial(int64_t special_id); // Initializes a special key with a special id.

};

// Retrieves the parent id from the key representation.
//
inline int64_t MDBKey::GetParent() const {
  int64_t result;
  memcpy(&result, rep_, sizeof(result));
  result = be64toh(result);
  return result >= 0 ? result >> 16 : result;
}

// Retrieves the partition id from the key representation.
//
inline int16_t MDBKey::GetPartition() const {
  int64_t result;
  memcpy(&result, rep_, sizeof(result));
  result = be64toh(result);
  return result >= 0 ? static_cast<int16_t>(result & 0xFFFF) : 0;
}

// Updates the key representation with the given information.
//
inline void MDBKey::Init(int64_t parent_id, int16_t partition_id) {
  int64_t id = (parent_id << 16) | (partition_id & 0xFFFF);
  id = htobe64(id);
  memcpy(rep_, &id, sizeof(id));
}

// Updates the key representation with the given special id.
//
inline void MDBKey::InitSpecial(int64_t special_id) {
  int64_t id = htobe64(special_id);
  memcpy(rep_, &id, sizeof(id));
}

// In IndexFS, each file system object is represented by a key-value pair. The value
// stores various file system attributes related to the object, such as its INODE number,
// file name, file size, permission bits, owner identity, zeroth server (for directories),
// and file contents (for small files).
//
// Since the size of the value will have significant effects on overall system
// performance, values are designed to be as small as possible.
// Now, each value consists of a fixed 64 bytes header, 3 length fields each sizing from 1
// to 4 bytes, and 3 raw data fields for file name, storage path, and file data respectively.
//
struct MDBValue {

  void SetFileStat(const FileStat &file_stat); // Reset file attributes

  FileStat* operator->() { return GetFileStat(); }

  MDBValue(const char* buffer, size_t length) :
    name_(NULL), path_(NULL), data_(NULL), name_size_(0), path_size_(0), data_size_(0),
    size_(0), rep_(NULL) {
    Slice raw_bytes(buffer, length);
    Unmarshall(raw_bytes);
  }

  MDBValue(const std::string &name=std::string(),
           const std::string &path=std::string(),
           const std::string &data=std::string()) :
    name_(NULL), path_(NULL), data_(NULL), name_size_(0), path_size_(0), data_size_(0),
    size_(0), rep_(NULL) {
    Init(name, path, data);
  }

  ~MDBValue();

  inline Slice GetName() {
    return Slice(name_, name_size_);
  }

  inline Slice GetStoragePath() {
    return Slice(path_, path_size_);
  }

  inline Slice GetEmbeddedData() {
    return Slice(data_, data_size_);
  }

  inline FileStat* GetFileStat() {
    return reinterpret_cast<FileStat*>(rep_);
  }

  inline Slice ToSlice() {
    return Slice(reinterpret_cast<char*>(rep_), size_);
  }

  // Create a new MDB value based on an exiting value but with partially updated embedded data
  //
  MDBValue(const MDBValueRef &base, size_t offset, size_t size, const char* data);

  // Returns the length (in bytes) of the value.
  //
  size_t size() const { return size_; }

  // Returns a pointer to the beginning of the value.
  //
  const char* data() const { return reinterpret_cast<const char*>(rep_); }

 private:

  enum {
    kFileStatSize = sizeof(FileStat), /* 64 bytes */
  };

  // Transient helper fields
  char* name_;
  char* path_;
  char* data_;

  uint32_t name_size_;
  uint32_t path_size_;
  uint32_t data_size_;

  // --------------------------------------------------
  // Internal Representation
  // --------------------------------------------------
  // FileStat | NameLen | PathLen | DataLen | ..DATA..
  // --------------------------------------------------
  size_t size_;
  unsigned char* rep_;

  void Init(const Slice &name=Slice(),
            const Slice &path=Slice(),
            const Slice &data=Slice());

  void Unmarshall(const Slice &raw_bytes);

  // No copying allowed
  MDBValue(const MDBValue&);
  MDBValue& operator=(const MDBValue&);
};

inline MDBValue::~MDBValue() {
  if (rep_ != NULL) {
    delete rep_;
  }
}

inline void MDBValue::SetFileStat(const FileStat &file_stat) {
  memcpy(rep_, &file_stat, kFileStatSize);
}

// A read-only reference to a piece of memory to be interpreted as an MDBValue
// object. This helper structure is introduced to reduce unnecessary memory copy.
//
struct MDBValueRef {

  ~MDBValueRef() { /* empty */ }

  const FileStat* operator->() const { return GetFileStat(); }

  MDBValueRef(const Slice& slice) :
    name_(NULL), path_(NULL), data_(NULL), name_size_(0), path_size_(0), data_size_(0),
    size_(slice.size()), rep_(slice.data()) {
    Unmarshall();
  }

  MDBValueRef(const char* rep, size_t size) :
    name_(NULL), path_(NULL), data_(NULL), name_size_(0), path_size_(0), data_size_(0),
    size_(size), rep_(rep) {
    Unmarshall();
  }

  inline Slice GetName() const {
    return Slice(name_, name_size_);
  }

  inline Slice GetStoragePath() const {
    return Slice(path_, path_size_);
  }

  inline Slice GetEmbeddedData() const {
    return Slice(data_, data_size_);
  }

  inline const FileStat* GetFileStat() const {
    return reinterpret_cast<const FileStat*>(rep_);
  }

 private:

  void Unmarshall(); // re-construct in-memory representation

  char* name_;
  char* path_;
  char* data_;

  uint32_t name_size_;
  uint32_t path_size_;
  uint32_t data_size_;

  size_t size_;
  const char* rep_;

  friend class MDBValue;
  // No copying allowed
  MDBValueRef(const MDBValueRef&);
  MDBValueRef& operator=(const MDBValueRef&);
};

} /* namespace mdb */
} /* namespace indexfs */

#endif /* _INDEXFS_METADB_TYPES_H_ */
