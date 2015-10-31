// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "metadb/dbtypes.h"
#include "common/unit_test.h"

namespace indexfs { namespace mdb { namespace test {

// -------------------------------------------------------------
// Tests on Meta DB Keys
// -------------------------------------------------------------

struct MDBKeyTest {
};

TEST(MDBKeyTest, Consistency) {
  MDBKey key(-1);
  ASSERT_EQ(key.ToSlice().data(), key.data());
  ASSERT_EQ(key.ToSlice().size(), key.size());
  ASSERT_EQ(key.GetKeySize(), key.size());
  ASSERT_EQ(key.GetPrefixSize() + key.GetHashSize(), key.GetKeySize());
}

TEST(MDBKeyTest, KeySize) {
  const size_t hash_size = 8;
  const size_t prefix_size = 8;
  const size_t key_size = prefix_size + hash_size;
  MDBKey key(-1);
  ASSERT_EQ(key.GetHashSize(), hash_size);
  ASSERT_EQ(key.GetPrefixSize(), prefix_size);
  ASSERT_EQ(key.GetKeySize(), key_size);
}

TEST(MDBKeyTest, RootKey) {
  const int64_t parent_id = 0;
  const int16_t partition_id = 0;
  MDBKey key(parent_id, partition_id, "file");
  ASSERT_FALSE(key.IsSpecial());
  ASSERT_FALSE(key.IsSystemPartition());
  ASSERT_EQ(parent_id, key.GetParent());
  ASSERT_EQ(partition_id, key.GetPartition());
}

TEST(MDBKeyTest, NormalKey) {
  const int64_t parent_id = 512;
  const int16_t partition_id = 16;
  MDBKey key(parent_id, partition_id, "file");
  ASSERT_FALSE(key.IsSpecial());
  ASSERT_FALSE(key.IsSystemPartition());
  ASSERT_EQ(parent_id, key.GetParent());
  ASSERT_EQ(partition_id, key.GetPartition());
}

TEST(MDBKeyTest, SpecialKey) {
  const int64_t special_id = -1;
  MDBKey key(special_id);
  ASSERT_TRUE(key.IsSpecial());
  ASSERT_FALSE(key.IsSystemPartition());
  ASSERT_EQ(special_id, key.GetParent());
}

TEST(MDBKeyTest, SystemPartition) {
  const int16_t partition_id = -1;
  MDBKey key(0, partition_id);
  ASSERT_FALSE(key.IsSpecial());
  ASSERT_TRUE(key.IsSystemPartition());
  ASSERT_EQ(partition_id, key.GetPartition());
}

TEST(MDBKeyTest, ParentOrder) {
  const int64_t parent_id = 512;
  MDBKey key1(parent_id, 0, "file");
  MDBKey key2(parent_id + 1, 0, "file");
  ASSERT_TRUE(key1.ToSlice().compare(key2.ToSlice()) < 0);
}

TEST(MDBKeyTest, PartitionOrder) {
  const int64_t parent_id = 512;
  const int64_t partition_id = 2;
  MDBKey key1(parent_id, partition_id, "file");
  MDBKey key2(parent_id, partition_id + 1, "file");
  ASSERT_TRUE(key1.ToSlice().compare(key2.ToSlice()) < 0);
}

TEST(MDBKeyTest, Hash) {
  MDBKey key1(512, 0, "file1");
  MDBKey key2(512, 0, "file2");
  ASSERT_TRUE(key1.ToSlice().compare(key2.ToSlice()) != 0);
}

TEST(MDBKeyTest, EmptyHash) {
  char zero[1024]; // more than enough
  memset(zero, 0, sizeof(zero));
  MDBKey key1(-1);
  MDBKey key2(512, -1);
  MDBKey key3(512, 0, "");
  ASSERT_EQ(memcmp(key1.GetNameHash(), zero, key1.GetHashSize()), 0);
  ASSERT_EQ(memcmp(key2.GetNameHash(), zero, key2.GetHashSize()), 0);
  ASSERT_EQ(memcmp(key3.GetNameHash(), zero, key3.GetHashSize()), 0);
}

TEST(MDBKeyTest, Reinterpret) {
  const int64_t parent_id = 512;
  const int16_t partition_id = 16;
  MDBKey key1(parent_id, partition_id, "file");

  const std::string buffer(key1.data(), key1.size());
  const MDBKey* key2 = reinterpret_cast<const MDBKey*>(buffer.data());

  ASSERT_EQ(key2->GetParent(), parent_id);
  ASSERT_EQ(key2->GetPartition(), partition_id);
  ASSERT_EQ(key2->GetHashSize(), key1.GetHashSize());
  ASSERT_EQ(memcmp(key2->GetNameHash(), key1.GetNameHash(), key1.GetHashSize()), 0);
}

TEST(MDBKeyTest, Override) {
  const int64_t parent_id = 512;
  const int16_t old_partition_id = 0;
  const int16_t new_partition_id = 1;
  MDBKey key1(parent_id, old_partition_id, "file1");
  ASSERT_EQ(key1.GetParent(), parent_id);
  ASSERT_EQ(key1.GetPartition(), old_partition_id);

  MDBKey key2(parent_id, new_partition_id);
  memcpy(const_cast<char*>(key1.data()), key2.data(), key2.GetPrefixSize());
  ASSERT_EQ(key1.GetParent(), parent_id);
  ASSERT_EQ(key1.GetPartition(), new_partition_id);
}

// -------------------------------------------------------------
// Tests on Meta DB Value
// -------------------------------------------------------------

struct MDBValueTest {

  std::string name_;
  std::string path_;
  std::string data_;

  size_t CalculateValueSize();
};

size_t MDBValueTest::CalculateValueSize() {
  size_t size;
  size = sizeof(FileStat);
  size += VarintLength(name_.length()) + name_.length() + 1;
  size += VarintLength(path_.length()) + path_.length() + 1;
  size += VarintLength(data_.length()) + data_.length() + 1;
  return size;
}

TEST(MDBValueTest, Slice) {
  MDBValue val;
  ASSERT_EQ(val.ToSlice().data(), val.data());
  ASSERT_EQ(val.ToSlice().size(), val.size());
}

TEST(MDBValueTest, InodeNo) {
  MDBValue val;
  const int64_t inode_no = 12345;
  val->SetInodeNo(inode_no);
  ASSERT_EQ(val->InodeNo(), inode_no);
}

TEST(MDBValueTest, FileMode) {
  MDBValue val;
  const int32_t file_mode = 12;
  val->SetFileMode(file_mode);
  ASSERT_EQ(val->FileMode(), file_mode);
}

TEST(MDBValueTest, ZerothServer) {
  MDBValue val;
  const int16_t zeroth_server = 23;
  val->SetZerothServer(zeroth_server);
  ASSERT_EQ(val->ZerothServer(), zeroth_server);
}

TEST(MDBValueTest, EmptyValue) {
  MDBValue val;
  MDBValueRef ref(val.ToSlice());
  ASSERT_EQ(val.size(), CalculateValueSize());
  ASSERT_EQ(val.GetName().size(), 0);
  ASSERT_EQ(ref.GetName().size(), 0);
  ASSERT_EQ(val.GetStoragePath().size(), 0);
  ASSERT_EQ(ref.GetStoragePath().size(), 0);
  ASSERT_EQ(val.GetEmbeddedData().size(), 0);
  ASSERT_EQ(ref.GetEmbeddedData().size(), 0);
}

TEST(MDBValueTest, FileName) {
  name_ = "file";
  MDBValue val(name_);
  MDBValueRef ref(val.ToSlice());
  ASSERT_EQ(val.size(), CalculateValueSize());
  ASSERT_EQ(val.GetName().compare(name_), 0);
  ASSERT_EQ(ref.GetName().compare(name_), 0);
}

TEST(MDBValueTest, StoragePath) {
  path_ = "hdfs://localhost:8020/file";
  MDBValue val("", path_);
  MDBValueRef ref(val.ToSlice());
  ASSERT_EQ(val.size(), CalculateValueSize());
  ASSERT_EQ(val.GetStoragePath().compare(path_), 0);
  ASSERT_EQ(ref.GetStoragePath().compare(path_), 0);
}

TEST(MDBValueTest, EmbeddedData) {
  data_ = "Hello World!";
  MDBValue val("", "", data_);
  MDBValueRef ref(val.ToSlice());
  ASSERT_EQ(val.size(), CalculateValueSize());
  ASSERT_EQ(val.GetEmbeddedData().compare(data_), 0);
  ASSERT_EQ(ref.GetEmbeddedData().compare(data_), 0);
}

TEST(MDBValueTest, Unmarshall) {
  const int64_t inode_no = 12345;
  name_ = "file";
  path_ = "hdfs://localhost:8020/file";
  data_ = "Hello World!";
  MDBValue val1(name_, path_, data_);

  val1->SetInodeNo(inode_no);
  const std::string* buffer = new std::string(val1.data(), val1.size());

  MDBValueRef ref2(buffer->data(), buffer->length());
  ASSERT_EQ(ref2->InodeNo(), inode_no);
  ASSERT_EQ(ref2.GetName().compare(name_), 0);
  ASSERT_EQ(ref2.GetStoragePath().compare(path_), 0);
  ASSERT_EQ(ref2.GetEmbeddedData().compare(data_), 0);
  ASSERT_EQ(ref2.GetFileStat()->InodeNo(), inode_no);

  MDBValue val2(buffer->data(), buffer->size());
  delete buffer;

  ASSERT_EQ(val2.ToSlice().compare(val1.ToSlice()), 0);
  ASSERT_EQ(val2->InodeNo(), inode_no);
  ASSERT_EQ(val2.GetName().compare(name_), 0);
  ASSERT_EQ(val2.GetStoragePath().compare(path_), 0);
  ASSERT_EQ(val2.GetEmbeddedData().compare(data_), 0);
  ASSERT_EQ(val2.GetFileStat()->InodeNo(), inode_no);
}

static const char* kData = "Hello World!";
static const char* kUpdates = "ABCDE";

TEST(MDBValueTest, UpdateDataBase) {
  const int64_t inode_no = 12345;
  name_ = "file";
  path_ = "hdfs://localhost:8020/file";
  data_ = "Hello World!";
  MDBValue old_val(name_, path_, data_);
  old_val->SetInodeNo(inode_no);
  MDBValue new_val(MDBValueRef(old_val.ToSlice()), 0, 0, "");
  ASSERT_EQ(new_val.size(), CalculateValueSize());
  ASSERT_EQ(new_val->InodeNo(), old_val->InodeNo());
  ASSERT_EQ(new_val.GetName().compare(old_val.GetName()), 0);
  ASSERT_EQ(new_val.GetStoragePath().compare(old_val.GetStoragePath()), 0);
  ASSERT_EQ(new_val.GetEmbeddedData().compare(old_val.GetEmbeddedData()), 0);
}

TEST(MDBValueTest, UpdateDataInPlace1) {
  data_ = "ABCDE World!";
  MDBValue old_val(name_, path_, kData);
  MDBValue new_val(MDBValueRef(old_val.ToSlice()),
                   0,
                   strlen(kUpdates),
                   kUpdates);
  ASSERT_EQ(new_val.size(), CalculateValueSize());
  ASSERT_EQ(new_val.GetEmbeddedData().compare(data_), 0);
}

TEST(MDBValueTest, UpdateDataInPlace2) {
  data_ = "Hello ABCDE!";
  MDBValue old_val(name_, path_, kData);
  MDBValue new_val(MDBValueRef(old_val.ToSlice()),
                   strlen("Hello "),
                   strlen(kUpdates),
                   kUpdates);
  ASSERT_EQ(new_val.size(), CalculateValueSize());
  ASSERT_EQ(new_val.GetEmbeddedData().compare(data_), 0);
}

TEST(MDBValueTest, UpdateDataOverlap) {
  data_ = "Hello WorldABCDE";
  MDBValue old_val(name_, path_, kData);
  MDBValue new_val(MDBValueRef(old_val.ToSlice()),
                   strlen(kData) - 1,
                   strlen(kUpdates),
                   kUpdates);
  ASSERT_EQ(new_val.size(), CalculateValueSize());
  ASSERT_EQ(new_val.GetEmbeddedData().compare(data_), 0);
}

TEST(MDBValueTest, UpdateDataNonOverlap1) {
  data_ = "Hello World!ABCDE";
  MDBValue old_val(name_, path_, kData);
  MDBValue new_val(MDBValueRef(old_val.ToSlice()),
                   strlen(kData),
                   strlen(kUpdates),
                   kUpdates);
  ASSERT_EQ(new_val.size(), CalculateValueSize());
  ASSERT_EQ(new_val.GetEmbeddedData().compare(data_), 0);
}

TEST(MDBValueTest, UpdateDataNonOverlap2) {
  data_.append(kData);
  data_.append(std::string(3, '\0'));
  data_.append(kUpdates);
  MDBValue old_val(name_, path_, kData);
  MDBValue new_val(MDBValueRef(old_val.ToSlice()),
                   strlen(kData) + 3,
                   strlen(kUpdates),
                   kUpdates);
  ASSERT_EQ(new_val.size(), CalculateValueSize());
  ASSERT_EQ(new_val.GetEmbeddedData().compare(data_), 0);
}

} /* namespace test */
} /* namespace mdb*/
} /* namespace indexfs */


// Test Driver
using namespace indexfs::test;
int main(int argc, char* argv[]) {
  return RunAllTests();
}
