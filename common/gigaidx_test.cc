// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <sstream>
#include <math.h>
#include <stdio.h>

#include "common/options.h"
#include "common/logging.h"
#include "common/gigaidx.h"
#include "common/unit_test.h"

namespace indexfs { namespace test {

namespace {
static const int kBatchSize = 10000;
// Return the sum of the first 'size' elements in the array.
static
int ArraySum(int *array, int size) {
  int sum = 0;
  for (int i = 0; i < size; ++i) {
    sum += array[i];
  }
  return sum;
}
// Print the first 'size' elements in the array.
static
void PrintArray(int *array, int size) {
  std::stringstream ss;
  for (int i = 0; i < size; ++i) {
    ss << array[i] << ",";
  }
  fprintf(stderr, "%s\n", ss.str().c_str());
}
// Default bitmap configurations that are used in the following test cases.
static const int64_t kDirId = 512;
static const int16_t kZerothServer = 0;
#ifndef IDXFS_EXTRA_SCALE
static const int kMaxRadix = 8;
#else
static const int kMaxRadix = 14;
#endif
static const int kMaxNumBits = 1 << kMaxRadix;
static const int kNumSrvs = kMaxNumBits;
static const int kNumVirSrvs = kMaxNumBits;
}

struct DirIndexTest {
  DirIndex* idx_;
  DirIndexPolicy* policy_;
  virtual ~DirIndexTest() {
    delete idx_;
    delete policy_;
  }
  DirIndexTest() {
    policy_ = DirIndexPolicy::TEST_NewPolicy(kNumSrvs, kNumVirSrvs);
    idx_ = policy_->NewDirIndex(kDirId, kZerothServer);
  }
};

// -------------------------------------------------------------
// Test Cases
// -------------------------------------------------------------

TEST(DirIndexTest, InitState) {
  ASSERT_TRUE(idx_->GetBit(0));
  for (int i = 1; i < kMaxNumBits; i++) {
    ASSERT_TRUE(!idx_->GetBit(i));
  }
  ASSERT_EQ(idx_->FetchBitmapRadix(), 0);
  ASSERT_EQ(kMaxNumBits, DEFAULT_MAX_NUM_SERVERS);
}

TEST(DirIndexTest, Fields) {
  ASSERT_EQ(idx_->FetchDirId(), kDirId);
  ASSERT_EQ(idx_->FetchZerothServer(), kZerothServer);
  ASSERT_EQ(policy_->NumServers(), kNumSrvs);
  ASSERT_EQ(policy_->MaxNumVirtualServers(), kNumVirSrvs);
}

TEST(DirIndexTest, Bits1) {
  int i = 1;
# ifndef IDXFS_EXTRA_SCALE
  int indice[kMaxRadix + 1] = { 0, 1, 2, 4, 8, 16, 32, 64, 128 };
# else
  int indice[kMaxRadix + 1] = { 0, 1, 2, 4, 8, 16, 32, 64, 128,
                                256, 512, 1024, 2048, 4096, 8192 };
# endif
  while (i <= kMaxRadix) {
    idx_->SetBit(indice[i]);
    ASSERT_TRUE(idx_->GetBit(indice[i]));
    ASSERT_EQ(idx_->FetchBitmapRadix(), i);
    i++;
  }
  idx_->TEST_RevertAll();
  ASSERT_EQ(idx_->FetchBitmapRadix(), 0);
}

TEST(DirIndexTest, Bits2) {
  int i = 1;
  ASSERT_TRUE(idx_->GetBit(0));
# ifndef IDXFS_EXTRA_SCALE
  int indice[kMaxRadix + 1] = { 0, 1, 3, 7, 15, 31, 63, 127, 255 };
# else
  int indice[kMaxRadix + 1] = { 0, 1, 3, 7, 15, 31, 63, 127, 255,
                                511, 1023, 2047, 4095, 8191, 16383 };
# endif
  while (i <= kMaxRadix) {
    idx_->SetBit(indice[i]);
    ASSERT_TRUE(idx_->GetBit(indice[i]));
    ASSERT_EQ(idx_->FetchBitmapRadix(), i);
    i++;
  }
  idx_->TEST_RevertAll();
  ASSERT_EQ(idx_->FetchBitmapRadix(), 0);
}

TEST(DirIndexTest, Update1) {
  idx_->SetBit(1);
  idx_->SetBit(2);
  DirIndex* _idx = policy_->NewDirIndex(kDirId, kZerothServer);
  _idx->SetBit(1);
  _idx->SetBit(3);
  idx_->Update(_idx->ToSlice());
  ASSERT_TRUE(idx_->GetBit(0));
  ASSERT_TRUE(idx_->GetBit(1));
  ASSERT_TRUE(idx_->GetBit(2));
  ASSERT_TRUE(idx_->GetBit(3));
  ASSERT_EQ(idx_->FetchBitmapRadix(), 2);
  ASSERT_EQ(idx_->FetchDirId(), kDirId);
  ASSERT_EQ(idx_->FetchZerothServer(), kZerothServer);
  delete _idx;
}

#ifdef IDXFS_EXTRA_SCALE
TEST(DirIndexTest, Update2) {
  idx_->SetBit(1);
  idx_->SetBit(2);
  idx_->SetBit(4);
  idx_->SetBit(8);
  idx_->SetBit(16);
  idx_->SetBit(32);
  idx_->SetBit(64);
  idx_->SetBit(128);
  DirIndex* _idx = policy_->NewDirIndex(kDirId, kZerothServer);
  _idx->SetBit(1);
  _idx->SetBit(3);
  _idx->SetBit(7);
  _idx->SetBit(15);
  _idx->SetBit(31);
  _idx->SetBit(63);
  _idx->SetBit(127);
  _idx->SetBit(255);
  _idx->SetBit(511);
  idx_->Update(_idx->ToSlice());
  ASSERT_TRUE(idx_->GetBit(0));
  ASSERT_TRUE(idx_->GetBit(1));
  ASSERT_TRUE(idx_->GetBit(3));
  ASSERT_TRUE(idx_->GetBit(7));
  ASSERT_TRUE(idx_->GetBit(15));
  ASSERT_TRUE(idx_->GetBit(31));
  ASSERT_TRUE(idx_->GetBit(63));
  ASSERT_TRUE(idx_->GetBit(127));
  ASSERT_TRUE(idx_->GetBit(255));
  ASSERT_TRUE(idx_->GetBit(511));
  ASSERT_TRUE(idx_->GetBit(2));
  ASSERT_TRUE(idx_->GetBit(4));
  ASSERT_TRUE(idx_->GetBit(8));
  ASSERT_TRUE(idx_->GetBit(16));
  ASSERT_TRUE(idx_->GetBit(32));
  ASSERT_TRUE(idx_->GetBit(64));
  ASSERT_TRUE(idx_->GetBit(128));
  ASSERT_EQ(idx_->FetchBitmapRadix(), 9);
  ASSERT_EQ(idx_->FetchDirId(), kDirId);
  ASSERT_EQ(idx_->FetchZerothServer(), kZerothServer);
  delete _idx;
}
#endif

TEST(DirIndexTest, Recover1) {
  DirIndex* _idx = policy_->RecoverDirIndex(idx_->ToSlice());
  ASSERT_TRUE(_idx->GetBit(0));
  ASSERT_EQ(_idx->FetchBitmapRadix(), 0);
  ASSERT_EQ(_idx->FetchDirId(), kDirId);
  ASSERT_EQ(_idx->FetchZerothServer(), kZerothServer);
  delete _idx;
}

TEST(DirIndexTest, Recover2) {
  idx_->SetBit(1);
  idx_->SetBit(3);
  DirIndex* _idx = policy_->RecoverDirIndex(idx_->ToSlice());
  ASSERT_TRUE(_idx->GetBit(1));
  ASSERT_TRUE(_idx->GetBit(3));
  ASSERT_EQ(_idx->FetchBitmapRadix(), 2);
  ASSERT_EQ(_idx->FetchDirId(), kDirId);
  ASSERT_EQ(_idx->FetchZerothServer(), kZerothServer);
  delete _idx;
}

#ifdef IDXFS_EXTRA_SCALE
TEST(DirIndexTest, Recover3) {
  idx_->SetBit(1);
  idx_->SetBit(3);
  idx_->SetBit(7);
  idx_->SetBit(15);
  idx_->SetBit(31);
  idx_->SetBit(63);
  idx_->SetBit(127);
  idx_->SetBit(255);
  idx_->SetBit(511);
  DirIndex* _idx = policy_->RecoverDirIndex(idx_->ToSlice());
  ASSERT_TRUE(_idx->GetBit(1));
  ASSERT_TRUE(_idx->GetBit(3));
  ASSERT_TRUE(_idx->GetBit(7));
  ASSERT_TRUE(_idx->GetBit(15));
  ASSERT_TRUE(_idx->GetBit(31));
  ASSERT_TRUE(_idx->GetBit(63));
  ASSERT_TRUE(_idx->GetBit(127));
  ASSERT_TRUE(_idx->GetBit(255));
  ASSERT_TRUE(_idx->GetBit(511));
  ASSERT_EQ(_idx->FetchBitmapRadix(), 9);
  ASSERT_EQ(_idx->FetchDirId(), kDirId);
  ASSERT_EQ(_idx->FetchZerothServer(), kZerothServer);
  delete _idx;
}
#endif

TEST(DirIndexTest, Hash) {
  char hash1[128];
  ASSERT_TRUE(DirIndex::GetNameHash("file1", hash1) == 8);
  fprintf(stderr, "Hash: %02X-%02X-%02X-%02X-%02X-%02X-%02X-%02X (8 bytes)\n",
      (uint8_t) hash1[0], (uint8_t) hash1[1],
      (uint8_t) hash1[2], (uint8_t) hash1[3],
      (uint8_t) hash1[4], (uint8_t) hash1[5],
      (uint8_t) hash1[6], (uint8_t) hash1[7]);
  char hash2[128];
  ASSERT_TRUE(DirIndex::GetNameHash("file2", hash2) == 8);
  fprintf(stderr, "Hash: %02X-%02X-%02X-%02X-%02X-%02X-%02X-%02X (8 bytes)\n",
      (uint8_t) hash2[0], (uint8_t) hash2[1],
      (uint8_t) hash2[2], (uint8_t) hash2[3],
      (uint8_t) hash2[4], (uint8_t) hash2[5],
      (uint8_t) hash2[6], (uint8_t) hash2[7]);
  ASSERT_TRUE(memcmp(hash1, hash2, 8) != 0);
}

TEST(DirIndexTest, Split1) {
  int i = 1;
# ifndef IDXFS_EXTRA_SCALE
  int indice[kMaxRadix + 1] = { 0, 1, 2, 4, 8, 16, 32, 64, 128 };
# else
  int indice[kMaxRadix + 1] = { 0, 1, 2, 4, 8, 16, 32, 64, 128,
                                256, 512, 1024, 2048, 4096, 8192 };
# endif
  for (; i <= kMaxRadix; ++i) {
    ASSERT_TRUE(!idx_->GetBit(indice[i]));
    ASSERT_TRUE(idx_->IsSplittable(indice[0]));
    ASSERT_TRUE(idx_->NewIndexForSplitting(indice[0]) == indice[i]);
    idx_->SetBit(indice[i]);
  }
  ASSERT_TRUE(!idx_->IsSplittable(indice[0]));
}

TEST(DirIndexTest, Split2) {
  int i = 1;
# ifndef IDXFS_EXTRA_SCALE
  int indice[kMaxRadix + 1] = { 0, 1, 3, 7, 15, 31, 63, 127, 255 };
# else
  int indice[kMaxRadix + 1] = { 0, 1, 3, 7, 15, 31, 63, 127, 255,
                                511, 1023, 2047, 4095, 8191, 16383 };
# endif
  for (; i <= kMaxRadix; ++i) {
    ASSERT_TRUE(!idx_->GetBit(indice[i]));
    ASSERT_TRUE(idx_->IsSplittable(indice[i - 1]));
    ASSERT_TRUE(idx_->NewIndexForSplitting(indice[i - 1]) == indice[i]);
    idx_->SetBit(indice[i]);
  }
  ASSERT_TRUE(!idx_->IsSplittable(indice[kMaxRadix]));
}

TEST(DirIndexTest, SelectServer1) {
  idx_->SetBit(0);
  int info[kNumVirSrvs] = { 0 };
  for (int i = 0; i < kBatchSize; ++i) {
    std::stringstream ss;
    ss << "file" << i;
    info[idx_->SelectServer(ss.str())]++;
  }
  fprintf(stderr, "File numbers in each partition: ");
  PrintArray(info, 1 << 0);
  ASSERT_TRUE(ArraySum(info, 1 << 0) == kBatchSize);
}

TEST(DirIndexTest, SelectServer2) {
  idx_->SetBit(0);
  idx_->SetBit(1);
  int info[kNumVirSrvs] = { 0 };
  for (int i = 0; i < kBatchSize; ++i) {
    std::stringstream ss;
    ss << "file" << i;
    info[idx_->SelectServer(ss.str())]++;
  }
  fprintf(stderr, "File numbers in each partition: ");
  PrintArray(info, 1 << 1);
  ASSERT_TRUE(ArraySum(info, 1 << 1) == kBatchSize);
}

TEST(DirIndexTest, SelectServer3) {
  idx_->SetBit(0);
  idx_->SetBit(1);
  idx_->SetBit(2);
  int info[kNumVirSrvs] = { 0 };
  for (int i = 0; i < kBatchSize; ++i) {
    std::stringstream ss;
    ss << "file" << i;
    info[idx_->SelectServer(ss.str())]++;
  }
  fprintf(stderr, "File numbers in each partition: ");
  PrintArray(info, 1 << 2);
  ASSERT_TRUE(info[3] == 0);
  ASSERT_TRUE(ArraySum(info, 1 << 2) == kBatchSize);
}

TEST(DirIndexTest, SelectServer4) {
  idx_->SetBit(0);
  idx_->SetBit(1);
  idx_->SetBit(2);
  idx_->SetBit(4);
  int info[kNumVirSrvs] = { 0 };
  for (int i = 0; i < kBatchSize; ++i) {
    std::stringstream ss;
    ss << "file" << i;
    info[idx_->SelectServer(ss.str())]++;
  }
  fprintf(stderr, "File numbers in each partition: ");
  PrintArray(info, 1 << 3);
  ASSERT_TRUE(info[3] == 0);
  ASSERT_TRUE(info[5] == 0);
  ASSERT_TRUE(info[6] == 0);
  ASSERT_TRUE(info[7] == 0);
  ASSERT_TRUE(ArraySum(info, 1 << 3) == kBatchSize);
}

TEST(DirIndexTest, Migration1) {
  const int root_index = 0;
  int moved = 0;
  char hash[8] = { 0 };
  for (int i = 0; i < kBatchSize; ++i) {
    std::stringstream ss;
    ss << "file" << i;
    DirIndex::GetNameHash(ss.str(), hash);
    moved += DirIndex::ToBeMigrated(root_index, hash) ? 1 : 0;
  }
  ASSERT_TRUE(moved == kBatchSize);
  fprintf(stderr, "Root partition: ");
  fprintf(stderr, "%d/%d files starts in root\n", moved, kBatchSize);
}

TEST(DirIndexTest, Migration2) {
  const int child_index = 1;
  int moved = 0;
  char hash[8] = { 0 };
  for (int i = 0; i < kBatchSize; ++i) {
    std::stringstream ss;
    ss << "file" << i;
    DirIndex::GetNameHash(ss.str(), hash);
    moved += DirIndex::ToBeMigrated(child_index, hash) ? 1 : 0;
  }
  ASSERT_TRUE(moved > 0 && moved < kBatchSize);
  fprintf(stderr, "Child partition %d: ", child_index);
  fprintf(stderr, "%d/%d files will be migrated\n", moved, kBatchSize);
}

TEST(DirIndexTest, Migration3) {
  const int child_index = 2;
  const int parent_index = 1;
  int moved = 0, original = 0;
  char hash[8] = { 0 };
  for (int i = 0; i < kBatchSize; ++i) {
    std::stringstream ss;
    ss << "file" << i;
    DirIndex::GetNameHash(ss.str(), hash);
    moved += DirIndex::ToBeMigrated(child_index, hash) ? 1 : 0;
    original += DirIndex::ToBeMigrated(parent_index, hash) ? 0 : 1;
  }
  ASSERT_TRUE(moved > 0 && moved < kBatchSize);
  fprintf(stderr, "Child partition %d: ", child_index);
  fprintf(stderr, "%d/%d files will be migrated\n", moved, original);
}

TEST(DirIndexTest, Migration4) {
  const int child_index = 4;
  const int parent_index = 2;
  const int grand_parent_index = 1;
  int moved = 0, original = 0;
  char hash[8] = { 0 };
  for (int i = 0; i < kBatchSize; ++i) {
    std::stringstream ss;
    ss << "file" << i;
    DirIndex::GetNameHash(ss.str(), hash);
    moved += DirIndex::ToBeMigrated(child_index, hash) ? 1 : 0;
    original += DirIndex::ToBeMigrated(grand_parent_index, hash) ?
      0 : DirIndex::ToBeMigrated(parent_index, hash) ? 0 : 1;
  }
  ASSERT_TRUE(moved > 0 && moved < kBatchSize);
  fprintf(stderr, "Child partition %d: ", child_index);
  fprintf(stderr, "%d/%d files will be migrated\n", moved, original);
}

} /* namespace test */
} /* namespace indexfs */


// Test Driver
using namespace indexfs::test;
int main(int argc, char* argv[]) {
  return RunAllTests();
}
