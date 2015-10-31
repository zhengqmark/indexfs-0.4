// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "common/didxcache.h"
#include "common/logging.h"
#include "common/unit_test.h"

namespace indexfs { namespace test {

namespace {
static const int kCacheSize = 4096;
struct DirIndexCacheTest {
  DirIndexCache cache_;
  DirIndexCacheTest() : cache_(kCacheSize) { }
};
static DirIndexPolicy* index_policy = DirIndexPolicy::TEST_NewPolicy(32, 256);
}

TEST(DirIndexCacheTest, Insert) {
  DirIndex* dir_idx = index_policy->NewDirIndex(0, 0);
  DirIndexEntry* entry = cache_.Insert(dir_idx);
  ASSERT_EQ(entry->index, dir_idx);
  cache_.Evict(0);
  cache_.Release(entry);
}

TEST(DirIndexCacheTest, Get) {
  DirIndex* dir_idx = index_policy->NewDirIndex(0, 0);
  DirIndexEntry* entry1 = cache_.Insert(dir_idx);
  DirIndexEntry* entry2 = cache_.Get(0);
  ASSERT_TRUE(entry1 == entry2);
  cache_.Evict(0);
  cache_.Release(entry1);
  cache_.Release(entry2);
}

TEST(DirIndexCacheTest, Evit) {
  DirIndex* dir_idx1 = index_policy->NewDirIndex(0, 0);
  DirIndexEntry* entry1 = cache_.Insert(dir_idx1);
  cache_.Evict(0);
  DirIndex* dir_idx2 = index_policy->NewDirIndex(0, 0);
  DirIndexEntry* entry2 = cache_.Insert(dir_idx2);
  ASSERT_TRUE(entry1 != entry2);
  ASSERT_TRUE(entry2->index != dir_idx1);
  cache_.Evict(0);
  cache_.Release(entry1);
  cache_.Release(entry2);
}

TEST(DirIndexCacheTest, LRU) {
  for (int i = 0; i < kCacheSize * 2; ++i) {
    cache_.Release(
        cache_.Insert(index_policy->NewDirIndex(i, 0)));
  }
  int num = 0;
  for (int i = 0; i < kCacheSize * 2; ++i) {
    DirIndexEntry* e = cache_.Get(i);
    if (e != NULL) {
      num++;
      cache_.Release(e);
    }
  }
  ASSERT_TRUE(num == kCacheSize);
  for (int i = 0; i < kCacheSize * 2; ++i) {
    cache_.Evict(i);
  }
}

} // namespace test
} // namespace indexfs


// Test Driver
int main(int argc, char* argv[]) {
  return indexfs::test::RunAllTests();
}
