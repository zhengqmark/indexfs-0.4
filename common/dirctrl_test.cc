// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "common/dirctrl.h"
#include "common/logging.h"
#include "common/unit_test.h"

namespace indexfs { namespace test {

namespace {
struct DirCtrlTest {
  DirCtrlTable table_;
  DirCtrlTest() : table_() { }
};
}

TEST(DirCtrlTest, FetchNew) {
  DirCtrlBlock* blk = table_.Fetch(0);
  ASSERT_TRUE(blk->Empty());
  ASSERT_TRUE(!blk->disable_splitting);
  blk->size_map[0] = 0;
  blk->size_map[0]++;
  ASSERT_TRUE(blk->size_map[0] == 1);
  table_.Evict(0);
  table_.Release(blk);
}

TEST(DirCtrlTest, FetchExisting) {
  DirCtrlBlock* blk1 = table_.Fetch(0);
  DirCtrlBlock* blk2 = table_.Fetch(0);
  ASSERT_TRUE(blk1 == blk2);
  table_.Evict(0);
  table_.Release(blk1);
  table_.Release(blk2);
}

TEST(DirCtrlTest, Lock) {
  DirCtrlBlock* blk = table_.Fetch(0);
  blk->Lock();
  ASSERT_TRUE(blk->TestIfLocked());
# ifndef NDEBUG
  ASSERT_TRUE(blk->TestIfLockedByMe());
# endif
  blk->Unlock();
  ASSERT_TRUE(!blk->TestIfLocked());
# ifndef NDEBUG
  ASSERT_TRUE(!blk->TestIfLockedByMe());
# endif
  table_.Evict(0);
  table_.Release(blk);
}

TEST(DirCtrlTest, Evict) {
  DirCtrlBlock* blk1 = table_.Fetch(0);
  blk1->disable_splitting = true;
  ASSERT_TRUE(blk1->disable_splitting);
  table_.Evict(0);
  DirCtrlBlock* blk2 = table_.Fetch(0);
  ASSERT_TRUE(blk1 != blk2);
  ASSERT_TRUE(!blk2->disable_splitting);
  table_.Evict(0);
  table_.Release(blk1);
  table_.Release(blk2);
}

} // namespace test
} // namespace indexfs


// Test Driver
int main(int argc, char* argv[]) {
  return indexfs::test::RunAllTests();
}
