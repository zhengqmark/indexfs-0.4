// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "metadb/fstat.h"
#include "common/unit_test.h"

namespace indexfs { namespace mdb { namespace test {

struct FileStatTest {
};

TEST(FileStatTest, FileStatSize) {
  const size_t size = 64;
  ASSERT_EQ(sizeof(FileStat), size);
}

TEST(FileStatTest, InodeNo) {
  FileStat file_stat;
  const int64_t inode_no = 512;
  file_stat.SetInodeNo(inode_no);
  ASSERT_EQ(inode_no, file_stat.InodeNo());
  ASSERT_EQ(sizeof(inode_no), sizeof(file_stat.InodeNo()));
}

TEST(FileStatTest, FileSize) {
  FileStat file_stat;
  const int64_t file_size = 65536;
  file_stat.SetFileSize(file_size);
  ASSERT_EQ(file_size, file_stat.FileSize());
  ASSERT_EQ(sizeof(file_size), sizeof(file_stat.FileSize()));
}

TEST(FileStatTest, FileMode) {
  FileStat file_stat;
  const int32_t file_mode = 256;
  file_stat.SetFileMode(file_mode);
  ASSERT_EQ(file_mode, file_stat.FileMode());
  ASSERT_EQ(sizeof(file_mode), sizeof(file_stat.FileMode()));
}

TEST(FileStatTest, FileStatus) {
  FileStat file_stat;
  const int16_t file_status = 16;
  file_stat.SetFileStatus(file_status);
  ASSERT_EQ(file_status, file_stat.FileStatus());
  ASSERT_EQ(sizeof(file_status), sizeof(file_stat.FileStatus()));
}

TEST(FileStatTest, ZerothServer) {
  FileStat file_stat;
  const int16_t zeroth_server = 32;
  file_stat.SetZerothServer(zeroth_server);
  ASSERT_EQ(zeroth_server, file_stat.ZerothServer());
  ASSERT_EQ(sizeof(zeroth_server), sizeof(file_stat.ZerothServer()));
}

TEST(FileStatTest, Time) {
  FileStat file_stat;
  const int64_t time = 1048576;
  file_stat.SetTime(time);
  ASSERT_EQ(time, file_stat.ChangeTime());
  ASSERT_EQ(time, file_stat.ModifyTime());
  ASSERT_EQ(sizeof(time), sizeof(file_stat.ChangeTime()));
  ASSERT_EQ(sizeof(time), sizeof(file_stat.ModifyTime()));
}

} /* namespace test */
} /* namespace mdb */
} /* namespace indexfs */


// Test Driver
using namespace indexfs::test;
int main(int argc, char* argv[]) {
  return RunAllTests();
}
