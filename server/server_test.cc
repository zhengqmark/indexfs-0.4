// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <set>
#include <vector>
#include <sstream>
#include <iomanip>

#include <time.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>

#include "metadb/metadb_io.h"
#include "common/unit_test.h"
#include "server/fs_driver.h"
#include "server/index_ctx.h"

namespace indexfs { namespace test {

namespace {
static const int kBatchSize = 128;
static const int kDepth = 16;
static const int64_t kRtInodeNo = 0;
static int run_id = 0;
static const char* kRunPrefix = "/tmp/indexfs-test";
}

class IndexFSTest {
 public:

  IndexFSTest() { run_id++; }

  Status OpenContext();

  Status Mkdir(int64_t dir_id, const std::string& dir_name);
  Status Mknod(int64_t dir_id, const std::string& file_name);
  Status Getattr(int64_t dir_id, const std::string& obj_name);
  Status Setattr(int64_t dir_id, const std::string& obj_name);

 protected:
  int64_t last_inode_;
  StatInfo last_stat_;

  std::string srv_dir_;
  Env* env_;
  Config* options_;
  IndexContext* index_ctx_;
};

Status IndexFSTest::OpenContext() {
  last_inode_ = kRtInodeNo;
  std::stringstream ss;
  ss << kRunPrefix << "-" << time(NULL);
  ss << "-" << std::setfill('0') << std::setw(2) << run_id;
  ss << "-" << "idxctx" << "." << rand() % 65521;
  srv_dir_.assign(ss.str());
  options_ = Config::CreateServerTestingConfig(srv_dir_);
  env_ = GetSystemEnv(options_);
  CreateDirectory(env_, srv_dir_);
  CreateDirectory(env_, options_->GetFileDir());
  CreateDirectory(env_, options_->GetDBRootDir());
  CreateDirectory(env_, options_->GetDBHomeDir());
  CreateDirectory(env_, options_->GetDBSplitDir());
  index_ctx_ = new IndexContext(env_, options_);
  return index_ctx_->Open();
}

Status IndexFSTest::Mkdir(int64_t dir_id,
                          const std::string& dir_name) {
  OID obj_id;
  obj_id.dir_id = dir_id;
  obj_id.obj_name = dir_name;
  int16_t srv_id = index_ctx_->GetMyRank();
  int64_t inode_no = index_ctx_->NextInode();
  mode_t perm = S_IRWXU | S_IRWXG | S_IRWXO;
  Status s;
  s = index_ctx_->TEST_Mkdir(obj_id, 0, perm, inode_no, srv_id);
  if (s.ok()) {
    last_inode_ = inode_no;
  }
  return s;
}

Status IndexFSTest::Mknod(int64_t dir_id,
                          const std::string& file_name) {
  OID obj_id;
  obj_id.dir_id = dir_id;
  obj_id.obj_name = file_name;
  mode_t perm = S_IRWXU | S_IRWXG | S_IRWXO;
  return index_ctx_->TEST_Mknod(obj_id, 0, perm);
}

Status IndexFSTest::Getattr(int64_t dir_id,
                            const std::string& obj_name) {
  OID obj_id;
  obj_id.dir_id = dir_id;
  obj_id.obj_name = obj_name;
  StatInfo stat;
  Status s;
  s = index_ctx_->TEST_Getattr(obj_id, 0, &stat);
  if (s.ok()) {
    last_stat_ = stat;
  }
  return s;
}

Status IndexFSTest::Setattr(int64_t dir_id,
                            const std::string& obj_name) {
  OID obj_id;
  obj_id.dir_id = dir_id;
  obj_id.obj_name = obj_name;
  return index_ctx_->TEST_Setattr(obj_id, 0, last_stat_);
}

// -------------------------------------------------------------
// Test Cases
// -------------------------------------------------------------

TEST(IndexFSTest, Init) {
  ASSERT_OK(OpenContext());
  ASSERT_TRUE(index_ctx_->TEST_HasDir(kRtInodeNo));
  ASSERT_TRUE(index_ctx_->NextInode() > kRtInodeNo);
  ASSERT_TRUE(index_ctx_->GetMyRank() == options_->GetSrvId());
  ASSERT_TRUE(index_ctx_->GetNumServers() == options_->GetSrvNum());
}

TEST(IndexFSTest, Mkdir) {
  const char* dir_name = "dir_name";
  ASSERT_OK(OpenContext());
  for (int i = 0; i < kDepth; ++i) {
    int64_t parent_id = last_inode_;
    ASSERT_TRUE(index_ctx_->TEST_HasDir(parent_id));
    ASSERT_OK(Mkdir(parent_id, dir_name));
    ASSERT_TRUE(last_inode_ != parent_id);
    ASSERT_OK(Getattr(parent_id, dir_name));
    ASSERT_EQ(last_stat_.id, last_inode_);
    ASSERT_TRUE(S_ISDIR(last_stat_.mode));
    ASSERT_TRUE(Mkdir(parent_id, dir_name).IsAlreadyExists());
  }
}

TEST(IndexFSTest, Mknod) {
  const char* dir_name = "dir_name";
  const char* file_name = "file_name";
  ASSERT_OK(OpenContext());
  for (int i = 0; i < kDepth; ++i) {
    int64_t parent_id = last_inode_;
    ASSERT_TRUE(index_ctx_->TEST_HasDir(parent_id));
    ASSERT_OK(Mkdir(parent_id, dir_name));
    ASSERT_OK(Mknod(parent_id, file_name));
    ASSERT_OK(Getattr(parent_id, file_name));
    ASSERT_TRUE(S_ISREG(last_stat_.mode));
    ASSERT_TRUE(Mknod(parent_id, file_name).IsAlreadyExists());
  }
}

TEST(IndexFSTest, Attr) {
  const char* dir_name = "dir_name";
  const char* file_name = "file_name";
  ASSERT_OK(OpenContext());
  for (int i = 0; i < kDepth; ++i) {
    int64_t parent_id = last_inode_;
    ASSERT_OK(Mkdir(parent_id, dir_name));
    ASSERT_OK(Getattr(parent_id, dir_name));
    last_stat_.mode = last_stat_.mode;
    ASSERT_OK(Setattr(parent_id, dir_name));
    ASSERT_OK(Mknod(parent_id, file_name));
    ASSERT_OK(Getattr(parent_id, file_name));
    last_stat_.uid = last_stat_.uid;
    last_stat_.gid = last_stat_.gid;
    ASSERT_OK(Setattr(parent_id, file_name));
  }
}

TEST(IndexFSTest, ReadDir) {
  char buf[64];
  ASSERT_OK(OpenContext());
  const int64_t parent_id = last_inode_;
  std::set<std::string> names;
  for (int i = 0; i < kBatchSize; ++i) {
    snprintf(buf, 64, "file_%d", i);
    std::string name = buf;
    ASSERT_OK(Mknod(parent_id, name));
    names.insert(name);
  }
  int num_names = 0;
  DirScanner* ds = NULL;
  ds = index_ctx_->CreateDirScanner(parent_id, 0, "");
  for (; ds->Valid(); ds->Next()) {
    num_names++;
    std::string name;
    ds->RetrieveEntryName(&name);
    ASSERT_TRUE(names.find(name) != names.end());
  }
  delete ds;
  ASSERT_EQ(num_names, names.size());
}

TEST(IndexFSTest, BulkInsert) {
  char buf[64];
  ASSERT_OK(OpenContext());
  const int64_t parent_id = last_inode_;
  for (int i = 0; i < kBatchSize; ++i) {
    snprintf(buf, 64, "file_%d", i);
    std::string name = buf;
    ASSERT_OK(Mknod(parent_id, name));
  }
  const int64_t dir_id = index_ctx_->NextInode();
  std::stringstream ss;
  ss << options_->GetDBSplitDir() << "/test";
  CreateDirectory(env_, ss.str());
  const std::string sst_file = ss.str() + "/000001.sst";
  WritableFile* file;
  ASSERT_OK(env_->NewWritableFile(sst_file, &file));
  {
    MetaDBWriter writer(env_, file);
    DirScanner* ds = NULL;
    ds = index_ctx_->CreateDirScanner(parent_id, 0, "");
    for (; ds->Valid(); ds->Next()) {
      std::string name;
      ds->RetrieveEntryName(&name);
      writer.InsertFile(dir_id, 0, name);
    }
    ASSERT_OK(writer.Finish());
  }
  {
    MetaDBReader reader;
    ASSERT_OK(reader.Open(env_, sst_file));
    reader.PrintTable();
  }
  DirIndex* dir_idx = index_ctx_->TEST_NewDirIndex(dir_id);
  ASSERT_OK(index_ctx_->InstallSplit(dir_id,
      ss.str(), dir_idx->ToSlice(), 0, 0, 0, kBatchSize, kBatchSize));
  delete dir_idx;
  int num_names = 0;
  DirScanner* ds = NULL;
  ds = index_ctx_->CreateDirScanner(dir_id, 0, "");
  for (; ds->Valid(); ds->Next()) {
    num_names++;
    std::string name;
    ds->RetrieveEntryName(&name);
  }
  delete ds;
  ASSERT_EQ(num_names, kBatchSize);
}

TEST(IndexFSTest, BulkInsertExists) {
  ASSERT_OK(OpenContext());
  DirIndex* dir_idx = index_ctx_->TEST_NewDirIndex(last_inode_);
  ASSERT_TRUE(index_ctx_->InstallSplit(last_inode_,
      "sst_dir", dir_idx->ToSlice(), 0, 0, 0, 0, 0).IsAlreadyExists());
  delete dir_idx;
}

} // namespace test
} // namespace indexfs


// Test driver
int main(int argc, char* argv[]) {
  return indexfs::test::RunAllTests();
}
