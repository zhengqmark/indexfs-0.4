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

#include "metadb/dboptions.h"
#include "metadb/metadb.h"
#include "metadb/metadb_io.h"
#include "common/gigaidx.h"
#include "common/common.h"
#include "common/config.h"
#include "common/unit_test.h"

namespace indexfs { namespace test {

namespace {
static std::string FileName(int file_no) {
  char buffer[1024];
  sprintf(buffer, "file%d", file_no);
  return buffer;
}
static int run_id = 0;
static const int kBatchSize = 1000;
static const char* kRunPrefix = "/tmp/indexfs-test";
}

class MetaDBTest {
 public:

  MetaDBTest() { run_id++; }

  Status Init();
  Status Reinit();

  int CheckNamespace(int64_t dir_id, int16_t partition_id);
  Status PopulateNamespace(int64_t dir_id, int16_t partition_id);

 protected:
  Env* env_;
  MetaDB* mdb_;
  Config* config_;
  std::string srv_dir_;
};

Status MetaDBTest::Init() {
  std::stringstream ss;
  ss << kRunPrefix << "-" << time(NULL);
  ss << "-" << std::setfill('0') << std::setw(2) << run_id;
  ss << "-" << "metadb" << "." << rand() % 65521;
  srv_dir_.assign(ss.str());
  config_ = Config::CreateServerTestingConfig(srv_dir_);
  env_ = GetSystemEnv(config_);
  CreateDirectory(env_, srv_dir_);
  CreateDirectory(env_, config_->GetFileDir());
  CreateDirectory(env_, config_->GetDBRootDir());
  CreateDirectory(env_, config_->GetDBHomeDir());
  CreateDirectory(env_, config_->GetDBSplitDir());
  return MetaDB::Open(config_, &mdb_, env_);
}

Status MetaDBTest::Reinit() {
  delete mdb_;
  mdb_ = NULL;
  return MetaDB::Open(config_, &mdb_, env_);
}

int MetaDBTest::CheckNamespace(int64_t dir_id, int16_t partition_id) {
  int num_files = 0;
  for (int i = 0; i < kBatchSize; ++i) {
    std::string fname = FileName(i);
    KeyInfo key(dir_id, partition_id, fname);
    if (mdb_->EntryExists(key).ok()) {
      num_files++;
    }
  }
  return num_files;
}

Status MetaDBTest::PopulateNamespace(int64_t dir_id, int16_t partition_id) {
  Status s;
  for (int i = 0; i < kBatchSize && s.ok(); ++i) {
    std::string fname = FileName(i);
    KeyInfo key(dir_id, partition_id, fname);
    s = mdb_->NewFile(key);
  }
  return s;
}

// -------------------------------------------------------------
// Test Cases
// -------------------------------------------------------------

TEST(MetaDBTest, Init) {
  ASSERT_OK(Init());
  ASSERT_EQ(mdb_->GetCurrentInodeNo(), 0);
}

TEST(MetaDBTest, NonExisting) {
  StatInfo info;
  const std::string filename = "file_or_directory";
  KeyInfo key(0, 0, filename);
  ASSERT_OK(Init());
  ASSERT_OK(mdb_->DeleteEntry(key));
  ASSERT_TRUE(mdb_->EntryExists(key).IsNotFound());
  ASSERT_TRUE(mdb_->GetEntry(key, &info).IsNotFound());
  ASSERT_TRUE(mdb_->UpdateEntry(key, info).IsNotFound());
  ASSERT_TRUE(mdb_->SetFileMode(key, 0).IsNotFound());
}

TEST(MetaDBTest, CreateFile) {
  StatInfo info;
  const std::string filename = "file";
  KeyInfo key(0, 0, filename);
  ASSERT_OK(Init());
  ASSERT_OK(mdb_->NewFile(key));
  ASSERT_OK(mdb_->GetEntry(key, &info));
  ASSERT_EQ(info.id, -1);
  ASSERT_EQ(info.zeroth_server, -1);
  ASSERT_EQ(info.size, 0);
  ASSERT_TRUE(info.is_embedded);
  ASSERT_TRUE(S_ISREG(info.mode));
  ASSERT_TRUE(mdb_->NewFile(key).IsAlreadyExists());
}

TEST(MetaDBTest, CreateDirectory) {
  StatInfo info;
  const int16_t zeroth_server = 16;
  const std::string dirname = "directory";
  KeyInfo key(0, 0, dirname);
  ASSERT_OK(Init());
  int64_t inode_no = mdb_->ReserveNextInodeNo();
  ASSERT_OK(mdb_->NewDirectory(key, zeroth_server, inode_no));
  ASSERT_OK(mdb_->GetEntry(key, &info));
  ASSERT_EQ(info.id, inode_no);
  ASSERT_EQ(info.zeroth_server, zeroth_server);
  ASSERT_EQ(info.size, -1);
  ASSERT_TRUE(!info.is_embedded);
  ASSERT_TRUE(S_ISDIR(info.mode));
  ASSERT_TRUE(mdb_->NewDirectory(key, 0, inode_no).IsAlreadyExists());
}

TEST(MetaDBTest, DeleteEntry) {
  StatInfo info;
  const std::string filename = "file";
  KeyInfo key(0, 0, filename);
  ASSERT_OK(Init());
  ASSERT_OK(mdb_->NewFile(key));
  ASSERT_OK(mdb_->GetEntry(key, &info));
  ASSERT_OK(mdb_->DeleteEntry(key));
  ASSERT_OK(mdb_->DeleteEntry(key));
  ASSERT_TRUE(mdb_->GetEntry(key, &info).IsNotFound());
}

TEST(MetaDBTest, InsertEntry) {
  StatInfo info, info_inserted;
  info.id = -1;
  info.mode = 256;
  info.size = 1024;
  info.is_embedded = true;
  info.zeroth_server = 16;
  info.uid = info.gid = -1;
  info.ctime = info.mtime = 65536;
  const std::string filename = "file";
  KeyInfo key(0, 0, filename);
  ASSERT_OK(Init());
  ASSERT_OK(mdb_->InsertEntry(key, info));
  ASSERT_OK(mdb_->GetEntry(key, &info_inserted));
  ASSERT_EQ(info.id, info_inserted.id);
  ASSERT_EQ(info.mode, info_inserted.mode);
  ASSERT_EQ(info.size, info_inserted.size);
  ASSERT_EQ(info.is_embedded, info_inserted.is_embedded);
  ASSERT_EQ(info.zeroth_server, info_inserted.zeroth_server);
  ASSERT_EQ(info.uid, info_inserted.uid);
  ASSERT_EQ(info.gid, info_inserted.gid);
  ASSERT_EQ(info.ctime, info_inserted.ctime);
  ASSERT_EQ(info.mtime, info_inserted.mtime);
  ASSERT_TRUE(mdb_->InsertEntry(key, info).IsAlreadyExists());
}

TEST(MetaDBTest, UpdateEntry) {
  StatInfo info, info_updated;
  info.id = -1;
  info.mode = 256;
  info.size = 1024;
  info.is_embedded = false;
  info.zeroth_server = 16;
  info.uid = info.gid = -1;
  info.ctime = info.mtime = 65536;
  const std::string filename = "file";
  KeyInfo key(0, 0, filename);
  ASSERT_OK(Init());
  ASSERT_OK(mdb_->NewFile(key));
  ASSERT_OK(mdb_->UpdateEntry(key, info));
  ASSERT_OK(mdb_->GetEntry(key, &info_updated));
  ASSERT_EQ(info.id, info_updated.id);
  ASSERT_EQ(info.mode, info_updated.mode);
  ASSERT_EQ(info.size, info_updated.size);
  ASSERT_EQ(info.is_embedded, info_updated.is_embedded);
  ASSERT_EQ(info.zeroth_server, info_updated.zeroth_server);
  ASSERT_EQ(info.uid, info_updated.uid);
  ASSERT_EQ(info.gid, info_updated.gid);
  ASSERT_EQ(info.ctime, info_updated.ctime);
  ASSERT_EQ(info.mtime, info_updated.mtime);
  info.mode = 512;
  info.size = 2048;
  info.ctime = info.mtime = 131072;
  ASSERT_OK(mdb_->PutEntry(key, info));
  ASSERT_OK(mdb_->GetEntry(key, &info_updated));
  ASSERT_EQ(info.id, info_updated.id);
  ASSERT_EQ(info.mode, info_updated.mode);
  ASSERT_EQ(info.size, info_updated.size);
  ASSERT_EQ(info.is_embedded, info_updated.is_embedded);
  ASSERT_EQ(info.zeroth_server, info_updated.zeroth_server);
  ASSERT_EQ(info.uid, info_updated.uid);
  ASSERT_EQ(info.gid, info_updated.gid);
  ASSERT_EQ(info.ctime, info_updated.ctime);
  ASSERT_EQ(info.mtime, info_updated.mtime);
}

TEST(MetaDBTest, ResetMode) {
  StatInfo info;
  const std::string filename = "file";
  KeyInfo key(0, 0, filename);
  ASSERT_OK(Init());
  ASSERT_OK(mdb_->NewFile(key));
  ASSERT_OK(mdb_->GetEntry(key, &info));
  ASSERT_EQ(S_IRUSR & info.mode, S_IRUSR);
  ASSERT_EQ(S_IWUSR & info.mode, S_IWUSR);
  ASSERT_EQ(S_IXUSR & info.mode, 0);
  ASSERT_EQ(S_IRGRP & info.mode, S_IRGRP);
  ASSERT_EQ(S_IWGRP & info.mode, 0);
  ASSERT_EQ(S_IXGRP & info.mode, 0);
  ASSERT_EQ(S_IROTH & info.mode, S_IROTH);
  ASSERT_EQ(S_IWOTH & info.mode, 0);
  ASSERT_EQ(S_IXOTH & info.mode, 0);
  ASSERT_TRUE(S_ISREG(info.mode) && !S_ISDIR(info.mode));
  ASSERT_OK(mdb_->SetFileMode(key, 0));
  ASSERT_OK(mdb_->GetEntry(key, &info));
  ASSERT_EQ(S_IRUSR & info.mode, 0);
  ASSERT_EQ(S_IWUSR & info.mode, 0);
  ASSERT_EQ(S_IXUSR & info.mode, 0);
  ASSERT_EQ(S_IRGRP & info.mode, 0);
  ASSERT_EQ(S_IWGRP & info.mode, 0);
  ASSERT_EQ(S_IXGRP & info.mode, 0);
  ASSERT_EQ(S_IROTH & info.mode, 0);
  ASSERT_EQ(S_IWOTH & info.mode, 0);
  ASSERT_EQ(S_IXOTH & info.mode, 0);
  ASSERT_TRUE(S_ISREG(info.mode) && !S_ISDIR(info.mode));
  ASSERT_OK(mdb_->SetFileMode(key, -1));
  ASSERT_OK(mdb_->GetEntry(key, &info));
  ASSERT_EQ(S_IRUSR & info.mode, S_IRUSR);
  ASSERT_EQ(S_IWUSR & info.mode, S_IWUSR);
  ASSERT_EQ(S_IXUSR & info.mode, S_IXUSR);
  ASSERT_EQ(S_IRGRP & info.mode, S_IRGRP);
  ASSERT_EQ(S_IWGRP & info.mode, S_IWGRP);
  ASSERT_EQ(S_IXGRP & info.mode, S_IXGRP);
  ASSERT_EQ(S_IROTH & info.mode, S_IROTH);
  ASSERT_EQ(S_IWOTH & info.mode, S_IWOTH);
  ASSERT_EQ(S_IXOTH & info.mode, S_IXOTH);
  ASSERT_TRUE(S_ISREG(info.mode) && !S_ISDIR(info.mode));
  ASSERT_OK(mdb_->PutEntryWithMode(key, info, S_IRWXU));
  ASSERT_OK(mdb_->GetEntry(key, &info));
  ASSERT_EQ(S_IRUSR & info.mode, S_IRUSR);
  ASSERT_EQ(S_IWUSR & info.mode, S_IWUSR);
  ASSERT_EQ(S_IXUSR & info.mode, S_IXUSR);
  ASSERT_EQ(S_IRGRP & info.mode, 0);
  ASSERT_EQ(S_IWGRP & info.mode, 0);
  ASSERT_EQ(S_IXGRP & info.mode, 0);
  ASSERT_EQ(S_IROTH & info.mode, 0);
  ASSERT_EQ(S_IWOTH & info.mode, 0);
  ASSERT_EQ(S_IXOTH & info.mode, 0);
  ASSERT_TRUE(S_ISREG(info.mode) && !S_ISDIR(info.mode));
  ASSERT_OK(mdb_->PutEntryWithMode(key, info, S_IRWXO));
  ASSERT_OK(mdb_->GetEntry(key, &info));
  ASSERT_EQ(S_IRUSR & info.mode, 0);
  ASSERT_EQ(S_IWUSR & info.mode, 0);
  ASSERT_EQ(S_IXUSR & info.mode, 0);
  ASSERT_EQ(S_IRGRP & info.mode, 0);
  ASSERT_EQ(S_IWGRP & info.mode, 0);
  ASSERT_EQ(S_IXGRP & info.mode, 0);
  ASSERT_EQ(S_IROTH & info.mode, S_IROTH);
  ASSERT_EQ(S_IWOTH & info.mode, S_IWOTH);
  ASSERT_EQ(S_IXOTH & info.mode, S_IXOTH);
  ASSERT_TRUE(S_ISREG(info.mode) && !S_ISDIR(info.mode));
}

TEST(MetaDBTest, ListEmptyDirectory) {
  const std::string start_hash;
  KeyOffset offset(0, 0, start_hash);
  ASSERT_OK(Init());
  DirScanner* scanner = mdb_->CreateDirScanner(offset);
  ASSERT_TRUE(scanner != NULL);
  ASSERT_FALSE(scanner->Valid());
  delete scanner;
  std::vector<std::string> names;
  ASSERT_OK(mdb_->ListEntries(offset, &names, NULL));
  ASSERT_EQ(names.size(), 0);
}

TEST(MetaDBTest, ListDirectory) {
  const std::string start_hash;
  KeyOffset offset(0, 0, start_hash);
  const std::string filename1 = "file1";
  const std::string filename2 = "file2";
  const std::string filename3 = "file3";
  KeyInfo key1(0, 0, filename1);
  KeyInfo key2(0, 0, filename2);
  KeyInfo key3(0, 0, filename3);
  ASSERT_OK(Init());
  ASSERT_OK(mdb_->NewFile(key1));
  ASSERT_OK(mdb_->NewFile(key2));
  ASSERT_OK(mdb_->NewFile(key3));
  std::set<std::string> all_names;
  all_names.insert(filename1);
  all_names.insert(filename2);
  all_names.insert(filename3);
  std::string name;
  DirScanner* scanner = mdb_->CreateDirScanner(offset);
  ASSERT_TRUE(scanner != NULL);
  ASSERT_TRUE(scanner->Valid());
  scanner->RetrieveEntryName(&name);
  ASSERT_TRUE(all_names.find(name) != all_names.end());
  scanner->Next();
  ASSERT_TRUE(scanner->Valid());
  scanner->RetrieveEntryName(&name);
  ASSERT_TRUE(all_names.find(name) != all_names.end());
  scanner->Next();
  ASSERT_TRUE(scanner->Valid());
  scanner->RetrieveEntryName(&name);
  ASSERT_TRUE(all_names.find(name) != all_names.end());
  scanner->Next();
  ASSERT_FALSE(scanner->Valid());
  delete scanner;
  std::vector<std::string> names;
  ASSERT_OK(mdb_->ListEntries(offset, &names, NULL));
  ASSERT_EQ(names.size(), 3);
  ASSERT_TRUE(all_names.find(names[0]) != all_names.end());
  ASSERT_TRUE(all_names.find(names[1]) != all_names.end());
  ASSERT_TRUE(all_names.find(names[2]) != all_names.end());
}

TEST(MetaDBTest, Bitmap) {
  const int64_t dir_id = 512;
  const int16_t zeroth_server = 0;
  DirIndexPolicy* policy = DirIndexPolicy::TEST_NewPolicy(1, 256);
  DirIndex* dir_idx = policy->NewDirIndex(dir_id, zeroth_server);
  dir_idx->SetBit(1);
  dir_idx->SetBit(3);
  const Slice& dmap_data = dir_idx->ToSlice();
  std::string dmap_buffer;
  ASSERT_OK(Init());
  ASSERT_OK(mdb_->InsertMapping(dir_id, dmap_data));
  ASSERT_OK(mdb_->GetMapping(dir_id, &dmap_buffer));
  ASSERT_EQ(dmap_buffer.size(), dmap_data.size());
  ASSERT_EQ(memcmp(dmap_buffer.data(), dmap_data.data(), dmap_data.size()), 0);
  DirIndex* dir_idx2 = policy->RecoverDirIndex(dmap_buffer);
  ASSERT_TRUE(dir_idx2->GetBit(1));
  ASSERT_TRUE(dir_idx2->GetBit(3));
  ASSERT_EQ(dir_idx2->FetchDirId(), dir_id);
  ASSERT_EQ(dir_idx2->FetchZerothServer(), zeroth_server);
}

TEST(MetaDBTest, EmbeddedIO) {
  StatInfo info;
  int32_t size;
  char buffer[4096];
  const char* data = "Hello World!";
  const std::string filename = "file";
  KeyInfo key(0, 0, filename);
  ASSERT_OK(Init());
  ASSERT_OK(mdb_->NewFile(key));
  ASSERT_OK(mdb_->FetchData(key, &size, buffer));
  ASSERT_EQ(size, 0);
  ASSERT_OK(mdb_->WriteData(key, 0, strlen(data), data));
  ASSERT_OK(mdb_->FetchData(key, &size, buffer));
  ASSERT_EQ(size, strlen(data));
  ASSERT_EQ(memcmp(buffer, data, size), 0);
  ASSERT_OK(mdb_->GetEntry(key, &info));
  ASSERT_EQ(info.size, strlen(data));
}

TEST(MetaDBTest, Extraction) {
  const int64_t dir_id = 0;
  const int16_t old_partition_id = 0;
  const int16_t new_partition_id = 1;
  uint64_t min_seq, max_seq;
  std::stringstream ss;
  ss << config_->GetDBSplitDir() << "/" << "test";
  ASSERT_OK(Init());
  ASSERT_OK(PopulateNamespace(dir_id, old_partition_id));
  int num_files = CheckNamespace(dir_id, old_partition_id);
  ASSERT_EQ(num_files, static_cast<int>(kBatchSize));
  BulkExtractor* extractor = mdb_->CreateBulkExtractor(ss.str());
  ASSERT_TRUE(extractor != NULL);
  extractor->SetDirectory(dir_id);
  extractor->SetOldPartition(old_partition_id);
  extractor->SetNewPartition(new_partition_id);
  ASSERT_OK(extractor->Extract(&min_seq, &max_seq));
  fprintf(stderr, "Extraction completed, min_seq=%lu, max_seq=%lu\n", min_seq, max_seq);
  ASSERT_TRUE(env_->FileExists(ss.str()));
  std::vector<std::string> sst_files;
  ASSERT_OK(env_->GetChildren(ss.str(), &sst_files));
  int num_files_extracted = 0;
  for (std::vector<std::string>::iterator it = sst_files.begin(); it != sst_files.end(); ++it) {
    if (it->compare(".") != 0 && it->compare("..") != 0) {
      int num_entries;
      MetaDBReader reader;
      std::stringstream sss;
      sss << ss.str() << "/" << *it;
      ASSERT_OK(reader.Open(env_, sss.str()));
      reader.PrintTable();
      num_entries = reader.ListEntries(dir_id, new_partition_id);
      num_files_extracted += num_entries;
    }
  }
  ASSERT_EQ(extractor->GetNumEntriesExtracted(), num_files_extracted);
  ASSERT_OK(extractor->Commit());
  ASSERT_FALSE(env_->FileExists(ss.str()));
  delete extractor;
  int num_files_remained = CheckNamespace(dir_id, old_partition_id);
  ASSERT_EQ(num_files_remained + num_files_extracted, num_files);
  fprintf(stderr, "Extraction committed, %d/%d files remain in old partition\n", num_files_remained, num_files);
}

TEST(MetaDBTest, BulkInsertion) {
  const int64_t dir_id = 0;
  const int16_t old_partition_id = 0;
  const int16_t new_partition_id = 1;
  uint64_t min_seq, max_seq;
  std::stringstream ss;
  ss << config_->GetDBSplitDir() << "/" << "test";
  ASSERT_OK(Init());
  ASSERT_OK(PopulateNamespace(dir_id, old_partition_id));
  int num_files = CheckNamespace(dir_id, old_partition_id);
  ASSERT_EQ(num_files, static_cast<int>(kBatchSize));
  BulkExtractor* extractor = mdb_->CreateBulkExtractor(ss.str());
  ASSERT_TRUE(extractor != NULL);
  extractor->SetDirectory(dir_id);
  extractor->SetOldPartition(old_partition_id);
  extractor->SetNewPartition(new_partition_id);
  ASSERT_OK(extractor->Extract(&min_seq, &max_seq));
  fprintf(stderr, "Extraction completed, min_seq=%lu, max_seq=%lu\n", min_seq, max_seq);
  ASSERT_TRUE(env_->FileExists(ss.str()));
  ASSERT_OK(mdb_->BulkInsert(min_seq, max_seq, ss.str()));
  int num_files_moved = CheckNamespace(dir_id, new_partition_id);
  ASSERT_EQ(extractor->GetNumEntriesExtracted(), num_files_moved);
  ASSERT_OK(extractor->Commit());
  ASSERT_FALSE(env_->FileExists(ss.str()));
  delete extractor;
  int num_files_remained = CheckNamespace(dir_id, old_partition_id);
  ASSERT_EQ(num_files_moved + num_files_remained, num_files);
  fprintf(stderr, "Bulk insertion completed, %d/%d files have been moved\n", num_files_moved, num_files);
}

TEST(MetaDBTest, LocalBulkInsert) {
  const int64_t dir_id = 0;
  const int16_t old_partition_id = 0;
  const int16_t new_partition_id = 1;
  uint64_t min_seq, max_seq;
  ASSERT_OK(Init());
  ASSERT_OK(PopulateNamespace(dir_id, old_partition_id));
  int num_files = CheckNamespace(dir_id, old_partition_id);
  ASSERT_EQ(num_files, static_cast<int>(kBatchSize));
  BulkExtractor* extractor = mdb_->CreateLocalBulkExtractor();
  ASSERT_TRUE(extractor != NULL);
  extractor->SetDirectory(dir_id);
  extractor->SetOldPartition(old_partition_id);
  extractor->SetNewPartition(new_partition_id);
  ASSERT_OK(extractor->Extract(&min_seq, &max_seq));
  ASSERT_OK(extractor->Commit());
  int num_files_moved = CheckNamespace(dir_id, new_partition_id);
  ASSERT_EQ(extractor->GetNumEntriesExtracted(), num_files_moved);
  delete extractor;
  int num_files_remained = CheckNamespace(dir_id, old_partition_id);
  ASSERT_EQ(num_files_moved + num_files_remained, num_files);
  fprintf(stderr, "Local bulk insertion completed, %d/%d files have been moved\n", num_files_moved, num_files); 
}

TEST(MetaDBTest, Restart) {
  const int64_t dir_id = 0;
  const int16_t partition_id = 0;
  ASSERT_OK(Init());
  ASSERT_EQ(mdb_->GetCurrentInodeNo(), 0);
  int64_t inode_no = mdb_->ReserveNextInodeNo();
  ASSERT_EQ(inode_no, DEFAULT_MAX_NUM_SERVERS);
  ASSERT_OK(PopulateNamespace(dir_id, partition_id));
  int num_files_before = CheckNamespace(dir_id, partition_id);
  ASSERT_EQ(num_files_before, static_cast<int>(kBatchSize));
  fprintf(stderr, "Inserted %d files into the file system; rebooting ... \n", num_files_before);
  ASSERT_OK(Reinit());
  std::vector<std::string> sst_files;
  ASSERT_OK(env_->GetChildren(config_->GetDBHomeDir(), &sst_files));
  ASSERT_TRUE(sst_files.size() > 0);
  int num_files_stored = 0;
  for (std::vector<std::string>::iterator it = sst_files.begin(); it != sst_files.end(); ++it) {
    if (it->length() > 3 && it->substr(it->length() - 4) == ".sst") {
      int num_entries;
      MetaDBReader reader;
      std::stringstream sss;
      sss << config_->GetDBHomeDir() << "/" << *it;
      ASSERT_OK(reader.Open(env_, sss.str()));
      reader.PrintTable();
      num_entries = reader.ListEntries(dir_id, partition_id);
      num_files_stored += num_entries;
    }
  }
  ASSERT_EQ(num_files_stored, num_files_before);
  ASSERT_EQ(mdb_->GetCurrentInodeNo(), inode_no);
  int num_files_after = CheckNamespace(dir_id, partition_id);
  fprintf(stderr, "Reboot completed; found %d files from the original file system\n", num_files_after);
  ASSERT_EQ(num_files_after, num_files_before);
}

} /* namespace test */
} /* namespace indexfs */


// Test Driver
int main(int argc, char* argv[]) {
  return indexfs::test::RunAllTests();
}
