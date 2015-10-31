// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <sstream>
#include <iomanip>
#include "leveldb/env.h"
#include "env/obj_io.h"
#include "env/obj_set.h"
#include "env/io_logger.h"
#include "util/testharness.h"

namespace leveldb {

namespace {
static int run_id = 0;
static const std::string kRunPrefix = "/tmp/leveldb-test";
static const std::string kRootDir = "/src/leveldb";
static const std::string kDefaultSet = kRootDir + "/l0";
}

class ObjSetTest {
 public:
  IO* io_;
  ObjEnv* objs_;
  void Init();
  virtual ~ObjSetTest() {
    delete objs_;
    // IO is auto release by ObjEnv.
  }
  ObjSetTest() : io_(NULL), objs_(NULL) { Init(); }
};

void ObjSetTest::Init() {
  run_id++;
  std::stringstream ss;
  ss << kRunPrefix << "-" << time(NULL);
  ss << "-" << std::setfill('0') << std::setw(2) << run_id;
  ss << "-" << "objset" << "." << rand() % 65521;
  std::string root_dir = ss.str();
  ASSERT_OK(Env::Default()->CreateDir(root_dir));
  io_ = IO::PrefixedIO(IO::PosixIO(), std::string(root_dir + "/obj_"));
  objs_ = ObjEnv::TEST_NewInstance(io_, kDefaultSet, kRootDir);
}

TEST(ObjSetTest, DefaultSet) {
  const std::string data = "hello";
  const std::string obj = "o1";
  const std::string obj_path = kDefaultSet + "/" + obj;
  ASSERT_OK(objs_->HasSet(kDefaultSet));
  ASSERT_TRUE(objs_->HasObject(obj_path).IsNotFound());
  std::string oname;
  ASSERT_OK(objs_->GetLogObjName(kDefaultSet, &oname));
  ASSERT_TRUE(io_->Exists(oname));
  WritableFile* wf1;
  ASSERT_OK(objs_->CreateWritableObject(obj_path, &wf1));
  ASSERT_OK(wf1->Append(data));
  ASSERT_OK(wf1->Close());
  delete wf1;
  ASSERT_OK(objs_->HasObject(obj_path));
  uint64_t size1;
  ASSERT_OK(objs_->GetObjectSize(obj_path, &size1));
  ASSERT_EQ(size1, data.length());
  std::vector<std::string> names;
  ASSERT_OK(objs_->ListSet(kDefaultSet, &names));
  ASSERT_TRUE(names.size() == 1);
  ASSERT_TRUE(names[0] == obj);
  WritableFile* wf2;
  ASSERT_OK(objs_->CreateWritableObject(obj_path, &wf2));
  ASSERT_OK(wf2->Close());
  delete wf2;
  ASSERT_OK(objs_->HasObject(obj_path));
  uint64_t size2;
  ASSERT_OK(objs_->GetObjectSize(obj_path, &size2));
  ASSERT_EQ(size2, 0);
  ASSERT_OK(objs_->DeleteObject(obj_path));
  ASSERT_TRUE(objs_->HasObject(obj_path).IsNotFound());
  uint64_t size3;
  ASSERT_TRUE(objs_->GetObjectSize(obj_path, &size3).IsNotFound());
  ASSERT_OK(objs_->SealSet(kDefaultSet));
  IOLogReporter* reporter = IOLogReporter::PrinterReporter();
  SequentialFile* file;
  ASSERT_OK(io_->NewSequentialObject(oname, &file));
  ASSERT_OK(IOLogReader(file).Recover(reporter));
  delete reporter;
}

TEST(ObjSetTest, TempSet1) {
  const std::string set = "s1";
  const std::string set_path = kRootDir + "/" + set;
  const std::string obj = "o1";
  const std::string obj_path = set_path + "/" + obj;
  ASSERT_TRUE(objs_->HasSet(set_path).IsNotFound());
  ASSERT_OK(objs_->CreateSet(set_path));
  ASSERT_OK(objs_->HasSet(set_path));
  std::string oname;
  ASSERT_OK(objs_->GetLogObjName(set_path, &oname));
  ASSERT_TRUE(io_->Exists(oname));
  WritableFile* wf;
  ASSERT_OK(objs_->CreateWritableObject(obj_path, &wf));
  ASSERT_OK(wf->Close());
  delete wf;
  ASSERT_OK(objs_->HasObject(obj_path));
  std::vector<std::string> names;
  ASSERT_OK(objs_->ListAllSets(&names));
  ASSERT_TRUE(names.size() == 2);
  ASSERT_TRUE(names[0] == set || names[1] == set);
  ASSERT_OK(objs_->DeleteObject(obj_path));
  ASSERT_OK(objs_->SealSet(set_path));
  IOLogReporter* reporter = IOLogReporter::PrinterReporter();
  SequentialFile* file;
  ASSERT_OK(io_->NewSequentialObject(oname, &file));
  ASSERT_OK(IOLogReader(file).Recover(reporter));
  delete reporter;
  ASSERT_OK(objs_->DeleteSet(set_path));
  ASSERT_TRUE(!io_->Exists(oname));
  ASSERT_TRUE(objs_->HasSet(set_path).IsNotFound());
}

TEST(ObjSetTest, TempSet2) {
  const std::string set = "s1";
  const std::string set_path = kRootDir + "/" + set;
  const std::string obj = "o1";
  const std::string obj_path = set_path + "/" + obj;
  ASSERT_OK(objs_->CreateSet(set_path));
  std::string oname;
  ASSERT_OK(objs_->GetLogObjName(set_path, &oname));
  ASSERT_TRUE(io_->Exists(oname));
  WritableFile* wf;
  ASSERT_OK(objs_->CreateWritableObject(obj_path, &wf));
  ASSERT_OK(wf->Close());
  delete wf;
  ASSERT_OK(objs_->ForgetSet(set_path));
  ASSERT_TRUE(objs_->HasSet(set_path).IsNotFound());
  ASSERT_TRUE(io_->Exists(oname));
  ASSERT_OK(objs_->LoadSet(set_path));
  ASSERT_OK(objs_->HasSet(set_path));
  ASSERT_TRUE(io_->Exists(oname));
  std::vector<std::string> names;
  ASSERT_OK(objs_->ListSet(set_path, &names));
  ASSERT_TRUE(names.size() == 1);
  ASSERT_TRUE(names[0] == obj);
  ASSERT_TRUE(objs_->DeleteSet(set_path).IsIOError());
}

} // namespace leveldb

int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}
