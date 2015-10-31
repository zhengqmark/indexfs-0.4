// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/status.h"
#include "leveldb/env.h"
#include "env/io_logger.h"
#include "util/testharness.h"

namespace leveldb {

static const std::string kLogName = "/tmp/test.wal";

class IOLoggerTest {
 public:
  Status OpenLogger();
  Status OpenLogReader();
  Env* env_;
  IOLog* log_;
  IOLogReader* reader_;
  virtual ~IOLoggerTest() {
    delete log_;
    delete reader_;
  }
  IOLoggerTest() : log_(NULL), reader_(NULL) {
    env_ = Env::Default();
  }
};

Status IOLoggerTest::OpenLogger() {
  Status s;
  env_->DeleteFile(kLogName);
  WritableFile* file;
  s = env_->NewWritableFile(kLogName, &file);
  if (!s.ok()) {
    return s;
  }
  log_ = new IOLog(file);
  return s;
}

Status IOLoggerTest::OpenLogReader() {
  Status s;
  SequentialFile* file;
  s = env_->NewSequentialFile(kLogName, &file);
  if (!s.ok()) {
    return s;
  }
  reader_ = new IOLogReader(file);
  return s;
}

TEST(IOLoggerTest, LogAndRecover) {
  const std::string set1 = "set1";
  const std::string set2 = "set2";
  const std::string obj1 = "obj1";
  const std::string obj2 = "obj2";
  ASSERT_OK(OpenLogger());
  ASSERT_OK(log_->NewSet(set1));
  ASSERT_OK(log_->NewSet(set2));
  ASSERT_OK(log_->NewObject(set1, obj1));
  ASSERT_OK(log_->NewObject(set2, obj2));
  ASSERT_OK(log_->DeleteObject(set2, obj2));
  ASSERT_OK(log_->DeleteObject(set1, obj1));
  ASSERT_OK(log_->DeleteSet(set2));
  delete log_;
  log_ = NULL;
  ASSERT_OK(OpenLogReader());
  IOLogReporter* reporter = IOLogReporter::PrinterReporter();
  ASSERT_OK(reader_->Recover(reporter));
  delete reporter;
  delete reader_;
  reader_ = NULL;
}

} // namespace leveldb

int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}
