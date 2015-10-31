// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "common/unit_test.h"

#include "util/leveldb_io.h"
#include "util/leveldb_reader.h"

namespace indexfs { namespace test {

namespace {
static const int kBatchSize = 4;
struct LevelDBTest {
  MDBOptionsFactory opt_factory_;
  Env* env() { return opt().env; }
  const Options& opt() { return opt_factory_.Get(); }
};
}

TEST(LevelDBTest, IO) {
  std::stringstream ss;
  ss << "/tmp/test-" << rand() % 65527 << ".sst";
  env()->DeleteFile(ss.str());
  WritableFile* file;
  ASSERT_OK(env()->NewWritableFile(ss.str(), &file));
  {
    MDBTableBuilder builder(opt(), file);
    char key[128];
    char val[128];
    for (int i = 0; i < kBatchSize; ++i) {
      int key_size = sprintf(key, "key-%05d", i) + 1;
      int val_size = sprintf(val, "val-%d", rand()) + 1;
      builder.Put(Slice(key, key_size), Slice(val, val_size));
    }
    ASSERT_OK(builder.Seal());
    ASSERT_EQ(builder->NumEntries(), kBatchSize);
  }
  MDBTablePrinter printer;
  ASSERT_OK(printer.Open(opt(), ss.str()));
  ASSERT_EQ(printer.PrintTable(), kBatchSize);
}


TEST(LevelDBTest, MergeIO) {
  std::vector<std::string> fnames;
  for (int i = 0; i < kBatchSize; ++i) {
    std::stringstream ss;
    ss << "/tmp/test-" << rand() % 65527 << ".sst";
    env()->DeleteFile(ss.str());
    WritableFile* file;
    ASSERT_OK(env()->NewWritableFile(ss.str(), &file));
    {
      MDBTableBuilder builder(opt(), file);
      char key[128];
      char val[128];
      for (int j = i * kBatchSize; j < (i + 1) * kBatchSize; ++j) {
        int key_size = sprintf(key, "key-%05d", j) + 1;
        int val_size = sprintf(val, "val-%d", rand()) + 1;
        builder.Put(Slice(key, key_size), Slice(val, val_size));
      }
      ASSERT_OK(builder.Seal());
      ASSERT_EQ(builder->NumEntries(), kBatchSize);
    }
    fnames.push_back(ss.str());
  }
  MDBTableSetPrinter printer;
  ASSERT_OK(printer.Open(opt(), fnames));
  ASSERT_EQ(printer.PrintTableSet(), kBatchSize * kBatchSize);
}

} // namespace test
} // namespace indexfs


// Test driver
int main(int argc, char* argv[]) {
  return indexfs::test::RunAllTests();
}
