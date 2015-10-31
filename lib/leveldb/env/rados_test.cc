// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/status.h"
#include "env/obj_io.h"
#include "util/testharness.h"

namespace leveldb {

class IORadosTest {
 public:
  IO* io_;
  IORadosTest() {
    IO* rados = IO::RadosIO("/tmp/ceph.conf", "metadata");
    io_ = IO::PrefixedIO(rados, "indexfs_");
  }
  virtual ~IORadosTest() {
    delete io_;
  }
};

TEST(IORadosTest, ObjectExistence) {
  const std::string obj = "a";
  io_->DeleteObject(obj);
  WritableFile* wf;
  ASSERT_OK(io_->NewWritableObject(obj, &wf));
  ASSERT_OK(wf->Close());
  delete wf;
  ASSERT_TRUE(io_->Exists(obj));
  io_->DeleteObject(obj);
  ASSERT_TRUE(!io_->Exists(obj));
}

TEST(IORadosTest, ObjectIO) {
  const std::string obj = "a";
  const char str[] = "hello";
  io_->DeleteObject(obj);
  WritableFile* wf;
  ASSERT_OK(io_->NewWritableObject(obj, &wf));
  ASSERT_OK(wf->Append(Slice(str, strlen(str))));
  ASSERT_OK(wf->Sync());
  ASSERT_OK(wf->Close());
  delete wf;
  uint64_t size;
  ASSERT_OK(io_->GetObjectSize(obj, &size));
  ASSERT_TRUE(size == strlen(str));
  SequentialFile* sf;
  ASSERT_OK(io_->NewSequentialObject(obj, &sf));
  char buf[64];
  Slice s;
  ASSERT_OK(sf->Read(strlen(str), &s, buf));
  ASSERT_TRUE(s.compare(Slice(str, strlen(str))) == 0);
  ASSERT_OK(sf->Read(strlen(str), &s, buf)); // end of file
  ASSERT_TRUE(s.size() == 0);
  delete sf;
}

TEST(IORadosTest, ObjectCopy) {
  const std::string obj1 = "a";
  const std::string obj2 = "b";
  const char str[] = "hello";
  io_->DeleteObject(obj1);
  io_->DeleteObject(obj2);
  WritableFile* wf;
  ASSERT_OK(io_->NewWritableObject(obj1, &wf));
  ASSERT_OK(wf->Append(Slice(str, strlen(str))));
  ASSERT_OK(wf->Sync());
  ASSERT_OK(wf->Close());
  delete wf;
  ASSERT_OK(io_->CopyObject(obj1, obj2));
  SequentialFile* sf;
  ASSERT_OK(io_->NewSequentialObject(obj2, &sf));
  char buf[64];
  Slice s;
  ASSERT_OK(sf->Read(strlen(str), &s, buf));
  ASSERT_TRUE(s.compare(Slice(str, strlen(str))) == 0);
  ASSERT_OK(sf->Read(strlen(str), &s, buf)); // end of file
  ASSERT_TRUE(s.size() == 0);
  delete sf;
}

} // namespace leveldb

int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}
