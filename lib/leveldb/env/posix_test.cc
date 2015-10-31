// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/env.h"
#include "env/obj_io.h"
#include "port/port.h"
#include "util/testharness.h"

namespace leveldb {

static const int kDelayMicros = 100000;

class EnvPosixTest {
 private:
  port::Mutex mu_;
  std::string events_;

 public:
  Env* env_;
  EnvPosixTest() : env_(Env::Default()) { }
};

static void SetBool(void* ptr) {
  reinterpret_cast<port::AtomicPointer*>(ptr)->NoBarrier_Store(ptr);
}

TEST(EnvPosixTest, RunImmediately) {
  port::AtomicPointer called (NULL);
  env_->Schedule(&SetBool, &called);
  Env::Default()->SleepForMicroseconds(kDelayMicros);
  ASSERT_TRUE(called.NoBarrier_Load() != NULL);
}

TEST(EnvPosixTest, RunMany) {
  port::AtomicPointer last_id (NULL);

  struct CB {
    port::AtomicPointer* last_id_ptr;   // Pointer to shared slot
    uintptr_t id;             // Order# for the execution of this callback

    CB(port::AtomicPointer* p, int i) : last_id_ptr(p), id(i) { }

    static void Run(void* v) {
      CB* cb = reinterpret_cast<CB*>(v);
      void* cur = cb->last_id_ptr->NoBarrier_Load();
      ASSERT_EQ(cb->id-1, reinterpret_cast<uintptr_t>(cur));
      cb->last_id_ptr->Release_Store(reinterpret_cast<void*>(cb->id));
    }
  };

  // Schedule in different order than start time
  CB cb1(&last_id, 1);
  CB cb2(&last_id, 2);
  CB cb3(&last_id, 3);
  CB cb4(&last_id, 4);
  env_->Schedule(&CB::Run, &cb1);
  env_->Schedule(&CB::Run, &cb2);
  env_->Schedule(&CB::Run, &cb3);
  env_->Schedule(&CB::Run, &cb4);

  Env::Default()->SleepForMicroseconds(kDelayMicros);
  void* cur = last_id.Acquire_Load();
  ASSERT_EQ(4, reinterpret_cast<uintptr_t>(cur));
}

struct State {
  port::Mutex mu;
  int val;
  int num_running;
};

static void ThreadBody(void* arg) {
  State* s = reinterpret_cast<State*>(arg);
  s->mu.Lock();
  s->val += 1;
  s->num_running -= 1;
  s->mu.Unlock();
}

TEST(EnvPosixTest, StartThread) {
  State state;
  state.val = 0;
  state.num_running = 3;
  for (int i = 0; i < 3; i++) {
    env_->StartThread(&ThreadBody, &state);
  }
  while (true) {
    state.mu.Lock();
    int num = state.num_running;
    state.mu.Unlock();
    if (num == 0) {
      break;
    }
    Env::Default()->SleepForMicroseconds(kDelayMicros);
  }
  ASSERT_EQ(state.val, 3);
}

class IOPosixTest {
 public:
  IO* io_;
  IOPosixTest() {
    io_ = IO::PrefixedIO(IO::PosixIO(), "/tmp/obj_");
  }
  virtual ~IOPosixTest() {
    delete io_;
  }
};

TEST(IOPosixTest, ObjectExistence) {
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

TEST(IOPosixTest, ObjectIO) {
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

TEST(IOPosixTest, ObjectCopy) {
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

}  // namespace leveldb

int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}
