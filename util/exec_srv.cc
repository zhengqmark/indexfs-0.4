// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <deque>
#include <stdexcept>
#include <pthread.h>

#include "util/exec_srv.h"
#include "common/common.h"
#include "common/logging.h"

namespace indexfs {

namespace {
struct Thread {
  void Start();
  void Join();
  void Detach();
  pthread_t tid;
  void* arg;
  void*(*start_routine)(void*);
};
void Thread::Start() {
  int ret = pthread_create(&tid, NULL, start_routine, arg);
  CHECK(ret == 0) << "Cannot create new thread";
}
void Thread::Join() {
  int ret = pthread_join(tid, NULL);
  CHECK(ret == 0) << "Cannot join existing thread";
}
void Thread::Detach() {
  int ret = pthread_detach(tid);
  CHECK(ret == 0) << "Cannot detach from existing thread";
}
}

class OnDemandExecService: virtual public ExecService {
 public:
  void SubmitTask(Runnable* task);
  virtual ~OnDemandExecService() { }
};

namespace {
static
void* ExecTask(void* arg) {
  Runnable* task = reinterpret_cast<Runnable*>(arg);
  try {
    task->Run();
  } catch (IOError &io) {
    LOG(ERROR) << "Fail to execute task: " << io.message;
    throw io;
  } catch (ServerInternalError &ie) {
    LOG(ERROR) << "Fail to execute task: " << ie.message;
    throw ie;
  }
  delete task;
  return NULL;
}
}

void OnDemandExecService::SubmitTask(Runnable* task) {
  Thread t;
  t.arg = task;
  t.start_routine = &ExecTask;
  t.Start();
  t.Detach();
}

ExecService* ExecService::Default() {
  return new OnDemandExecService();
}

} // namespace indexfs
