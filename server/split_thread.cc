// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "server/split_thread.h"

namespace indexfs {

static pthread_t CreateThread(void*(*func)(void*), void* arg) {
  pthread_t tid;
  if (pthread_create(&tid, NULL, func, arg) < 0) {
    perror("Fail to create thread!");
    abort();
  }
  return tid;
}

static void JoinThread(pthread_t tid) {
  if (pthread_join(tid, NULL) < 0) {
    perror("Fail to join thread!");
    abort();
  }
}

void PthreadCall(const char* label, int result) {
  if (result != 0) {
    fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
    exit(1);
  }
}

SplitThread::SplitThread(Monitor* monitor) :
              monitor_(monitor), thread_(0),
              started_thread_(false), done_(false) {
  server_ = new MetadataServer();
  PthreadCall("mutex_init", pthread_mutex_init(&mu_, NULL));
  PthreadCall("cvar_init", pthread_cond_init(&signal_, NULL));
}

SplitThread::~SplitThread() {
  if (!done_) {
    Stop();
  }
  delete server_;
}

void SplitThread::Start() {
  thread_ = CreateThread(&Run, this);
}


void SplitThread::AddSplitTask(int dir_id, int index) {
  PthreadCall("lock", pthread_mutex_lock(&mu_));

  // Start background thread if necessary
  if (!started_thread_) {
    Start();
  }

  // If the queue is currently empty, the background thread may currently be
  // waiting.
  bool wakeup = queue_.empty();

  // Add to priority queue
  queue_.push_back(SplitItem(dir_id, index));

  if (wakeup)
    PthreadCall("signal", pthread_cond_signal(&signal_));

  PthreadCall("unlock", pthread_mutex_unlock(&mu_));
}

void SplitThread::ExecuteThread() {
  started_thread_ = true;

  while (!done_) {
    // Wait until there is an item that is ready to run
    PthreadCall("lock", pthread_mutex_lock(&mu_));
    while (queue_.empty()) {
      PthreadCall("wait", pthread_cond_wait(&signal_, &mu_));
    }

    int dir_id = queue_.front().dir_id;
    int index = queue_.front().index;
    queue_.pop_front();

    PthreadCall("unlock", pthread_mutex_unlock(&mu_));

    DirHandle hdir = server_->FetchDir(dir_id);
    server_->Split(dir_id, index, hdir);
  }
}

void* SplitThread::Run(void* arg) {
  SplitThread* split = reinterpret_cast<SplitThread*>(arg);
  split->ExecuteThread();
  return NULL;
}

void SplitThread::Stop() {
  done_ = true;
  JoinThread(thread_);
}

} // namespace indexfs
