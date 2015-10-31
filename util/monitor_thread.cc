// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>

#include "util/monitor_thread.h"

namespace indexfs {

namespace {
static void JoinThread(pthread_t tid) {
  if (pthread_join(tid, NULL) < 0) {
    perror("Fail to join thread!");
    abort();
  }
}
static pthread_t CreateThread(void*(*func)(void*), void* arg) {
  pthread_t tid;
  if (pthread_create(&tid, NULL, func, arg) < 0) {
    perror("Fail to create thread!");
    abort();
  }
  return tid;
}
static const int TSDB_PORT = 10600;
static const std::string TSDB_ADDRESS = "127.0.0.1";
}

MonitorThread::MonitorThread(Monitor *monitor,
        int frequency) :
        done_(false),
        tid_(0),
        frequency_(frequency),
        monitor_(monitor) {
  DLOG_ASSERT(monitor_ != NULL && frequency_ > 0);
}

MonitorThread::~MonitorThread() {
  if (!done_) {
    done_ = true;
  }
  JoinThread(tid_);
}

void MonitorThread::SendMetrics() {
  std::stringstream stream;
  monitor_->GetCurrentStatus(&stream);
  std::string buf = stream.str();
  try {
    socket_.sendTo(buf.data(), buf.size(), TSDB_ADDRESS, TSDB_PORT);
  } catch (SocketException &e) {
  }
}

void MonitorThread::Start() {
  tid_ = CreateThread(&Run, this);
}

void* MonitorThread::Run(void* arg) {
  MonitorThread* ctx = reinterpret_cast<MonitorThread*>(arg);
  while (!ctx->done_) {
    Env::Default()->SleepForMicroseconds(ctx->frequency_ * 1000000);
    ctx->SendMetrics();
  };
  return NULL;
}

} // namespace indexfs
