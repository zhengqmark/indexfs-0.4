// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "common/logging.h"

namespace indexfs {

void Logger::FlushLogFiles() { google::FlushLogFiles(google::INFO); }

namespace {
static const char* NULL_LOG_FILE = "/dev/null";
static inline
void InternalLogOpen(
    const char* log_fname) {
#ifndef NDEBUG
  FLAGS_minloglevel = 0;
  FLAGS_logbuflevel = -1;
#else
  FLAGS_minloglevel = 1;
  FLAGS_logbuflevel = 0;
#endif
  if (log_fname == NULL) {
    FLAGS_logtostderr = true;
    log_fname = NULL_LOG_FILE;
  }
  google::InitGoogleLogging(log_fname);
}
}

void Logger::Initialize(const char* log_fname) {
  InternalLogOpen(log_fname);
  google::InstallFailureSignalHandler();
}

void Logger::Shutdown() { google::ShutdownGoogleLogging(); }

} /* namespace indexfs */
