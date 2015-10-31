// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <signal.h>
#include <stdlib.h>

#include "common/logging.h"
#include "server/fs_driver.h"

// Import source code version
#ifndef PACKAGE_VERSION
#define PACKAGE_VERSION "0.0.0"
#endif

using leveldb::NewMemLinkEnv;

DEFINE_int32(srvid, -1, "set indexfs server rank");

using indexfs::ServerDriver;
using indexfs::Config;
using indexfs::Env;
using indexfs::GetSystemEnv;
using indexfs::Logger;
using indexfs::LoadServerConfig;
using indexfs::GetLogFileName;
using indexfs::GetDefaultLogDir;
using indexfs::SetVersionString;
using indexfs::SetUsageMessage;
using indexfs::ParseCommandLineFlags;

// Global state
// -----------------------------------------------
static Env* env = NULL;
static Config* config = NULL;
static ServerDriver* driver = NULL;
// -----------------------------------------------

namespace {
static
void SignalHandler(int sig) {
  if (driver != NULL) {
    driver->Shutdown();
  }
  LOG(INFO) << "Receive external signal to stop server ...";
}
}

int main(int argc, char* argv[]) {
//-----------------------------------------------------------------
  FLAGS_logfn = "indexfs_server";
  FLAGS_srvid = -1;
  FLAGS_logbufsecs = 5;
  FLAGS_log_dir = GetDefaultLogDir();
  SetVersionString(PACKAGE_VERSION);
  SetUsageMessage("indexfs server");
  ParseCommandLineFlags(&argc, &argv, true);
  srand(FLAGS_srvid);
  Logger::Initialize(GetLogFileName());
//-----------------------------------------------------------------
  config = LoadServerConfig(FLAGS_srvid);
  env = GetSystemEnv(config);
  if (config->HasOldData()) {
    // We will have to link old SSTables into
    // our current DB home
    env = NewMemLinkEnv(env);
  }
  driver = ServerDriver::NewIndexFSDriver(env, config);
  driver->PrepareContext();
  driver->SetupMonitoring();
  driver->OpenServer();
  signal(SIGINT, &SignalHandler);
  signal(SIGTERM, &SignalHandler);
  driver->Start(); // Run forever until interrupted
//-----------------------------------------------------------------
  ServerDriver* _driver_ = driver;
  driver = NULL;
  delete _driver_;
//-----------------------------------------------------------------
  Logger::Shutdown();
  DLOG(INFO) << "Everything disposed, server will now shutdown";
  return 0;
}
