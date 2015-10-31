// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include <sstream>
#include "io_task.h"
#include "io_client.h"
#include <gflags/gflags.h>

namespace indexfs { namespace mpi {

// Common task settings shared by all defined tasks
DEFINE_bool(enable_prepare,
    true, "Set false to skip the prepare phase");
DEFINE_bool(enable_main,
    true, "Set false to skip the main phase");
DEFINE_bool(enable_clean,
    true, "Set false to skip the cleaning phase");
DEFINE_int32(random_seed,
    -1, "The random seed to use during this particular test run");
DEFINE_bool(ignore_errors,
    false, "Set to ignore any IO errors that occur during the main phase and clean phase");
DEFINE_string(run_id,
    "0", "A unique string identifier for a particular task run");
DEFINE_string(log_file,
    "/tmp/detailed_perf_info.log", "Write detailed IO measurements to this particular log file");

// Use IndexFS by default
DEFINE_string(fs,
    "indexfs", "Set the backend FS, options including \"indexfs\", \"pvfs\", and \"localfs\"");

static IOClient* CreateIOClient(int my_rank, int comm_sz) {
  if (FLAGS_fs == "indexfs")
    return IOClientFactory::GetIndexFSClient(my_rank, comm_sz, FLAGS_run_id);

  if (FLAGS_fs == "pvfs")
    return IOClientFactory::GetOrangeFSClient(my_rank, FLAGS_run_id);

  if (FLAGS_fs == "localfs")
    return IOClientFactory::GetLocalFSClient(my_rank, FLAGS_run_id);

  my_rank == 0 ?
    fprintf(stderr, "no matching FS found: %s\n", FLAGS_fs.c_str()) : 0;
  return NULL; // No matching FS backend found
}

static unsigned int GetRandomSeed() {
  return FLAGS_random_seed < 0 ? getpid() : FLAGS_random_seed;
}

static FILE* OpenLogFile(int rank) {
  std::stringstream ss;
  ss << FLAGS_log_file << "." << rank;
  std::string path = ss.str();
  FILE* f = fopen(path.c_str(), "w");
  if (f == NULL) {
    fprintf(stderr, "cannot open log file %s: %s\n",
        path.c_str(), strerror(errno));
  }
  return f;
}

IOTask::~IOTask() {
  if (IO_ != NULL) {
    IO_->Dispose();
    delete IO_;
  }
  if (LOG_ != NULL) {
    fclose(LOG_);
  }
}

IOTask::IOTask(int my_rank, int comm_sz)
  : my_rank_(my_rank), comm_sz_(comm_sz), listener_(NULL) {
  srand(GetRandomSeed());
  LOG_ = OpenLogFile(my_rank);
  IO_ = CreateIOClient(my_rank, comm_sz);
}

void IOTask::SetListener(IOListener* listener) {
  listener_ = listener;
}

} /* namespace mpi */ } /* namespace indexfs */
