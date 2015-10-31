// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <mpi.h>
#include <stdlib.h>
#include <unistd.h>
#include "io_task.h"
#include <gflags/gflags.h>

namespace indexfs { namespace mpi {

DEFINE_int32(rw_ratio,
    80, "Metadata read/write ratio, percentage of read operations");
DEFINE_int32(req_wait,
    10000, "Number of microseconds to wait (in average) between two requests");
DEFINE_int32(test_length,
    300, "The total amount of time (in seconds) that the test should run");
DEFINE_string(target_dir, "/__test_dir__", "");

namespace {

class CacheTest: public IOTask {

  static inline
  int MetadataRead(IOClient* IO, IOListener* L) {
    Status s = IO->GetAttr(FLAGS_target_dir);
    if (!s.ok()) {
      if (L != NULL) {
        L->IOFailed("getattr");
      }
      throw IOError(FLAGS_target_dir, "getattr", s.ToString());
    }
    if (L != NULL) {
      L->IOPerformed("getattr");
    }
    return 0;
  }

  static inline
  int MetadataWrite(IOClient* IO, IOListener* L) {
    Status s = IO->ResetMode(FLAGS_target_dir);
    if (!s.ok()) {
      if (L != NULL) {
        L->IOFailed("chmod");
      }
      throw IOError(FLAGS_target_dir, "chmod", s.ToString());
    }
    if (L != NULL) {
      L->IOPerformed("chmod");
    }
    return 0;
  }

  int PrintSettings() {
    return printf("Test Settings:\n"
      "total processes -> %d\n"
      "rw_ratio -> %d\n"
      "req_wait -> %d\n"
      "test_length -> %d\n"
      "backend_fs -> %s\n"
      "bulk_insert -> %s\n"
      "ignore_errors -> %s\n"
      "log_file -> %s\n"
      "run_id -> %s\n",
      comm_sz_,
      FLAGS_rw_ratio,
      FLAGS_req_wait,
      FLAGS_test_length,
      FLAGS_fs.c_str(),
      GetBoolString(FLAGS_bulk_insert),
      GetBoolString(FLAGS_ignore_errors),
      FLAGS_log_file.c_str(),
      FLAGS_run_id.c_str());
  }

 public:

  CacheTest(int my_rank, int comm_sz)
    : IOTask(my_rank, comm_sz) {
  }

  virtual void Prepare() {
    Status s = IO_->Init();
    if (!s.ok()) {
      throw IOError("init", s.ToString());
    }
    if (my_rank_ == 0) {
      Status s = IO_->MakeDirectories(FLAGS_target_dir);
      if (!s.ok()) {
        if (listener_ != NULL) {
          listener_->IOFailed("mkdirs");
        }
        throw IOError(FLAGS_target_dir, "mkdirs", s.ToString());
      }
      if (listener_ != NULL) {
        listener_->IOPerformed("mkdirs");
      }
    }
  }

  virtual void Run() {
    double start = MPI_Wtime();
    do {
      int wait = rand() % (2 * FLAGS_req_wait) + 1;
      usleep(wait);
      int rw = rand() % 100; // [0, 100)
      rw < FLAGS_rw_ratio ?
        MetadataRead(IO_, listener_) : MetadataWrite(IO_, listener_);
    } while (MPI_Wtime() - start < FLAGS_test_length);
  }

  virtual void Clean() {
    if (my_rank_ == 0) {
      IO_->Remove(FLAGS_target_dir);
      if (listener_ != NULL) {
        listener_->IOPerformed("remove");
      }
    }
  }

  virtual bool CheckPrecondition() {
    if (IO_ == NULL || LOG_ == NULL) {
      return false; // err has already been printed elsewhere
    }
    my_rank_ == 0 ? PrintSettings() : 0;
    // All will check, yet only the zeroth process will do the printing
    return true;
  }

};

} /* anonymous namespace */


IOTask* IOTaskFactory::GetCacheTestTask(int my_rank, int comm_sz) {
  return new CacheTest(my_rank, comm_sz);
}

} /* namespace mpi */ } /* namespace indexfs */
