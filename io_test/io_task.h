// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_MPI_IO_TASK_H_
#define _INDEXFS_MPI_IO_TASK_H_

#include <stdio.h>
#include "io_client.h"
#include <gflags/gflags.h>

namespace indexfs { namespace mpi {

struct IOListener {
  virtual ~IOListener() { }
  virtual void IOPerformed(const char* op) = 0;
  virtual void IOFailed(const char* op) = 0;
};

struct IOError {
  int dir_;
  int file_;
  std::string path_;
  std::string op_;
  std::string cause_;
  IOError(const char* op, const std::string &cause)
    : dir_(-1), file_(-1), op_(op), cause_(cause) {
  }
  IOError(int dir, const char* op, const std::string &cause)
    : dir_(dir), file_(-1), op_(op), cause_(cause) {
  }
  IOError(int dir, int file, const char* op, const std::string &cause)
    : dir_(dir), file_(file), op_(op), cause_(cause) {
  }
  IOError(const std::string &path, const char* op, const std::string &cause)
    : dir_(-1), file_(-1), path_(path), op_(op), cause_(cause) {
  }
};

DECLARE_bool(enable_prepare); // Skip the prepare phase?
DECLARE_bool(enable_main); // Skip the main phase?
DECLARE_bool(enable_clean); // Skip the cleaning phase?
DECLARE_string(fs); // The backend FS to use
DECLARE_string(run_id); // A unique identifier for a particular task run
DECLARE_string(log_file); // File to hold performance data
DECLARE_bool(ignore_errors); // Ignore individual op errors?

class IOTask {
 public:

  IOTask(int my_rank, int comm_sz);
  void SetListener(IOListener* listener);

  virtual ~IOTask();
  virtual void Prepare() = 0;
  virtual void PreRun() { }
  virtual void Run() = 0;
  virtual void PostRun() { }
  virtual void Clean() = 0;
  virtual bool CheckPrecondition() = 0;

 protected:

  int my_rank_;
  int comm_sz_;
  FILE* LOG_;
  IOClient* IO_;
  IOListener* listener_;

  static inline
  const char* GetBoolString(bool flag) {
    return flag ? "Yes" : "No";
  }

 private:
  // No copying allowed
  IOTask(const IOTask&);
  IOTask& operator=(const IOTask&);
};

struct IOTaskFactory {
  // basic RPC baseline
  static IOTask* GetRPCTestTask(int my_rank, int comm_sz);
  // FS metadata injection performance
  static IOTask* GetTreeTestTask(int my_rank, int comm_sz);
  // FS general metadata performance
  static IOTask* GetReplayTestTask(int my_rank, int comm_sz);
  // FS client-side cache effectiveness
  static IOTask* GetCacheTestTask(int my_rank, int comm_sz);
  // Parallel LevelDB major compaction
  static IOTask* GetCompactionTestTask(int my_rank, int comm_sz);
};

} /* namespace mpi */ } /* namespace indexfs */

#endif /* _INDEXFS_MPI_IO_TASK_H_ */
