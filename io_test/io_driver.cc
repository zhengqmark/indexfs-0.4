// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "io_task.h"
#include "common/config.h"
#include "common/logging.h"

#include <mpi.h>
#include <stdlib.h>
#include <gflags/gflags.h>

using ::indexfs::mpi::IOTask;
using ::indexfs::mpi::IOError;
using ::indexfs::mpi::IOListener;
using ::indexfs::mpi::IOTaskFactory;

// Use TreeTest by default
DEFINE_string(task,
    "tree", "Set the benchmark suite [tree|cache|replay|rpc|sstcomp]");

DEFINE_int32(rank,
    -1, "Set the rank of a particular driver instance");

DEFINE_int32(verbose,
    0, "Set a larger number to get more detailed per-client runtime status");

static
IOTask* FetchTask(int my_rank, int comm_sz) {
  IOTask* result = NULL;
  const char* task = FLAGS_task.c_str();
  if (FLAGS_task == "tree") {
    task = "TreeTest";
    result = IOTaskFactory::GetTreeTestTask(my_rank, comm_sz);
  }
  else if (FLAGS_task == "replay") {
    task = "TraceReplayTest";
    result = IOTaskFactory::GetReplayTestTask(my_rank, comm_sz);
  }
  else if (FLAGS_task == "sstcomp") {
    task = "ParallelCompactionTest";
    result = IOTaskFactory::GetCompactionTestTask(my_rank, comm_sz);
  }
  else if (FLAGS_task == "cache") {
    task = "PathnameLookupCacheTest";
    result = IOTaskFactory::GetCacheTestTask(my_rank, comm_sz);
  }
  else if (FLAGS_task == "rpc") {
    task = "RPCTest";
    result = IOTaskFactory::GetRPCTestTask(my_rank, comm_sz);
  }
  if (my_rank == 0) {
    if (result != NULL) {
      fprintf(stderr, "== Run %s ==\n", task);
    } else {
      fprintf(stderr, "No matching task found: %s\n", FLAGS_task.c_str());
    }
  }
  return result; // No matching benchmark task found
}

namespace {

class Operator {
 public:

  Operator(IOTask* task)
    : err_(false), task_(task) {
  }

  ~Operator() {
    if (FLAGS_verbose >= 2) {
      int id;
      MPI_Comm_rank(MPI_COMM_WORLD, &id);
      printf("# Proc #%d finished -- waiting peers to complete ...\n", id);
    }
    int r = err_ ? FAIL : SUCC;
    MPI_Allreduce(&r, &global_ret_, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
  }

  void CheckPrecondition() {
    err_ = !task_->CheckPrecondition();
  }

#define MPI_SYNC_OPERATION(op)                  \
  void op() {                                   \
    net_dura_ = 0;                              \
    double start = MPI_Wtime();                 \
    try {                                       \
      task_->op();                              \
    } catch (IOError &err) {                    \
      err_ = true;                              \
      throw err;                                \
    }                                           \
    net_dura_ = MPI_Wtime() - start;            \
  }

  MPI_SYNC_OPERATION(Prepare)
  MPI_SYNC_OPERATION(PreRun)
  MPI_SYNC_OPERATION(Run)
  MPI_SYNC_OPERATION(PostRun)
  MPI_SYNC_OPERATION(Clean)

#undef MPI_SYNC_OPERATION

  static bool OK() {
    return global_ret_ == SUCC;
  }

  static double GetDura() {
    return net_dura_;
  }

 private:
  bool err_;
  IOTask* task_;
  static int global_ret_;
  static double net_dura_;
  enum { SUCC = 0, FAIL = 1 };
};

double Operator::net_dura_ = 0;

int Operator::global_ret_ = SUCC;

class IOTestDriver: public IOListener {
 public:

  virtual ~IOTestDriver() {
    if (task_ != NULL) {
      delete task_;
    }
    MPI_Finalize();
  }

  IOTestDriver(int* argc, char*** argv) : ops_(0), err_(0) {
    int my_rank;
    int comm_sz;

    MPI_Init(argc, argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);

    // This only makes sense in unit testing
    if (FLAGS_rank >= 0) {
      my_rank = FLAGS_rank;
      printf("Reset rank to %d\n", my_rank);
    }

    my_rank_ = my_rank;
    comm_sz_ = comm_sz;
    task_ = FetchTask(my_rank_, comm_sz_); // Obtain the benchmark
  }

  void Exexcute() {
    if (task_ == NULL)
      return;
    CheckPrecondition();
    if (!Operator::OK())
      return;
    task_->SetListener(this);

    // Let's perform the test
    LogStatus("== IO Test Begin ==");
    RunTest();
    LogStatus("== IO Test Completed ==");
  }

  void RunTest() {
    int total_ops, total_err;
    double start, dura, global_dura;

    // Prepare Phase
    err_ = ops_ = 0;
    LogStatus("Prepare Phase ...");
    MPI_Barrier(MPI_COMM_WORLD);
    start = MPI_Wtime();
    Prepare();
    dura = MPI_Wtime() - start;

    if (!Operator::OK())
      return;

    MPI_Reduce(&ops_, &total_ops, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&err_, &total_err, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&dura, &global_dura, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    LogOpCount(total_ops, total_err, global_dura);

    // Main Phase
    err_ = ops_ = 0;
    LogStatus("Main Phase ...");
    MPI_Barrier(MPI_COMM_WORLD);
    start = MPI_Wtime();
    Run();
    dura = MPI_Wtime() - start;

    if (!Operator::OK())
      return;

    ReportLocalResult(ops_, Operator::GetDura());
    MPI_Reduce(&ops_, &total_ops, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&err_, &total_err, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&dura, &global_dura, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    LogOpCount(total_ops, total_err, global_dura);

    // Post Run
    err_ = ops_ = 0;
    LogStatus("Post Run ...");
    MPI_Barrier(MPI_COMM_WORLD);
    start = MPI_Wtime();
    PostRun();
    dura = MPI_Wtime() - start;

    if (!Operator::OK())
     return;

    ReportLocalResult(ops_, Operator::GetDura());
    MPI_Reduce(&ops_, &total_ops, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&err_, &total_err, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&dura, &global_dura, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    LogOpCount(total_ops, total_err, global_dura);

    // Clean Phase
    err_ = ops_ = 0;
    LogStatus("Clean Phase ...");
    MPI_Barrier(MPI_COMM_WORLD);
    start = MPI_Wtime();
    Clean();
    dura = MPI_Wtime() - start;

    if (!Operator::OK())
      return;

    ReportLocalResult(ops_, Operator::GetDura());
    MPI_Reduce(&ops_, &total_ops, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&err_, &total_err, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&dura, &global_dura, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    LogOpCount(total_ops, total_err, global_dura);
  }

  virtual void IOFailed(const char* op) {
    err_++;
  }
  virtual void IOPerformed(const char* op) {
    ops_++;
  }

 private:

  void Prepare() {
    Operator op(task_);
    op.Prepare();
  }

  void PreRun() {
    Operator op(task_);
    op.PreRun();
  }

  void Run() {
    Operator op(task_);
    op.Run();
  }

  void PostRun() {
    Operator op(task_);
    op.PostRun();
  }

  void Clean() {
    Operator op(task_);
    op.Clean();
  }

  void CheckPrecondition() {
    Operator op(task_);
    op.CheckPrecondition();
  }

  inline void LogStatus(const char* msg) {
    if (my_rank_ == 0) {
      printf("%s\n", msg);
    }
  }

  inline void LogOpCount(int ops, int err, double dura) {
    if (my_rank_ == 0) {
      printf("-- Performed %d ops in %.3f seconds: %d succ, %d fail\n",
        ops + err, dura, ops, err);
    }
  }

  inline void ReportLocalResult(int count, double dura) {
    if (FLAGS_verbose >= 1) {
      if (count > 0 && dura > 0) {
        printf("# Proc #%d completed with %.3f ops/s in avg\n",
          my_rank_, count / dura);
      }
    }
  }

  int ops_;
  int err_;
  int my_rank_;
  int comm_sz_;
  IOTask* task_;
};

} /* anonymous namespace */

using ::indexfs::GetDefaultLogDir;
using ::indexfs::SetVersionString;
using ::indexfs::SetUsageMessage;
using ::indexfs::ParseCommandLineFlags;

int main(int argc, char** argv) {
  FLAGS_logfn = "indexfs_iotest";
  FLAGS_log_dir = GetDefaultLogDir();
  SetVersionString("1.0");
  SetUsageMessage("IndexFS's IO Benchmark");
  ParseCommandLineFlags(&argc, &argv, true);

  IOTestDriver d(&argc, &argv);
  try {
    d.Exexcute();
  } catch (IOError &err) {
    if (err.dir_>= 0) {
      if (err.file_ >= 0) {
         fprintf(stderr, "error performing %s at dir %d file %d: %s\n",
           err.op_.c_str(), err.dir_, err.file_, err.cause_.c_str());
      } else {
        fprintf(stderr, "error performing %s at dir %d: %s\n",
           err.op_.c_str(), err.dir_, err.cause_.c_str());
      }
    } else if (err.path_.length() > 0) {
      fprintf(stderr, "error performing %s at %s: %s\n",
          err.op_.c_str(), err.path_.c_str(), err.cause_.c_str());
    } else {
      fprintf(stderr, "error performing %s: %s\n", err.op_.c_str(), err.cause_.c_str());
    }
  }
  return 0;
}
