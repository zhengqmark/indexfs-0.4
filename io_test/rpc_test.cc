// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdlib.h>
#include <unistd.h>

#include "io_task.h"
#include <gflags/gflags.h>

namespace indexfs { namespace mpi {

namespace {

class RPCTest: public IOTask {

  int PrintSettings() {
    return printf("Test Settings:\n"
      "total processes -> %d\n"
      "backend_fs -> %s\n"
      "run_id -> %s\n",
      comm_sz_,
      FLAGS_fs.c_str(),
      FLAGS_run_id.c_str());
  }

  static inline
  void RPC_GO(IOClient* IO, IOListener* L) {
    IO->Noop();
    if (L != NULL) L->IOPerformed("rpc");
  }

 public:

  RPCTest(int my_rank, int comm_sz)
    : IOTask(my_rank, comm_sz) {
  }

  virtual void Prepare() {
    Status s = IO_->Init();
    if (!s.ok()) {
      throw IOError("init", s.ToString());
    }
  }

  virtual void Run() {
    long num_rpc = 1000 * 1000;
    while (--num_rpc >= 0) RPC_GO(IO_, listener_);
  }

  virtual void Clean() {
    long num_rpc = 1000 * 1000;
    while (--num_rpc >= 0) RPC_GO(IO_, listener_);
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

IOTask* IOTaskFactory::GetRPCTestTask(int my_rank, int comm_sz) {
  return new RPCTest(my_rank, comm_sz);
}

} /* namespace mpi */ } /* namespace indexfs */
