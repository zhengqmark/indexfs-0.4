// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/exec_srv.h"
#include "server/index_server.h"

namespace indexfs {

class SplitGuard {
 public:
  explicit SplitGuard(BulkExtractor* ext)
      :ext_(ext) {
  }
  ~SplitGuard() {
    delete ext_;
  }

 private:
  BulkExtractor* ext_;

  // No copying allowed
  SplitGuard(const SplitGuard&);
  SplitGuard& operator=(const SplitGuard&);
};

struct SplitTask: virtual public Runnable {
  virtual ~SplitTask() { }
  int64_t dir_id;
  int16_t index;
  IndexServer* idx_srv;
  void Run() {
    idx_srv->DoSplit(dir_id, index);
  }
};

} // namespace indexfs