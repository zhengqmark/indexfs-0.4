// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef SPLIT_THREAD_H_
#define SPLIT_THREAD_H_

#include <deque>

#include "server/server.h"

namespace indexfs {

class SplitThread {
public:
  SplitThread(Monitor* monitor);

  virtual ~SplitThread();

  void Start();

  void AddSplitTask(int dir_id, int index);

  void Stop();

private:
  static void* Run(void* arg);

  void ExecuteThread();

  MetadataServer* server_;
  Monitor* monitor_;

  pthread_mutex_t mu_;
  pthread_cond_t signal_;
  pthread_t thread_;
  bool started_thread_;
  bool done_;

  struct SplitItem {
    SplitItem(int did, int dindex) : dir_id(did), index(dindex) {}
    int dir_id, index;
  };
  typedef std::deque<SplitItem> SplitQueue;
  SplitQueue queue_;
};

} // namespace indexfs

#endif /* SPLIT_THREAD_H_ */
