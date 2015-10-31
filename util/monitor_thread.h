// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_UTIL_MONITORTHREAD_H_
#define _INDEXFS_UTIL_MONITORTHREAD_H_

#include "util/monitor.h"
#include "leveldb/env.h"
#include "leveldb/util/socket.h"

namespace indexfs {

using leveldb::Env;
using leveldb::UDPSocket;
using leveldb::SocketException;

class MonitorThread {
 public:

  MonitorThread(Monitor *monitor, int frequency = 2);
  virtual ~MonitorThread();

  void Start();
  void Shutdown() { done_ = true; }

 private:

  void SendMetrics();
  static void* Run(void* arg);

  volatile bool done_;
  pthread_t tid_;
  const int frequency_;
  UDPSocket socket_;
  Monitor* const monitor_;

  // No copying allowed
  MonitorThread(const MonitorThread&);
  MonitorThread& operator=(const MonitorThread&);
};

} // namespace indexfs

#endif /* _INDEXFS_UTIL_MONITORTHREAD_H_ */
