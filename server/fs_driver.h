// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_SERVER_FSDRIVER_H_
#define _INDEXFS_SERVER_FSDRIVER_H_

#include "thrift/MetadataIndexService.h"

#include "common/options.h"
#include "common/common.h"
#include "common/config.h"
#include "common/logging.h"

#include "util/monitor.h"
#include "util/monitor_thread.h"

namespace indexfs {

class ServerDriver {
 public:
  static ServerDriver* NewIndexFSDriver(Env* env, Config* config);
  virtual ~ServerDriver() { }
  virtual void Start() = 0;
  virtual void Shutdown() = 0;
  virtual void OpenServer() = 0;
  virtual void PrepareContext() = 0;
  virtual void SetupMonitoring() = 0;

 protected:
  Env* env_;
  Config* config_;
  ServerDriver(Env* env, Config* config);
};

enum MetadataServerOps {
  oAccess,
  oRenew,
  oGetattr,
  oMknod,
  oMkdir,
  oCreateEntry,
  oCreateZeroth,
  oChmod,
  oChown,
  oRemove,
  oRename,
  oReaddir,
  oReadBitmap,
  oUpdateBitmap,
  oSplit,
  oInsertSplit,
  oOpen,
  oRead,
  oWrite,
  oClose,
  kNumSrvOps
};

extern Monitor* CreateMonitorForServer(int server_id);

} // namespace indexfs

#endif /* _INDEXFS_SERVER_FSDRIVER_H_ */
