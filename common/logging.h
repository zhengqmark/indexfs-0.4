// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_COMMON_LOGGING_H_
#define _INDEXFS_COMMON_LOGGING_H_

#include <string>
#include <sstream>
#include <glog/logging.h>

namespace indexfs {

using google::SetUsageMessage;
using google::SetVersionString;
using google::ParseCommandLineFlags;

struct Logger {

  // Ensure all buffered log entries get
  // flushed to the underlying storage system.
  //
  static void FlushLogFiles();

  // Shutdown the log sub-system.
  //
  static void Shutdown();

  // Open the log sub-system. Use the specified
  // file name to create the underlying log file.
  //
  static void Initialize(const char* log_fname);
};

} /* namespace indexfs */

#endif /* _INDEXFS_COMMON_LOGGING_H_ */
