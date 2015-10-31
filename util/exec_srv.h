// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_UTIL_EXEC_SERVICE_H_
#define _INDEXFS_UTIL_EXEC_SERVICE_H_

namespace indexfs {

class Runnable {
 public:
  virtual ~Runnable() { }
  virtual void Run() = 0;
};

class ExecService {
 public:
  ExecService() { }
  virtual ~ExecService() { }
  virtual void SubmitTask(Runnable* task) = 0;

  static ExecService* Default();

 private:
  // No copying allowed
  ExecService(const ExecService&);
  ExecService& operator=(const ExecService&);
};

} // namespace indexfs

#endif /* _INDEXFS_UTIL_EXEC_SERVICE_H_ */
