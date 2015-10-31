// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_MPI_IO_CLIENT_H_
#define _INDEXFS_MPI_IO_CLIENT_H_

#include "common/common.h"
#include "leveldb/util/histogram.h"

#include <gflags/gflags.h>

namespace indexfs { namespace mpi {

// Immutable string
typedef const std::string Path;

// Abstract FS Interface
class IOClient;

DECLARE_bool(bulk_insert); // For indexfs only
DECLARE_bool(batch_creates); // For indexfs only

// Flags not visible in non-debug builds
#ifndef NDEBUG
DECLARE_bool(print_ops); // Print operation traces to STDOUT
#endif

//////////////////////////////////////////////////////////////////////////////////
// IO CLIENT FACTORY
//

struct IOClientFactory {
  // Using IndexFS Interface
  static IOClient* GetIndexFSClient(int rank, int size, const std::string &id);
  // Using Standard POSIX API (or FUSE)
  static IOClient* GetLocalFSClient(int rank, const std::string &id);
  // Using OrangeFS (a.k.a. PVFS2) Interface
  static IOClient* GetOrangeFSClient(int rank, const std::string &id);
};

//////////////////////////////////////////////////////////////////////////////////
// IO CLIENT INTERFACE
//

class IOClient {
 protected:
  IOClient() {}

  // To be called by IO factories only
  static IOClient* NewLocalFSClient();
  static IOClient* NewOrangeFSClient();
  static IOClient* NewIndexFSClient(int my_rank, int comm_sz);

 public:
  virtual ~IOClient();

  virtual Status Init             ()                                 = 0;
  virtual Status Dispose          ()                                 = 0;

  virtual Status NewFile          (int dno, int fno, const std::string &prefix);
  virtual Status GetAttr          (int dno, int fno, const std::string &prefix);
  virtual Status MakeDirectory    (int dno, const std::string &prefix);
  virtual Status SyncDirectory    (int dno, const std::string &prefix);

  virtual Status NewFile          (Path &path)                       = 0;
  virtual Status MakeDirectory    (Path &path)                       = 0;
  virtual Status MakeDirectories  (Path &path)                       = 0;
  virtual Status SyncDirectory    (Path &path)                       = 0;
  virtual Status ResetMode        (Path &path)                       = 0;
  virtual Status GetAttr          (Path &path)                       = 0;
  virtual Status ListDirectory    (Path &path)                       = 0;
  virtual Status Remove           (Path &path)                       = 0;
  virtual Status Rename           (Path &source, Path &destination)  = 0;

  virtual void Noop() {}
  virtual void PrintMeasurements(FILE* output) {}
  virtual Status FlushWriteBuffer() { return Status::OK(); }

 private:
  friend class IOClientFactory;
  // No copying allowed
  IOClient(const IOClient&);
  IOClient& operator=(const IOClient&);
};

//////////////////////////////////////////////////////////////////////////////////
// IO MEASUREMENT CONTROL INTERFACE
//

DECLARE_string(tsdb_ip); // TSDB's IP address
DECLARE_int32(tsdb_port); // TSDB's UDP port

struct IOMeasurements {
  static void EnableMonitoring(IOClient* cli, bool enable);
  static void Reset(IOClient* cli);
  static void PrintMeasurements(IOClient* cli, FILE* output);
};

} /* namespace mpi */ } /* namespace indexfs */

#endif /* _INDEXFS_MPI_IO_CLIENT_H_ */
