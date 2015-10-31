// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_CLIENT_INTERFACE_H_
#define _INDEXFS_CLIENT_INTERFACE_H_

#include <sys/stat.h>

#include "common/config.h"
#include "common/common.h"

namespace indexfs {

class Client;
class ClientFactory {
 public:
  // The default client implementation will send each
  // file system operation to the server for synchronous execution.
  static Client* GetClient(Config* config);

  // Different from traditional file system clients,
  // a batch client is able to execute file system operations locally
  // at client side (under the protection of a server issued lease on a
  // subtree). To commit changes to the global namespace, the client
  // later bulk insert all buffered file system mutations (represented by
  // a set of sstables) to the server, who then checks and merges these
  // updates in a single batch (using a single sstable bulk insertion).
  static Client* GetBatchClient(Config* config);
};

class FileHandle {
 public:

  virtual ~FileHandle();

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const;

  Status Write(uint64_t offset, const Slice& data);

 private:
  int fd_;
  int mode_;
  StatInfo stat_;

  FileHandle() : fd_(-1), mode_(0) { }

  friend class Client;
  // No copying allowed
  FileHandle(const FileHandle&);
  FileHandle& operator=(const FileHandle&);
};

/* -----------------------------------------------------------------
 * Main Client Interface
 * -----------------------------------------------------------------
 */
class Client {
 protected:
  // No public creates
  explicit Client() { }

 private:
  // No copying allowed
  Client(const Client&);
  Client& operator=(const Client&);

 public:

  /* Miscellaneous Performance Benchmarking Utilities */
  virtual void PrintMeasurements(FILE* output) = 0;

  virtual ~Client() { }

  virtual Status Init() = 0;
  virtual Status Noop() = 0;
  virtual Status Dispose() = 0;
  virtual Status FlushWriteBuffer() = 0;

  virtual Status Mknod(const std::string& path, i16 perm) = 0;
  virtual Status Mknod_Flush() = 0;
  virtual Status Mknod_Buffered(const std::string& path, i16 perm) = 0;
  virtual Status Mkdir(const std::string& path, i16 perm) = 0;
  virtual Status Mkdir_Presplit(const std::string& path, i16 perm) = 0;
  virtual Status Getattr(const std::string& path, StatInfo* info) = 0;
  virtual Status Chmod(const std::string& path, i16 perm, bool* is_dir) = 0;
  virtual Status Chown(const std::string& path, i16 uid, i16 gid, bool* is_dir) = 0;

  virtual Status AccessDir(const std::string& path) = 0;
  virtual Status ListDir(const std::string& path,
      NameList* names, StatList stats) = 0;
  virtual Status ReadDir(const std::string& path, NameList* names) = 0;

  virtual Status Close(FileHandle* handle) = 0;
  virtual Status Open(const std::string& path, int mode, FileHandle** handle) = 0;
};

} /* namespace indexfs */

#endif /* _INDEXFS_CLIENT_INTERFACE_H_ */
