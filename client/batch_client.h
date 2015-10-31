// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_BATCH_CLIENT_H_
#define _INDEXFS_BATCH_CLIENT_H_

#include "common/config.h"
#include "common/gigaidx.h"
#include "metadb/metadb.h"
#include "client/client.h"
#include "client/client_impl.h"

DECLARE_bool(batch_client_remote_read_mode);

namespace indexfs {

class BatchClient: virtual public Client {
 public:

  BatchClient(Config* config, Env* env);

  virtual ~BatchClient();

  Status Init();
  Status Noop();
  Status Dispose();
  Status FlushWriteBuffer();

  void PrintMeasurements(FILE* output) { }

  Status Mknod(const std::string& path, i16 perm);
  Status Mknod_Flush();
  Status Mknod_Buffered(const std::string& path, i16 perm);
  Status Mkdir(const std::string& path, i16 perm);
  Status Mkdir_Presplit(const std::string& path, i16 perm);
  Status Chmod(const std::string& path, i16 perm, bool* is_dir);
  Status Chown(const std::string& path, i16 uid, i16 gid, bool* is_dir);

  Status Getattr(const std::string& path, StatInfo* info);
  Status AccessDir(const std::string& path);
  Status ListDir(const std::string& path,
     NameList* names, StatList stats);
  Status ReadDir(const std::string& path, NameList* names);

  Status Close(FileHandle* handle);
  Status Open(const std::string& path, int mode, FileHandle** handle);

 private:
  RPC* rpc_;
  MetaDB* mdb_;

  Env* env_;
  Config* config_;

  DirIndexPolicy* dir_policy_;
  DirIndex* dir_idx_;
  std::string last_dir_;
  Status ResolvePath(const std::string& path, OID* oid, int16_t* zeroth_server);

  // No copy allowed
  BatchClient(const BatchClient&);
  BatchClient& operator=(const BatchClient&);
};

} /* namespace indexfs */

#endif /* _INDEXFS_BATCH_CLIENT_H_ */
