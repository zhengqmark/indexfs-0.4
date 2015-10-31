// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_CLIENT_IMPL_H_
#define _INDEXFS_CLIENT_IMPL_H_

#include "ipc/rpc.h"
#include "util/str_hash.h"
#include "client/client.h"

#include "common/logging.h"
#include "common/dirlock.h"
#include "common/dirguard.h"
#include "common/lookupcache.h"

namespace indexfs {

// Utility class for performing RPC operations.
// Note that each RPCEngine object holds a short-term reference to a
// directory index object actively cached in the directory index cache.
//
// REQUIRES: a referenced directory index object cannot be destroyed
//           during the entire lift-cycle of a RPCEngine object.
//
class RPCEngine {
 public:

  explicit RPCEngine(DirIndex* dir_idx, RPC* rpc) :
      rpc_(rpc), dir_idx_(dir_idx) {
  }

  ~RPCEngine() { }

  Status Access(const OID& oid, LookupInfo* info);
  Status Renew(const OID& oid, LookupInfo* info);

  Status Mknod(const OID& oid, i16 perm);
  Status Mknods(const OIDS& oids, int srv_id, i16 perm);
  Status Mkdir(const OID& oid, i16 perm, i16 hint_srv);
  Status Msdir(const OID& oid, i16 perm, i16 hint_srv);
  Status Chmod(const OID& oid, i16 perm, bool* is_dir);
  Status Chown(const OID& oid, i16 uid, i16 gid, bool* is_dir);

  Status Getattr(const OID& oid, StatInfo* info);
  Status ReadDir(i64 dir_id, NameList* names);
  Status ListDir(i64 dir_id, NameList* names, StatList* stats);

 private:
  int srv_id_;
  RPC* rpc_;
  DirIndex* dir_idx_;

  // Max number of server redirection allowed
  enum { kNumRedirect = 10 };

  // No copying allowed
  RPCEngine(const RPCEngine&);
  RPCEngine& operator=(const RPCEngine&);
};

class ClientImpl: virtual public Client {
 public:

  ClientImpl(Config* config, Env* env);

  virtual ~ClientImpl();

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
  DirIndexCache* index_cache_;
  DirIndexPolicy* index_policy_;

  Env* env_;
  Config* config_;
  LookupCache* lookup_cache_;

  static int RandomServer(const std::string& path) {
    return GetStrHash(path.data(), path.size(), 0)
            % DEFAULT_MAX_NUM_SERVERS;
  }

  struct MknodBuffer;
  std::map<int64_t, MknodBuffer*> mknod_bufmap_;
  typedef std::map<int64_t, MknodBuffer*>::iterator BufferIter;

  Status FlushBuffer(MknodBuffer* buffer);
  Status Lookup(const OID& oid, int16_t zeroth_server,
      LookupInfo* info, bool is_renew);
  DirIndexEntry* FetchIndex(int64_t dir_id, int16_t zeroth_server);
  Status ResolvePath(const std::string& path, OID* oid, int16_t* zeroth_server);

  // No copy allowed
  ClientImpl(const ClientImpl&);
  ClientImpl& operator=(const ClientImpl&);
};

} /* namespace indexfs */

#endif /* _INDEXFS_CLIENT_IMPL_H_ */
