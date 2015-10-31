// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_INDEX_SERVER_H_
#define _INDEXFS_INDEX_SERVER_H_

#include "ipc/rpc.h"
#include "common/leasectrl.h"
#include "util/exec_srv.h"
#include "server/fs_driver.h"
#include "server/index_ctx.h"

namespace indexfs {

class IndexServer: virtual public MetadataIndexServiceIf {
 public:

  IndexServer(IndexContext* ctx, Monitor* monitor, RPC* rpc);

  virtual ~IndexServer();

  void Ping() { }
  void FlushDB();

  void Getattr(StatInfo& _return, const OID& obj_id);
  void Renew(LookupInfo& _return, const OID& obj_id);
  void Access(LookupInfo& _return, const OID& obj_id);
  void Readdir(EntryList& _return, i64 dir_id, i16 index);

  void Mknod(const OID& obj_id, i16 perm);
  void Mknod_Bulk(const OIDS& obj_ids, i16 perm);
  bool Chmod(const OID& obj_id, i16 perm);
  bool Chown(const OID& obj_id, i16 uid, i16 gid);
  void Mkdir_Presplit(const OID& obj_id, i16 perm,
      i16 hint_srv1, i16 hint_srv2);
  void Mkdir(const OID& obj_id, i16 perm, i16 hint_srv1, i16 hint_srv2);

  void CreateZeroth(i64 dir_id, i16 zeroth_server);
  void ReadBitmap(std::string& _return, i64 dir_id);
  void UpdateBitmap(i64 dir_id, const std::string& dmap_data);

  void DoSplit(i64 dir_id, i16 index);
  void InsertSplit(i64 dir_id, i16 parent_index, i16 child_index,
      const std::string& path_split_files, const std::string& dmap_data,
      i64 min_seq, i64 max_seq, i64 num_entries);

 private:
  Mutex mtx_;
  Monitor* monitor_;
  IndexContext* ctx_;
  RPC* rpc_;
  LeaseTable* lease_table_;
  ExecService* exec_srv_;

  void Lookup(const OID& oid, i16 index, DirGuard& dir_guard,
      LookupInfo* info);
  void SetDirAttr(const OID& oid, i16 index, DirGuard& dir_guard,
      const StatInfo& info);
  void TriggerDirSplitting(i64 dir_id, i16 index, DirGuard& dir_guard);

  // No copying allowed
  IndexServer(const IndexServer&);
  IndexServer& operator=(const IndexServer&);
};

} // namespace indexfs

#endif /* _INDEXFS_INDEX_SERVER_H_ */
