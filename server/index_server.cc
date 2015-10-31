// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <fcntl.h>
#include <sys/stat.h>

#include "server/fs_errors.h"
#include "server/index_server.h"

namespace indexfs {

IndexServer::IndexServer(IndexContext* ctx, Monitor* monitor, RPC* rpc) :
    monitor_(monitor), ctx_(ctx), rpc_(rpc) {
  lease_table_ = new LeaseTable();
  exec_srv_ = ExecService::Default();
}

IndexServer::~IndexServer() {
  delete exec_srv_;
  delete lease_table_;
}

void IndexServer::FlushDB() {
  MaybeThrowException(ctx_->Flush());
}

namespace {
using apache::thrift::TException;
static
void RPC_CreateZeroth(RPC* rpc, int srv,
        i64 dir_id, i16 zero_srv) {
  MutexLock lock(rpc->GetMutex(srv));
  try {
    rpc->GetClient(srv)->CreateZeroth(dir_id, zero_srv);
  } catch (TException &tx) {
    std::string err_msg = tx.what();
    LOG(ERROR) << "RPC execution [" << __func__ << "] failed: " << err_msg;
    SERVER_INTERNAL_ERROR(err_msg);
  }
}
}

namespace {
using apache::thrift::TException;
static
void Local_CreatePartition(IndexContext* ctx,
        i64 dir_id, i16 idx, const std::string& dmap_data) {
  ctx->InstallSplit_Unlocked(dir_id,
          std::string(), dmap_data, idx, idx, 0, 0, 0);
}
static
void RPC_CreateParition(RPC* rpc, int srv,
        i64 dir_id, i16 idx, const std::string& dmap_data) {
  MutexLock lock(rpc->GetMutex(srv));
  try {
    rpc->GetClient(srv)->InsertSplit(dir_id,
            idx, idx, std::string(), dmap_data, 0, 0, 0);
  } catch (TException &tx) {
    std::string err_msg = tx.what();
    LOG(ERROR) << "RPC execution [" << __func__ << "] failed: " << err_msg;
    SERVER_INTERNAL_ERROR(err_msg);
  }
}
}

// Creates the zeroth partition for a given directory.
//
// REQUIRES: the directory being created must be new.
// REQUIRES: the specified "zeroth_server" must map to this server.
//
void IndexServer::CreateZeroth(i64 dir_id, i16 zeroth_server) {
  MonitorHelper helper(oCreateZeroth, monitor_);
  MaybeThrowException(ctx_->InstallZeroth(dir_id, zeroth_server));
}


// Prepares the control data for a given directory
// and performs a series of local consistency checks:
//
// EXCEPTION: if we don't have the index for the given directory
// NOTE: by ``index'' we means the directory partition map
// EXCEPTION: if we are not responsible for the specified object name
//
#define _DIR_GUARD(dir_id, obj_name)                                      \
  DirGuard::DirData dir_data = ctx_->FetchDir(dir_id);                    \
  MaybeThrowUnknownDirException(dir_data);                                \
  DirGuard dir_guard(dir_data);                                           \
  DirLock lock(&dir_guard);                                               \
  if (!obj_name.empty()) {                                                \
    obj_idx = dir_guard.GetIndex(obj_name);                               \
    MaybeThrowRedirectException(dir_guard, obj_idx, ctx_->GetMyRank());   \
  }

// Obtain the directory lock.
//
// INPUT: directory id (int64_t)
//
#define DIR_LOCK(dir_id)            \
  int obj_idx = 0;                  \
  _DIR_GUARD(dir_id, std::string()) \

// Obtain the object lock.
//
// INPUT: object id structure (struct OID)
//
// All object locks are currently represented by parent directory locks.
//
#define OBJ_LOCK(obj_id) _DIR_GUARD(obj_id.dir_id, obj_id.obj_name)


// Performs client-side path lookup entry renewal.
//
void IndexServer::Renew(LookupInfo& _return, const OID& obj_id) {
  MonitorHelper helper(oRenew, monitor_);
  int obj_idx = 0;
  OBJ_LOCK(obj_id);
  Lookup(obj_id, obj_idx, dir_guard, &_return);
}

// Validates name existence and access permissions
// in order to release a new path lookup entry to the requesting client.
//
void IndexServer::Access(LookupInfo& _return, const OID& obj_id) {
  MonitorHelper helper(oAccess, monitor_);
  int obj_idx = 0;
  OBJ_LOCK(obj_id);
  Lookup(obj_id, obj_idx, dir_guard, &_return);
}

// Retrieves the attributes for a given file system object.
//
// REQUIRES: the specified name must map to an existing object.
//
void IndexServer::Getattr(StatInfo& _return,
        const OID& obj_id) {
  MonitorHelper helper(oGetattr, monitor_);
  int obj_idx = 0;
  OBJ_LOCK(obj_id);
  MaybeThrowException(ctx_->Getattr_Unlocked(obj_id, obj_idx, &_return));
}

// Creates a new file under a given directory.
//
// REQUIRES: the specified file name must not collide with existing names.
//
void IndexServer::Mknod(const OID& obj_id, i16 perm) {
  MonitorHelper helper(oMknod, monitor_);
  int obj_idx = 0;
  OBJ_LOCK(obj_id);

  // TASK-I: link the new file
  MaybeThrowException(ctx_->Mknod_Unlocked(obj_id, obj_idx, perm));

  // TASK-II: increase directory size
  DLOG_ASSERT(dir_guard.HasPartitionData(obj_idx));
  dir_guard.InceaseAndGetPartitionSize(obj_idx, 1);

  TriggerDirSplitting(obj_id.dir_id, obj_idx, dir_guard);
}

// Creates multiple files under a given directory
// as an atomic operation with all-or-nothing semantics.
//
// REQUIRES: all specified file names must not collide with existing names.
//
void IndexServer::Mknod_Bulk(const OIDS& obj_ids, i16 perm) {
#if 1
  OID obj_id;
  obj_id.dir_id = obj_ids.dir_id;
  obj_id.path_depth = obj_ids.path_depth;
  std::vector<std::string>::const_iterator it;
  for (it = obj_ids.obj_names.begin(); it != obj_ids.obj_names.end(); ++it) {
    obj_id.obj_name = *it;
    Mknod(obj_id, perm);
  }
#else
  MonitorHelper helper(oMknod, monitor_);
  DIR_LOCK(obj_ids.dir_id);
  std::set<int> idx_set;
  OID obj_id;
  obj_id.dir_id = obj_ids.dir_id;
  obj_id.path_depth = obj_ids.path_depth;

  std::vector<std::string>::const_iterator it = obj_ids.obj_names.begin();
  for (; it != obj_ids.obj_names.end(); ++it) {
    obj_id.obj_name = *it;
    obj_idx = dir_guard.GetIndex(obj_id.obj_name);
    MaybeThrowRedirectException(dir_guard, obj_idx, ctx_->GetMyRank());
    MaybeThrowException(ctx_->Mknod_Unlocked(obj_id, obj_idx, perm));

    idx_set.insert(obj_idx);
    DLOG_ASSERT(dir_guard.HasPartitionData(obj_idx));
    dir_guard.InceaseAndGetPartitionSize(obj_idx, 1);
  }

  std::set<int>::iterator idx_it = idx_set.begin();
  for (; idx_it != idx_set.end(); ++idx_it) {
    TriggerDirSplitting(obj_ids.dir_id, *idx_it, dir_guard);
  }
#endif
}

// Creates a new directory under a given parent directory.
// The requesting client should suggest 2 servers
// so that one of the two may become the home server for the new directory.
//
// REQUIRES: hint servers must cover the entire virtual server space.
// REQUIRES: the specified directory name must not collide with existing names.
//
void IndexServer::Mkdir(const OID& obj_id,
        i16 perm, i16 hint_server1, i16 hint_server2) {
  MonitorHelper helper(oMkdir, monitor_);
  int obj_idx = 0;
  OBJ_LOCK(obj_id);

  // TASK-I: allocate a new inode number
  int64_t new_inode = ctx_->NextInode();

  // TASK-II: link the new directory
  MaybeThrowException(ctx_->Mkdir_Unlocked(obj_id, obj_idx,
          perm, new_inode, hint_server1));

  // TASK-III: install the zeroth partition
  int home_srv = (U16INT(hint_server1)) % ctx_->GetNumServers();
  if (home_srv == ctx_->GetMyRank()) {
    CreateZeroth(new_inode, hint_server1);
  } else {
    RPC_CreateZeroth(rpc_, home_srv, new_inode, hint_server1);
  }

  // TASK-IV: increase the directory size
  DLOG_ASSERT(dir_guard.HasPartitionData(obj_idx));
  dir_guard.InceaseAndGetPartitionSize(obj_idx, 1);

  TriggerDirSplitting(obj_id.dir_id, obj_idx, dir_guard);
}

// Creates a new directory under a given parent directory
// and immediately splits this new directory to all metadata servers.
//
// REQUIRES: hint servers must cover the entire virtual server space.
// REQUIRES: the specified directory name must not collide with existing names.
//
void IndexServer::Mkdir_Presplit(const OID& obj_id,
        i16 perm, i16 hint_server1, i16 hint_server2) {
  MonitorHelper helper(oMkdir, monitor_);
  int obj_idx = 0;
  OBJ_LOCK(obj_id);

  // TASK-I: allocate a new inode number
  int64_t new_inode = ctx_->NextInode();

  // TASK-II: link the new directory
  MaybeThrowException(ctx_->Mkdir_Unlocked(obj_id, obj_idx,
          perm, new_inode, hint_server1));

  // TASK-III: initialize directory index
  DirIndex* dir_idx = ctx_->NewDirIndex(new_inode, hint_server1);
  for (int i = 0; i < ctx_->GetNumServers(); ++i) {
    dir_idx->SetBit(i);
  }

  const Slice& slice = dir_idx->ToSlice();
  std::string dmap_data(slice.data(), slice.size());
  delete dir_idx;

  // TASK-IV: install all partitions
  for (int i = 0; i < ctx_->GetNumServers(); ++i) {
    int srv_id = DirIndex::MapIndexToServer(i,
            U16INT(hint_server1), ctx_->GetNumServers());
    if (srv_id == ctx_->GetMyRank()) {
      Local_CreatePartition(ctx_, new_inode, i, dmap_data);
    } else {
      RPC_CreateParition(rpc_, srv_id, new_inode, i, dmap_data);
    }
  }

  // TASK-V: increase the directory size
  DLOG_ASSERT(dir_guard.HasPartitionData(obj_idx));
  dir_guard.InceaseAndGetPartitionSize(obj_idx, 1);

  TriggerDirSplitting(obj_id.dir_id, obj_idx, dir_guard);
}

// Changes the access permission of a given file system object.
//
// REQUIRES: the specified name must map to an existing object.
// RETURNS: ture if the specified file system object is a directory.
//
bool IndexServer::Chmod(const OID& obj_id, i16 perm) {
  MonitorHelper helper(oChmod, monitor_);
  int obj_idx = 0;
  OBJ_LOCK(obj_id);

  // TASK-I: obtain object's current attributes
  StatInfo stat;
  MaybeThrowException(ctx_->Getattr_Unlocked(obj_id, obj_idx, &stat));

  // TASK-II: reset access permission
  if (S_ISDIR(stat.mode)) {
    SetDirAttr(obj_id, obj_idx, dir_guard, stat);
    return true;
  } else {
    MaybeThrowException(ctx_->Setattr_Unlocked(obj_id, obj_idx, stat));
    return false;
  }
}

// Changes the ownership of a given file system object.
//
// REQUIRES: the specified name must map to an existing object.
// RETURNS: true if the specified file system object is a directory.
//
bool IndexServer::Chown(const OID& obj_id, i16 uid, i16 gid) {
  MonitorHelper helper(oChown, monitor_);
  int obj_idx = 0;
  OBJ_LOCK(obj_id);

  // TASK-I: obtain object's current attributes
  StatInfo stat;
  MaybeThrowException(ctx_->Getattr_Unlocked(obj_id, obj_idx, &stat));

  // TASK-II: reset ownership
  if (S_ISDIR(stat.mode)) {
    SetDirAttr(obj_id, obj_idx, dir_guard, stat);
    return true;
  } else {
    MaybeThrowException(ctx_->Setattr_Unlocked(obj_id, obj_idx, stat));
    return false;
  }
}

void IndexServer::SetDirAttr(const OID& obj_id, i16 obj_idx,
        DirGuard& dir_guard, const StatInfo& info) {
  dir_guard.Lock_AssertHeld();
  DLOG_ASSERT(S_ISDIR(info.mode));
  LeaseEntry* lease = NULL;
  lease = lease_table_->Get(obj_id);
  if (lease == NULL) {
    lease = lease_table_->New(obj_id, info);
  }
  LeaseGuard lease_guard(lease);
  WriteLock lock(lease, &dir_guard, ctx_->GetEnv());
  MaybeThrowException(ctx_->Setattr_Unlocked(obj_id, obj_idx, info));
  DLOG_ASSERT(lease->inode_no == info.id);
  lease->uid = info.uid;
  lease->gid = info.gid;
  lease->perm = info.mode;
  DLOG_ASSERT(lease->zeroth_server == info.zeroth_server);
}

void IndexServer::Lookup(const OID& obj_id, i16 obj_idx,
        DirGuard& dir_guard, LookupInfo* info) {
  dir_guard.Lock_AssertHeld();
  LeaseEntry* lease = NULL;
  lease = lease_table_->Get(obj_id);
  if (lease == NULL) {
    StatInfo stat;
    MaybeThrowException(ctx_->Getattr_Unlocked(obj_id, obj_idx, &stat));
    if (!S_ISDIR(stat.mode)) {
      throw DirectoryExpectedError();
    }
    lease = lease_table_->New(obj_id, stat);
  }
  LeaseGuard lease_guard(lease);
  {
    // Automatically update lease due when disposed
    ReadLock lock(lease, &dir_guard, ctx_->GetEnv());
    info->id = lease->inode_no;
    info->uid = lease->uid;
    info->gid = lease->gid;
    info->perm = lease->perm;
    info->zeroth_server = lease->zeroth_server;
  }
  // Set updated lease due
  info->lease_due = LeaseTable::FetchLeaseDue(lease);
}

namespace {
class ScannerGuard {
 public:
  explicit ScannerGuard(DirScanner* ds)
      :ds_(ds) {
  }
  ~ScannerGuard() {
    delete ds_;
  }

 private:
  DirScanner* ds_;

  // No copying allowed
  ScannerGuard(const ScannerGuard&);
  ScannerGuard& operator=(const ScannerGuard&);
};
}

// Scans a given directory partition.
//
// REQUIRES: the specified directory partition must exist.
// REQUIRES: the specified directory id must map to an existing directory.
//
void IndexServer::Readdir(EntryList& _return,
        i64 dir_id, i16 index) {
  MonitorHelper helper(oReaddir, monitor_);
  DIR_LOCK(dir_id);
  DirScanner* ds = NULL;
  ds = ctx_->CreateDirScanner(dir_id, index, std::string());
  {
    ScannerGuard scanner_guard(ds);
    std::string name;
    for (; ds->Valid(); ds->Next()) {
      ds->RetrieveEntryName(&name);
      _return.entries.push_back(name);
    }
  }
  dir_guard.PutDirIndex(&_return.dmap_data);
}

// Retrieves the current directory partition map.
//
// REQUIRES: the specified directory id must map to an existing directory.
//
void IndexServer::ReadBitmap(std::string& _return, i64 dir_id) {
  MonitorHelper helper(oReadBitmap, monitor_);
  DIR_LOCK(dir_id);
  dir_guard.PutDirIndex(&_return);
}

// Updates the current directory partition map.
//
// REQUIRES: the specified directory id must map to an existing directory.
// REQUIRES: the specified directory partition map
//           must be well-formatted and must describe the specified directory.
//
void IndexServer::UpdateBitmap(i64 dir_id,
        const std::string& dmap_data) {
  MonitorHelper helper(oUpdateBitmap, monitor_);
  DIR_LOCK(dir_id);
  const DirIndex* dir_idx = dir_guard.UpdateDirIndex(dmap_data);
  MaybeThrowException(ctx_->SetDirIndex_Unlocked(dir_idx));
}

} // namespace indexfs
