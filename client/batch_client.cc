// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <sys/stat.h>
#include "common/options.h"
#include "client/batch_client.h"

namespace indexfs {

static const bool kRPCLazyInitialize = true;

static const int64_t kHardCodedDirId = DEFAULT_MAX_NUM_SERVERS;
static const int16_t kHardCodedZerothServer = ROOT_ZEROTH_SERVER;

Status BatchClient::Noop() {
  return Status::OK();
}

Status BatchClient::AccessDir(const std::string& path) {
  return Status::NotSupported(Slice());
}

Status BatchClient::ResolvePath(const std::string& path,
                                OID* oid, int16_t* zeroth_server) {
  Status s;
  if (last_dir_.empty() ||
      path.length() <= last_dir_.length() + 1 ||
      path.compare(0, last_dir_.length(), last_dir_) != 0) {
    s = Status::NotFound(Slice());
  } else {
    *zeroth_server = kHardCodedZerothServer;
    oid->path_depth = -1;
    oid->dir_id = kHardCodedDirId;
    oid->obj_name = path.substr(last_dir_.length() + 1);
  }
  return s;
}

namespace {
static
Status CreateDir(Env* env, const std::string& dir) {
  Status s;
  if (!env->FileExists(dir)) {
    s = env->CreateDir(dir);
  }
  return s;
}
}

Status BatchClient::Init() {
  Status s;
  if (!FLAGS_batch_client_remote_read_mode) {
    CreateDir(env_, config_->GetDBRootDir());
    CreateDir(env_, config_->GetDBHomeDir());
    if (config_->HasOldData()) {
      s = MetaDB::Repair(config_, env_);
    }
    if (s.ok()) {
      s = MetaDB::Open(config_, &mdb_, env_);
    }
    if (!s.ok()) {
      return s;
    }
  } else {
    // No need to open MDB
    // All data will be fetched from remote servers
  }
  return kRPCLazyInitialize ? s : rpc_->Init();
}

Status BatchClient::FlushWriteBuffer() {
  Status s;
  if (mdb_ != NULL) {
    s = mdb_->Flush();
  }
  return s;
}

Status BatchClient::Dispose() {
  if (mdb_ != NULL) {
    Status s_ = mdb_->Flush();
    if (!s_.ok()) {
      LOG(ERROR) << "Fail to flush DB: " << s_.ToString();
    }
    delete mdb_;
    mdb_ = NULL;
  }
  return rpc_->Shutdown();
}

Status BatchClient::Mkdir(const std::string& path, i16 perm) {
  last_dir_ = path;
  if (FLAGS_batch_client_remote_read_mode) {
    delete dir_idx_;
    DLOG_ASSERT(dir_policy_ != NULL);
    dir_idx_ = dir_policy_->NewDirIndex(kHardCodedDirId, kHardCodedZerothServer);
    for (int i = 0; i < dir_policy_->NumServers(); i++) {
      dir_idx_->SetBit(i);
    }
  }
  return Status::OK();
}

Status BatchClient::Mkdir_Presplit(const std::string& path, i16 perm) {
  return Mkdir(path, perm);
}

namespace {
static
Status Local_Mknod(MetaDB* db, const OID& oid) {
  DLOG_ASSERT(db != NULL);
  return db->NewFile(KeyInfo(oid.dir_id, 0, oid.obj_name));
}
}

Status BatchClient::Mknod(const std::string& path, i16 perm) {
  Status s;
  OID oid;
  int16_t zeroth_server;
  s = ResolvePath(path, &oid, &zeroth_server);
  if (!s.ok()) {
    return s;
  }
  return Local_Mknod(mdb_, oid);
}

Status BatchClient::Mknod_Flush() {
  return Status::OK();
}

Status BatchClient::Mknod_Buffered(const std::string& path, i16 perm) {
  return Mknod(path, perm);
}

namespace {
static Status Local_Getattr(MetaDB* db, const OID& oid,
                            StatInfo* info) {
  DLOG_ASSERT(db != NULL);
  return db->GetEntry(KeyInfo(oid.dir_id, 0, oid.obj_name), info);
}
}

namespace {
static Status Remote_Getattr(RPC* rpc, const OID& oid, DirIndex* dir_idx,
                             StatInfo* info) {
  DLOG_ASSERT(dir_idx != NULL);
  Status s;
  int srv_id = dir_idx->SelectServer(oid.obj_name);
  rpc->GetClient(srv_id)->Getattr(*info, oid);
  return s;
}
}

Status BatchClient::Getattr(const std::string& path, StatInfo* info) {
  Status s;
  OID oid;
  int16_t zeroth_server;
  s = ResolvePath(path, &oid, &zeroth_server);
  if (!s.ok()) {
    return s;
  }
  if (!FLAGS_batch_client_remote_read_mode) {
    s = Local_Getattr(mdb_, oid, info);
  } else {
    s = Remote_Getattr(rpc_, oid, dir_idx_, info);
  }
  return s;
}

namespace {
static Status Local_Chmod(MetaDB* db, const OID& oid,
                          i16 perm, bool* is_dir) {
  DLOG_ASSERT(db != NULL);
  Status s;
  StatInfo info;
  KeyInfo key(oid.dir_id, 0, oid.obj_name);
  s = db->GetEntry(key, &info);
  if (s.ok()) {
    s = db->PutEntryWithMode(key, info, perm);
    if (s.ok()) {
      *is_dir = S_ISDIR(info.mode);
    }
  }
  return s;
}
}

Status BatchClient::Chmod(const std::string& path, i16 perm, bool* is_dir) {
  Status s;
  OID oid;
  int16_t zeroth_server;
  s = ResolvePath(path, &oid, &zeroth_server);
  if (!s.ok()) {
    return s;
  }
  return Local_Chmod(mdb_, oid, perm, is_dir);
}

Status BatchClient::Chown(const std::string& path, i16 uid, i16 gid, bool* is_dir) {
  return Status::NotSupported(Slice());
}

} /* namespace indexfs */
