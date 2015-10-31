// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/env.h"
#include "client/batch_client.h"

DEFINE_bool(batch_client_remote_read_mode, false, "remote read mode");

namespace indexfs {

using leveldb::NewMemLinkEnv;

BatchClient::BatchClient(Config* config, Env* env) :
    mdb_(NULL),
    env_(env),
    config_(config),
    dir_policy_(NULL),
    dir_idx_(NULL) {
  rpc_ = RPC::CreateRPC(config_);
  if (FLAGS_batch_client_remote_read_mode) {
    dir_policy_ = DirIndexPolicy::Default(config);
  }
}

BatchClient::~BatchClient() {
  delete mdb_;
  delete rpc_;
  delete dir_idx_;
  delete dir_policy_;
}

Client* ClientFactory::GetBatchClient(Config* config) {
  DLOG_ASSERT(config->IsBatchClient());
  Env* env = GetSystemEnv(config);
  if (!FLAGS_batch_client_remote_read_mode && config->HasOldData()) {
    // We will have to link old SSTables into
    // our current DB home
    env = NewMemLinkEnv(env);
  }
  return new BatchClient(config, env);
}

Status BatchClient::Close(FileHandle* handle) {
  return Status::Corruption("Not implemented");
}

Status BatchClient::Open(const std::string& path, int mode, FileHandle** handle) {
  return Status::Corruption("Not implemented");
}

Status BatchClient::ReadDir(const std::string& path, NameList* names) {
  return Status::Corruption("Not implemented");
}

Status BatchClient::ListDir(const std::string& path, NameList* names, StatList stats) {
  return Status::Corruption("Not implemented");
}

} /* namespace indexfs */

