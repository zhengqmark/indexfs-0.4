// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "client/client.h"
#include "client/client_impl.h"

namespace indexfs {

Client* ClientFactory::GetClient(Config* config) {
  DLOG_ASSERT(config->IsClient());
  Env* env = GetSystemEnv(config);
  return new ClientImpl(config, env);
}

ClientImpl::~ClientImpl() {
  delete rpc_;
  delete lookup_cache_;
  delete index_cache_;
}

ClientImpl::ClientImpl(Config* config, Env* env) :
    env_(env),
    config_(config) {
  rpc_ = RPC::CreateRPC(config_);
  index_policy_ = DirIndexPolicy::Default(config_);
  index_cache_ = new DirIndexCache(config_->GetDirMappingCacheSize());
  lookup_cache_ = new LookupCache(config_->GetDirEntryCacheSize());
}

} /* namespace indexfs */
