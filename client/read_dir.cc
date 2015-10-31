// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <vector>

#include "client/client_impl.h"

namespace indexfs {

namespace {
static
Status RPC_Readdir(RPC* rpc, int srv,
        i64 dir_id, i16 index, EntryList* list) {
  Status s;
  try {
    rpc->GetClient(srv)->Readdir(*list, dir_id, index);
  } catch (IOError &io) {
    s = Status::IOError(io.message);
  } catch (ServerInternalError &ie) {
    s = Status::Corruption(ie.message);
  }
  return s;
}
}

Status RPCEngine::ReadDir(i64 dir_id, NameList* names) {
  for (int16_t idx = 0;
       idx < (1 << dir_idx_->FetchBitmapRadix());
       ++idx) {
    if (dir_idx_->GetBit(idx)) {
      EntryList list;
      srv_id_ = dir_idx_->GetServerForIndex(idx);
      Status s_ = RPC_Readdir(rpc_, srv_id_, dir_id, idx, &list);
      if (s_.ok()) {
        names->insert(names->end(), list.entries.begin(), list.entries.end());
        dir_idx_->Update(list.dmap_data);
      }
    }
  }
  return Status::OK();
}

Status ClientImpl::ReadDir(const std::string& path, NameList* names) {
  Status s;
  OID oid;
  int16_t zeroth_server;
  s = ResolvePath(std::string(path + "/_"), &oid, &zeroth_server);
  if (!s.ok()) {
    return s;
  }
  DirIndexEntry* entry = FetchIndex(oid.dir_id, zeroth_server);
  if (entry == NULL) {
    s = Status::Corruption("Missing index");
  }
  if (s.ok()) {
    DirIndexGuard idx_guard(entry);
    s = RPCEngine(entry->index, rpc_).ReadDir(oid.dir_id, names);
  }
  return s;
}

Status RPCEngine::ListDir(i64 dir_id, NameList* names, StatList* stats) {
  return Status::Corruption("Not implemented");
}

Status ClientImpl::ListDir(const std::string& path, NameList* names, StatList stats) {
  return Status::Corruption("Not implemented");
}

} // namespace indexfs
