// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "client/rpc_exec.h"
#include "client/client_impl.h"

namespace indexfs {

namespace {
static
Status RPC_Msdir(RPC* rpc, int srv, const OID& oid,
        i16 perm, i16 hint_srv) {
  Status s;
  DLOG_ASSERT(rpc != NULL);
  try {
    rpc->GetClient(srv)->Mkdir_Presplit(oid, perm, hint_srv, hint_srv);
  } catch (FileAlreadyExistsException &ae) {
    s = Status::AlreadyExists(Slice());
  }
  return s;
}
}

Status RPCEngine::Msdir(const OID& oid, i16 perm, i16 hint_srv) {
  EXEC_WITH_RETRY_TRY() {
    return RPC_Msdir(rpc_, srv_id_, oid, perm, hint_srv);
  }
  EXEC_WITH_RETRY_CATCH();
}

Status ClientImpl::Mkdir_Presplit(const std::string& path, i16 perm) {
  Status s;
  OID oid;
  int16_t zeroth_server;
  s = ResolvePath(path, &oid, &zeroth_server);
  if (!s.ok()) {
    return s;
  }
  DirIndexEntry* entry = FetchIndex(oid.dir_id, zeroth_server);
  if (entry == NULL) {
    s = Status::Corruption("Missing index");
  }
  if (s.ok()) {
    DirIndexGuard idx_guard(entry);
    s = RPCEngine(entry->index, rpc_).Msdir(oid, perm, RandomServer(path));
  }
  return s;
}

} // namespace indexfs
