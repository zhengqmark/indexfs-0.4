// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "client/client_impl.h"

namespace indexfs {

// Resolve the given full path by looking up intermediate entries until
// we reach the last component.
//
Status ClientImpl::ResolvePath(const std::string& path,
                               OID* oid, int16_t* zeroth_server) {
  Status s;
  *zeroth_server = -1;
  oid->dir_id = -1;
  oid->path_depth = 0;
  oid->obj_name = "/";
  if (path.empty()) {
    return Status::InvalidArgument("Empty path");
  }
  if (path.at(0) != '/') {
    return Status::InvalidArgument("Relative path");
  }
  if (path.size() == 1) {
    return s; // This is root, which always exists
  }
  if (path.at(path.length() - 1) == '/') {
    return Status::InvalidArgument("Path ends with slash");
  }
  *zeroth_server = 0;
  oid->dir_id = 0;
  size_t now = 0, last = 0, end = path.rfind("/");
  while (last < end) {
    now = path.find("/", last + 1);
    if (now - last > 1) {
      oid->path_depth++;
      oid->obj_name = path.substr(last + 1, now - last - 1);
      LookupEntry* entry = lookup_cache_->Get(*oid);
      if (entry == NULL || env_->NowMicros() > entry->lease_due) {
        LookupInfo info;
        s = Lookup(*oid, *zeroth_server, &info, entry != NULL);
        if (!s.ok()) {
          lookup_cache_->Release(entry);
          return s;
        }
        if (entry == NULL) {
          entry = lookup_cache_->New(*oid, info);
        } else {
          entry->uid = info.uid;
          entry->gid = info.gid;
          entry->perm = info.perm;
          entry->lease_due = info.lease_due;
        }
        DLOG_ASSERT(entry->inode_no == info.id);
        DLOG_ASSERT(entry->zeroth_server == info.zeroth_server);
      }
      *zeroth_server = entry->zeroth_server;
      oid->dir_id = entry->inode_no;
      lookup_cache_->Release(entry);
    }
    last = now;
  }
  oid->path_depth++;
  oid->obj_name = path.substr(end + 1);
  return s;
}

// Determine the existence of a specific directory
// by consulting the local pathname lookup cache at the client if possible.
//
Status ClientImpl::AccessDir(const std::string& path) {
  OID oid;
  int16_t zeroth_server;
  return ResolvePath(std::string(path + "/_"), &oid, &zeroth_server);
}

namespace {
static
Status RPC_ReadBitmap(RPC* rpc, int srv,
        i64 dir_id, std::string* dmap_data) {
  Status s;
  try {
    rpc->GetClient(srv)->ReadBitmap(*dmap_data, dir_id);
  } catch (IOError &ioe) {
    s = Status::IOError(ioe.message);
  } catch (ServerInternalError &sie) {
    s = Status::Corruption(sie.message);
  } catch (UnrecognizedDirectoryError &ude) {
    s = Status::Corruption("Unrecognized directory id");
  }
  return s;
}
}

// Retrieves the directory index for a given directory.
// If possible, the index is directly obtained from the local
// cache albeit with a possible stale value.
//
// Returns NULL if no such index can be found either from the local cache
// or from the remote server, or if the index we retrieved cannot be resolved.
//
DirIndexEntry* ClientImpl::FetchIndex(int64_t dir_id, int16_t zeroth_server) {
  DirIndexEntry* result = index_cache_->Get(dir_id);
  if (result == NULL) {
    Status s_;
    std::string dmap_data;
    s_ = RPC_ReadBitmap(rpc_, (U16INT(zeroth_server))
            % config_->GetSrvNum(), dir_id, &dmap_data);
    if (s_.ok()) {
      DirIndex* dir_idx = index_policy_->RecoverDirIndex(dmap_data);
      if (dir_idx != NULL) {
        result = index_cache_->Insert(dir_idx);
      }
    }
  }
  return result;
}

} // namespace indexfs
