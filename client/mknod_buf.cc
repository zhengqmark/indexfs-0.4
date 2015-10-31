// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <gflags/gflags.h>
#include <vector>

#include "client/rpc_exec.h"
#include "client/client_impl.h"

namespace indexfs {

typedef std::string Req;
typedef std::vector<Req> ReqQueue;

DEFINE_int32(mknod_bufsize, 128, "Max number of mknods buffered per server");

// Buffered file creations under a particular directory
// that is assumed to have been pre-split to all metadata servers.
// We further assume that each file is created with a same permission.
//
class ClientImpl::MknodBuffer {
 public:

  ~MknodBuffer() {
#   ifndef NDEBUG
    for (int i = 0; i < bufs_.size(); ++i) {
      if (bufs_[i].size() > 0) {
        DLOG(WARNING) << "Discarding a non-empty mknod_buffer";
      }
    }
#   endif
  }

  MknodBuffer(int64_t dir_id, int16_t dir_depth,
          int16_t zeroth_server, int num_srvs)
    : dir_id_(dir_id),
      dir_depth_(dir_depth),
      zeroth_server_(zeroth_server),
      bufs_(num_srvs) {
  }

  Status MaybeFlushQueue(ReqQueue* queue, int srv_id,
          DirIndex* dir_idx, RPC* rpc, bool forced) {
    Status s;
    if (forced || queue->size() >= FLAGS_mknod_bufsize) {
      OIDS oids;
      oids.dir_id = dir_id_;
      oids.path_depth = dir_depth_;
      oids.obj_names.insert(oids.obj_names.begin(),
              queue->begin(), queue->end());
      s = RPCEngine(dir_idx, rpc).Mknods(oids, srv_id, 0);
      if (s.ok()) {
        queue->clear();
      }
    }
    return s;
  }

  Status Flush(DirIndex* dir_idx, RPC* rpc) {
    Status s;
    for (int i = 0; s.ok() && i < bufs_.size(); ++i) {
      if (bufs_[i].size() > 0) {
        s = MaybeFlushQueue(&bufs_[i], i, dir_idx, rpc, true);
      }
    }
    return s;
  }

  Status Mknod(const std::string& name, DirIndex* dir_idx, RPC* rpc) {
    int srv_id = dir_idx->SelectServer(name);
    DLOG_ASSERT(srv_id < bufs_.size());
    ReqQueue* queue = &bufs_[srv_id];
    queue->push_back(name);
    return MaybeFlushQueue(queue, srv_id, dir_idx, rpc, false);
  }

 private:
  int64_t dir_id_;
  int16_t dir_depth_;
  int16_t zeroth_server_;
  std::vector<ReqQueue> bufs_;

  friend class ClientImpl;
  // No copying allowed
  MknodBuffer(const MknodBuffer&);
  MknodBuffer& operator=(const MknodBuffer&);
};

// -------------------------------------------------------------
// Mknod_Buffered
// -------------------------------------------------------------

Status ClientImpl::FlushBuffer(MknodBuffer* buffer) {
  Status s;
  DirIndexEntry* entry = FetchIndex(buffer->dir_id_, buffer->zeroth_server_);
  if (entry == NULL) {
    // Maybe the directory itself has been removed.
    // To prevent this, we could use a directory lease.
    s = Status::Corruption("No such directory or index");
  }
  if (s.ok()) {
    DirIndexGuard idx_guard(entry);
    s = buffer->Flush(entry->index, rpc_);
  }
  return s;
}

namespace {
static
Status RPC_Mknods(RPC* rpc, int srv, const OIDS& oids, i16 perm) {
  Status s;
  try {
    rpc->GetClient(srv)->Mknod_Bulk(oids, perm);
  } catch (FileAlreadyExistsException &ae) {
    s = Status::AlreadyExists(Slice());
  }
  return s;
}
}

Status RPCEngine::Mknods(const OIDS& oids, int srv_id, i16 perm) {
  Status s;
  if (oids.obj_names.size() > 0) {
#   ifndef NDEBUG
    for (int i = 0; i < oids.obj_names.size(); ++i) {
      DLOG_ASSERT(srv_id == dir_idx_->SelectServer(oids.obj_names[i]));
    }
#   endif
    try {
      return RPC_Mknods(rpc_, srv_id, oids, perm);
    } catch (ServerRedirectionException &se) {
      s = Status::Corruption("Stale directory index");
    }
  }
  return s;
}

Status ClientImpl::Mknod_Flush() {
  Status s;
  BufferIter it = mknod_bufmap_.begin();
  while (s.ok() && it != mknod_bufmap_.end()) {
    s = FlushBuffer(it->second);
    if (s.ok()) {
      delete it->second;
      BufferIter it_ = it;
      ++it;
      mknod_bufmap_.erase(it_);
    }
  }
  return s;
}

Status ClientImpl::Mknod_Buffered(const std::string& path, i16 perm) {
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
    MknodBuffer* buffer;
    if (mknod_bufmap_.count(oid.dir_id) > 0) {
      buffer = mknod_bufmap_[oid.dir_id];
    } else {
      buffer = new MknodBuffer(oid.dir_id,
              oid.path_depth, zeroth_server, index_policy_->NumServers());
      mknod_bufmap_.insert(std::make_pair(oid.dir_id, buffer));
    }
    DLOG_ASSERT(buffer != NULL);
    s = buffer->Mknod(oid.obj_name, entry->index, rpc_);
  }
  return s;
}

} // namespace indexfs
