// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>

#include "common/config.h"
#include "common/logging.h"
#include "server/index_ctx.h"

DEFINE_bool(index_context_new_dir_if_not_found, false, "create dir on demand");

namespace indexfs {

Status IndexContext::Mknod_Unlocked(const OID& oid,
                                    int16_t idx, mode_t mode) {
  DLOG_ASSERT(mdb_ != NULL);
  KeyInfo key(oid.dir_id, idx, oid.obj_name);
  return mdb_->NewFile(key);
}

Status IndexContext::Mkdir_Unlocked(const OID& oid,
                                    int16_t idx, mode_t mode,
                                    int64_t inode_no, int16_t zero_srv) {
  DLOG_ASSERT(mdb_ != NULL);
  KeyInfo key(oid.dir_id, idx, oid.obj_name);
  return mdb_->NewDirectory(key, zero_srv, inode_no);
}

Status IndexContext::Getattr_Unlocked(const OID& oid,
                                      int16_t idx, StatInfo* info) {
  DLOG_ASSERT(mdb_ != NULL);
  KeyInfo key(oid.dir_id, idx, oid.obj_name);
  return mdb_->GetEntry(key, info);
}

Status IndexContext::Setattr_Unlocked(const OID& oid,
                                      int16_t idx, const StatInfo& info) {
  DLOG_ASSERT(mdb_ != NULL);
  KeyInfo key(oid.dir_id, idx, oid.obj_name);
  return mdb_->PutEntry(key, info);
}

Status IndexContext::FetchDirIndex_Unlocked(int64_t dir_id,
                                            DirIndex** dir_idx) {
  DLOG_ASSERT(mdb_ != NULL);
  Status s;
  std::string dmap_data;
  s = mdb_->GetMapping(dir_id, &dmap_data);
  if (s.ok()) {
    *dir_idx = index_policy_->RecoverDirIndex(dmap_data);
  } else {
    if (!FLAGS_index_context_new_dir_if_not_found) {
      *dir_idx = NULL;
    } else {
      s = Status::OK();
      *dir_idx = index_policy_->NewDirIndex(dir_id, options_->GetSrvId());
    }
  }
  return s;
}

Status IndexContext::AddDirIndex_Unlocked(const DirIndex* dir_idx) {
  DLOG_ASSERT(mdb_ != NULL);
  return mdb_->InsertMapping(dir_idx->FetchDirId(), dir_idx->ToSlice());
}

Status IndexContext::SetDirIndex_Unlocked(const DirIndex* dir_idx) {
  DLOG_ASSERT(mdb_ != NULL);
  return mdb_->UpdateMapping(dir_idx->FetchDirId(), dir_idx->ToSlice());
}

namespace {
// The minimum sizes of directory entry
// cache and the directory index cache.
// In real cluster, much larger sizes should be used.
//
static const int kMinIndexCacheSize = 4096;
static int IndexCacheSize(Config* config) {
  return std::max(config->GetDirMappingCacheSize(), kMinIndexCacheSize);
}
}

IndexContext::~IndexContext() {
  delete mdb_;
  delete ctrl_table_;
  delete index_cache_;
}

IndexContext::IndexContext(Env* env, Config* options)
  : env_(env),
    options_(options),
    mdb_(NULL) {
  ctrl_table_ = new DirCtrlTable();
  index_cache_ = new DirIndexCache(IndexCacheSize(options_));
  index_policy_ = DirIndexPolicy::Default(options_);
}

Status IndexContext::Flush() {
  DLOG_ASSERT(mdb_ != NULL);
  Status s;
  s = mdb_->Flush();
  return s;
}

Status IndexContext::Open() {
  int64_t rt_id = ROOT_DIR_ID;
  int16_t rt_srv = ROOT_ZEROTH_SERVER;
  DLOG_ASSERT(mdb_ == NULL);

  Status s;
  if (options_->HasOldData()) {
    s = MetaDB::Repair(options_, env_);
  }
  if (s.ok()) {
    s = MetaDB::Open(options_, &mdb_, env_);
  }
  if (!s.ok()) {
    return s;
  }

  DirIndex* rt_idx = NULL;
  s = FetchDirIndex_Unlocked(rt_id, &rt_idx);
  if (s.IsNotFound()) {
    if (rt_srv % options_->GetSrvNum() != options_->GetSrvId()) {
      s = Status::OK();
    } else {
      rt_idx = index_policy_->NewDirIndex(rt_id, rt_srv);
      s = AddDirIndex_Unlocked(rt_idx);
      if (!s.ok()) {
        delete rt_idx;
        return s;
      }
      DirCtrlBlock* rt_blk = ctrl_table_->Fetch(rt_id);
      rt_blk->size_map[0] = 0;
      ctrl_table_->Release(rt_blk);
    }
  }
  else if (s.ok()) {
    DirCtrlBlock* rt_blk = ctrl_table_->Fetch(rt_id);
    int idx = 0;
    while (idx < (1 << rt_idx->FetchBitmapRadix())) {
      if (rt_idx->GetBit(idx) &&
          rt_idx->GetServerForIndex(idx) == options_->GetSrvId()) {
        rt_blk->size_map[idx] = 0;
      }
      idx++;
    }
    ctrl_table_->Release(rt_blk);
  }

  if (rt_idx != NULL) {
    DLOG_ASSERT(rt_idx->FetchDirId() == rt_id);
    index_cache_->Release(index_cache_->Insert(rt_idx));
  }

  return s;
}

bool IndexContext::TEST_HasDir(int64_t dir_id) {
  DirGuard::DirData dir_data = FetchDir(dir_id);
  bool empty = DirGuard::Empty(dir_data);
  if (!empty) {
    DirGuard dir_guard(dir_data);
  }
  return !empty;
}

Status IndexContext::TEST_Getattr(const OID& oid, int16_t idx,
                                  StatInfo* info) {
  Status s;
  DirCtrlBlock* ctrl_blk = ctrl_table_->Fetch(oid.dir_id);
  DirCtrlGuard ctrl_guard(ctrl_blk);
  RawLock lock(ctrl_blk);
  if (ctrl_blk->size_map.count(idx) == 0) {
    s = Status::NotFound("No such partition");
  }
  if (s.ok()) {
    s = Getattr_Unlocked(oid, idx, info);
  }
  return s;
}

Status IndexContext::TEST_Setattr(const OID& oid, int16_t idx,
                                  const StatInfo& info) {
  Status s;
  DirCtrlBlock* ctrl_blk = ctrl_table_->Fetch(oid.dir_id);
  DirCtrlGuard ctrl_guard(ctrl_blk);
  RawLock lock(ctrl_blk);
  if (ctrl_blk->size_map.count(idx) == 0) {
    s = Status::NotFound("No such partition");
  }
  if (s.ok()) {
    s = Setattr_Unlocked(oid, idx, info);
  }
  return s;
}

Status IndexContext::TEST_Mknod(const OID& oid, int16_t idx, mode_t mode) {
  Status s;
  DirCtrlBlock* ctrl_blk = ctrl_table_->Fetch(oid.dir_id);
  DirCtrlGuard ctrl_guard(ctrl_blk);
  RawLock lock(ctrl_blk);
  if (ctrl_blk->size_map.count(idx) == 0) {
    s = Status::NotFound("No such partition");
  }
  if (s.ok()) {
    s = Mknod_Unlocked(oid, idx, mode);
    if (s.ok()) {
      ctrl_blk->size_map[idx]++;
    }
  }
  return s;
}

Status IndexContext::TEST_Mkdir(const OID& oid, int16_t idx,
                                mode_t mode, int64_t inode_no, int16_t zero_srv) {
  Status s;
  DirCtrlBlock* ctrl_blk = ctrl_table_->Fetch(oid.dir_id);
  DirCtrlGuard ctrl_guard(ctrl_blk);
  RawLock lock(ctrl_blk);
  if (ctrl_blk->size_map.count(idx) == 0) {
    s = Status::NotFound("No such partition");
  }
  if (s.ok()) {
    s = Mkdir_Unlocked(oid, idx, mode, inode_no, zero_srv);
    if (s.ok()) {
      ctrl_blk->size_map[idx]++;
      s = InstallZeroth_Unlocked(inode_no, zero_srv);
    }
  }
  return s;
}

DirGuard::DirData IndexContext::FetchDir(int64_t dir_id) {
  Status s;
  DirIndexEntry* index_entry = NULL;

  DirCtrlBlock* ctrl_blk = ctrl_table_->Fetch(dir_id);
  index_entry = index_cache_->Get(dir_id);
  if (index_entry == NULL) {
    RawLock lock(ctrl_blk);
    index_entry = index_cache_->Get(dir_id);
    if (index_entry == NULL) {
      DirIndex* dir_idx = NULL;
      s = FetchDirIndex_Unlocked(dir_id, &dir_idx);
      if (s.ok()) {
        index_entry = index_cache_->Insert(dir_idx);
      }
    }
  }

  if (!s.ok()) {
    ctrl_table_->Release(ctrl_blk);
    if (s.IsNotFound() && ctrl_blk->Empty()) {
      ctrl_table_->Evict(dir_id);
    }
    return DirGuard::DirData(NULL, NULL);
  }

  DLOG_ASSERT(index_entry != NULL);
  DLOG_ASSERT(index_entry->index->FetchDirId() == dir_id);
  return DirGuard::DirData(ctrl_blk, index_entry);
}

Status IndexContext::InstallZeroth_Unlocked(int64_t dir_id,
                                            int16_t zeroth_server) {
  DLOG_ASSERT(mdb_ != NULL);
  Status s;
  DirCtrlBlock* ctrl_blk = ctrl_table_->Fetch(dir_id);
  DirCtrlGuard ctrl_guard(ctrl_blk);

  if (!ctrl_blk->Empty()) {
    if (ctrl_blk->NumPartitions() != 1 ||
        ctrl_blk->size_map.count(0) == 0 ||
        ctrl_blk->size_map[0] != 0) {
      s = Status::AlreadyExists("Partition already exists");
    }
  }

  if (s.ok()) {
    DirIndex* dir_idx = index_policy_->NewDirIndex(dir_id, zeroth_server);
    if (dir_idx->GetServerForIndex(0) != GetMyRank()) {
      s = Status::InvalidArgument("Not the right server");
    }
    if (s.ok()) {
      s = AddDirIndex_Unlocked(dir_idx);
    }
    if (s.ok()) {
      ctrl_blk->size_map[0] = 0;
      // Assuming we are going to need this directory index pretty soon.
      index_cache_->Release(index_cache_->Insert(dir_idx));
    }
    if (!s.ok()) {
      delete dir_idx;
      if (!s.IsAlreadyExists() && ctrl_blk->Empty()) {
        ctrl_table_->Evict(dir_id);
      }
    }
  }

  DLOG_IF(INFO, s.ok()) << "Partition Creation "
          "[dir=" << dir_id << "][index=0] done << 0 entries deposited";

  return s;
}

Status IndexContext::InstallZeroth(int64_t dir_id, int16_t zeroth_server) {
  DirCtrlBlock* ctrl_blk = ctrl_table_->Fetch(dir_id);
  DirCtrlGuard ctrl_guard(ctrl_blk);
  RawLock lock(ctrl_blk);
  return InstallZeroth_Unlocked(dir_id, zeroth_server);
}

Status IndexContext::InstallSplit_Unlocked(int64_t dir_id,
                                           const std::string& sst_dir,
                                           const Slice& dmap_data,
                                           int16_t parent_index,
                                           int16_t child_index,
                                           uint64_t min_seq, uint64_t max_seq,
                                           int64_t num_entries) {
  DLOG_ASSERT(mdb_ != NULL);
  Status s;
  DirCtrlBlock* ctrl_blk = ctrl_table_->Fetch(dir_id);
  DirCtrlGuard ctrl_guard(ctrl_blk);

  DirIndex* dir_idx = index_policy_->RecoverDirIndex(dmap_data);
  DLOG_ASSERT(dir_idx != NULL);
  DLOG_ASSERT(dir_idx->FetchDirId() == dir_id);

  DLOG_ASSERT(dir_idx->GetBit(parent_index));
  if (!dir_idx->GetBit(child_index)) {
    dir_idx->SetBit(child_index);
  }
  if (dir_idx->GetServerForIndex(child_index) != GetMyRank()) {
    s = Status::InvalidArgument("Not the right server");
  }

  if (s.ok()) {
    if (ctrl_blk->Empty()) {
      s = AddDirIndex_Unlocked(dir_idx);
    } else if (ctrl_blk->size_map.count(child_index) != 0) {
      if (ctrl_blk->NumPartitions() == 1 &&
          ctrl_blk->size_map[child_index] == 0) {
        s = AddDirIndex_Unlocked(dir_idx);
      } else {
        s = Status::AlreadyExists("Partition already exists");
      }
    } else {
      DirIndex* old_idx = NULL;
      s = FetchDirIndex_Unlocked(dir_id, &old_idx);
      if (s.ok()) {
        dir_idx->Update(*old_idx);
        delete old_idx;
        s = SetDirIndex_Unlocked(dir_idx);
      }
    }
  }

  if (s.ok()) {
    if (num_entries > 0 && !sst_dir.empty()) {
      s = mdb_->BulkInsert(min_seq, max_seq, sst_dir);
    }
    if (s.ok()) {
      ctrl_blk->size_map[child_index] = 0;
      ctrl_blk->size_map[child_index] += num_entries;
      index_cache_->Evict(dir_id);
      index_cache_->Release(index_cache_->Insert(dir_idx));
    }
  }

  if (!s.ok()) {
    delete dir_idx;
    if (!s.IsAlreadyExists() && ctrl_blk->Empty()) {
      ctrl_table_->Evict(dir_id);
    }
  }

  DLOG_IF(INFO, s.ok()) << "Partition Creation "
          "[dir=" << dir_id << "][index=" << child_index << "] done"
          " << " << num_entries << " entries deposited";

  return s;
}

Status IndexContext::InstallSplit(int64_t dir_id,
                                  const std::string& sst_dir,
                                  const Slice& dmap_data,
                                  int16_t parent_index,
                                  int16_t child_index,
                                  uint64_t min_seq, uint64_t max_seq,
                                  int64_t num_entries) {
  DirCtrlBlock* ctrl_blk = ctrl_table_->Fetch(dir_id);
  DirCtrlGuard ctrl_guard(ctrl_blk);
  RawLock lock(ctrl_blk);
  return InstallSplit_Unlocked(dir_id, sst_dir, dmap_data,
          parent_index, child_index, min_seq, max_seq, num_entries);
}

BulkExtractor*
IndexContext::CreateBulkExtractor(int64_t dir_id,
                                  int16_t src_idx, int16_t src_srv,
                                  int16_t dst_idx, int16_t dst_srv) {
  DLOG_ASSERT(mdb_ != NULL);
  std::stringstream ss;
  ss << GetSplitDir() << "/d" << dir_id;
  ss << "-p" << src_idx << "p" << dst_idx;
  ss << "-s" << src_srv << "s" << dst_srv;
  BulkExtractor* bk_ext = mdb_->CreateBulkExtractor(ss.str());
  bk_ext->SetDirectory(dir_id);
  bk_ext->SetOldPartition(src_idx);
  bk_ext->SetNewPartition(dst_idx);
  return bk_ext;
}

BulkExtractor*
IndexContext::CreateLocalBulkExtractor(int64_t dir_id,
                                       int16_t src_idx, int16_t src_srv,
                                       int16_t dst_idx, int16_t dst_srv) {
  DLOG_ASSERT(mdb_ != NULL);
  DLOG_ASSERT(src_srv == dst_srv);
  BulkExtractor* bk_ext = mdb_->CreateLocalBulkExtractor();
  bk_ext->SetDirectory(dir_id);
  bk_ext->SetOldPartition(src_idx);
  bk_ext->SetNewPartition(dst_idx);
  return bk_ext;
}

DirScanner*
IndexContext::CreateDirScanner(int64_t dir_id,
                               int16_t index, const std::string& start_hash) {
  DLOG_ASSERT(mdb_ != NULL);
  return mdb_->CreateDirScanner(KeyOffset(dir_id, index, start_hash));
}

} // namespace indexfs
