// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifdef RADOS
#include "env/obj_set.h"
namespace indexfs {
using leveldb::ObjEnv;
using leveldb::ObjEnvFactory;
}
#endif

#include "common/logging.h"
#include "server/fs_errors.h"
#include "server/bulk_insert.h"

namespace indexfs {

void IndexServer::TriggerDirSplitting(i64 dir_id, i16 index, DirGuard& dir_guard) {
  dir_guard.Lock_AssertHeld();
  int size = dir_guard.GetPartitionSize(index);
  if (size <= ctx_->MaxPartSize()) {
    return;
  }
  if (!dir_guard.EligibleToSplit(index)) {
    return;
  }
  // Avoid concurrent splitting within a single directory
  dir_guard.DisableSplitting();
  SplitTask* t =  new SplitTask();
  t->dir_id = dir_id;
  t->index = index;
  t->idx_srv = this;
  exec_srv_->SubmitTask(t);
}

void IndexServer::DoSplit(i64 dir_id, i16 index) {
  MonitorHelper helper(oSplit, monitor_);

  MutexLock split_lock(&mtx_);
  DirGuard::DirData dir_data = ctx_->FetchDir(dir_id);
  DLOG_ASSERT(!DirGuard::Empty(dir_data));
  DirGuard dir_guard(dir_data);
  DirLock lock(&dir_guard);

  int src_idx = index;
  int src_srv = dir_guard.ToServer(src_idx);
  DLOG_ASSERT(src_srv == ctx_->GetMyRank());
  int dst_idx = dir_guard.NextIndex(src_idx);
  int dst_srv = dir_guard.ToServer(dst_idx);

  bool is_local = dst_srv == ctx_->GetMyRank();
  std::string type = is_local ?
          "Local Directory Split" : "Distributed Directory Split";
  uint64_t start_ts = ctx_->GetEnv()->NowMicros();

# ifndef NDEBUG
  int old_total_partition_size = dir_guard.GetTotalPartitionSize();
# endif

  BulkExtractor* bk_ext = NULL;
  if (!is_local) {
    bk_ext = ctx_->CreateBulkExtractor(dir_id,
            src_idx, src_srv, dst_idx, dst_srv);
  } else {
    bk_ext = ctx_->CreateLocalBulkExtractor(dir_id,
            src_idx, src_srv, dst_idx, dst_srv);
  }

  SplitGuard sg(bk_ext);
  uint64_t min_seq, max_seq;
  MaybeThrowException(bk_ext->Extract(&min_seq, &max_seq));
  int num_entries = bk_ext->GetNumEntriesExtracted();

  std::string dmap_data;
  dir_guard.Set(dst_idx);
  dir_guard.PutDirIndex(&dmap_data);

  if (!is_local) {
    std::string sst_dir = bk_ext->GetBulkExtractOutputDir();
    // Sync the SSTable directory
#   ifdef RADOS
    ObjEnv* obj_env = ObjEnvFactory::FetchObjEnv();
    DLOG_ASSERT(obj_env != NULL);
    obj_env->SyncSet(sst_dir);
#   endif
    {
      MutexLock lock(rpc_->GetMutex(dst_srv));
      rpc_->GetClient(dst_srv)->InsertSplit(dir_id, src_idx, dst_idx,
              sst_dir, dmap_data, min_seq, max_seq, num_entries);
    }
    MaybeThrowException(ctx_->SetDirIndex_Unlocked(dir_guard.FetchDirIndex()));
  } else {
    MaybeThrowException(ctx_->InstallSplit_Unlocked(dir_id,
            std::string(), dmap_data, src_idx, dst_idx, 0, 0, num_entries));
  }

  dir_guard.InceaseAndGetPartitionSize(src_idx, -1 * num_entries);
  MaybeThrowException(bk_ext->Commit());
  dir_guard.EnableSplitting();

# ifndef NDEBUG
  if (is_local) {
    int new_total_partition_size = dir_guard.GetTotalPartitionSize();
    DLOG_ASSERT(old_total_partition_size == new_total_partition_size);
  }
# endif

  int home_srv = dir_guard.ToServer(0);
  if (home_srv != ctx_->GetMyRank()) {
    MutexLock lock(rpc_->GetMutex(home_srv));
    rpc_->GetClient(home_srv)->UpdateBitmap(dir_id, dmap_data);
  }

  uint64_t finish_ts = ctx_->GetEnv()->NowMicros();
  uint64_t duration = (finish_ts - start_ts) / 1000;

  LOG(INFO) << type << " [dir=" << dir_id << "]"
          "[index=" << src_idx << "(me)->" << dst_idx << "] done"
          " >> " << num_entries << " entries moved - " << duration << " ms";
}

void IndexServer::InsertSplit(i64 dir_id,
                              i16 parent_index, i16 child_index,
                              const std::string& sst_dir,
                              const std::string& dmap_data,
                              i64 min_seq, i64 max_seq, i64 num_entries) {

  MonitorHelper helper(oInsertSplit, monitor_);
  // Load the SSTable directory
# ifdef RADOS
  ObjEnv* obj_env = ObjEnvFactory::FetchObjEnv();
  DLOG_ASSERT(obj_env != NULL);
  if (!sst_dir.empty()) {
    obj_env->LoadSet(sst_dir);
  }
# endif
  MaybeThrowException(ctx_->InstallSplit(dir_id, sst_dir,
          dmap_data, parent_index, child_index, min_seq, max_seq, num_entries));
  // Unload the SSTable directory
# ifdef RADOS
  if (!sst_dir.empty()) {
    obj_env->ForgetSet(sst_dir);
  }
# endif
  LOG(INFO) << "Bulk Insertion [dir=" << dir_id << "]"
          << "[index=" << parent_index << "->" << child_index << "(me)] done"
          " >> " << num_entries << " entries inserted";
}

} // namespace indexfs
