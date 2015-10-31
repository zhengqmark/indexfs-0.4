// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_INDEX_CONTEXT_H_
#define _INDEXFS_INDEX_CONTEXT_H_

#include "metadb/metadb.h"
#include "common/dirlock.h"
#include "common/dirguard.h"

namespace indexfs {

class IndexContext {
 public:

  IndexContext(Env* env, Config* options);

  virtual ~IndexContext();

  Env* GetEnv() { return env_; }

  Status Open();
  Status Flush();
  bool TEST_HasDir(int64_t dir_id);
  DirGuard::DirData FetchDir(int64_t dir_id);

  Status AddDirIndex_Unlocked(const DirIndex* dir_idx);
  Status SetDirIndex_Unlocked(const DirIndex* dir_idx);
  Status FetchDirIndex_Unlocked(int64_t dir_id, DirIndex** dir_idx);

  // Fetch server id from local options
  int GetMyRank() {
    return options_->GetSrvId();
  }

  // Fetch the total number of servers from local options
  int GetNumServers() {
    return options_->GetSrvNum();
  }

  // Fetch the directory to store temporary directory splitting files.
  std::string GetSplitDir() {
    return options_->GetDBSplitDir();
  }

  // Fetch the maximum partition size from local options
  int MaxPartSize() {
    return options_->GetSplitThreshold();
  }

  // Retrieve the next available inode number
  int64_t NextInode() {
    return mdb_->ReserveNextInodeNo();
  }

  DirIndex* NewDirIndex(int64_t dir_id, int16_t srv_id) {
    return index_policy_->NewDirIndex(dir_id, srv_id);
  }

  DirIndex* TEST_NewDirIndex(int64_t dir_id) {
    return index_policy_->NewDirIndex(dir_id, options_->GetSrvId());
  }

  Status InstallSplit(int64_t dir_id,
                      const std::string& sst_dir,
                      const Slice& dmap_data,
                      int16_t parent_index, int16_t child_index,
                      uint64_t min_seq, uint64_t max_seq,
                      int64_t num_entries);
  Status InstallSplit_Unlocked(int64_t dir_id,
                      const std::string& sst_dir,
                      const Slice& dmap_data,
                      int16_t parent_index, int16_t child_index,
                      uint64_t min_seq, uint64_t max_seq,
                      int64_t num_entries);

  Status InstallZeroth(int64_t dir_id, int16_t zeroth_server);
  Status InstallZeroth_Unlocked(int64_t dir_id, int16_t zeroth_server);

  DirScanner* CreateDirScanner(int64_t dir_id, int16_t index,
                               const std::string& start_hash);

  BulkExtractor* CreateBulkExtractor(int64_t dir_id,
                                     int16_t src_idx, int16_t src_srv,
                                     int16_t dst_idx, int16_t dst_srv);
  BulkExtractor* CreateLocalBulkExtractor(int64_t dir_id,
                                     int16_t src_idx, int16_t src_srv,
                                     int16_t dst_idx, int16_t dst_srv);

  Status TEST_Mknod(const OID& oid, int16_t idx,
                    mode_t mode);
  Status TEST_Mkdir(const OID& oid, int16_t idx,
                    mode_t mode, int64_t inode_no, int16_t zero_srv);
  Status TEST_Getattr(const OID& oid, int16_t idx, StatInfo* info);
  Status TEST_Setattr(const OID& oid, int16_t idx, const StatInfo& info);

  Status Mknod_Unlocked(const OID& oid, int16_t idx,
                        mode_t mode);
  Status Mkdir_Unlocked(const OID& oid, int16_t idx,
                        mode_t mode, int64_t inode_no, int16_t zero_srv);
  Status Getattr_Unlocked(const OID& oid, int16_t idx, StatInfo* info);
  Status Setattr_Unlocked(const OID& oid, int16_t idx, const StatInfo& info);

 private:
  Env* env_;
  Config* options_;
  MetaDB* mdb_;
  DirCtrlTable* ctrl_table_;

  DirIndexCache* index_cache_;
  DirIndexPolicy* index_policy_;

  // No copying allowed
  IndexContext(const IndexContext&);
  IndexContext& operator=(const IndexContext&);
};

} // namespace indexfs

#endif /* _INDEXFS_INDEX_CONTEXT_H_ */
