// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_COMMON_DIRGUARD_H_
#define _INDEXFS_COMMON_DIRGUARD_H_

#include "common/logging.h"
#include "common/dirctrl.h"
#include "common/didxcache.h"

namespace indexfs {

class DirGuard {
 public:

  typedef std::pair<DirCtrlBlock*, DirIndexEntry*> DirData;

  static bool Empty(const DirData& dir_data) {
    return dir_data.first == NULL || dir_data.second == NULL;
  }

  explicit DirGuard(const DirData& dir_data);

  ~DirGuard();

  void Lock() {
    ctrl_block()->Lock();
  }

  void Unlock() {
    ctrl_block()->Unlock();
  }

  void Wait() {
    ctrl_block()->Wait();
  }

  void NotifyAll() {
    ctrl_block()->NotifyAll();
  }

  bool Lock_IsInUse() {
    return ctrl_block()->TestIfLocked();
  }

  void Lock_AssertHeld() {
#   ifndef NDEBUG
    DLOG_ASSERT(ctrl_block()->TestIfLockedByMe());
#   endif
  }

  int GetIndex(const std::string& name) const {
    return dir_idx_->GetIndex(name);
  }

  int ToServer(int index) const {
    return dir_idx_->GetServerForIndex(index);
  }

  const DirIndex* FetchDirIndex() const {
    return dir_idx_;
  }

  void PutDirIndex(std::string* buffer) const {
    Slice dmap_data = dir_idx_->ToSlice();
    buffer->assign(dmap_data.data(), dmap_data.size());
  }

  const DirIndex* Set(int index) {
    dir_idx_->SetBit(index);
    return dir_idx_;
  }

  const DirIndex* UpdateDirIndex(const Slice& dmap_data) {
    dir_idx_->Update(dmap_data);
    return dir_idx_;
  }

  int FetchZerothServer() const {
    return dir_idx_->FetchZerothServer();
  }

  int NextIndex(int index) {
    return dir_idx_->NewIndexForSplitting(index);
  }

  bool IsSplittingDisabled() {
    return ctrl_block()->disable_splitting;
  }

  void DisableSplitting() {
    ctrl_block()->disable_splitting = true;
  }

  void EnableSplitting() {
    ctrl_block()->disable_splitting = false;
  }

  bool HasPartitionData(int index) {
    return ctrl_block()->size_map.count(index) != 0;
  }

  int GetPartitionSize(int index) {
    return ctrl_block()->size_map[index];
  }

  int GetTotalPartitionSize() {
    return ctrl_block()->TotalPartitionSize();
  }

  int InceaseAndGetPartitionSize(int index, int delta) {
    int size = ctrl_block()->size_map[index];
    ctrl_block()->size_map[index] = size + delta;
    return ctrl_block()->size_map[index];
  }

  bool EligibleToSplit(int index) {
    return IsSplittingDisabled() ? false : dir_idx_->IsSplittable(index);
  }

 private:
  DirIndex* dir_idx_;
  DirCtrlTable* ctrl_table_;
  DirIndexCache* index_cache_;

  DirData dir_data_;

   // No copying allowed
  DirGuard(const DirGuard&);
  DirGuard& operator=(const DirGuard&);
  DirCtrlBlock* ctrl_block() { return dir_data_.first; }
};

} // namespace indexfs

#endif /* _INDEXFS_COMMON_DIRGUARD_H_ */
