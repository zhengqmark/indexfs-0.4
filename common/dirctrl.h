// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_COMMON_DIRCTRL_H_
#define _INDEXFS_COMMON_DIRCTRL_H_

#include <map>

#include "common/logging.h"
#include "common/common.h"
#include "common/options.h"

namespace indexfs {

class DirCtrlTable;
class DirCtrlBlock;
class DirCtrlGuard;

// -------------------------------------------------------------
// Directory Control Block
// -------------------------------------------------------------

class DirCtrlBlock {
 public:

  DirCtrlBlock();

  void Lock() {
    MutexLock lock(&mtx_);
    DoLock();
  }

  void Unlock() {
    MutexLock lock(&mtx_);
    DoUnlock();
  }

  void Wait() {
    MutexLock lock(&mtx_);
    DoUnlock();
    dir_cv_.Wait();
    DoLock();
  }

  void NotifyAll() {
    dir_cv_.SignalAll();
  }

  bool TestIfLocked() {
    MutexLock lock(&mtx_);
    return locked_;
  }

# ifndef NDEBUG
  bool TestIfLockedByMe() {
    MutexLock lock(&mtx_);
    return locked_ && lock_owner_ == pthread_self();
  }
# endif

  bool disable_splitting;
  std::map<int, int> size_map;

  int TotalPartitionSize() {
    int sum = 0;
    std::map<int, int>::iterator it;
    for (it = size_map.begin(); it != size_map.end();
         it++) {
      sum += it->second;
    }
    return sum;
  }

  DirCtrlTable* GetTable() { return table_; }
  bool Empty() const { return size_map.empty(); }
  size_t NumPartitions() const { return size_map.size(); }

 private:
  bool locked_;
  Mutex mtx_;
  CondVar dir_cv_;
  CondVar lock_cv_;

# ifndef NDEBUG
  pthread_t lock_owner_;
# endif

  void DoLock() {
    mtx_.AssertHeld();
    while (locked_) {
      lock_cv_.Wait();
    }
#   ifndef NDEBUG
    DLOG_ASSERT(lock_owner_ == -1);
    lock_owner_ = pthread_self();
#   endif
    locked_ = true;
  }

  void DoUnlock() {
    mtx_.AssertHeld();
    locked_ = false;
#   ifndef NDEBUG
    DLOG_ASSERT(lock_owner_ == pthread_self());
    lock_owner_ = -1;
#   endif
    lock_cv_.SignalAll();
  }

  friend class DirCtrlTable;
  DirCtrlTable* table_;
  Cache::Handle* handle_;

  // No copying allowed
  DirCtrlBlock(const DirCtrlBlock&);
  DirCtrlBlock& operator=(const DirCtrlBlock&);
};

// -------------------------------------------------------------
// Directory Control Table
// -------------------------------------------------------------

class DirCtrlTable {
 public:

  void Evict(int64_t dir_id);
  DirCtrlBlock* Fetch(int64_t dir_id);
  void Release(DirCtrlBlock* ctrl_blk);

  DirCtrlTable(int cap = (1 << 30)) { cache_ = NewLRUCache(cap); }

  virtual ~DirCtrlTable() { delete cache_; }

 private:
  Cache* cache_;

  // No copying allowed
  DirCtrlTable(const DirCtrlTable&);
  DirCtrlTable& operator=(const DirCtrlTable&);
};

// -------------------------------------------------------------
// Exception-safe Control Block Guard
// -------------------------------------------------------------

class DirCtrlGuard {
 public:
  explicit DirCtrlGuard(DirCtrlBlock* cb) : cb_(cb) {
    table_ = cb_->GetTable();
  }
  ~DirCtrlGuard() {
    table_->Release(cb_);
  }

 private:
  DirCtrlTable* table_;
  DirCtrlBlock* const cb_;

  // No copying allowed
  DirCtrlGuard(const DirCtrlGuard&);
  DirCtrlGuard& operator=(const DirCtrlGuard&);
};

} // namespace indexfs

#endif /* _INDEXFS_COMMON_DIRCTRL_H_ */
