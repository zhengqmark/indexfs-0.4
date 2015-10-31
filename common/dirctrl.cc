// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <string>

#include "common/logging.h"
#include "common/dirctrl.h"

namespace indexfs {

DirCtrlBlock::DirCtrlBlock()
  : disable_splitting(false),
    size_map(),
    locked_(false),
    mtx_(),
    dir_cv_(&mtx_),
    lock_cv_(&mtx_) {
# ifndef NDEBUG
  lock_owner_ = -1; // this means nobody owns the lock
# endif
}

namespace {
static
void DeleteCtrlBlock(const Slice& key, void* value) {
  if (value != NULL) {
    delete reinterpret_cast<DirCtrlBlock*>(value);
  }
}
}

void DirCtrlTable::Evict(int64_t dir_id) {
  std::string key;
  PutFixed64(&key, dir_id);
  cache_->Erase(key);
}

void DirCtrlTable::Release(DirCtrlBlock* ctrl_blk) {
  if (ctrl_blk != NULL) {
    DLOG_ASSERT(ctrl_blk->handle_ != NULL);
    cache_->Release(ctrl_blk->handle_);
  }
}

DirCtrlBlock* DirCtrlTable::Fetch(int64_t dir_id) {
  std::string key;
  PutFixed64(&key, dir_id);
  Cache::Handle* handle = cache_->Lookup(key);
  if (handle == NULL) {
    DirCtrlBlock* ctrl_blk = new DirCtrlBlock();
    handle = cache_->Insert(key, ctrl_blk, 1, &DeleteCtrlBlock);
    ctrl_blk->table_ = this;
    ctrl_blk->handle_ = handle;
  }
  void* value = cache_->Value(handle);
  DirCtrlBlock* ctrl_blk = reinterpret_cast<DirCtrlBlock*>(value);
  DLOG_ASSERT(ctrl_blk->handle_ == handle);
  return ctrl_blk;
}

} // namespace indexfs
