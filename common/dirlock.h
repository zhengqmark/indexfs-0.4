// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_COMMON_DIRLOCK_H_
#define _INDEXFS_COMMON_DIRLOCK_H_

#include "common/dirguard.h"
#include "common/dirctrl.h"
#include "common/didxcache.h"

namespace indexfs {

class DirLock {
 public:
  explicit DirLock(DirGuard* dg) : dg_(dg) {
    this->dg_->Lock();
  }
  ~DirLock() { this->dg_->Unlock(); }

 private:
  DirGuard* dg_;

  // No copying allowed
  DirLock(const DirLock&);
  DirLock& operator=(const DirLock&);
};

class RawLock {
 public:
  explicit RawLock(DirCtrlBlock* cb) : cb_(cb) {
    this->cb_->Lock();
  }
  ~RawLock() { this->cb_->Unlock(); }

 private:
  DirCtrlBlock* cb_;

  // No copying allowed
  RawLock(const RawLock&);
  RawLock& operator=(const RawLock&);
};

} // namespace indexfs

#endif /* _INDEXFS_COMMON_DIRLOCK_H_ */
