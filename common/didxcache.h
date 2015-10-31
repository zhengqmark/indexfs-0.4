// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_COMMON_DIRIDX_CACHE_H_
#define _INDEXFS_COMMON_DIRIDX_CACHE_H_

#include "common/common.h"
#include "common/options.h"
#include "common/gigaidx.h"

namespace indexfs {

class DirIndexCache;
class DirIndexEntry;
class DirIndexGuard;

// -------------------------------------------------------------
// Directory Index Entry
// -------------------------------------------------------------

class DirIndexEntry {
 public:

  DirIndexEntry(DirIndex* index) : index(index),
      cache_(NULL), handle_(NULL) {
  }

  ~DirIndexEntry() {
    delete index;
  }

  DirIndex* const index;

  DirIndexCache* GetCache() { return cache_; }

 private:
  DirIndexCache* cache_;
  Cache::Handle* handle_;
  friend class DirIndexCache;

  // No copying allowed
  DirIndexEntry(const DirIndexEntry&);
  DirIndexEntry& operator=(const DirIndexEntry&);
};

// -------------------------------------------------------------
// Directory Index Cache
// -------------------------------------------------------------

class DirIndexCache {
 public:

  void Evict(int64_t dir_id);
  void Release(DirIndexEntry* entry);
  DirIndexEntry* Get(int64_t dir_id);
  DirIndexEntry* Insert(DirIndex* dir_idx);

  DirIndexCache(int cap = 4096) { cache_ = NewLRUCache(cap); }

  virtual ~DirIndexCache() { delete cache_; }

 private:
  Cache* cache_;

  // No copying allowed
  DirIndexCache(const DirIndexCache&);
  DirIndexCache& operator=(const DirIndexCache&);
};

// -------------------------------------------------------------
// Exception-safe Index Guard
// -------------------------------------------------------------

class DirIndexGuard {
 public:
  explicit DirIndexGuard(DirIndexEntry* ie) : ie_(ie) {
    cache_ = ie_->GetCache();
  }
  ~DirIndexGuard() {
    cache_->Release(ie_);
  }

 private:
  DirIndexCache* cache_;
  DirIndexEntry* const ie_;

  // No copying allowed
  DirIndexGuard(const DirIndexGuard&);
  DirIndexGuard& operator=(const DirIndexGuard&);
};

} // namespace indexfs

#endif /* _INDEXFS_COMMON_DIRIDX_CACHE_H_ */
