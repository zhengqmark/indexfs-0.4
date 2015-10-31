// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_COMMON_LOOKUPCACHE_H_
#define _INDEXFS_COMMON_LOOKUPCACHE_H_

#include "common/common.h"
#include "common/options.h"

namespace indexfs {

class LookupCache;
class LookupEntry {
 public:

  LookupEntry() { }

  int64_t inode_no;
  int16_t perm;
  int16_t uid;
  int16_t gid;
  int16_t zeroth_server;
  int64_t lease_due;

  LookupCache* GetCache() { return cache_; }

 private:
  LookupCache* cache_;
  Cache::Handle* handle_;
  friend class LookupCache;

  // No copying allowed
  LookupEntry(const LookupEntry&);
  LookupEntry& operator=(const LookupEntry&);
};

class LookupCache {
 public:

  void Evict(const OID& oid);
  void Release(LookupEntry* entry);
  LookupEntry* Get(const OID& oid);
  LookupEntry* New(const OID& oid, const LookupInfo& info);

  LookupCache(int cap = (1 << 30)) { cache_ = NewLRUCache(cap); }

  virtual ~LookupCache() { delete cache_; }

 private:
  Cache* cache_;

  // No copying allowed
  LookupCache(const LookupCache&);
  LookupCache& operator=(const LookupCache&);
};

} // namespace indexfs

#endif /* _INDEXFS_COMMON_LOOKUPCACHE_H_ */
