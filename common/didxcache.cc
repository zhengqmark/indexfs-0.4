// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "common/logging.h"
#include "common/didxcache.h"

namespace indexfs {

namespace {
static
void DeleteIndexEntry(const Slice& key, void* value) {
  if (value != NULL) {
    delete reinterpret_cast<DirIndexEntry*>(value);
  }
}
}

void DirIndexCache::Evict(int64_t dir_id) {
  std::string key;
  PutFixed64(&key, dir_id);
  cache_->Erase(key);
}

void DirIndexCache::Release(DirIndexEntry* entry) {
  if (entry != NULL) {
    DLOG_ASSERT(entry->handle_ != NULL);
    cache_->Release(entry->handle_);
  }
}

DirIndexEntry* DirIndexCache::Get(int64_t dir_id) {
  std::string key;
  PutFixed64(&key, dir_id);
  Cache::Handle* handle = cache_->Lookup(key);
  if (handle == NULL) {
    return NULL;
  }
  void* value = cache_->Value(handle);
  DirIndexEntry* entry = reinterpret_cast<DirIndexEntry*>(value);
  DLOG_ASSERT(entry->handle_ == handle);
  return entry;
}

DirIndexEntry* DirIndexCache::Insert(DirIndex* dir_idx) {
  int64_t dir_id = dir_idx->FetchDirId();
  std::string key;
  PutFixed64(&key, dir_id);
  DLOG_ASSERT(cache_->Lookup(key) == NULL);
  DirIndexEntry* entry = new DirIndexEntry(dir_idx);
  Cache::Handle* handle = cache_->Insert(key, entry, 1, &DeleteIndexEntry);
  entry->cache_ = this;
  entry->handle_ = handle;
  return entry;
}

} // namespace indexfs
