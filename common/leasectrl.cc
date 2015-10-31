// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "common/logging.h"
#include "common/leasectrl.h"

namespace indexfs {

namespace {
enum LeaseState {
  kFree,
  kRead,
  kWrite,
};
static const uint64_t kEpsilon = 10 * 1000;
static const uint64_t kLeaseTime = 1000 * 1000;
}

LeaseEntry::LeaseEntry() :
    lease_state_(kFree), lease_due_(0) {
}

ReadLock::ReadLock(LeaseEntry* entry, DirGuard* guard, Env* env) :
    env_(env), guard_(guard), entry_(entry) {
  guard_->Lock_AssertHeld();
  while (entry_->lease_state_ == kWrite) {
    uint64_t now = env_->NowMicros();
    if (now + kEpsilon < entry_->lease_due_) {
      break;
    }
    guard_->Wait(); // Wait for the current writer
  }
  if (entry_->lease_state_ == kFree) {
    entry_->lease_state_ = kRead;
  }
}

ReadLock::~ReadLock() {
  DLOG_ASSERT(entry_->lease_state_ != kFree);
  if (entry_->lease_state_ == kRead) {
    uint64_t new_due = env_->NowMicros() + kLeaseTime;
    DLOG_ASSERT(new_due >= entry_->lease_due_);
    entry_->lease_due_ = new_due;
  }
}

WriteLock::WriteLock(LeaseEntry* entry, DirGuard* guard, Env* env) :
    env_(env), guard_(guard), entry_(entry) {
  guard_->Lock_AssertHeld();
  while (entry_->lease_state_ == kWrite) {
    guard_->Wait(); // Wait for the current writer
  }
  if (entry_->lease_state_ == kFree) {
    entry_->lease_state_ = kWrite;
  }
  while (entry_->lease_state_ == kRead) {
    entry_->lease_state_ = kWrite; // Avoid lease getting renewed
    uint64_t now = env_->NowMicros();
    if (now <= entry_->lease_due_ + kEpsilon) {
      uint64_t wait = entry_->lease_due_ + kEpsilon - now;
      guard_->Unlock();
      env_->SleepForMicroseconds(wait);
      guard_->Lock();
    }
  }
}

WriteLock::~WriteLock() {
  entry_->lease_due_ = 0;
  entry_->lease_state_ = kFree;
  guard_->NotifyAll();
}

namespace {
static
void DeleteLeaseEntry(const Slice& key, void* value) {
  if (value != NULL) {
    delete reinterpret_cast<LeaseEntry*>(value);
  }
}
}

void LeaseTable::Evict(const OID& oid) {
  std::string key;
  PutFixed64(&key, oid.dir_id);
  key.append(oid.obj_name);
  cache_->Erase(key);
}

void LeaseTable::Release(LeaseEntry* entry) {
  if (entry != NULL) {
    DLOG_ASSERT(entry->handle_ != NULL);
    cache_->Release(entry->handle_);
  }
}

LeaseEntry* LeaseTable::Get(const OID& oid) {
  std::string key;
  PutFixed64(&key, oid.dir_id);
  key.append(oid.obj_name);
  Cache::Handle* handle = cache_->Lookup(key);
  if (handle == NULL) {
    return NULL;
  }
  void* value = cache_->Value(handle);
  LeaseEntry* entry = reinterpret_cast<LeaseEntry*>(value);
  DLOG_ASSERT(entry->handle_ == handle);
  return entry;
}

LeaseEntry* LeaseTable::New(const OID& oid, const StatInfo& info) {
  std::string key;
  PutFixed64(&key, oid.dir_id);
  key.append(oid.obj_name);
  DLOG_ASSERT(cache_->Lookup(key) == NULL);
  LeaseEntry* entry = new LeaseEntry();
  entry->inode_no = info.id;
  entry->uid = info.uid;
  entry->gid = info.gid;
  entry->perm = info.mode;
  entry->zeroth_server = info.zeroth_server;
  Cache::Handle* handle = cache_->Insert(key, entry, 1, &DeleteLeaseEntry);
  entry->table_ = this;
  entry->handle_ = handle;
  return entry;
}

} // namespace indexfs
