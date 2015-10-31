// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <assert.h>

#include "env/obj_set.h"
#include "env/obj_io.h"
#include "env/env_impl.h"
#include "env/io_logger.h"

namespace leveldb {

// Create a new write ahead log for the given set.
// NOTE: this will truncate the existing write ahead log if that exists.
//
Status ObjEnv::CreateLogForSet(const std::string& set, IOLog** log_ptr) {
  Status s;
  WritableFile* file;
  assert(io_ != NULL);
  s = io_->NewWritableObject(set, &file);
  if (!s.ok()) {
    *log_ptr = NULL;
  } else {
    *log_ptr = new IOLog(file);
  }
  return s;
}

// Destroy an IOLog and remove the underlying data object.
// Since the underlying data object will be deleted any way, it will
// be useless for it to be sync'ed before getting removed.
//
Status ObjEnv::DestroyLog(const std::string& set, IOLog* io_log) {
  delete io_log;
  assert(io_ != NULL);
  return io_->DeleteObject(set);
}

// Sync the underlying data object and release the log.
//
Status ObjEnv::SyncAndCloseLog(const std::string& set, IOLog* io_log) {
  Status s;
  if (io_log != NULL) {
    s = io_log->Sync();
    delete io_log;
  }
  return s;
}

// Returns true iff the specified name exists
// within the given set.
// REQUIRE: external synchronization.
// This can be used to mimic env->FileExists(...).
//
inline bool
ObjEnv::Exists(const std::string& name, ObjSet& obj_set) {
  assert(obj_set.first != NULL);
  return obj_set.first->find(name) != obj_set.first->end();
}

// Insert the specified name into the given set.
// Return Status::AlreadyExists if that name already exists.
// REQUIRE: external synchronization.
// This can be used to mimic env->NewWritableFile(...).
//
Status ObjEnv::AddName(const std::string& set, const std::string& name,
                       ObjSet& obj_set) {
  Status s;
  if (obj_set.second != NULL) {
    if (Exists(name, obj_set)) {
      s = Status::AlreadyExists(Slice());
    }
    if (s.ok()) {
      s = obj_set.second->NewObject(set, name);
      if (s.ok()) {
        assert(obj_set.first != NULL);
        obj_set.first->insert(name);
      }
    }
  } else {
    s = Status::Corruption("Set read only");
  }
  return s;
}

// Remove the specified name from the given set.
// Return Status::NotFound if no such name can be found.
// REQUIRE: external synchronization.
// This can be used to mimic env->DeleteFile(...).
//
Status ObjEnv::RemoveName(const std::string& set, const std::string& name,
                          ObjSet& obj_set) {
  Status s;
  if (obj_set.second != NULL) {
    assert(obj_set.first != NULL);
    ObjIter it = obj_set.first->find(name);
    if (it == obj_set.first->end()) {
      s = Status::NotFound(Slice());
    }
    if (s.ok()) {
      s = obj_set.second->DeleteObject(set, name);
      if (s.ok()) {
        obj_set.first->erase(it);
      }
    }
  } else {
    s = Status::Corruption("Set read only");
  }
  return s;
}

// Retrieve all names within a given set.
// Current implementation will always return Status::OK.
// REQUIRE: external synchronization.
// This can be used to mimic env->GetChildren(...).
//
Status ObjEnv::FetchAllNames(const std::string& set,
                             std::vector<std::string>* names,
                             ObjSet& obj_set) {
  Status s;
  assert(obj_set.first != NULL);
  size_t skip = set.length() + 1;
  for (ObjIter it = obj_set.first->begin(); it != obj_set.first->end(); ++it) {
    names->push_back(it->substr(skip));
  }
  return s;
}

// Return a pointer to a named object set if that set exists.
// REQUIRE: external synchronization.
//
inline ObjEnv::ObjSet*
ObjEnv::FetchSet(const std::string& name) {
  SetIter it = obj_sets_.find(name);
  return it != obj_sets_.end() ? &it->second : NULL;
}

// Return all existing object sets we known about.
//
Status ObjEnv::ListAllSets(std::vector<std::string>* names) {
  Status s;
  MutexLock lock(&mu_);
  SetIter it = obj_sets_.begin();
  for (; it != obj_sets_.end(); ++it) {
    names->push_back(it->first);
  }
  return s;
}

// Check the existence of a given object set
// using only in-memory states.
// Return Status::NotFound if no such set exists.
//
Status ObjEnv::HasSet(const std::string& spath) {
  Status s;
  std::string set;
  s = ResolveSetPath(spath, &set);
  if (!s.ok()) {
    return s;
  }
  MutexLock lock(&mu_);
  return FetchSet(set) != NULL ? s : Status::NotFound(Slice());
}

// Create a new object set as well as its corresponding write ahead
// log in the underlying storage to ensure durability and fault-tolerance.
// Return Status::AlreadyExists if that set already exists.
// Return Status::Corruption if the entire env is read only.
//
Status ObjEnv::CreateSet(const std::string& spath) {
  Status s;
  std::string set;
  s = ResolveSetPath(spath, &set);
  if (!s.ok()) {
    return s;
  }
  MutexLock lock(&mu_);
  if (FetchSet(set) != NULL) {
    return Status::AlreadyExists(Slice());
  }
  if (spath != default_set_path_) {
    if (default_log_ != NULL) {
      s = default_log_->NewSet(set);
    } else {
      s = Status::Corruption("Env read only");
    }
  }
  if (s.ok()) {
    IOLog* io_log;
    s = CreateLogForSet(set, &io_log);
    if (s.ok()) {
      std::pair<SetIter, bool> r = obj_sets_.insert(
          std::make_pair(set,
              std::make_pair(new std::set<std::string>(), io_log)));
      assert(r.second == true);
    }
  }
  return s;
}

// Permanently remove the given object set, including its
// corresponding write ahead log stored in the underlying storage.
// Return Status::NotFound if no such set exists.
// Return Status::IOError if the given set contains objects.
// Return Status::Corruption if the entire env is read only.
//
Status ObjEnv::DeleteSet(const std::string& spath) {
  Status s;
  std::string set;
  s = ResolveSetPath(spath, &set);
  if (!s.ok()) {
    return s;
  }
  MutexLock lock(&mu_);
  ObjSet* obj_set = FetchSet(set);
  if (obj_set == NULL) {
    return Status::NotFound(Slice());
  }
  if (obj_set->first->size() > 0) {
    return Status::IOError("Set not empty");
  }
  if (spath != default_set_path_) {
    if (default_log_ != NULL) {
      s = default_log_->DeleteSet(set);
    } else {
      s = Status::Corruption("Env read only");
    }
  }
  if (s.ok()) {
    Status s_ = DestroyLog(set, obj_set->second);
    if (!s_.ok()) {
      fprintf(stderr, "Fail to destroy the WAL for set %s\n", set.c_str());
    }
    delete obj_set->first;
    size_t n = obj_sets_.erase(set);
    assert(n == 1);
  }
  return s;
}

// Retrieve all the object names within a given object set.
// Return Status::NotFound if no such set exists.
//
Status ObjEnv::ListSet(const std::string& spath,
                       std::vector<std::string>* names) {
  Status s;
  std::string set;
  s = ResolveSetPath(spath, &set);
  if (!s.ok()) {
    return s;
  }
  MutexLock lock(&mu_);
  ObjSet* obj_set = FetchSet(set);
  if (obj_set == NULL) {
    return Status::NotFound(Slice());
  }
  return FetchAllNames(set, names, *obj_set);
}

// Force synchronize the internal WAL associated with
// the specified object set and mark set as read-only.
// Return Status::NotFound if no such set exists.
// No action will be taken if the specified object set is already sealed.
//
Status ObjEnv::SealSet(const std::string& spath) {
  Status s;
  std::string set;
  s = ResolveSetPath(spath, &set);
  if (!s.ok()) {
    return s;
  }
  MutexLock lock(&mu_);
  ObjSet* obj_set = FetchSet(set);
  if (obj_set == NULL) {
    return Status::NotFound(Slice());
  }
  s = SyncAndCloseLog(set, obj_set->second);
  obj_set->second = NULL;
  return s;
}

// Synchronize the internal WAL associated with the specified
// object set.
// Return Status::NotFound if no such set exists.
// No action will be taken if the specified object set is read-only.
//
Status ObjEnv::SyncSet(const std::string& spath) {
  Status s;
  std::string set;
  s = ResolveSetPath(spath, &set);
  if (!s.ok()) {
    return s;
  }
  MutexLock lock(&mu_);
  ObjSet* obj_set = FetchSet(set);
  if (obj_set == NULL) {
    return Status::NotFound(Slice());
  }
  if (obj_set->second != NULL) {
    s = obj_set->second->Sync();
  }
  return s;
}

// Fetch the name of the underlying data object backing
// the WAL of the specified object set.
//
Status ObjEnv::GetLogObjName(const std::string& spath, std::string* oname) {
  Status s;
  std::string set;
  s = ResolveSetPath(spath, &set);
  if (!s.ok()) {
    return s;
  }
  oname->assign(set);
  return s;
}

// Check the existence of a given object.
// Return Status::NotFound if no such object or set exists.
//
Status ObjEnv::HasObject(const std::string& opath) {
  Status s;
  std::string set;
  std::string name;
  s = ResolveObjectPath(opath, &set, &name);
  if (!s.ok()) {
    return s;
  }
  MutexLock lock(&mu_);
  ObjSet* obj_set = FetchSet(set);
  if (obj_set == NULL) {
    return Status::NotFound(Slice());
  }
  assert(io_ != NULL);
  if (io_->Exists(name)) {
    s = Exists(name, *obj_set) ?
        s : Status::Corruption(Slice());
  } else {
    s = Exists(name, *obj_set) ?
        Status::Corruption(Slice()) : Status::NotFound(Slice());
  }
  return s;
}

// Permanently remove the object from the given set and
// delete its data from the underlying storage.
// Return Status::NotFound if no such object or set exists.
//
Status ObjEnv::DeleteObject(const std::string& opath) {
  Status s;
  std::string set;
  std::string name;
  s = ResolveObjectPath(opath, &set, &name);
  if (!s.ok()) {
    return s;
  }
  MutexLock lock(&mu_);
  ObjSet* obj_set = FetchSet(set);
  if (obj_set == NULL || !Exists(name, *obj_set)) {
    return Status::NotFound(Slice());
  }
  assert(io_ != NULL);
  s = io_->DeleteObject(name);
  if (!s.ok()) {
    return s;
  }
  return RemoveName(set, name, *obj_set);
}

// Retrieve the size of a given object.
// Return Status::NotFound if no such object or set exists.
//
Status ObjEnv::GetObjectSize(const std::string& opath,
                                 uint64_t* size) {
  Status s;
  std::string set;
  std::string name;
  s = ResolveObjectPath(opath, &set, &name);
  if (!s.ok()) {
    return s;
  }
  MutexLock lock(&mu_);
  ObjSet* obj_set = FetchSet(set);
  if (obj_set == NULL || !Exists(name, *obj_set)) {
    return Status::NotFound(Slice());
  }
  assert(io_ != NULL);
  return io_->GetObjectSize(name, size);
}

// Atomically override an object using the given piece of data.
// Create this object if no such object currently exists.
// Return Status::NotFound if the specified parent set cannot be found.
//
Status ObjEnv::PutObject(const std::string& opath, const Slice& data) {
  Status s;
  std::string set;
  std::string name;
  s = ResolveObjectPath(opath, &set, &name);
  if (!s.ok()) {
    return s;
  }
  MutexLock lock(&mu_);
  ObjSet* obj_set = FetchSet(set);
  if (obj_set == NULL) {
    return Status::NotFound(Slice());
  }
  assert(io_ != NULL);
  s = io_->PutObject(name, data);
  if (!s.ok()) {
    return s;
  }
  return Exists(name, *obj_set) ? s : AddName(set, name, *obj_set);
}

// Open an append-only handle for a given object.
// Create this object if no such object currently exists.
// Return Status::NotFound if the specified parent set cannot be found.
//
Status ObjEnv::CreateWritableObject(const std::string& opath,
                                        WritableFile** result) {
  Status s;
  std::string set;
  std::string name;
  s = ResolveObjectPath(opath, &set, &name);
  if (!s.ok()) {
    return s;
  }
  MutexLock lock(&mu_);
  ObjSet* obj_set = FetchSet(set);
  if (obj_set == NULL) {
    return Status::NotFound(Slice());
  }
  assert(io_ != NULL);
  s = io_->NewWritableObject(name, result);
  if (!s.ok()) {
    return s;
  }
  return Exists(name, *obj_set) ? s : AddName(set, name, *obj_set);
}

// Open a read-only handle of a given object for sequential reads.
// Return Status::NotFound if no such object exists.
// Return Status::NotFound if the specified parent set cannot be found.
//
Status ObjEnv::CreateSequentialObject(const std::string& opath,
                                          SequentialFile** result) {
  Status s;
  std::string set;
  std::string name;
  s = ResolveObjectPath(opath, &set, &name);
  if (!s.ok()) {
    return s;
  }
  MutexLock lock(&mu_);
  ObjSet* obj_set = FetchSet(set);
  if (obj_set == NULL || !Exists(name, *obj_set)) {
    return Status::NotFound(Slice());
  }
  assert(io_ != NULL);
  return io_->NewSequentialObject(name, result);
}

// Open a read-only handle of a given object for random reads.
// Return Status::NotFound if no such object exists.
// Return Status::NotFound if the specified parent set cannot be found.
//
Status ObjEnv::CreateRandomAccessObject(const std::string& opath,
                                            RandomAccessFile** result) {
  Status s;
  std::string set;
  std::string name;
  s = ResolveObjectPath(opath, &set, &name);
  if (!s.ok()) {
    return s;
  }
  MutexLock lock(&mu_);
  ObjSet* obj_set = FetchSet(set);
  if (obj_set == NULL || !Exists(name, *obj_set)) {
    return Status::NotFound(Slice());
  }
  assert(io_ != NULL);
  return io_->NewRandomAccessObject(name, result);
}

// Create or override an object by copying from another existing object.
// Return Status::NotFound if the source object cannot be found.
// Return Status::NotFound if either of the two parent sets cannot be found.
//
Status ObjEnv::CopyObject(const std::string& spath, const std::string& tpath) {
  Status s;
  std::string s_set;
  std::string s_name;
  s = ResolveObjectPath(spath, &s_set, &s_name);
  if (!s.ok()) {
    return s;
  }
  std::string t_set;
  std::string t_name;
  s = ResolveObjectPath(tpath, &t_set, &t_name);
  if (!s.ok()) {
    return s;
  }
  MutexLock lock(&mu_);
  ObjSet* target_set = FetchSet(t_set);
  if (target_set == NULL) {
    return Status::NotFound(Slice());
  }
  ObjSet* src_set = FetchSet(s_set);
  if (src_set == NULL || !Exists(s_name, *src_set)) {
    return Status::NotFound(Slice());
  }
  assert(io_ != NULL);
  s = io_->CopyObject(s_name, t_name);
  if (!s.ok()) {
    return s;
  }
  return Exists(t_name, *target_set) ? s : AddName(t_set, t_name, *target_set);
}

// Simple IOLogReporter implementation developed to recover
// an existing object set from a log object in the underlying storage.
//
namespace {
struct SetBuilder: virtual public IOLogReporter {
  typedef std::set<std::string>::iterator Iter;
  std::string set_name;
  std::set<std::string>* obj_names;
  SetBuilder(const std::string& set_name) :
    set_name(set_name),
    obj_names(new std::set<std::string>()) {
  }
  virtual ~SetBuilder() {
    delete obj_names;
  }
  void SetDeleted(const std::string& set) {
    fprintf(stderr, "Ignore log entry %s:%s\n",
        __func__, set.c_str());
  }
  void ObjectDeleted(const std::string& set, const std::string& name) {
    assert(set_name == set);
    int n = obj_names->erase(name);
    if (n == 0) {
      fprintf(stderr, "Error recovering set %s: %s not found\n",
          set_name.c_str(), name.c_str());
    }
  }
  void SetCreated(const std::string& set) {
    fprintf(stderr, "Ignore log entry %s:%s\n",
        __func__, set.c_str());
  }
  void ObjectCreated(const std::string& set, const std::string& name) {
    assert(set_name == set);
    std::pair<Iter, bool> r = obj_names->insert(name);
    if (!r.second) {
      fprintf(stderr, "Error recovering set %s: %s already exists\n",
          set_name.c_str(), name.c_str());
    }
  }
};
}

// Load an existing object set from the underlying storage
// and recover its in-memory state.
// Return Status::NotFound if that set cannot be found.
// Return Status::AlreadyExists if that set already exists in memory.
//
Status ObjEnv::LoadSet(const std::string& spath) {
  Status s;
  std::string set;
  s = ResolveSetPath(spath, &set);
  if (!s.ok()) {
    return s;
  }
  assert(spath != default_set_path_);
  {
    MutexLock lock(&mu_);
    if (FetchSet(set) != NULL) {
      return Status::AlreadyExists(Slice());
    }
  }
  SequentialFile* file;
  assert(io_ != NULL);
  s = io_->NewSequentialObject(set, &file);
  if (!s.ok()) {
    return s;
  }
  SetBuilder builder(set);
  s = IOLogReader(file).Recover(&builder);
  if (!s.ok()) {
    return s;
  }
  MutexLock lock2(&mu_);
  if (FetchSet(set) != NULL) {
    return Status::AlreadyExists(Slice());
  }
  IOLog* io_log = NULL;
  std::pair<SetIter, bool> r = obj_sets_.insert(
      std::make_pair(set, std::make_pair(builder.obj_names, io_log)));
  assert(r.second == true);
  builder.obj_names = NULL;
  return s;
}

// Remove an existing object set from the memory.
// Return Status::NotFound if that set cannot be found.
//
Status ObjEnv::ForgetSet(const std::string& spath) {
  Status s;
  std::string set;
  s = ResolveSetPath(spath, &set);
  if (!s.ok()) {
    return s;
  }
  assert(spath != default_set_path_);
  MutexLock lock(&mu_);
  ObjSet* obj_set = FetchSet(set);
  if (obj_set == NULL) {
    return Status::NotFound(Slice());
  }
  s = SyncAndCloseLog(set, obj_set->second);
  delete obj_set->first;
  size_t n = obj_sets_.erase(set);
  assert(n == 1);
  return s;
}

ObjEnv::ObjEnv(IO* io,
               const std::string& default_set_path,
               const std::string& path_root,
               bool io_shared,
               bool read_only) :
               default_set_path_(default_set_path),
               path_root_(path_root),
               io_(io),
               io_shared_(io_shared),
               default_log_(NULL) {

  assert(StartsWith(default_set_path_, path_root_));
  assert(io_ != NULL);
  if (!read_only) {
    Status s_ = CreateSet(default_set_path_);
    if (!s_.ok()) {
      fprintf(stderr, "Fail to create default set: %s\n", s_.ToString().c_str());
      abort();
    }
    assert(obj_sets_.size() == 1);
    SetIter it = obj_sets_.begin();
    default_log_ = it->second.second;
    assert(default_log_ != NULL);
  }
}

ObjEnv::~ObjEnv() {
  for (SetIter it = obj_sets_.begin(); it != obj_sets_.end(); ++it) {
    Status s_ = SyncAndCloseLog(it->first, it->second.second);
    if (!s_.ok()) {
      fprintf(stderr, "Fail to sync log for set %s: %s\n",
          it->first.c_str(), s_.ToString().c_str());
    }
    delete it->second.first;
  }
  if (!io_shared_) {
    delete io_;
  }
}

// Global singleton instance
//
ObjEnv* ObjEnvFactory::obj_env_ = NULL;

ObjEnvFactory::ObjEnvFactory(IO* io, const std::string& io_prefix) {
  assert(obj_env_ == NULL);
  io_ = IO::PrefixedIO(io, io_prefix);
}

ObjEnv* ObjEnvFactory::NewObjEnv(const std::string& root,
                                 const std::string& default_set) {
  assert(obj_env_ == NULL);
  obj_env_ = new ObjEnv(io_, default_set, root, false, false);
  return obj_env_;
}

ObjEnv* ObjEnvFactory::NewReadOnlyObjEnv(const std::string& root,
                                         const std::string& default_set) {
  assert(obj_env_ == NULL);
  obj_env_ = new ObjEnv(io_, default_set, root, false, true);
  return obj_env_;
}

} // namespace leveldb
