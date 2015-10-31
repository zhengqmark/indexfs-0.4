// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_OBJ_SET_H_
#define STORAGE_OBJ_SET_H_

#include <stdio.h>
#include <stdlib.h>
#include <set>
#include <map>
#include <vector>

#include "leveldb/env.h"
#include "leveldb/status.h"
#include "util/mutexlock.h"

namespace leveldb {

class IO;
class IOLog;
class ObjEnv;
class ObjEnvFactory;

class ObjEnv {
 public:
  virtual ~ObjEnv();
  Status ListAllSets(std::vector<std::string>* names);
  Status HasSet(const std::string& spath);
  Status LoadSet(const std::string& spath);
  Status ForgetSet(const std::string& spath);
  Status CreateSet(const std::string& spath);
  Status DeleteSet(const std::string& spath);
  Status ListSet(const std::string& spath,
      std::vector<std::string>* names);
  Status SealSet(const std::string& spath);
  Status SyncSet(const std::string& spath);
  Status GetLogObjName(const std::string& spath, std::string* oname);
  Status HasObject(const std::string& opath);
  Status GetObjectSize(const std::string& opath, uint64_t* size);
  Status PutObject(const std::string& opath, const Slice& data);
  Status DeleteObject(const std::string& opath);
  Status CopyObject(const std::string& spath, const std::string& tpath);
  Status CreateWritableObject(const std::string& opath,
      WritableFile** result);
  Status CreateSequentialObject(const std::string& opath,
      SequentialFile** result);
  Status CreateRandomAccessObject(const std::string& opath,
      RandomAccessFile** result);

  static ObjEnv* TEST_NewInstance(IO* io,
      const std::string& default_set_path, const std::string& path_root) {
    return new ObjEnv(io, default_set_path, path_root, false, false);
  }

 private:

  ObjEnv(IO* io,
         const std::string& default_set_path,
         const std::string& path_root,
         bool io_shared = false, bool read_only = false);

  // No copying allowed
  ObjEnv(const ObjEnv&);
  ObjEnv& operator=(const ObjEnv&);

  friend class ObjEnvFactory;
  typedef std::pair<std::set<std::string>*, IOLog*> ObjSet;
  typedef std::set<std::string>::iterator ObjIter;
  ObjSet* FetchSet(const std::string& name);
  bool Exists(const std::string& name, ObjSet& obj_set);
  Status AddName(const std::string& set, const std::string& name,
      ObjSet& obj_set);
  Status RemoveName(const std::string& set, const std::string& name,
      ObjSet& obj_set);
  Status FetchAllNames(const std::string& set, std::vector<std::string>* names,
      ObjSet& obj_set);
  std::string default_set_path_;
  std::string path_root_;
  IO* io_;
  bool io_shared_;
  IOLog* default_log_;
  port::Mutex mu_;
  std::map<std::string, ObjSet> obj_sets_;
  typedef std::map<std::string, ObjSet>::iterator SetIter;
  Status CreateLogForSet(const std::string& set, IOLog** log_ptr);
  Status DestroyLog(const std::string& set, IOLog* io_log);
  Status SyncAndCloseLog(const std::string& set, IOLog* io_log);
  Status ResolveObjectPath(const std::string& opath,
        std::string* set, std::string* name);
  Status ResolveSetPath(const std::string& spath, std::string* set);
};

class ObjEnvFactory {
 private:
  IO* io_;
  static ObjEnv* obj_env_;

 public:
  virtual ~ObjEnvFactory() { }

  ObjEnvFactory(IO* io, const std::string& io_prefix);

  static ObjEnv* FetchObjEnv() { return obj_env_; }

  ObjEnv* NewObjEnv(const std::string& root, const std::string& default_set);
  ObjEnv* NewReadOnlyObjEnv(const std::string& root, const std::string& default_set);
};

} // namespace leveldb

#endif /* STORAGE_OBJ_SET_H_ */
