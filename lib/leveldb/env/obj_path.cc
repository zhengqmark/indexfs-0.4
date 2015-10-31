// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <assert.h>
#include "env/obj_set.h"
#include "env/env_impl.h"

namespace leveldb {

namespace {
static
void CheckPath(const std::string& path, const std::string& root) {
  assert(StartsWith(path, root));
  assert(path.length() > root.length() + 1);
  assert(path[root.length()] == '/');
}
}

// Resolve a given path pointing to a set.
// ----------------------------
//  <path_root> / <set_name>
// ----------------------------
Status ObjEnv::ResolveSetPath(const std::string& spath,
                              std::string* set) {
  Status s;
  CheckPath(spath, path_root_);
# ifndef NDEBUG
  size_t d = spath.find('/', path_root_.length() + 1);
  if (d != std::string::npos) {
    return Status::InvalidArgument(Slice());
  }
# endif
  set->assign(spath, path_root_.length() + 1, std::string::npos);
  return s;
}

// Resolve a given path pointing to a set.
// ------------------------------------------
//  <path_root> / <set_name> / <object_name>
// ------------------------------------------
Status ObjEnv::ResolveObjectPath(const std::string& opath,
                                 std::string* set, std::string* name) {
  Status s;
  CheckPath(opath, path_root_);
  size_t d = opath.find('/', path_root_.length() + 1);
# ifndef NDEBUG
  if (d == std::string::npos) {
    return Status::InvalidArgument(Slice());
  }
# endif
  set->assign(opath, path_root_.length() + 1, d - path_root_.length() - 1);
  name->assign(opath, path_root_.length() + 1, std::string::npos);
  (*name)[set->length()] = '_';
# ifndef NDEBUG
  if (name->find('/', 0) != std::string::npos) {
    return Status::InvalidArgument(Slice());
  }
# endif
  return s;
}

} // namespace leveldb
