// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_ENV_RADOS_H_
#define STORAGE_LEVELDB_INCLUDE_ENV_RADOS_H_

namespace leveldb {
class Env;
struct RadosOptions {
  bool read_only;
  const char* db_root;
  const char* db_home;
  RadosOptions()
        : read_only(false),
          db_root("/tmp"),
          db_home("/tmp/indexfs") {
  }
};
extern Env* GetOrNewRadosEnv(const RadosOptions& options);
}

#endif /* STORAGE_LEVELDB_INCLUDE_ENV_RADOS_H_ */
