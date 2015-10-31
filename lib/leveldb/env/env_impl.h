// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_ENV_IMPL_H_
#define STORAGE_ENV_IMPL_H_

#include <string>
#include "db/filename.h"

namespace leveldb {

#define E_TMP ".dbtmp"
#define E_DAT ".dat"
#define E_SST ".sst"
#define E_LOG ".log"
#define F_CUR "CURRENT"
#define F_DSC "MANIFEST"

// Return true if the file path ends with the given suffix
extern bool EndsWith(const std::string& fname,
                     const std::string& suffix);

// Return true if the file path starts with the given prefix
extern bool StartsWith(const std::string& fname,
                       const std::string& prefix);

// Return true if the last component of the path starts with the given prefix
extern bool LastComponentStartsWith(const std::string& fname,
                                    const std::string& prefix);

// Return true if the specified file should be
// stored on the underlying HDFS cluster
extern bool OnHDFS(const std::string& fname);

// Return true if the specified type of file must
// be externally stored on the underlying RADOS cluster.
extern bool OnRados(FileType type);

// Return the file type given the specified file name.
extern FileType ResolveFileType(const std::string& fname);

} // namespace leveldb

#endif /* STORAGE_ENV_IMPL_H_ */
