// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_INCLUDE_COMMON_H_
#define _INDEXFS_INCLUDE_COMMON_H_

#include "thrift/indexfs_types.h" /* Add RPC types */

#include "leveldb/port/port.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/slice.h"
#include "leveldb/cache.h"
#include "leveldb/options.h"
#include "leveldb/util/coding.h"
#include "leveldb/util/mutexlock.h"

namespace indexfs {

typedef std::vector<StatInfo> StatList;
typedef std::vector<std::string> NameList;

typedef const int16_t i16;
#define U16INT(i16) static_cast<uint16_t>(i16)
typedef const int64_t i64;
#define U64INT(i64) static_cast<uint64_t>(i64)

// -------------------------------------------------------------
// Common LevelDB types
// -------------------------------------------------------------

using leveldb::Env;
using leveldb::Slice;
using leveldb::Status;
using leveldb::Cache;
using leveldb::NewLRUCache;
using leveldb::MutexLock;
using leveldb::port::CondVar;
using leveldb::port::Mutex;
using leveldb::Options;
using leveldb::WritableFile;
using leveldb::RandomAccessFile;

// -------------------------------------------------------------
// Reuse LevelDB encoding/decoding algorithms
// -------------------------------------------------------------

using leveldb::PutFixed64;
using leveldb::VarintLength;
using leveldb::GetVarint32Ptr;
using leveldb::GetVarint64Ptr;

using leveldb::EncodeFixed16;
using leveldb::EncodeFixed32;
using leveldb::EncodeFixed64;
using leveldb::DecodeFixed16;
using leveldb::DecodeFixed32;
using leveldb::DecodeFixed64;
using leveldb::EncodeVarint32;
using leveldb::EncodeVarint64;

// -------------------------------------------------------------

} /* namespace indexfs */

#endif /* _INDEXFS_INCLUDE_COMMON_H_ */
