// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "metadb/dboptions.h"
#include "common/logging.h"

#include "leveldb/env.h"
#include "leveldb/cache.h"
#include "leveldb/options.h"
#include "leveldb/comparator.h"
#include "leveldb/filter_policy.h"

namespace indexfs {
namespace mdb {

// Import LevelDB types
//
using leveldb::BytewiseComparator;
using leveldb::kNoCompression;
using leveldb::kSnappyCompression;
using leveldb::NewLRUCache;
using leveldb::FilterPolicy;
using leveldb::NewBloomFilterPolicy;

namespace {

static inline const
FilterPolicy* NewLevelDBFilterPolicy(int bits_per_key) {
  if (bits_per_key <= 0) {
    return NULL;
  }
  return NewBloomFilterPolicy(bits_per_key);
}

static inline
void ApplyCommonOptions(Options* options, Config* config, Env* env) {
  options->env = env != NULL ? env : GetSystemEnv(config);
  options->info_log = NULL;
  options->max_open_files = DEFAULT_LEVELDB_MAX_OPEN_FILES;
  options->block_size = DEFAULT_LEVELDB_BLOCK_SIZE;
  options->max_sst_file_size = DEFAULT_LEVELDB_SSTABLE_SIZE;
  options->write_buffer_size = DEFAULT_LEVELDB_WRITE_BUFFER_SIZE;
  options->level_factor = DEFAULT_LEVELDB_LEVEL_FACTOR;
  options->level_zero_factor = DEFAULT_LEVELDB_ZERO_FACTOR;
  options->compression = DEFAULT_LEVELDB_COMPRESSION ?
        kSnappyCompression : kNoCompression;
  options->enable_monitor_thread = DEFAULT_LEVELDB_MONITORING;
  options->comparator = BytewiseComparator();
  options->filter_policy = NewLevelDBFilterPolicy(DEFAULT_LEVELDB_FILTER_BYTES);
}

} // namespace

void DefaultLevelDBOptionInitializer(Options* options,
                                     Config* config, Env* env) {
  DLOG_ASSERT(config->IsServer());
  ApplyCommonOptions(options, config, env);
  options->disable_compaction = false;
  options->disable_write_ahead_log = false;
  options->block_cache = NewLRUCache(DEFAULT_LEVELDB_CACHE_SIZE);
}

void BatchClientLevelDBOptionInitializer(Options* options,
                                         Config* config, Env* env) {
  DLOG_ASSERT(config->IsBatchClient());
  ApplyCommonOptions(options, config, env);
  options->disable_compaction = true;
  options->disable_write_ahead_log = false;
  options->block_cache = NewLRUCache(BATCH_CLIENT_LEVELDB_CACHE_SIZE);
}

} /* namespace mdb */
} /* namespace indexfs */
