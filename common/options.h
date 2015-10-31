// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_LEGACY_OPTIONS_H_
#define _INDEXFS_LEGACY_OPTIONS_H_

#include <limits.h>
#ifndef PATH_MAX
#define PATH_MAX        4096
#endif
#ifndef HOST_NAME_MAX
#define HOST_NAME_MAX   64
#endif

#define ROOT_DIR_ID             0
#define ROOT_ZEROTH_SERVER      0
#define PARENT_OF_ROOT          0
#define PARTITION_OF_ROOT       0

// Default server port number
#define DEFAULT_SRV_PORT 10086

// Default directory bulk insertion size
#define DEFAULT_DIR_BULK_SIZE    (1<<10)
// Default bulk insertion size
#define DEFAULT_BULK_SIZE        (1<<20)
// Default directory split threshold
#define DEFAULT_DIR_SPLIT_THR    (1<<11)
// Default size of the directory entry cache
#define DEFAULT_DENT_CACHE_SIZE  (1<<16)
// Default size of the directory mapping cache
#define DEFAULT_DMAP_CACHE_SIZE  (1<<15)

// Default server limits
#ifndef IDXFS_EXTRA_SCALE
#define DEFAULT_MAX_NUM_SERVERS     256
#else
#define DEFAULT_MAX_NUM_SERVERS     16384
#endif

// LevelDB Tuning Knobs
#ifndef IDXFS_ENABLE_COMPRESSION
#define DEFAULT_LEVELDB_COMPRESSION false
#else
#define DEFAULT_LEVELDB_COMPRESSION true
#endif

#define DEFAULT_SMALLFILE_THRESHOLD     65536
#define DEFAULT_LEVELDB_MONITORING      false
#define DEFAULT_LEVELDB_SAMPLING_INTERVAL  1
#define DEFAULT_LEVELDB_FILTER_BYTES    14
#define DEFAULT_LEVELDB_MAX_OPEN_FILES  128
#define DEFAULT_LEVELDB_SYNC_INTERVAL   5
#define DEFAULT_LEVELDB_USE_COLUMNDB    false
#define DEFAULT_LEVELDB_ZERO_FACTOR     10.0
#define DEFAULT_LEVELDB_LEVEL_FACTOR    10.0
#define DEFAULT_LEVELDB_BLOCK_SIZE         (64 << 10)
#define DEFAULT_LEVELDB_SSTABLE_SIZE       (32 << 20)
#define DEFAULT_LEVELDB_WRITE_BUFFER_SIZE  (32 << 20)
#define DEFAULT_LEVELDB_CACHE_SIZE         (512 << 20)
#define BATCH_CLIENT_LEVELDB_CACHE_SIZE    DEFAULT_LEVELDB_CACHE_SIZE

#endif /* _INDEXFS_LEGACY_OPTIONS_H_ */
