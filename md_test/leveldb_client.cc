// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <time.h>
#include <unistd.h>

#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/cache.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "leveldb/filter_policy.h"

#ifdef IDXFS_USE_CITYHASH
#include "city.h"
#endif

#ifdef IDXFS_USE_SMALL_STAT
#include "filestat.h"
#endif

#if defined(IDXFS_USE_SHA1) or defined(IDXFS_USE_SHA1_HEX)
extern "C" {
#include "sha.h"
}
#endif

#include "common/config.h"
#include "common/logging.h"
#include "client/libclient.h"

using ::leveldb::DB;
using ::leveldb::Env;
using ::leveldb::Cache;
using ::leveldb::Status;
using ::leveldb::Slice;
using ::leveldb::Options;
using ::leveldb::WriteOptions;
using ::leveldb::ReadOptions;
using ::leveldb::kNoCompression;
using ::leveldb::NewLRUCache;
using ::leveldb::NewZigzagFilterPolicy;
using ::leveldb::ColumnDBOpen;

using ::indexfs::Logger;
using ::indexfs::Config;
using ::indexfs::LoadClientConfig;
using ::indexfs::GetLogFileName;
using ::indexfs::GetConfigFileName;
using ::indexfs::GetServerListFileName;
using ::indexfs::GetDefaultLogDir;

// Disable gflag in favor of ENV-based options setting.
// This is a simple way to avoid gflag's interference with mdtest's
// command-line-based options parsing.
//
namespace indexfs {

DEFINE_string(logfn, "libclient",
    "please ignore this option -- libclient only log to stderr");

DEFINE_string(configfn, "", /* empty */
    "please use ENV 'IDXFS_CONFIG_FILE' to set this option");

DEFINE_string(srvlstfn, "", /* empty */
    "please use ENV 'IDXFS_SERVER_LIST' to set this option");

#ifdef HDFS
DEFINE_string(hconfigfn, "", /* empty */
    "please use ENV 'IDXFS_HDFS_CONFIG_FILE' to set this option");
#endif

} /* namespace indexfs */

extern "C" {

//////////////////////////////////////////////////////////////////////////////////
// GLOBAL VARIABLES
//

#ifdef IDXFS_USE_SYS_STAT
static struct stat stats;
#endif

#ifdef IDXFS_USE_SMALL_STAT
static struct filestat stats;
#endif

static const uint64_t inode = 512; // Assuming ony 1 dir will even been created!
static char key[64] = { 0 };
static char value[256 + 256 + 256 + 4096] = { 0 };

static int LogErrorAndReturn(Status &st) {
  if (!st.ok()) {
    std::string err = st.ToString();
    fprintf(stderr, "%s\n", err.c_str());
    return -1;
  }
  return 0;
}

//////////////////////////////////////////////////////////////////////////////////
// LEVELDB OPTIONS
//

#ifdef IDXFS_USE_HDFS
static const char* hdfs_ip = "hadoop-master";
static const int hdfs_port = 8020;
#endif

static DB* db = NULL;
static Env* env = NULL;
static Config* conf = NULL;
static char db_dir[256] = { 0 };
static char db_area[256] = { 0 };
static char host_name[128] = { 0 };

static ReadOptions read_opts;
static WriteOptions write_opts;

static const size_t max_files = 100;
static const int bloom_filter_bits = 14;
static const size_t block_size = (64 << 10);
static const size_t table_size = (32 << 20);
static const size_t buffer_size = (32 << 20);

#ifndef IDXFS_USE_PVFS
static const bool use_column_db = true;
static const size_t cache_size = (32 << 20);
#else
static const bool use_column_db = false;
static const size_t cache_size = (64 << 20);
static const size_t pvfs_buffer = (64 << 10);
#endif

static inline void InitInputOutputOpts() {
#ifdef IDXFS_IO_SYNC
  write_opts.sync = true;
#endif
}

static inline Options InitOpts() {
  Options opts;
  opts.env = env;
  opts.server_id = 0;
  opts.error_if_exists = true;
  opts.create_if_missing = true;
#ifdef IDXFS_DISABLE_WAL
  opts.disable_write_ahead_log = true;
#endif
  opts.max_open_files = max_files;
  opts.block_size = block_size;
  opts.max_sst_file_size = table_size;
  opts.write_buffer_size = buffer_size;
  opts.block_cache = NewLRUCache(cache_size);
#ifdef IDXFS_USE_BLOOM_FILTER
  opts.filter_policy = NewZigzagFilterPolicy(bloom_filter_bits);
#endif
  opts.compression = kNoCompression; // disable compression
  InitInputOutputOpts();
  return opts;
}

//////////////////////////////////////////////////////////////////////////////////
// LIFE-CYCLE MANAGEMENT
//

#ifdef IDXFS_COUNT_KV_SIZE
static size_t total_keys = 0;
static size_t total_bytes = 0;
static size_t total_entries = 0;
#endif

void IDX_Destroy() {
  delete db;
#ifdef IDXFS_COUNT_KV_SIZE
  printf("\n== LevelDB Summary\n");
  printf("Total entries inserted: %lu\n", total_entries);
  printf("Total key generated: %lu bytes\n", total_keys);
  printf("Total value generated: %lu bytes\n", total_bytes);
#endif
  Logger::Shutdown();
}

static void FetchDBDataDir() {
  strncpy(db_area, conf->GetLevelDBDir().c_str(), 256);
  gethostname(host_name, 128);
  char* c = strchr(host_name, '.');
  if (c != NULL) {
    *c = 0;
  }
  const char* pattern = "%s/bulkin_%s_%d";
  snprintf(db_dir, 256, pattern, db_area, host_name, getpid());
}

static Status CreateDBDataDir(Env* env) {
  Status s;
  if (!env->FileExists(db_area)) {
    s = env->CreateDir(db_area);
  }
  if (env->FileExists(db_area) && !env->FileExists(db_dir)) {
    s = env->CreateDir(db_dir);
  }
  return s;
}

static Status SetupLocalEnv() {
  env = Env::Default();
#ifdef IDXFS_USE_HDFS
  env = Env::HDFSEnv(hdfs_ip, hdfs_port);
#endif
#ifdef IDXFS_USE_PVFS
  env = Env::PVFSEnv(pvfs_buffer);
#endif
  FetchDBDataDir();
  return CreateDBDataDir(env);
}

int IDX_Init(struct conf_t* dummy) {
  Logger::Initialize(NULL);
  conf = LoadClientConfig();
  Status s = SetupLocalEnv();
  if (!s.ok()) {
    return LogErrorAndReturn(s);
  }
  Options opts = InitOpts();
  char name[256];
  snprintf(name, 256, "%s/l0", db_dir);
  if (!use_column_db) {
    s = DB::Open(opts, name, &db);
  } else {
    s = ColumnDBOpen(opts, name, &db);
  }
  return LogErrorAndReturn(s);
}

//////////////////////////////////////////////////////////////////////////////////
// METADATA OPERATIONS
//

#ifdef IDXFS_USE_SYS_STAT
static const size_t stat_offset = 0;
static const size_t flag_offset = stat_offset + sizeof(struct stat);
static const size_t nlen_offset = flag_offset + sizeof(int);
static const size_t name_offset = nlen_offset + sizeof(size_t);
static const size_t plen_offset = name_offset + sizeof(char*);
static const size_t path_offset = plen_offset + sizeof(size_t);
static const size_t data_offset = path_offset + sizeof(char*);
#endif

#ifdef IDXFS_USE_SMALL_STAT
static const size_t stat_offset = 0;
static const size_t nlen_offset = stat_offset + sizeof(struct filestat);
static const size_t plen_offset = nlen_offset + sizeof(size_t);
static const size_t data_offset = plen_offset + sizeof(size_t);
#endif

static const size_t head_size = data_offset;

#ifdef IDXFS_USE_CITYHASH
static const size_t hash_size = sizeof(uint64_t);
#endif

#ifdef IDXFS_USE_SHA1
static const size_t hash_size = SHA1_HASH_SIZE;
#endif

#ifdef IDXFS_USE_SHA1_HEX
static const size_t hash_size = SHA1_HASH_SIZE * 2;
#endif

static const size_t hash_offset = 2 * sizeof(uint64_t);
static const size_t key_size = hash_offset + hash_size;

static
void check_existence(const Slice &key) {
#ifndef IDXFS_CHECK_BY_GET
  db->Exists(read_opts, key);
#else
  std::string dummy;
  db->Get(read_opts, key, &dummy);
#endif
}

int IDX_Mknod(const char *path, mode_t mode) {

#ifdef IDXFS_USE_SYS_STAT
  stats.st_ino = -1;
  stats.st_size = 0;
  stats.st_mode = mode;
  stats.st_mtime = stats.st_ctime = stats.st_atime = time(NULL);
  *reinterpret_cast<struct stat*>(value + stat_offset) = stats;
#endif

#ifdef IDXFS_USE_SMALL_STAT
  stats.fs_ino = -1;
  stats.fs_size = 0;
  stats.fs_mode = mode;
  stats.fs_mtim.tv_sec = stats.fs_ctim.tv_sec = time(NULL);
  *reinterpret_cast<struct filestat*>(value + stat_offset) = stats;
#endif

  const char* name = strrchr(path, '/') + 1;
  size_t name_len = strlen(name);
  strncpy(value + data_offset, name, name_len + 2);
  size_t value_size = head_size + name_len + 2;

  *reinterpret_cast<uint64_t*>(key) = inode;
  *reinterpret_cast<uint64_t*>(key + sizeof(uint64_t)) = 0;

#ifdef IDXFS_USE_CITYHASH
  *reinterpret_cast<uint64_t*>(key + hash_offset)
    = CityHash64(name, name_len);
#endif

#ifdef IDXFS_USE_SHA1
  shahash(reinterpret_cast<uint8_t*>(const_cast<char*>(name)), name_len,
    reinterpret_cast<uint8_t*>(key + hash_offset));
#endif

#ifdef IDXFS_USE_SHA1_HEX
  uint8_t hash[SHA1_HASH_SIZE];
  shahash(reinterpret_cast<uint8_t*>(const_cast<char*>(name)), name_len, hash);
  binary2hex(hash, SHA1_HASH_SIZE, key + hash_offset);
#endif

  *reinterpret_cast<size_t*>(value + nlen_offset) = name_len;
  *reinterpret_cast<size_t*>(value + plen_offset) = 0;

#ifdef IDXFS_NAME_CHECK
  check_existence(Slice(key, key_size));
#endif

  Status s = db->Put(write_opts, Slice(key, key_size), Slice(value, value_size));

#ifdef IDXFS_COUNT_KV_SIZE
  if (s.ok()) {
    total_entries++;
    total_keys += key_size;
    total_bytes += value_size;
  }
#endif

  return LogErrorAndReturn(s);
}

int IDX_Mkdir(const char *path, mode_t mode) {
  return 0; // No directories will be created
}

int IDX_Unlink(const char *path) {
  const char* name = strrchr(path, '/') + 1;
  size_t name_len = strlen(name);

  *reinterpret_cast<uint64_t*>(key) = inode;
  *reinterpret_cast<uint64_t*>(key + sizeof(uint64_t)) = 0;

#ifdef IDXFS_USE_CITYHASH
  *reinterpret_cast<uint64_t*>(key + hash_offset)
    = CityHash64(name, name_len);
#endif

#ifdef IDXFS_USE_SHA1
  shahash(reinterpret_cast<uint8_t*>(const_cast<char*>(name)), name_len,
    reinterpret_cast<uint8_t*>(key + hash_offset));
#endif

#ifdef IDXFS_USE_SHA1_HEX
  uint8_t hash[SHA1_HASH_SIZE];
  shahash(reinterpret_cast<uint8_t*>(const_cast<char*>(name)), name_len, hash);
  binary2hex(hash, SHA1_HASH_SIZE, key + hash_offset);
#endif

  Status s = db->Delete(write_opts, Slice(key, key_size));

  if (s.IsNotFound()) {
    s = Status::OK();
    fprintf(stderr, "warning: %s not found!\n", path);
  }

  return LogErrorAndReturn(s);
}

int IDX_Chmod(const char *path, mode_t mode) {
  Status s = Status::Corruption("Not Supported", "chmod");
  return LogErrorAndReturn(s);
}

int IDX_GetAttr(const char *path, struct stat *buf) {
  const char* name = strrchr(path, '/') + 1;
  size_t name_len = strlen(name);

  *reinterpret_cast<uint64_t*>(key) = inode;
  *reinterpret_cast<uint64_t*>(key + sizeof(uint64_t)) = 0;

#ifdef IDXFS_USE_CITYHASH
  *reinterpret_cast<uint64_t*>(key + hash_offset)
    = CityHash64(name, name_len);
#endif

#ifdef IDXFS_USE_SHA1
  shahash(reinterpret_cast<uint8_t*>(const_cast<char*>(name)), name_len,
    reinterpret_cast<uint8_t*>(key + hash_offset));
#endif

#ifdef IDXFS_USE_SHA1_HEX
  uint8_t hash[SHA1_HASH_SIZE];
  shahash(reinterpret_cast<uint8_t*>(const_cast<char*>(name)), name_len, hash);
  binary2hex(hash, SHA1_HASH_SIZE, key + hash_offset);
#endif

  std::string value;
  Status s = db->Get(read_opts, Slice(key, key_size), &value);

  if (s.ok()) {
    buf->st_ino = -1;
    buf->st_size = 0;
    buf->st_mode = 0;
    buf->st_atime = time(NULL);
  }

  return LogErrorAndReturn(s);
}

int IDX_Create(const char *path, mode_t mode) {
  return IDX_Mknod(path, mode);
}

int IDX_Rmdir(const char *path) {
  return 0; // No directories will be created after all
}

int IDX_RecMknod(const char *path, mode_t mode) {
  return IDX_Mknod(path, mode); // fall back to mknod
}

int IDX_RecMkdir(const char *path, mode_t mode) {
  return IDX_Mkdir(path, mode); // fall back to mkdir
}

int IDX_Access(const char* path) {
  return -1; // Nothing pre-exists - you have to create it
}

//////////////////////////////////////////////////////////////////////////////////
// IO OPERATIONS
//

int IDX_Fsync(int fd) {
  return 0;
}

int IDX_Close(int fd) {
  return 0;
}

int IDX_Open(const char *path, int flags, int *fd) {
  return 0;
}

int IDX_Read(int fd, void *buf, size_t size) {
  return size;
}

int IDX_Write(int fd, const void *buf, size_t size) {
  return size;
}

int IDX_Pread(int fd, void *buf, off_t offset, size_t size) {
  return size;
}

int IDX_Pwrite(int fd, const void *buf, off_t offset, size_t size) {
  return size;
}

} /* end extern "C" */
