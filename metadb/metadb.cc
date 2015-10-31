// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifdef RADOS
#include "env/obj_set.h"
using leveldb::ObjEnv;
using leveldb::ObjEnvFactory;
#endif

#include <algorithm>
#include <sstream>
#include <iomanip>
#include <time.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>

#include "common/gigaidx.h"
#include "common/logging.h"
#include "metadb/metadb.h"
#include "metadb/dbtypes.h"
#include "metadb/dboptions.h"
#include "util/leveldb_types.h"
#include "util/leveldb_reader.h"

namespace indexfs {
namespace mdb {

class LevelMDB;

// Local configuration.
//
static const bool kDeleteCheck = false;

// Default permission bits.
//
static const mode_t DEFAULT_FILE_MODE = /* 644 */
    S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH | S_IFREG;
static const mode_t DEFAULT_DIR_MODE = /* 755 */
    S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH | S_IFDIR;

// Error status using classic POSIX messages.
//
static const
Status ERR_NOT_FOUND = Status::NotFound("No such file or directory");
static const
Status ERR_ALREADY_EXISTS = Status::AlreadyExists("File exists");
static const
Status ERR_OP_NOT_SUPPORTED = Status::NotSupported("Operation not supported");

// Overrides the original name hash with the specified hash prefix.
//
static inline
void PutHash(MDBKey *key, const Slice &hash) {
  if (!hash.empty()) {
    size_t hash_size = std::min(key->GetHashSize(), hash.size());
    memcpy(key->GetNameHash(), hash.data(), hash_size);
  }
}

// Default DirScanner implementation built on top of LevelDB.
//
struct MDBDirScanner: public DirScanner {

  virtual ~MDBDirScanner() {
    if (it_ != NULL) {
      delete it_;
    }
  }

  Iterator* it_;
  int64_t parent_id_;
  int16_t partition_id_;

  virtual void Next() { it_->Next(); }
  virtual bool Valid();

  virtual void RetrieveEntryStat(StatInfo *info);
  virtual void RetrieveEntryName(std::string *name);
  virtual void RetrieveEntryHash(std::string *hash);
};

// A specific BulkExtractor implementation built on top of LevelDB
// that performs both bulk extraction and bulk insertion
// but only on a single DB instance.
//
struct MDBLocalBulkExtractor:
        virtual public BulkExtractor {

  virtual ~MDBLocalBulkExtractor() {
    if (batch_ != NULL) {
      delete batch_;
    }
  }

  WriteBatch* batch_;
  int num_entries_extracted_;

  DB* db_;
  LevelMDB* mdb_;
  const std::string null_output_dir_;

  Status Commit();
  Status Extract(uint64_t *min_seq, uint64_t *max_seq);

  int GetNumEntriesExtracted() { return num_entries_extracted_; }
  const std::string& GetBulkExtractOutputDir() { return null_output_dir_; }
};

// The default BulkExtractor implementation built on top of LevelDB.
//
struct MDBBulkExtractor:
        virtual public BulkExtractor {

  virtual ~MDBBulkExtractor() {
    if (batch_ != NULL) {
      delete batch_;
    }
  }

  WriteBatch* batch_;
  int num_entries_extracted_;

  DB* db_;
  Env* env_;
  LevelMDB* mdb_;
  std::string dir_path_;
  std::string sstable_path_;

  // Setup the temporary sstable file for storing
  // a sorted list of bulk insertion entries to be extracted from the DB.
  //
  Status PrepareFile(WritableFile **sst_file);

  Status Commit();
  Status Extract(uint64_t *min_seq, uint64_t *max_seq);

  int GetNumEntriesExtracted() { return num_entries_extracted_; }
  const std::string& GetBulkExtractOutputDir() { return dir_path_; }
};

// Default MetaDB implementation built on top of LevelDB.
//
class LevelMDB: virtual public MetaDB {
 public:

  LevelMDB(Config* config, Env* env = NULL);
  virtual ~LevelMDB();

  int64_t GetCurrentInodeNo();

  int64_t ReserveNextInodeNo();

  Status Flush();

  Status Init(OptionInitializer opt_initializer);

  DirScanner* CreateDirScanner(const KeyOffset &offset);

  Status PutEntry(const KeyInfo &key, const StatInfo &info);

  Status PutEntryWithMode(const KeyInfo &key, const StatInfo &info, mode_t new_mode);

  Status EntryExists(const KeyInfo &key);

  Status DeleteEntry(const KeyInfo &key);

  Status GetEntry(const KeyInfo &key, StatInfo *info);

  Status UpdateEntry(const KeyInfo &key, const StatInfo &info);

  Status InsertEntry(const KeyInfo &key, const StatInfo &info);

  Status SetFileMode(const KeyInfo &key, mode_t new_mode);

  Status NewFile(const KeyInfo &key);

  Status NewDirectory(const KeyInfo &key, int16_t zeroth_server, int64_t inode_no);

  Status GetMapping(int64_t dir_id, std::string *dmap_data);

  Status UpdateMapping(int64_t dir_id, const Slice &dmap_data);

  Status InsertMapping(int64_t dir_id, const Slice &dmap_data);

  BulkExtractor* CreateLocalBulkExtractor();

  BulkExtractor* CreateBulkExtractor(const std::string &tmp_path);

  Status BulkInsert(uint64_t min_seq, uint64_t max_seq, const std::string &tmp_path);

  Status ListEntries(const KeyOffset &offset, NameList *names, StatList *infos);

  Status FetchData(const KeyInfo &key, int32_t *size, char *buffer);

  Status WriteData(const KeyInfo &key, uint32_t offset, uint32_t size, const char *data);

 private:

  enum FileStatus {
    kRegular = 0,
    kEmbedded = 1,
  };

  static inline bool IsEmbedded(int16_t status) {
    return (status & kEmbedded) == kEmbedded;
  }

  enum SpecialKeys {
    kInodeKey = -1,
  };

  Status SaveInodeCounter(); // save to disk
  Status RetrieveInodeCounter(); // read from disk

  Status CreateNewFileSystem(const std::string &db_path); // initialize an empty namespace
  Status LoadExistingFileSystem(const std::string &db_path); // restore existing namespace

  Config* config_;
  Env* user_env_; // This can be NULL
  DB* db_;
  Options options_; // Use options_.env to access Env
  Options builder_options_;

  Mutex inode_mu_;
  int64_t inode_counter_;

  // LevelDB write options
  WriteOptions write_sync_;
  WriteOptions write_async_;

  // LevelDB read options
  ReadOptions read_fill_cache_;
  ReadOptions read_pass_cache_;

  friend class MDBDirScanner;
  friend class MDBBulkExtractor;
  friend class MDBLocalBulkExtractor;

  // No copying allowed
  LevelMDB(const LevelMDB&);
  LevelMDB& operator=(const LevelMDB&);
};

LevelMDB::~LevelMDB() {
  if (db_ != NULL) {
    SaveInodeCounter();
    delete db_;
  }
  if (options_.block_cache != NULL) {
    delete options_.block_cache;
  }
  // Leave comparator and filter_policy
}

LevelMDB::LevelMDB(Config* config, Env* env) :
    config_(config), user_env_(env), db_(NULL), inode_counter_(0) {

  DLOG_ASSERT(config_ != NULL);

  write_sync_.sync = true;
  write_async_.sync = false;
  read_fill_cache_.fill_cache = true;
  read_pass_cache_.fill_cache = false;
}

Status LevelMDB::Flush() {
  DLOG_ASSERT(db_ != NULL);
  return db_->Flush();
}

Status LevelMDB::Init(OptionInitializer opt_initializer) {
  DLOG_ASSERT(db_ == NULL);

  opt_initializer(&options_, config_, user_env_);
  DLOG_ASSERT(options_.env != NULL);
  DLOG_ASSERT(options_.block_cache != NULL);

  builder_options_ = options_;
  DLOG_ASSERT(options_.comparator != NULL);
  builder_options_.comparator = NewInternalComparator(options_.comparator);
  DLOG_ASSERT(options_.filter_policy != NULL);
  builder_options_.filter_policy = NewInternalFilterPolicy(options_.filter_policy);

  std::string db_home = config_->GetDBHomeDir();
  return (!options_.env->FileExists(db_home + "/CURRENT")) ?
    CreateNewFileSystem(db_home) : LoadExistingFileSystem(db_home);
}

Status LevelMDB::CreateNewFileSystem(const std::string &db_path) {
  Status s;
  options_.create_if_missing = true;
  options_.error_if_exists = true;
  s = DB::Open(options_, db_path, &db_);
  if (!s.ok()) {
    return Status::Corruption("Cannot open LevelDB", s.ToString());
  }
  if (config_->IsServer()) {
    inode_counter_ = config_->GetSrvId();
    s = SaveInodeCounter();
    if (!s.ok()) {
      return Status::Corruption("Cannot initialize inode counter", s.ToString());
    }
  }
  return s;
}

Status LevelMDB::LoadExistingFileSystem(const std::string &db_path) {
  Status s;
  options_.create_if_missing = false;
  options_.error_if_exists = false;
  s = DB::Open(options_, db_path, &db_);
  if (!s.ok()) {
    return Status::Corruption("Cannot open LevelDB", s.ToString());
  }
  if (config_->IsServer()) {
    s = RetrieveInodeCounter();
    if (!s.ok()) {
      if (config_->HasOldData()) {
        inode_counter_ = config_->GetSrvId();
        s = SaveInodeCounter();
      } else {
        s = Status::Corruption("Cannot fetch inode counter", s.ToString());
      }
    }
  }
  return s;
}

Status LevelMDB::SaveInodeCounter() {
  MDBKey key(kInodeKey);
  std::string value;
  MutexLock lock(&inode_mu_);
  PutFixed64(&value, inode_counter_);
  return db_->Put(write_sync_, key.ToSlice(), value);
}

Status LevelMDB::RetrieveInodeCounter() {
  MDBKey key(kInodeKey);
  std::string value;
  MutexLock lock(&inode_mu_);
  Status s = db_->Get(read_fill_cache_, key.ToSlice(), &value);
  if (s.ok()) {
    inode_counter_ = DecodeFixed64(value.data());
  }
  return s;
}

inline
int64_t LevelMDB::GetCurrentInodeNo() {
  MutexLock lock(&inode_mu_);
  return inode_counter_;
}

inline
int64_t LevelMDB::ReserveNextInodeNo() {
  MutexLock lock(&inode_mu_);
  inode_counter_ += DEFAULT_MAX_NUM_SERVERS;
  return inode_counter_;
}

Status LevelMDB::PutEntry(const KeyInfo &key,
        const StatInfo &info) {
  MDBKey mdb_key(key.parent_id_, key.partition_id_, key.file_name_);
  MDBValue mdb_val(key.file_name_);
  mdb_val->SetInodeNo(info.id);
  mdb_val->SetFileSize(info.size);
  mdb_val->SetFileMode(info.mode);
  mdb_val->SetFileStatus(info.is_embedded ? kEmbedded : kRegular);
  mdb_val->SetZerothServer(info.zeroth_server);
  mdb_val->SetUserId(-1);
  mdb_val->SetGroupId(-1);
  mdb_val->SetChangeTime(info.ctime);
  mdb_val->SetModifyTime(info.mtime);
  return db_->Put(write_async_, mdb_key.ToSlice(), mdb_val.ToSlice());
}

Status LevelMDB::PutEntryWithMode(const KeyInfo &key,
        const StatInfo &info, mode_t new_mode) {
  MDBKey mdb_key(key.parent_id_, key.partition_id_, key.file_name_);
  new_mode &= (S_IRWXU | S_IRWXG | S_IRWXO);
  mode_t old_mode = info.mode & ~(S_IRWXU | S_IRWXG | S_IRWXO);
  MDBValue mdb_val(key.file_name_);
  mdb_val->SetInodeNo(info.id);
  mdb_val->SetFileSize(info.size);
  mdb_val->SetFileMode(old_mode | new_mode);
  mdb_val->SetFileStatus(info.is_embedded ? kEmbedded : kRegular);
  mdb_val->SetZerothServer(info.zeroth_server);
  mdb_val->SetUserId(-1);
  mdb_val->SetGroupId(-1);
  mdb_val->SetChangeTime(info.ctime);
  mdb_val->SetModifyTime(info.mtime);
  return db_->Put(write_async_, mdb_key.ToSlice(), mdb_val.ToSlice());
}

Status LevelMDB::SetFileMode(const KeyInfo &key,
        mode_t new_mode) {
  MDBKey mdb_key(key.parent_id_, key.partition_id_, key.file_name_);
  std::string buffer;
  Status s = db_->Get(read_fill_cache_, mdb_key.ToSlice(), &buffer);
  if (!s.ok()) {
    return s.IsNotFound() ? ERR_NOT_FOUND : s;
  }
  char* ptr = const_cast<char*>(buffer.data());
  FileStat* file_stat = reinterpret_cast<FileStat*>(ptr);
  new_mode &= (S_IRWXU | S_IRWXG | S_IRWXO);
  mode_t old_mode = file_stat->FileMode() & ~(S_IRWXU | S_IRWXG | S_IRWXO);
  file_stat->SetFileMode(old_mode | new_mode);
  return db_->Put(write_async_, mdb_key.ToSlice(), buffer);
}

Status LevelMDB::EntryExists(const KeyInfo &key) {
  MDBKey mdb_key(key.parent_id_, key.partition_id_, key.file_name_);
  return db_->Exists(read_fill_cache_, mdb_key.ToSlice());
}

Status LevelMDB::DeleteEntry(const KeyInfo &key) {
  MDBKey mdb_key(key.parent_id_, key.partition_id_, key.file_name_);
  if (kDeleteCheck) {
    std::string buffer;
    Status s = db_->Get(read_fill_cache_, mdb_key.ToSlice(), &buffer);
    if (!s.ok()) {
      return s.IsNotFound() ? ERR_NOT_FOUND : s;
    }
    const char* ptr = buffer.data();
    const FileStat* file_stat = reinterpret_cast<const FileStat*>(ptr);
    if (S_ISDIR(file_stat->FileMode())) {
      return ERR_OP_NOT_SUPPORTED;
    }
  }
  return db_->Delete(write_async_, mdb_key.ToSlice());
}

Status LevelMDB::GetEntry(const KeyInfo &key,
        StatInfo *info) {
  MDBKey mdb_key(key.parent_id_, key.partition_id_, key.file_name_);
  std::string buffer;
  Status s = db_->Get(read_fill_cache_, mdb_key.ToSlice(), &buffer);
  if (!s.ok()) {
    return s.IsNotFound() ? ERR_NOT_FOUND : s;
  }
  const char* ptr = buffer.data();
  const FileStat* file_stat = reinterpret_cast<const FileStat*>(ptr);
  info->id = file_stat->InodeNo();
  info->size = file_stat->FileSize();
  info->mode = file_stat->FileMode();
  info->is_embedded = IsEmbedded(file_stat->FileStatus());
  info->zeroth_server = file_stat->ZerothServer();
  info->gid = info->uid = -1;
  info->ctime = file_stat->ChangeTime();
  info->mtime = file_stat->ModifyTime();
  return Status::OK();
}

Status LevelMDB::UpdateEntry(const KeyInfo &key,
        const StatInfo &info) {
  MDBKey mdb_key(key.parent_id_, key.partition_id_, key.file_name_);
  std::string buffer;
  Status s = db_->Get(read_fill_cache_, mdb_key.ToSlice(), &buffer);
  if (!s.ok()) {
    return s.IsNotFound() ? ERR_NOT_FOUND : s;
  }
  char* ptr = const_cast<char*>(buffer.data());
  FileStat* file_stat = reinterpret_cast<FileStat*>(ptr);
  file_stat->SetInodeNo(info.id);
  file_stat->SetFileSize(info.size);
  file_stat->SetFileMode(info.mode);
  file_stat->SetFileStatus(info.is_embedded ? kEmbedded : kRegular);
  file_stat->SetZerothServer(info.zeroth_server);
  file_stat->SetUserId(-1);
  file_stat->SetGroupId(-1);
  file_stat->SetChangeTime(info.ctime);
  file_stat->SetModifyTime(info.mtime);
  return db_->Put(write_async_, mdb_key.ToSlice(), buffer);
}

Status LevelMDB::InsertEntry(const KeyInfo &key,
        const StatInfo &info) {
  MDBKey mdb_key(key.parent_id_, key.partition_id_, key.file_name_);
  Status s = db_->Exists(read_fill_cache_, mdb_key.ToSlice());
  if (!s.IsNotFound()) {
    return s.ok() ? ERR_ALREADY_EXISTS : s;
  }
  MDBValue mdb_val(key.file_name_);
  mdb_val->SetInodeNo(info.id);
  mdb_val->SetFileSize(info.size);
  mdb_val->SetFileMode(info.mode);
  mdb_val->SetFileStatus(info.is_embedded ? kEmbedded : kRegular);
  mdb_val->SetZerothServer(info.zeroth_server);
  mdb_val->SetUserId(-1);
  mdb_val->SetGroupId(-1);
  mdb_val->SetChangeTime(info.ctime);
  mdb_val->SetModifyTime(info.mtime);
  return db_->Put(write_async_, mdb_key.ToSlice(), mdb_val.ToSlice());
}

Status LevelMDB::NewFile(const KeyInfo &key) {
  MDBKey mdb_key(key.parent_id_, key.partition_id_, key.file_name_);
  Status s = db_->Exists(read_fill_cache_, mdb_key.ToSlice());
  if (!s.IsNotFound()) {
    return s.ok() ? ERR_ALREADY_EXISTS : s;
  }
  MDBValue mdb_val(key.file_name_);
  mdb_val->SetInodeNo(-1);
  mdb_val->SetFileSize(0);
  mdb_val->SetFileMode(DEFAULT_FILE_MODE);
  mdb_val->SetFileStatus(kEmbedded);
  mdb_val->SetZerothServer(-1);
  mdb_val->SetUserId(-1);
  mdb_val->SetGroupId(-1);
  mdb_val->SetTime(time(NULL));
  return db_->Put(write_async_, mdb_key.ToSlice(), mdb_val.ToSlice());
}

Status LevelMDB::NewDirectory(const KeyInfo &key,
        int16_t zeroth_server, int64_t inode_no) {
  MDBKey mdb_key(key.parent_id_, key.partition_id_, key.file_name_);
  Status s = db_->Exists(read_fill_cache_, mdb_key.ToSlice());
  if (!s.IsNotFound()) {
    return s.ok() ? ERR_ALREADY_EXISTS : s;
  }
  MDBValue mdb_val(key.file_name_);
  mdb_val->SetInodeNo(inode_no);
  mdb_val->SetFileSize(-1);
  mdb_val->SetFileMode(DEFAULT_DIR_MODE);
  mdb_val->SetFileStatus(kRegular);
  mdb_val->SetZerothServer(zeroth_server);
  mdb_val->SetUserId(-1);
  mdb_val->SetGroupId(-1);
  mdb_val->SetTime(time(NULL));
  return db_->Put(write_async_, mdb_key.ToSlice(), mdb_val.ToSlice());
}

Status LevelMDB::GetMapping(int64_t dir_id,
        std::string *dmap_data) {
  MDBKey mdb_key(dir_id, -1);
  Status s = db_->Get(read_fill_cache_, mdb_key.ToSlice(), dmap_data);
  return s.IsNotFound() ? ERR_NOT_FOUND : s;
}

Status LevelMDB::UpdateMapping(int64_t dir_id,
        const Slice &dmap_data) {
  MDBKey mdb_key(dir_id, -1);
  Status s = db_->Exists(read_fill_cache_, mdb_key.ToSlice());
  if (!s.ok()) {
    return s.IsNotFound() ? ERR_NOT_FOUND : s;
  }
  return db_->Put(write_async_, mdb_key.ToSlice(), dmap_data);
}

Status LevelMDB::InsertMapping(int64_t dir_id,
        const Slice &dmap_data) {
  MDBKey mdb_key(dir_id, -1);
  Status s = db_->Exists(read_fill_cache_, mdb_key.ToSlice());
  if (!s.IsNotFound()) {
    return s.ok() ? ERR_ALREADY_EXISTS : s;
  }
  return db_->Put(write_async_, mdb_key.ToSlice(), dmap_data);
}

Status LevelMDB::ListEntries(const KeyOffset &offset,
        NameList *names, StatList *infos) {
  MDBKey start_key(offset.parent_id_, offset.partition_id_);
  PutHash(&start_key, offset.start_hash_);
  MDBIterator it(db_->NewIterator(read_pass_cache_));
  for (it->Seek(start_key.ToSlice()); it->Valid(); it->Next()) {
    const MDBKey* key = reinterpret_cast<const MDBKey*>(it->key().data());
    if (key->GetParent() != offset.parent_id_ ||
        key->GetPartition() != offset.partition_id_) {
      break;
    }
    MDBValueRef val(it->value());
    if (infos != NULL) {
      StatInfo info;
      info.id = val->InodeNo();
      info.size = val->FileSize();
      info.mode = val->FileMode();
      info.is_embedded = IsEmbedded(val->FileStatus());
      info.zeroth_server = val->ZerothServer();
      info.uid = info.gid = -1;
      info.ctime = val->ChangeTime();
      info.mtime = val->ModifyTime();
      infos->push_back(info);
    }
    if (names != NULL) {
      names->push_back(val.GetName().ToString());
    }
  }
  return Status::OK();
}

Status LevelMDB::FetchData(const KeyInfo &key,
        int32_t *size, char *databuf) {
  MDBKey mdb_key(key.parent_id_, key.partition_id_, key.file_name_);
  std::string buffer;
  Status s = db_->Get(read_fill_cache_, mdb_key.ToSlice(), &buffer);
  if (!s.ok()) {
    return s.IsNotFound() ? ERR_NOT_FOUND : s;
  }
  MDBValueRef val(buffer.data(), buffer.size());
  if (!IsEmbedded(val->FileStatus())) {
    *size = -1;
    return Status::OK();
  }
  const Slice& data = val.GetEmbeddedData();
  DLOG_ASSERT(data.size() <= DEFAULT_SMALLFILE_THRESHOLD);
  *size = static_cast<int32_t>(data.size());
  memcpy(databuf, data.data(), data.size());
  return Status::OK();
}

Status LevelMDB::WriteData(const KeyInfo &key,
        uint32_t offset, uint32_t size, const char *data) {
  DLOG_ASSERT(offset + size <= DEFAULT_SMALLFILE_THRESHOLD);
  MDBKey mdb_key(key.parent_id_, key.partition_id_, key.file_name_);
  std::string buffer;
  Status s = db_->Get(read_fill_cache_, mdb_key.ToSlice(), &buffer);
  if (!s.ok()) {
    return s.IsNotFound() ? ERR_NOT_FOUND : s;
  }
  MDBValueRef old_val(buffer.data(), buffer.size());
  DLOG_ASSERT(IsEmbedded(old_val->FileStatus()));
  DLOG_ASSERT(old_val.GetStoragePath().size() == 0);
  DLOG_ASSERT(old_val.GetEmbeddedData().size() <= DEFAULT_SMALLFILE_THRESHOLD);
  MDBValue new_val(old_val, offset, size, data);
  new_val->SetFileSize(new_val.GetEmbeddedData().size());
  return db_->Put(write_async_, mdb_key.ToSlice(), new_val.ToSlice());
}

DirScanner* LevelMDB::CreateDirScanner(const KeyOffset &offset) {
  MDBKey start_key(offset.parent_id_, offset.partition_id_);
  PutHash(&start_key, offset.start_hash_);
  MDBDirScanner* scanner = new MDBDirScanner();
  scanner->it_ = db_->NewIterator(read_pass_cache_);
  scanner->it_->Seek(start_key.ToSlice());
  scanner->parent_id_ = offset.parent_id_;
  scanner->partition_id_ = offset.partition_id_;
  return scanner;
}

BulkExtractor* LevelMDB::CreateLocalBulkExtractor() {
  MDBLocalBulkExtractor* extractor = new MDBLocalBulkExtractor();
  extractor->db_ = db_;
  extractor->mdb_ = this;
  extractor->batch_ = new WriteBatch();
  extractor->num_entries_extracted_ = 0;
  return extractor;
}

BulkExtractor* LevelMDB::CreateBulkExtractor(const std::string &tmp_path) {
  MDBBulkExtractor* extractor = new MDBBulkExtractor();
  extractor->db_ = db_;
  extractor->env_ = options_.env;
  extractor->mdb_ = this;
  extractor->dir_path_ = tmp_path;
  extractor->batch_ = new WriteBatch();
  extractor->num_entries_extracted_ = 0;
  return extractor;
}

Status LevelMDB::BulkInsert(uint64_t min_seq, uint64_t max_seq,
        const std::string &tmp_path) {
  return db_->BulkInsert(write_async_, tmp_path, min_seq, max_seq);
}

// --------------------------------------------
// Directory Scanner
// --------------------------------------------

bool MDBDirScanner::Valid() {
  if (!it_->Valid()) {
    return false;
  }
  const MDBKey* key = reinterpret_cast<const MDBKey*>(it_->key().data());
  return key->GetParent() == parent_id_ && key->GetPartition() == partition_id_;
}

void MDBDirScanner::RetrieveEntryStat(StatInfo *info) {
  DLOG_ASSERT(it_->Valid());
  MDBValueRef val(it_->value());
  info->id = val->InodeNo();
  info->size = val->FileSize();
  info->mode = val->FileMode();
  info->is_embedded = LevelMDB::IsEmbedded(val->FileStatus());
  info->zeroth_server = val->ZerothServer();
  info->uid = info->gid = -1;
  info->ctime = val->ChangeTime();
  info->mtime = val->ModifyTime();
}

void MDBDirScanner::RetrieveEntryName(std::string *name) {
  DLOG_ASSERT(it_->Valid());
  MDBValueRef val(it_->value());
  const Slice& name_buffer = val.GetName();
  name->assign(name_buffer.data(), name_buffer.size());
}

void MDBDirScanner::RetrieveEntryHash(std::string *hash) {
  DLOG_ASSERT(it_->Valid());
  const MDBKey* key = reinterpret_cast<const MDBKey*>(it_->key().data());
  hash->assign(key->GetNameHash(), key->GetHashSize());
}

// --------------------------------------------
// Bulk Extractor
// --------------------------------------------

namespace {
static
const MDBKey* ToMDBKey(const Slice& key) {
  return reinterpret_cast<const MDBKey*>(key.data());
}
}

Status MDBLocalBulkExtractor::Commit() {
  Status s;
  if (num_entries_extracted_ > 0) {
    s = db_->Write(mdb_->write_async_, batch_);
  }
  return s;
}

Status MDBLocalBulkExtractor::Extract(uint64_t *min_seq, uint64_t *max_seq) {
  Status s;
  int num_entries_moved = 0;
  MDBIterator it(db_->NewIterator(mdb_->read_pass_cache_));
  for (it->Seek(MDBKey(dir_id_, old_partition_).ToSlice());
       it->Valid(); it->Next()) {
    const MDBKey* key = ToMDBKey(it->key());
    if (key->GetParent() != dir_id_ ||
        key->GetPartition() != old_partition_) {
      break;
    }
    if (DirIndex::ToBeMigrated(new_partition_, key->GetNameHash())) {
      num_entries_moved++;
      MDBKey new_key(dir_id_, new_partition_);
      PutHash(&new_key, Slice(key->GetNameHash(), key->GetHashSize()));
      batch_->Delete(it->key());
      batch_->Put(new_key.ToSlice(), it->value());
    }
  }
  *min_seq = *max_seq = 0;
  num_entries_extracted_ = num_entries_moved;
  return s;
}

Status MDBBulkExtractor::Commit() {
  Status s;
  if (num_entries_extracted_ > 0) {
    s = db_->Write(mdb_->write_async_, batch_);
  }
# if !defined(HDFS)
  if (s.ok()) {
    s = env_->DeleteFile(sstable_path_);
  }
# endif
  return !s.ok() ? s : env_->DeleteDir(dir_path_);
}

Status MDBBulkExtractor::PrepareFile(WritableFile **sst_file) {
  Status s;
  if (!env_->FileExists(dir_path_)) {
    s = env_->CreateDir(dir_path_);
    if (!s.ok()) {
      return s;
    }
  }
  sstable_path_ = TableFileName(dir_path_, 1);
  return env_->NewWritableFile(sstable_path_, sst_file);
}

Status MDBBulkExtractor::Extract(uint64_t *min_seq, uint64_t *max_seq) {
  *min_seq = *max_seq = 0;
  WritableFile* sst_file;
  Status s = PrepareFile(&sst_file);
  if (!s.ok()) {
    return s;
  }
  int num_entries_moved = 0;
  MDBTableBuilder builder(mdb_->builder_options_, sst_file);
  char key_space[128];
  MDBIterator it(db_->NewIterator(mdb_->read_pass_cache_));
  for (it->Seek(MDBKey(dir_id_, old_partition_).ToSlice());
       it->Valid(); it->Next()) {
    const Slice& internal_key = it->internalkey();
    const MDBKey* key = ToMDBKey(internal_key);
    if (key->GetParent() != dir_id_ ||
        key->GetPartition() != old_partition_) {
      break;
    }
    if (DirIndex::ToBeMigrated(new_partition_, key->GetNameHash())) {
      batch_->Delete(it->key());
      MDBKey new_key(dir_id_, new_partition_);
      DLOG_ASSERT(internal_key.size() < sizeof(key_space));
      memcpy(key_space, internal_key.data(), internal_key.size());
      memcpy(key_space, new_key.data(), new_key.GetPrefixSize());
      builder->Add(Slice(key_space, internal_key.size()), it->value());
      uint64_t seq = MDBHelper::FetchSeqNum(internal_key);
      if (num_entries_moved == 0) {
        *min_seq = *max_seq = seq;
      }
      num_entries_moved++;
      *min_seq = std::min(*min_seq, seq);
      *max_seq = std::max(*max_seq, seq);
    }
  }
  num_entries_extracted_ = builder->NumEntries();
  DLOG_ASSERT(num_entries_extracted_ = num_entries_moved);
  return builder.Seal();
}

} /* namespace mdb */

// --------------------------------------------
// Default MetaDB Factory Method
// --------------------------------------------

Status LoadManifestFile(const std::string& manifest,
                        int num_tables,
                        Env* env, VersionMerger* merger) {
  Status s;
  SequentialFile* file;
  s = env->NewSequentialFile(manifest, &file);
  if (s.ok()) {
    LogReporter reporter;
    reporter.status = &s;
    LogReader reader(file, &reporter, false/*checksum*/, 0/*initial_offset*/);
    Slice record;
    std::string scratch;
    if (reader.ReadRecord(&record, &scratch) && s.ok()) {
      s = merger->Merge(record, num_tables);
    }
    if (reader.ReadRecord(&record, &scratch) && s.ok()) {
      s = Status::NotSupported("Multiple records within a manifest file");
    }
  }
  DLOG(INFO) << "Load manifest file: " << manifest << " " << s.ToString();
  delete file;
  return s;
}

Status LinkDataDirecrory(const std::string& sst_dir, int& sst_no,
                         Config* config, Env* env, VersionMerger* merger) {
  Status s;
  int num_tables = 0;
  std::string manifest;
  std::vector<std::string> names;
  s = env->GetChildren(sst_dir, &names);
  if (s.ok()) {
    for (size_t j = 0; j < names.size() && s.ok(); ++j) {
      if (IsManifestFile(names[j])) {
        if (manifest.empty()) {
          manifest = sst_dir + "/" + names[j];
        } else {
          s = Status::NotSupported("Multiple DB manifest files");
        }
      }
      if (IsSSTableFile(names[j])) {
        num_tables++;
        std::string src = sst_dir + "/" + names[j];
        std::string target = TableFileName(config->GetDBHomeDir(), ++sst_no);
        s = env->SymlinkFile(src, target);
        DLOG(INFO) << "Link table: "
                << src << "->" << target << " " << s.ToString();
      }
    }
    if (s.ok() && num_tables > 0) {
      if (manifest.empty()) {
        s = Status::NotFound("No DB manifest file");
      } else {
        s = LoadManifestFile(manifest, num_tables, env, merger);
      }
    }
  }
  return s;
}

Status MetaDB::Repair(Config* config, Env* env) {
  Status s;
  DLOG_ASSERT(config->HasOldData());
  VersionMerger merger;
  int sst_no = 1;
  int manifest_no = 1;
  const std::pair<std::string, int>& old_data = config->GetDBDataDirs();
  for (int i = 0; i < old_data.second && s.ok(); ++i) {
    std::stringstream ss;
    ss << old_data.first << i;
    std::string sst_dir = ss.str();
#   ifdef RADOS
    ObjEnv* obj_env = ObjEnvFactory::FetchObjEnv();
    assert(obj_env != NULL);
    s = obj_env->LoadSet(sst_dir);
    if (!s.ok()) {
      break;
    }
#   endif
    s = LinkDataDirecrory(sst_dir, sst_no, config, env, &merger);
  }
  if (s.ok()) {
    merger.Finish();
    std::string record;
    merger.EncodeTo(&record);
    std::string manifest = DescriptorFileName(config->GetDBHomeDir(), manifest_no);
    WritableFile* file;
    s = env->NewWritableFile(manifest, &file);
    if (s.ok()) {
      LogWriter writer(file);
      s = writer.AddRecord(record);
      if (s.ok()) {
        s = file->Sync();
      }
    }
    delete file;
    DLOG(INFO) << "New manifest file " << manifest << " " << s.ToString();
    if (s.ok()) {
      s = SetCurrentFile(env, config->GetDBHomeDir(), manifest_no);
      DLOG(INFO) << "Install current file " << s.ToString();
    }
  }
  return s;
}

Status MetaDB::Open(Config* config, MetaDB** dbptr, Env* env) {
  using mdb::LevelMDB;
  using mdb::DefaultLevelDBOptionInitializer;
  using mdb::BatchClientLevelDBOptionInitializer;

  Status s;
  *dbptr = NULL;
  LevelMDB* new_db = new LevelMDB(config, env);
  if (config->IsServer()) {
    s = new_db->Init(DefaultLevelDBOptionInitializer);
  } else if (config->IsBatchClient()) {
    s = new_db->Init(BatchClientLevelDBOptionInitializer);
  } else {
    abort();
  }
  if (!s.ok()) {
    delete new_db;
    return s;
  }
  *dbptr = new_db;
  return s;
}

} /* namespace indexfs */
