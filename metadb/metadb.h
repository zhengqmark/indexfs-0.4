// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_METADB_INTERFACE_H_
#define _INDEXFS_METADB_INTERFACE_H_

#include <string>
#include <vector>

#include "common/common.h"
#include "common/config.h"

namespace indexfs {

class KeyInfo;
class KeyOffset;

// An utility class capable of iterating the entries under a particular directory.
//
class DirScanner {
 public:

  virtual ~DirScanner() { }

  virtual void Next() = 0;
  virtual bool Valid() = 0;

  virtual void RetrieveEntryStat(StatInfo *info) = 0;
  virtual void RetrieveEntryName(std::string *name) = 0;
  virtual void RetrieveEntryHash(std::string *hash) = 0;

 protected:
  DirScanner() { }

  // No copying allowed
  DirScanner(const DirScanner&);
  DirScanner& operator=(const DirScanner&);
};

class BulkExtractor {
 public:

  virtual ~BulkExtractor() { }

  virtual int GetNumEntriesExtracted() = 0;
  virtual const std::string& GetBulkExtractOutputDir() = 0;

  virtual Status Commit() = 0;
  virtual Status Extract(uint64_t *min_seq, uint64_t *max_seq) = 0;

  void SetDirectory(int64_t dir_id) { dir_id_ = dir_id; }
  void SetOldPartition(int16_t old_id) { old_partition_ = old_id; }
  void SetNewPartition(int16_t new_id) { new_partition_ = new_id; }

 protected:
  int64_t dir_id_;
  int16_t old_partition_;
  int16_t new_partition_;

  BulkExtractor() : dir_id_(-1),
          old_partition_(-1), new_partition_(-1) {
  }

 private:
  // No copying allowed
  BulkExtractor(const BulkExtractor&);
  BulkExtractor& operator=(const BulkExtractor&);
};

// The underlying key-value store holding (part of) the file system namespace. It
// serves as the core processing and storage engine for various file system metadata
// operations such as file creation, directory creation, file stat retrieval, metadata
// updates, file removal, directory removal, and others.
//
class MetaDB {
 public:

  static Status Repair(Config* config, Env* env = NULL);

  // Open the underlying key-value store with the specified system
  // configurations.
  // Stores a pointer to a heap-allocated database in *dbptr and returns
  // OK on success.
  // Stores NULL in *dbptr and returns a non-OK status on error.
  // Note that caller should delete *dbptr when it is no longer needed.
  //
  static Status Open(Config* config, MetaDB** dbptr, Env* env = NULL);

  virtual ~MetaDB() { }
  virtual Status Flush() = 0;

  virtual int64_t GetCurrentInodeNo() = 0;
  virtual int64_t ReserveNextInodeNo() = 0;

  virtual Status EntryExists(const KeyInfo &key) = 0;

  virtual Status DeleteEntry(const KeyInfo &key) = 0;
  virtual Status GetEntry(const KeyInfo &key, StatInfo *info) = 0;
  virtual Status UpdateEntry(const KeyInfo &key, const StatInfo &info) = 0;
  virtual Status InsertEntry(const KeyInfo &key, const StatInfo &info) = 0;

  // Blindly put a new entry into the underlying DB
  // without checking the existence of entries currently
  // in the DB with the same key.
  // This call is introduced so that we can save a
  // LevelDB GET if we already know what that call may return.
  //
  virtual Status PutEntry(const KeyInfo &key,
      const StatInfo &info) = 0;
  // Blindly put a new entry into the underlying DB
  // without checking the existence of entries currently
  // in the DB with the same key.
  // Besides, update the mode according to the new mode.
  //
  virtual Status PutEntryWithMode(
      const KeyInfo &key, const StatInfo &info, mode_t new_mode) = 0;

  // Create a new file with no data associated with it.
  // Different from directories, files don't get inode#s.
  // Returns error if file with the same key already exists.
  //
  virtual Status NewFile(const KeyInfo &key) = 0;
  // Make a new directory with the given inode# and server id.
  // Directories are just inode entries associated with their own parent.
  // Entries under this new directory are stored separately (not
  // with the entry being created here) and are very likely on other servers.
  // Returns error if directory with the same key already exists.
  //
  virtual Status NewDirectory(const KeyInfo &key,
      int16_t zeroth_server, int64_t inode_no) = 0;
  // Override the original file mode according to the new mode.
  // Returns error if no file with the specified key is found.
  //
  virtual Status SetFileMode(const KeyInfo &key, mode_t new_mode) = 0;

  virtual Status GetMapping(int64_t dir_id, std::string *dmap_data) = 0;
  virtual Status UpdateMapping(int64_t dir_id, const Slice &dmap_data) = 0;
  virtual Status InsertMapping(int64_t dir_id, const Slice &dmap_data) = 0;

  virtual Status FetchData(const KeyInfo &key,
      int32_t *size, char *buffer) = 0;
  virtual Status WriteData(const KeyInfo &key,
      uint32_t offset, uint32_t size, const char *data) = 0;

  // List all objects (files or directories) under a given directory partition.
  virtual Status ListEntries(const KeyOffset &offset,
      NameList *names, StatList *infos) = 0;
  // Get a new directory scanner associated with a given directory partition.
  virtual DirScanner* CreateDirScanner(const KeyOffset &offset) = 0;

  virtual Status BulkInsert(uint64_t min_seq, uint64_t max_seq,
     const std::string &tmp_path) = 0;
  virtual BulkExtractor* CreateLocalBulkExtractor() = 0;
  virtual BulkExtractor* CreateBulkExtractor(const std::string &tmp_path) = 0;

 protected:
  // No public initialization
  explicit MetaDB() { }

 private:
  // No copying allowed
  MetaDB(const MetaDB&);
  MetaDB& operator=(const MetaDB&);
};

// A helper structure holding necessary information to form a "key".
//
struct KeyInfo {

  KeyInfo(int64_t parent_id,
          int16_t partition_id,
          const std::string &file_name)
    : parent_id_(parent_id), partition_id_(partition_id), file_name_(file_name) {
  }

  int64_t parent_id_; /* parent inode number */
  int16_t partition_id_; /* current partition index */
  const std::string &file_name_; /* name of the file or directory */
};

// A helper structure holding necessary information to start a directory scan.
//
struct KeyOffset {

  KeyOffset(int64_t parent_id,
            int16_t partition_id,
            const std::string &start_hash)
    : parent_id_(parent_id), partition_id_(partition_id), start_hash_(start_hash) {
  }

  int64_t parent_id_; /* parent inode number */
  int16_t partition_id_; /* current partition index */
  const std::string &start_hash_; /* name hash representing the starting position */
};

} /* namespace indexfs */

#endif /* _INDEXFS_METADB_INTERFACE_H_ */
