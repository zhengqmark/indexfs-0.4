// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include <deque>
#include <set>
#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "port/port.h"

namespace leveldb {

class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;
struct FileMetaData;

class DBImpl : public DB {
 public:
  DBImpl(const Options& options, const std::string& dbname);
  virtual ~DBImpl();

  // Implementations of the DB interface
  virtual Status Flush();
  virtual Status Put(const WriteOptions&, const Slice& key, const Slice& value);
  virtual Status Delete(const WriteOptions&, const Slice& key);
  virtual Status Write(const WriteOptions& options, WriteBatch* updates);
  virtual Status Get(const ReadOptions& options,
                     const Slice& key,
                     std::string* value);
  virtual Iterator* NewIterator(const ReadOptions&);
  virtual const Snapshot* GetSnapshot();
  virtual void ReleaseSnapshot(const Snapshot* snapshot);
  virtual bool GetProperty(const Slice& property, std::string* value);
  virtual void GetApproximateSizes(const Range* range, int n, uint64_t* sizes);
  virtual void CompactRange(const Slice* begin, const Slice* end);
  virtual Status BulkSplit(const WriteOptions& options, uint64_t sequence,
                           const Slice* begin, const Slice* end,
                           const std::string& dname);
  virtual Status BulkInsert(const WriteOptions& options,
                            const std::string& fname,
                            uint64_t min_sequence_number,
                            uint64_t max_sequence_number);

  // Extra methods (for testing) that are not in the public DB interface

  // Compact any files in the named level that overlap [*begin,*end]
  void TEST_CompactRange(int level, const Slice* begin, const Slice* end);

  // Force current memtable contents to be compacted.
  Status TEST_CompactMemTable();

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  Iterator* TEST_NewInternalIterator();

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t TEST_MaxNextLevelOverlappingBytes();

 private:
  friend class DB;
  struct CompactionState;
  struct DeletionState;
  struct Writer;

  Iterator* NewInternalIterator(const ReadOptions&,
                                SequenceNumber* latest_snapshot);

  Status NewDB();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  Status Recover(VersionEdit* edit);

  void MaybeIgnoreError(Status* s) const;

  // Delete any unneeded files and stale in-memory entries.
  void DeleteObsoleteFiles();

  // Compact the in-memory write buffer to disk.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  Status CompactMemTable();

  Status RecoverLogFile(uint64_t log_number,
                        VersionEdit* edit,
                        SequenceNumber* max_sequence);

  Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base);

  Status MakeRoomForWrite(bool force /* compact even if there is room? */);
  WriteBatch* BuildBatchGroup(Writer** last_writer);

  void MaybeScheduleCompaction();
  static void BGWork(void* db);
  void BackgroundCall();
  void BackgroundCompaction();
  void CleanupCompaction(CompactionState* compact);
  Status DoCompactionWork(CompactionState* compact);

  Status OpenCompactionOutputFile(CompactionState* compact);
  Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
  Status InstallCompactionResults(CompactionState* compact);

  void  LogLiveFiles(std::string location, VersionEdit *edit);
  void CleanupDeletion(DeletionState* deletion);
  Status OpenDeletionOutputFile(DeletionState* deletion);
  Status FinishDeletionOutputFile(DeletionState* deletion, Iterator* input);
  Status MigrateLevel0Table(const std::string& fname, FileMetaData& meta,
                            VersionEdit* edit);

  // Constant after construction
  Env* const env_;
  const InternalKeyComparator internal_comparator_;
  const InternalFilterPolicy internal_filter_policy_;
  const Options options_;  // options_.comparator == &internal_comparator_
  bool owns_info_log_;
  bool owns_cache_;
  const std::string dbname_;

  // table_cache_ provides its own synchronization
  TableCache* table_cache_;

  // Lock over the persistent DB state.  Non-NULL iff successfully acquired.
  FileLock* db_lock_;

  // State below is protected by mutex_
  port::Mutex mutex_;
  port::AtomicPointer shutting_down_;
  port::CondVar bg_cv_;          // Signalled when background work finishes
  MemTable* mem_;
  MemTable* imm_;                // Memtable being compacted
  port::AtomicPointer has_imm_;  // So bg thread can detect non-NULL imm_
  WritableFile* logfile_;
  uint64_t logfile_number_;
  log::Writer* log_;

  // Queue of writers.
  std::deque<Writer*> writers_;
  WriteBatch* tmp_batch_;

  SnapshotList snapshots_;

  // Set of table files to protect from deletion because they are
  // part of ongoing compactions.
  std::set<uint64_t> pending_outputs_;

  // Has a background compaction been scheduled or is running?
  bool bg_compaction_scheduled_;
  bool bg_bulkinsert_scheduled_;

  // Information for a manual compaction
  struct ManualCompaction {
    int level;
    bool done;
    const InternalKey* begin;   // NULL means beginning of key range
    const InternalKey* end;     // NULL means end of key range
    InternalKey tmp_storage;    // Used to keep track of compaction progress
  };
  ManualCompaction* manual_compaction_;

  VersionSet* versions_;

  // Have we encountered a background error in paranoid mode?
  Status bg_error_;

  // Per level compaction stats.  stats_[level] stores the stats for
  // compactions that produced data for the specified "level".
  struct CompactionStats {
    int64_t counter;
    int64_t micros;
    int64_t bytes_read;
    int64_t bytes_written;

    CompactionStats() : counter(0), micros(0), bytes_read(0), bytes_written(0) { }

    void Add(const CompactionStats& c) {
      this->counter += c.counter;
      this->micros += c.micros;
      this->bytes_read += c.bytes_read;
      this->bytes_written += c.bytes_written;
    }
  };
  CompactionStats stats_[config::kNumLevels];
  CompactionStats sum_stats_;

  struct OperationStats {
    int64_t get_count;
    int64_t write_count;

    OperationStats() : write_count(0), get_count(0) { }
  };
  OperationStats op_stats_;

  // No copying allowed
  DBImpl(const DBImpl&);
  void operator=(const DBImpl&);

  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }

  static bool db_impl_closed_;
  bool disable_compaction_;
  bool bg_monitor_in_loop_;
  port::Mutex mt_mutex_;
  port::CondVar mt_cv_;
  static void MonitorThread(void* db);
  void SendMetrics();
};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
extern Options SanitizeOptions(const std::string& db,
                               const InternalKeyComparator* icmp,
                               const InternalFilterPolicy* ipolicy,
                               const Options& src);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_
