// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_UTIL_LEVELDB_IO_H_
#define _INDEXFS_UTIL_LEVELDB_IO_H_

#include "leveldb/status.h"
#include "leveldb/slice.h"
#include "leveldb/options.h"
#include "leveldb/env.h"
#include "leveldb/cache.h"
#include "leveldb/write_batch.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "leveldb/comparator.h"
#include "leveldb/filter_policy.h"
#include "leveldb/util/arena.h"
#include "leveldb/util/coding.h"
#include "leveldb/table/merger.h"
#include "leveldb/db/dbformat.h"
#include "leveldb/db/filename.h"
#include "leveldb/db/memtable.h"
#include "leveldb/db/log_reader.h"
#include "leveldb/db/log_writer.h"
#include "leveldb/db/version_edit.h"

namespace indexfs {

// LevelDB common types
using leveldb::Env;
using leveldb::Slice;
using leveldb::Table;
using leveldb::TableBuilder;
using leveldb::Options;
using leveldb::ReadOptions;
using leveldb::WriteOptions;
using leveldb::Status;
using leveldb::Iterator;
using leveldb::Comparator;
using leveldb::InternalKeyComparator;
using leveldb::FilterPolicy;
using leveldb::WriteBatch;
using leveldb::WritableFile;
using leveldb::SequentialFile;
using leveldb::RandomAccessFile;

// LevelDB types embedded in each internal key
using leveldb::InternalKey;
using leveldb::ValueType;
using leveldb::kTypeValue;
using leveldb::kTypeDeletion;
using leveldb::SequenceNumber;

// LevelDB coding tools
using leveldb::VarintLength;
using leveldb::PutFixed32;
using leveldb::PutVarint32;
using leveldb::PutFixed64;
using leveldb::PutVarint64;
using leveldb::GetVarint32;
using leveldb::GetVarint64;
using leveldb::EncodeFixed32;
using leveldb::DecodeFixed32;
using leveldb::EncodeFixed64;
using leveldb::DecodeFixed64;

// LevelDB compression switch
using leveldb::kNoCompression;
using leveldb::kSnappyCompression;

// LevelDB internal data structure
using leveldb::Arena;
using leveldb::MemTable;
using leveldb::VersionEdit;
using leveldb::VersionMerger;

// LevelDB filename
using leveldb::SetCurrentFile;
using leveldb::ParseFileName;
using leveldb::CurrentFileName;
using leveldb::LogFileName;
using leveldb::TableFileName;
using leveldb::DescriptorFileName;

// Various LevelDB factory methods
using leveldb::BytewiseComparator;
using leveldb::NewMergingIterator;
using leveldb::NewInternalComparator;
using leveldb::NewBloomFilterPolicy;
using leveldb::NewInternalFilterPolicy;

// LevelDB logging
typedef leveldb::log::Reader LogReader;
typedef leveldb::log::Writer LogWriter;

struct LogReporter: public LogReader::Reporter {
  Status* status;
  virtual void Corruption(size_t bytes, const Status& s) {
    if (this->status->ok()) *this->status = s;
  }
};

// LevelDB helper classes
class MDBTable;
class MDBTableSet;
class MDBTableBuilder;

// Determine if a given file name represents a SSTable
extern bool IsSSTableFile(const std::string& name);
// Determine if a given file name represents a MANIFEST file
extern bool IsManifestFile(const std::string& name);

class MDBOptionsFactory {
 private:
  Options opt_;

 public:
  MDBOptionsFactory(Env* env = NULL);
  const Options& Get() const { return opt_; }
};


class MDBMemTable {
 public:
  explicit MDBMemTable(MemTable* mem) : mem_(mem) {
    mem_->Ref();
  }
  ~MDBMemTable() {
    mem_->Unref();
  }

  MemTable* operator->() { return mem_; }

 private:
  MemTable* mem_;
  // No copying allowed
  MDBMemTable(const MDBMemTable&);
  MDBMemTable& operator=(const MDBMemTable&);
};


class MDBIterator {
 public:

  explicit MDBIterator(Iterator* it) : it_(it) { }

  ~MDBIterator() {
    if (it_ != NULL) {
      delete it_;
    }
  }

  Iterator* operator->() { return it_; }

 private:
  Iterator* it_;
  // No copying allowed
  MDBIterator(const MDBIterator&);
  MDBIterator& operator=(const MDBIterator&);
};


class MDBTableBuilder {
 public:

  MDBTableBuilder(const Options& options,
                  WritableFile* file)
    : seq_(0),
      builder_(NULL),
      sst_file_(file) {
    builder_ = new TableBuilder(options, sst_file_, false);
  }

  ~MDBTableBuilder() {
    if (builder_ != NULL) {
      delete builder_;
    }
    if (sst_file_ != NULL) {
      sst_file_->Close();
      delete sst_file_;
    }
  }

  Status Seal() {
    Status s;
    if (builder_->NumEntries() == 0) {
      builder_->Abandon();
      return s;
    }
    s = builder_->Finish();
    if (!s.ok()) {
      return s;
    }
    return sst_file_->Sync();
  }

  TableBuilder* operator->() { return builder_; }
  // Put an user-level key/value pair into the table.
  // Use MDBTableBuilder->Add(...) for inserting internal key/value pairs.
  void Put(const Slice& key, const Slice& value);

 private:
  uint64_t seq_;
  TableBuilder* builder_;
  WritableFile* sst_file_;

  // Threshold for small keys
  enum { kSmallKey = 64 };

  // Avoid heap memory allocation for small keys
  void Put_SmallKey(const Slice& key, const Slice& value);

  // No copying allowed
  MDBTableBuilder(const MDBTableBuilder&);
  MDBTableBuilder& operator=(const MDBTableBuilder&);
};


class MDBTable {
 public:
  MDBTable() : table_(NULL),
    iterator_(NULL), file_size_(-1), file_(NULL) {
  }

  ~MDBTable() {
    delete iterator_;
    delete table_;
    delete file_;
  }

  Status Open(const Options& options, const std::string& fname) {
    Status s;
    Env* env = options.env;
    s = env->NewRandomAccessFile(fname, &file_);
    if (!s.ok()) {
      return s;
    }
    s = env->GetFileSize(fname, &file_size_);
    if (!s.ok()) {
      return s;
    }
    s = Table::Open(options, file_, file_size_, &table_);
    if (!s.ok()) {
      return s;
    }
    ReadOptions read_opt;
    read_opt.verify_checksums = options.paranoid_checks;
    read_opt.fill_cache = false;
    iterator_ = table_->NewIterator(read_opt);
    return s;
  }

  uint64_t size() { return file_size_; }
  Iterator* operator->() { return iterator_; }
  bool ok() { return table_ != NULL && iterator_ != NULL; }

 private:
  Table* table_;
  Iterator* iterator_;
  uint64_t file_size_;
  RandomAccessFile* file_;

  friend class MDBTableSet;
  // No copying allowed
  MDBTable(const MDBTable&);
  MDBTable& operator=(const MDBTable&);
};


class MDBTableSet {
 public:
  MDBTableSet() : total_size_(0),
    tables_(NULL), merging_iterator_(NULL) {
  }

  ~MDBTableSet() {
    delete merging_iterator_;
    delete [] tables_;
  }

  // Atomically opens a set of SSTables possibly containing overlapping keys.
  // Returns Status::OK() if all tables have been successfully opened.
  // Returns error otherwise and all opened tables will be released.
  Status Open(const Options& options,
              const std::vector<std::string>& fnames) {
    Status s;
    int size = fnames.size();
    tables_ = new MDBTable[size];
    Iterator** children = new Iterator*[size];
    for (int i = 0; i < size; ++i) {
      if (s.ok()) {
        s = tables_[i].Open(options, fnames[i]);
        if (s.ok()) {
          children[i] = tables_[i].iterator_;
        }
      }
    }
    if (s.ok()) {
      merging_iterator_ = NewMergingIterator(options.comparator,
                                             children, size);
      for (int i = 0; i < size; ++i) {
        tables_[i].iterator_ = NULL;
        total_size_ += tables_[i].file_size_;
      }
    }
    delete [] children;
    return s;
  }

  // Atomically opens all the SSTables within a given directory.
  Status Open(const Options& options, const std::string& dir);

  uint64_t size() { return total_size_; }
  Iterator* operator->() { return merging_iterator_; }
  bool ok() { return tables_ != NULL && merging_iterator_ != NULL; }

 private:
  uint64_t total_size_;
  MDBTable* tables_;
  Iterator* merging_iterator_;

  // No copying allowed
  MDBTableSet(const MDBTableSet&);
  MDBTableSet& operator=(const MDBTableSet&);
};

} // namespace indexfs

#endif /* _INDEXFS_UTIL_LEVELDB_IO_H_ */
