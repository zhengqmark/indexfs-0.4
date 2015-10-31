// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/column_db.h"

#include "leveldb/db.h"
#include "leveldb/env.h"
#include "util/coding.h"
#include "db/filename.h"
#include "db/dbformat.h"
#include "db/cdb_iter.h"
#include "util/mutexlock.h"

#include <stdio.h>
#include <stdlib.h>

namespace leveldb {

static const uint64_t kColumnMagicNumber = 0x18ca;

bool stringEndsWith(const std::string &src, const std::string &suffix) {
  if (src.length() >= suffix.length()) {
    return src.compare(src.length()-suffix.length(),
                       suffix.length(),
                       suffix) == 0;
  } else {
    return false;
  }
}

ColumnDB::ColumnDB(const Options& options, const std::string& dbname,
                   Status &s) :
  dbname_(dbname), env_(options.env), datafile_(NULL), indexdb_(NULL),
  membuf_(NULL), data_cache_(NULL), server_id_(-1), // FIXME
  log_number_(0), current_log_number_(0) {
  MutexLock mutex_lock(&mutex_);
  s = DB::Open(options, dbname, &indexdb_);
  if (!s.ok()) {
    printf("%s\n", s.ToString().c_str());
    return;
  }
  RecoverDB();

  int pos = dbname.find_last_of('/');
  dbname_prefix_ = dbname.substr(0, pos);
  s = NewDataFile();
  if (!s.ok()) {
    printf("%s\n", s.ToString().c_str());
    return;
  }
  membuf_ = new MemBuffer(63 << 20);
  data_cache_ = new DataCache(dbname_prefix_, &options, options.max_open_files);
}

void ColumnDB::RecoverDB() {
  log_number_ = server_id_ << 14;

  if (env_->FileExists(dbname_))  {
    std::vector<std::string> result;
    Status s = env_->GetChildren(dbname_, &result);
    if (s.ok()) {
      for (int i = 0; i < result.size(); ++i)
        if (stringEndsWith(result[i], ".dat")) {
          std::string num = result[i].substr(2, 6);
          uint64_t number = atoi(num.c_str());
          if (number > log_number_) {
            log_number_ = number;
          }
        }
      return;
    }
  }
}

ColumnDB::~ColumnDB() {
  MutexLock mutex_lock(&mutex_);
  if (datafile_ != NULL) {
    datafile_->Close();
    delete datafile_;
  }
  if (membuf_ != NULL) {
    delete membuf_;
  }
  if (data_cache_ != NULL) {
    delete data_cache_;
  }
  if (indexdb_ != NULL) {
    delete indexdb_;
  }
}

Status ColumnDB::NewDataFile() {
  uint64_t new_log_number = NewLogNumber();
  WritableFile* lfile;
  Status s = env_->NewWritableFile(DataFileName(dbname_prefix_, new_log_number),
                                   &lfile);
  if (s.ok()) {
    SetLogNumber(new_log_number);
    if (datafile_ != NULL) {
      datafile_->Close();
      delete datafile_;
    }
    datafile_ = lfile;
  } else {
    return s;
  }
}

Status ColumnDB::Flush() {
  return indexdb_->Flush();
}

Status ColumnDB::Put(const WriteOptions& opt, const Slice& key,
                     const Slice& value) {
  Status s;
  mutex_.Lock();
  uint64_t total_size = sizeof(uint64_t)+key.size()+value.size();
  if (!membuf_->HasEnough(total_size)) {
    s = NewDataFile();
    if (!s.ok())
      return s;
    membuf_->Truncate();
  }

  //Put header
  size_t location;
  char buf[sizeof(uint64_t)];
  // magic number--16b key size--28b  value size--20b
  EncodeFixed64(buf, (kColumnMagicNumber<<48)+(key.size()<<20)+(value.size()));
  membuf_->Append(Slice(buf, sizeof(buf)), location);
  datafile_->Append(Slice(buf, sizeof(buf)));

  //Put Key Value
  size_t tmp;
  membuf_->Append(key, tmp);
  membuf_->Append(value, tmp);
  s = datafile_->Append(key);
  if (!s.ok()) return s;
  s = datafile_->Append(value);
  if (!s.ok()) return s;
  if (opt.sync)
    datafile_->Flush();
  mutex_.Unlock();

  // lognumber--22b location--32b  value size/1KB--10b
  EncodeFixed64(buf, (GetLogNumber()<<42)+(location<<10)+
                     ((total_size+1023)/1024));
  indexdb_->Put(opt, key, Slice(buf, sizeof(buf)));

  return Status::OK();
}

Status ColumnDB::Delete(const WriteOptions& opt, const Slice& key) {
  return indexdb_->Delete(opt, key);
}

Status ColumnDB::Write(const WriteOptions& options, WriteBatch* updates) {
  return indexdb_->Write(options, updates);
}

Status ColumnDB::InternalGet(const ReadOptions& options,
                             uint64_t file_number, uint64_t offset,
                             uint64_t buf_size, char* buf,
                             Slice* result) {
  Status s;

  if (file_number == GetLogNumber()) {
    mutex_.Lock();
    if (file_number == GetLogNumber()) {
      s = membuf_->Get(offset, buf_size, result, buf);
      mutex_.Unlock();
    } else {
      mutex_.Unlock();
      s = data_cache_->Get(options, file_number, offset, buf_size,
                           result, buf);
    }
  } else {
    s = data_cache_->Get(options, file_number, offset, buf_size,
                         result, buf);
  }
  if (!s.ok())
    return s;

  uint64_t header = DecodeFixed64(result->data());
  uint64_t magic_number = header >> 48;
  if (magic_number != kColumnMagicNumber) {
    return Status::IOError("Magic Number Not Match");
  }
  header = header & ((1L<<48)-1);
  uint64_t key_size = header >> 20;
  uint64_t val_size = header & ((1L<<20)-1);
  if (key_size + val_size + sizeof(header) > result->size()) {
    return Status::IOError("Failed to read a full key value pair.");
  }
  *result = Slice(result->data()+sizeof(uint64_t)+key_size, val_size);
  return s;
}

Status ColumnDB::Get(const ReadOptions& options,
                     const Slice& key,
                     std::string* value) {
  std::string location_val;
  Status s = indexdb_->Get(options, key, &location_val);
  if (!s.ok()) return s;

  uint64_t file_loc = DecodeFixed64(location_val.c_str());
  uint64_t file_number, offset, buf_size;
  DecodeFileLoc(file_loc, &file_number, &offset, &buf_size);
  char buf[buf_size];
  Slice result;
  s = InternalGet(options, file_number, offset, buf_size, buf, &result);

  if (s.ok()) {
    value->assign(result.data(), result.size());
  }

  return s;
}

Status ColumnDB::Exists(const ReadOptions& options,
                        const Slice& key) {
  std::string location_val;
  Status s = indexdb_->Get(options, key, &location_val);
  return s;
}

Iterator* ColumnDB::NewIterator(const ReadOptions& opt) {
  return NewColumnDBIterator(opt, this, indexdb_->NewIterator(opt));;
}

const Snapshot* ColumnDB::GetSnapshot() {
  return NULL;
}

void ColumnDB::ReleaseSnapshot(const Snapshot* snapshot) {
}

bool ColumnDB::GetProperty(const Slice& property, std::string* value) {
  return indexdb_->GetProperty(property, value);
}

void ColumnDB::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
  indexdb_->GetApproximateSizes(range, n, sizes);
}

void ColumnDB::CompactRange(const Slice* begin, const Slice* end) {
  indexdb_->CompactRange(begin, end);
}

Status ColumnDB::BulkSplit(const WriteOptions& options, uint64_t sequence,
                           const Slice* begin, const Slice* end,
                           const std::string& dname) {
  return indexdb_->BulkSplit(options, sequence, begin, end, dname);
}

Status ColumnDB::BulkInsert(const WriteOptions& options,
                  const std::string& fname,
                  uint64_t min_sequence_number,
                  uint64_t max_sequence_number) {
  return indexdb_->BulkInsert(options, fname,
                              min_sequence_number, max_sequence_number);;
}

Status ColumnDBOpen(const Options& options,
                    const std::string& name,
                    DB** dbptr) {
  Status s;
  DB* db = new ColumnDB(options, name, s);
  if (!s.ok()) {
    return s;
  }
  (*dbptr) = db;
  return Status::OK();
}

} // namespace leveldb
