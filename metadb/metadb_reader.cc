// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/leveldb_io.h"
#include "metadb/dbtypes.h"
#include "metadb/metadb_io.h"

namespace indexfs {

struct MetaDBReader::Rep {
  MDBTable table_;
  std::string file_name_;
};

MetaDBReader::MetaDBReader() {
  rep_ = new Rep();
}

MetaDBReader::~MetaDBReader() {
  delete rep_;
}

namespace {
static inline
std::string FetchFileName(const std::string& path) {
  size_t offset;
  if ((offset = path.rfind('/')) == std::string::npos) {
    return path;
  }
  return path.substr(offset + 1);
}
}

Status MetaDBReader::Open(Env* env, const std::string& fname) {
  MDBOptionsFactory factory(env);
  Status s;
  s = rep_->table_.Open(factory.Get(), fname);
  if (s.ok()) {
    rep_->table_->SeekToFirst();
    rep_->file_name_ = FetchFileName(fname);
  }
  return s;
}

bool MetaDBReader::HasNext() {
  DLOG_ASSERT(rep_->table_.ok());
  return rep_->table_->Valid();
}

void MetaDBReader::Next() {
  DLOG_ASSERT(rep_->table_.ok());
  return rep_->table_->Next();
}

std::pair<Slice, Slice> MetaDBReader::Fetch() {
  DLOG_ASSERT(rep_->table_.ok());
  return std::make_pair(rep_->table_->key(), rep_->table_->value());
}

using mdb::MDBKey;
using mdb::MDBValueRef;

namespace {
static inline
const MDBKey* ToKey(const char* ptr) {
  return reinterpret_cast<const MDBKey*>(ptr);
}
}

int MetaDBReader::ListEntries(int64_t dir_id, int16_t partition_id) {
  DLOG_ASSERT(rep_->table_.ok());
  int num_entries = 0;
  for(rep_->table_->SeekToFirst();
      rep_->table_->Valid(); rep_->table_->Next()) {
    const MDBKey* key = ToKey(rep_->table_->key().data());
    if (key->GetParent() == dir_id &&
        key->GetPartition() == partition_id) {
      num_entries++;
    }
  }
  return num_entries;
}

void MetaDBReader::PrintTable() {
  DLOG_ASSERT(rep_->table_.ok());
  int64_t last_dir = 0;
  int16_t last_partition = 0;
  fprintf(stderr, "Reading SSTable:\n");
  fprintf(stderr, ">> File Name: %s\n", rep_->file_name_.c_str());
  fprintf(stderr, ">> File Size: %lu bytes\n", rep_->table_.size());
  double _total_value_size = 0;
  int num_entries = 0, _num_entries = 0;
  for(rep_->table_->SeekToFirst();
      rep_->table_->Valid(); rep_->table_->Next()) {
    num_entries++;
    const MDBKey* key = ToKey(rep_->table_->key().data());
    if (last_dir != key->GetParent() ||
        last_partition != key->GetPartition()) {
      if (_num_entries != 0) {
        fprintf(stderr,
            "|- Found %d entries with "
               "[dir=%ld, partition=%hd], "
               "avg value size: %.2f bytes\n",
            _num_entries, last_dir, last_partition, _total_value_size / _num_entries);
      }
      _num_entries = 0;
      _total_value_size = 0;
    }
    _num_entries++;
    last_dir = key->GetParent();
    last_partition = key->GetPartition();
    _total_value_size += rep_->table_->value().size();
  }
  if (_num_entries != 0) {
    fprintf(stderr,
        "|- Found %d entries with "
           "[dir=%ld, partition=%hd], "
           "avg value size: %.2f bytes\n",
        _num_entries, last_dir, last_partition, _total_value_size / _num_entries);
  }
  fprintf(stderr, ">> Total entries found: %d\n", num_entries);
}

} /* namespace indexfs */
