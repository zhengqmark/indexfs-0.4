// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_UTIL_LEVELDB_READER_H_
#define _INDEXFS_UTIL_LEVELDB_READER_H_

#include <stdio.h>
#include <string>

#include "util/leveldb_io.h"

namespace indexfs {

class MDBHelper {
 public:
  static
  uint64_t FetchSeqNum(const Slice& key) {
    return DecodeFixed64(key.data() + key.size() - 8) >> 8;
  }
  static
  std::string FetchLastComponent(const std::string& path) {
    size_t offset = path.rfind('/');
    return offset == std::string::npos ? path : path.substr(offset + 1);
  }
};

class MDBTablePrinter {
 public:

  MDBTablePrinter() { }

  Status Open(const Options& opt,
              const std::string& file_path) {
    Status s;
    s = table_.Open(opt, file_path);
    file_name_ = MDBHelper::FetchLastComponent(file_path);
    return s;
  }

  int PrintTable() {
    assert(table_.ok());
    fprintf(stderr, "Reading SSTable:\n");
    fprintf(stderr, ">> File Name: %s\n", file_name_.c_str());
    fprintf(stderr, ">> File Size: %lu bytes\n", table_.size());
    int num_entries = 0;
    double key_size = 0, value_size = 0;
    for(table_->SeekToFirst();
        table_->Valid(); table_->Next()) {
      num_entries++;
      Slice key = table_->key();
      Slice value = table_->value();
      key_size += key.size();
      value_size += value.size();
      fprintf(stderr, "|- %s (%lu) -> %s\n",
              key.data(), MDBHelper::FetchSeqNum(key), value.data());
    }
    fprintf(stderr, ">> Total entries: %d\n", num_entries);
    fprintf(stderr, ">> Avg key size: %.2f\n", key_size / num_entries);
    fprintf(stderr, ">> Avg value size: %.2f\n", value_size / num_entries);
    return num_entries;
  }

 private:
  MDBTable table_;
  std::string file_name_;

  // No copying allowed
  MDBTablePrinter(const MDBTablePrinter&);
  MDBTablePrinter& operator=(const MDBTablePrinter&);
};

class MDBTableSetPrinter {
 public:

  MDBTableSetPrinter() { }

  Status Open(const Options& opt,
              const std::vector<std::string>& file_paths) {
    Status s;
    s = set_.Open(opt, file_paths);
    for (size_t i = 0; i < file_paths.size(); ++i) {
      file_names_.push_back(MDBHelper::FetchLastComponent(file_paths[i]));
    }
    return s;
  }

  int PrintTableSet() {
    assert(set_.ok());
    fprintf(stderr, "Reading SSTable Set:\n");
    std::vector<std::string>::const_iterator it;
    for (it = file_names_.begin(); it != file_names_.end(); ++it) {
      fprintf(stderr, ">> File Name: %s\n", it->c_str());
    }
    fprintf(stderr, ">> Set Size: %lu bytes\n", set_.size());
    int num_entries = 0;
    double key_size = 0, value_size = 0;
    for(set_->SeekToFirst();
        set_->Valid(); set_->Next()) {
      num_entries++;
      Slice key = set_->key();
      Slice value = set_->value();
      key_size += key.size();
      value_size += value.size();
      fprintf(stderr, "|- %s (%lu) -> %s\n",
              key.data(), MDBHelper::FetchSeqNum(key), value.data());
    }
    fprintf(stderr, ">> Total entries: %d\n", num_entries);
    fprintf(stderr, ">> Avg key size: %.2f\n", key_size / num_entries);
    fprintf(stderr, ">> Avg value size: %.2f\n", value_size / num_entries);
    return num_entries;
  }

 private:
  MDBTableSet set_;
  std::vector<std::string> file_names_;

  // No copying allowed
  MDBTableSetPrinter(const MDBTableSetPrinter&);
  MDBTableSetPrinter& operator=(const MDBTableSetPrinter&);
};

} // namespace indexfs

#endif /* _INDEXFS_UTIL_LEVELDB_READER_H_ */
