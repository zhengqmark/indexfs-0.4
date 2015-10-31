// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_METADB_IOUTIL_H_
#define _INDEXFS_METADB_IOUTIL_H_

#include "common/common.h"

namespace indexfs {

// Utility class for investigating data within individual SSTables.
//
class MetaDBReader {
 public:

  MetaDBReader();

  ~MetaDBReader();

  Status Open(Env* env, const std::string& fname);

  bool HasNext();

  void Next();

  std::pair<Slice, Slice> Fetch();

  // Print a summary of the data within the opened SSTable.
  void PrintTable();

  // Returns the number of entries under the given directory partition.
  int ListEntries(int64_t dir_id, int16_t partition_id);

 private:
  struct Rep;
  Rep* rep_;

  // No copying allowed
  MetaDBReader(const MetaDBReader&);
  MetaDBReader& operator=(const MetaDBReader&);
};

// Utility class for inserting data into individual SSTables.
//
class MetaDBWriter {
 public:

  MetaDBWriter(Env* env, WritableFile* file);

  ~MetaDBWriter();

  // Seal the SSTable and sync data to disk.
  // Recall that SSTables are immutable data structures.
  Status Finish();

  // Insert a file entry into the SSTable being generated.
  // Must insert entries in sorted order. Note that entries within a
  // single directory partition are not sorted according to their names,
  // but instead they are sorted according to the hash of their names.
  void InsertFile(int64_t dir_id, int16_t partition_id, const std::string& name);

 private:
  struct Rep;
  Rep* rep_;

  // No copying allowed
  MetaDBWriter(const MetaDBWriter&);
  MetaDBWriter& operator=(const MetaDBWriter&);
};

} /* namespace indexfs */

#endif /* _INDEXFS_METADB_IOUTIL_H_ */
