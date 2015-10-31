// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>

#include <vector>
#include <algorithm>

#include "util/leveldb_io.h"
#include "metadb/dbtypes.h"
#include "metadb/metadb_io.h"

namespace indexfs {

struct MetaDBWriter::Rep {
  MDBTableBuilder builder_;
  Rep(const Options& opt, WritableFile* file)
      : builder_(opt, file) {
  }
};

MetaDBWriter::~MetaDBWriter() {
  delete rep_;
}

MetaDBWriter::MetaDBWriter(Env* env, WritableFile* file) {
  MDBOptionsFactory factory(env);
  rep_ = new Rep(factory.Get(), file);
}

Status MetaDBWriter::Finish() {
  return rep_->builder_.Seal();
}

using mdb::MDBKey;
using mdb::MDBValue;

namespace {
static const int16_t kDataEmbedded = 1;
static const int32_t kFileMode = (S_IRUSR | S_IFREG);
}

void MetaDBWriter::InsertFile(int64_t dir_id,
        int16_t partition_id, const std::string& name) {
  MDBValue mdb_val(name);
  mdb_val->SetInodeNo(-1);
  mdb_val->SetFileSize(0);
  mdb_val->SetFileMode(kFileMode);
  mdb_val->SetFileStatus(kDataEmbedded);
  mdb_val->SetZerothServer(-1);
  mdb_val->SetUserId(-1);
  mdb_val->SetGroupId(-1);
  mdb_val->SetTime(time(NULL));
  MDBKey mdb_key(dir_id, partition_id, name);
  rep_->builder_.Put(mdb_key.ToSlice(), mdb_val.ToSlice());
}

} /* namespace indexfs */
