// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "common/options.h"
#include "util/leveldb_io.h"

namespace indexfs {

bool IsManifestFile(const std::string& name) {
  size_t prefix_size = strlen("MANIFEST");
  return name.length() > prefix_size &&
          name.compare(0, prefix_size, "MANIFEST") == 0;
}

bool IsSSTableFile(const std::string& name) {
  size_t suffix_size = strlen(".sst");
  return name.length() > suffix_size &&
          name.compare(name.length() - suffix_size, suffix_size, ".sst") == 0;
}

Status MDBTableSet::Open(const Options& options, const std::string& dir) {
  Status s;
  std::vector<std::string> names;
  s = options.env->GetChildren(dir, &names);
  if (!s.ok()) {
    return s;
  }
  std::vector<std::string> fnames;
  std::vector<std::string>::iterator it;
  for (it = names.begin(); it != names.end(); ++it) {
    if (IsSSTableFile(*it)) {
      fnames.push_back(dir + "/" + *it);
    }
  }
  return Open(options, fnames);
}

namespace {
// Factory method for
// internal LevelDB key comparator.
//
static inline
const Comparator* CreateComparator() {
  return NewInternalComparator(BytewiseComparator());
}

// Factory method for
// internal LevelDB filter policy.
//
static inline
const FilterPolicy* CreateFilterPolicy() {
  return DEFAULT_LEVELDB_FILTER_BYTES <= 0 ? NULL : NewInternalFilterPolicy(
    NewBloomFilterPolicy(DEFAULT_LEVELDB_FILTER_BYTES));
}
}

namespace {
static void ObtainDefaultOptions(Options* opt, Env* env) {
  opt->block_cache = NULL;
  opt->env = env == NULL ? Env::Default() : env;
  opt->comparator = CreateComparator();
  opt->filter_policy = CreateFilterPolicy();
  opt->block_size = DEFAULT_LEVELDB_BLOCK_SIZE;
  opt->compression = DEFAULT_LEVELDB_COMPRESSION ? kSnappyCompression : kNoCompression;
}
}

MDBOptionsFactory::MDBOptionsFactory(Env* env) {
  ObtainDefaultOptions(&opt_, env);
}

void MDBTableBuilder::Put(const Slice& key, const Slice& value) {
  size_t key_size = key.size();
  if (key_size <= kSmallKey) {
    Put_SmallKey(key, value);
    return;
  }
  uint64_t seq = ++seq_;
  char* buf = new char[key_size + 8];
  char* p = buf;
  memcpy(p, key.data(), key_size);
  p += key_size;
  EncodeFixed64(p, (seq << 8) | kTypeValue);
  builder_->Add(Slice(buf, key_size + 8), value);
  delete [] buf;
}

void MDBTableBuilder::Put_SmallKey(const Slice& key, const Slice& value) {
  size_t key_size = key.size();
  uint64_t seq = ++seq_;
  char buf[2 * kSmallKey];
  char* p = buf;
  memcpy(p, key.data(), key_size);
  p += key_size;
  EncodeFixed64(p, (seq << 8) | kTypeValue);
  builder_->Add(Slice(buf, key_size + 8), value);
}

} // namespace indexfs
