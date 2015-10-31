// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/data_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "util/coding.h"

namespace leveldb {

static void DeleteEntry(const Slice& key, void* value) {
  RandomAccessFile* tf = reinterpret_cast<RandomAccessFile*>(value);
  if (tf != NULL)
    delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  if (cache != NULL && h != NULL)
    cache->Release(h);
}

DataCache::DataCache(const std::string& dbname,
                     const Options* options,
                     int entries)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {
}

DataCache::~DataCache() {
  if (cache_ != NULL)
    delete cache_;
}

Status DataCache::FindTable(uint64_t file_number, Cache::Handle** handle) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if (*handle == NULL) {
    std::string fname = DataFileName(dbname_, file_number);
    RandomAccessFile* file = NULL;
    s = env_->NewRandomAccessFile(fname, &file);

    if (!s.ok()) {
      delete file;
    } else {
      *handle = cache_->Insert(key, file, 1, &DeleteEntry);
    }
  }
  return s;
}

/*
Iterator* DataCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number,
                                  uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != NULL) {
    *tableptr = NULL;
  }

  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != NULL) {
    *tableptr = table;
  }
  return result;
}
*/

Status DataCache::Get(const ReadOptions& options,
                      uint64_t file_number,
                      uint64_t offset,
                      uint64_t size,
                      Slice* result,
                      char* scratch) {
  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, &handle);
  if (s.ok()) {
    RandomAccessFile* t =
      reinterpret_cast<RandomAccessFile*>(cache_->Value(handle));
    s = t->Read(offset, size, result, scratch);
    cache_->Release(handle);
  }
  return s;
}

void DataCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb
