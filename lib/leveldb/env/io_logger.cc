// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdint.h>
#include <stdio.h>
#include "util/coding.h"
#include "env/io_logger.h"

namespace leveldb {

enum LogType {
  kNewSet,
  kDelSet,
  kNewObject,
  kDelObject,
};

static const bool kSyncLog = false;
static const size_t kNameBufferSize = 128;

// Insert an new log entry formated as follows to record
// the creation of a new set.
//
// -------------------------------------------------
//  | kNewSet (1-byte) | name length |  set name  |
// -------------------------------------------------
Status IOLog::NewSet(const std::string& set) {
  Status s;
  std::string entry;
  entry.push_back(static_cast<uint8_t>(kNewSet));
  PutFixed32(&entry, set.length());
  entry.append(set);
  s = file_->Append(entry);
  return kSyncLog ? file_->Sync() : s;
}

// Insert an new log entry formated as follows to record
// the deletion of an existing set.
//
// -------------------------------------------------
//  | kDelSet (1-byte) | name length |  set name  |
// -------------------------------------------------
Status IOLog::DeleteSet(const std::string& set) {
  Status s;
  std::string entry;
  entry.push_back(static_cast<uint8_t>(kDelSet));
  PutFixed32(&entry, set.length());
  entry.append(set);
  s = file_->Append(entry);
  return kSyncLog ? file_->Sync() : s;
}

// Insert an new log entry formated as follows to record
// the creation of a new object under a specified set.
//
// ----------------------------------------------------------------------------------
//  | kNewObject (1-byte) | name length |  set name  | name length |  object name  |
// ----------------------------------------------------------------------------------
Status IOLog::NewObject(const std::string& set, const std::string& name) {
  Status s;
  std::string entry;
  entry.push_back(static_cast<uint8_t>(kNewObject));
  PutFixed32(&entry, set.length());
  entry.append(set);
  PutFixed32(&entry, name.length());
  entry.append(name);
  s = file_->Append(entry);
  return kSyncLog ? file_->Sync() : s;
}

// Insert an new log entry formated as follows to record
// the deletion of an existing object from a specified set.
//
// ----------------------------------------------------------------------------------
//  | kDelObject (1-byte) | name length |  set name  | name length |  object name  |
// ----------------------------------------------------------------------------------
Status IOLog::DeleteObject(const std::string& set, const std::string& name) {
  Status s;
  std::string entry;
  entry.push_back(static_cast<uint8_t>(kDelObject));
  PutFixed32(&entry, set.length());
  entry.append(set);
  PutFixed32(&entry, name.length());
  entry.append(name);
  s = file_->Append(entry);
  return kSyncLog ? file_->Sync() : s;
}

Status IOLogReader::Recover(IOLogReporter* reporter) {
  Status s;
  while (s.ok()) {
    s = RecoverOneEntry(reporter);
  }
  return s.IsNotFound() ? Status::OK() : s;
}

static
Status ReadSize(SequentialFile* file, char* scratch, uint32_t* size) {
  Status s;
  Slice result;
  s = file->Read(sizeof(uint32_t), &result, scratch);
  if (!s.ok()) {
    return s;
  }
  if (result.size() < sizeof(uint32_t)) {
    return Status::Corruption(Slice());
  }
  *size = DecodeFixed32(scratch);
  if (*size > kNameBufferSize) {
    return Status::BufferFull(Slice());
  }
  return s;
}

static
Status ReadName(SequentialFile* file, size_t n, char* scratch) {
  Status s;
  Slice result;
  s = file->Read(n, &result, scratch);
  if (!s.ok()) {
    return s;
  }
  if (result.size() < n) {
    return Status::Corruption(Slice());
  }
  return s;
}

Status IOLogReader::RecoverOneEntry(IOLogReporter* reporter) {
  Status s;
  uint8_t type;
  Slice result;
  s = file_->Read(sizeof(uint8_t), &result, reinterpret_cast<char*>(&type));
  if (!s.ok()) {
    return s;
  }
  if (result.size() == sizeof(uint8_t)) {
    switch (type) {
    case kNewSet:
    case kDelSet:
    case kNewObject:
    case kDelObject:
      break;
    default:
      return Status::Corruption("Unknown entry type");
    }
    uint32_t space;
    char set_name[kNameBufferSize];
    uint32_t set_name_len;
    char obj_name[kNameBufferSize];
    uint32_t obj_name_len;
    s = ReadSize(file_, reinterpret_cast<char*>(&space), &set_name_len);
    if (!s.ok()) {
      return s;
    }
    s = ReadName(file_, set_name_len, set_name);
    if (!s.ok()) {
      return s;
    }
    if (type == kNewSet) {
      reporter->SetCreated(std::string(set_name, set_name_len));
      return s;
    }
    if (type == kDelSet) {
      reporter->SetDeleted(std::string(set_name, set_name_len));
      return s;
    }
    s = ReadSize(file_, reinterpret_cast<char*>(&space), &obj_name_len);
    if (!s.ok()) {
      return s;
    }
    s = ReadName(file_, obj_name_len, obj_name);
    if (!s.ok()) {
      return s;
    }
    if (type == kNewObject) {
      reporter->ObjectCreated(std::string(set_name, set_name_len),
          std::string(obj_name, obj_name_len));
      return s;
    }
    if (type == kDelObject) {
      reporter->ObjectDeleted(std::string(set_name, set_name_len),
          std::string(obj_name, obj_name_len));
      return s;
    }
  }
  return Status::NotFound(Slice()); // end of file
}

namespace {
class StderrLogReporter: virtual public IOLogReporter {
 public:
  StderrLogReporter() { }
  virtual ~StderrLogReporter() { }
  void SetCreated(const std::string& set) {
    fprintf(stderr, "%s - %s\n",
        __func__, set.c_str());
  }
  void ObjectCreated(const std::string& set,
                     const std::string& name) {
    fprintf(stderr, "%s - %s/%s\n", __func__, set.c_str(), name.c_str());
  }
  void SetDeleted(const std::string& set) {
    fprintf(stderr, "%s - %s\n",
        __func__, set.c_str());
  }
  void ObjectDeleted(const std::string& set,
                     const std::string& name) {
    fprintf(stderr, "%s - %s/%s\n", __func__, set.c_str(), name.c_str());
  }
};
}

IOLogReporter* IOLogReporter::PrinterReporter() {
  return new StderrLogReporter();
}

} // namespace leveldb
