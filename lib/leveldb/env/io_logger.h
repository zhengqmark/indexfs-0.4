// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_IO_LOGGER_H_
#define STORAGE_IO_LOGGER_H_

#include "leveldb/env.h"

namespace leveldb {

class IOLog {
 private:
  WritableFile* file_;
  // No copying allowed
  IOLog(const IOLog&);
  IOLog& operator=(const IOLog&);

 public:
  IOLog(WritableFile* file) : file_(file) { }
  virtual ~IOLog() {
    if (file_ != NULL) {
      file_->Close();
      delete file_;
    }
  }
  Status Sync() { return file_->Sync(); }
  Status NewSet(const std::string& set);
  Status NewObject(const std::string& set, const std::string& name);
  Status DeleteSet(const std::string& set);
  Status DeleteObject(const std::string& set, const std::string& name);
};

class IOLogReporter;
class IOLogReader {
 private:
  SequentialFile* file_;
  // No copying allowed
  IOLogReader(const IOLogReader&);
  IOLogReader& operator=(const IOLogReader&);

 public:
  IOLogReader(SequentialFile* file) : file_(file) { }
  virtual ~IOLogReader() {
    delete file_;
  }
  Status Recover(IOLogReporter* reporter);
  Status RecoverOneEntry(IOLogReporter* reporter);
};

class IOLogReporter {
 public:
  static IOLogReporter* PrinterReporter();
  virtual ~IOLogReporter() { }
  virtual void SetCreated(const std::string& set) = 0;
  virtual void ObjectCreated(const std::string& set, const std::string& name) = 0;
  virtual void SetDeleted(const std::string& set) = 0;
  virtual void ObjectDeleted(const std::string& set, const std::string& name) = 0;
};

} // namespace leveldb

#endif /* STORAGE_IO_LOGGER_H_ */
