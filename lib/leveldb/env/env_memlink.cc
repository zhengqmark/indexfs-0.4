// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <map>
#include "leveldb/env.h"
#include "env/env_impl.h"
#include "util/mutexlock.h"

namespace leveldb {

namespace {

class MemLinkEnv: public EnvWrapper {
 public:

  MemLinkEnv(Env* src_env) : EnvWrapper(src_env) { }

  virtual ~MemLinkEnv() { }

  virtual Status LinkFile(const std::string& src,
                          const std::string& target) {
    Status s;
    MutexLock lock(&mu_);
    if (link_table_.count(src) == 0) {
      link_table_[target] = src;
    } else {
      s = Status::AlreadyExists(Slice());
    }
    return s;
  }

  virtual Status SymlinkFile(const std::string& src,
                             const std::string& target) {
    Status s = LinkFile(src, target);
    return s;
  }

  Status CopyLink(const std::string& link,
                  const std::string& new_link) {
    Status s;
    MutexLock lock(&mu_);
    if (link_table_.count(new_link) == 0) {
      link_table_[new_link] = link_table_[link];
    } else {
      s = Status::AlreadyExists(Slice());
    }
    return s;
  }

  virtual Status CopyFile(const std::string& src,
                          const std::string& target) {
    Status s;
    MutexLock lock(&mu_);
    s = link_table_.count(src) != 0 ?
      CopyLink(src, target) : this->target()->CopyFile(src,target);
    return s;
  }

  virtual Status RenameFile(const std::string& src,
                            const std::string& target) {
    Status s;
    MutexLock lock(&mu_);
    s = link_table_.count(src) != 0 ?
      ImmutableLinkError() : this->target()->RenameFile(src, target);
    return s;
  }

  static bool IsChildOf(const std::string& dir,
                        const std::string& file) {
    if (!StartsWith(file, dir)) {
      return false;
    }
    return file.length() > dir.length() + 1 &&
        file[dir.length()] == '/' &&
        file.find('/', dir.length() + 1) == std::string::npos;
  }

  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) {
    Status s;
    s = target()->GetChildren(dir, result);
    if (s.ok()) {
      MutexLock lock(&mu_);
      std::map<std::string, std::string>::iterator it;
      for (it = link_table_.begin(); it != link_table_.end(); ++it) {
        if (IsChildOf(dir, it->first)) {
          result->push_back(it->first.substr(dir.length() + 1));
        }
      }
    }
    return s;
  }

  Status RemoveLink(const std::string& link) {
    Status s;
    link_table_.erase(link);
    return s;
  }

  virtual Status DeleteFile(const std::string& fname) {
    Status s;
    MutexLock lock(&mu_);
    s = link_table_.count(fname) != 0 ?
      RemoveLink(fname) : this->target()->DeleteFile(fname);
    return s;
  }

  virtual Status NewWritableFile(const std::string& fname,
                                 WritableFile** result) {
    Status s;
    *result = NULL;
    MutexLock lock(&mu_);
    s = link_table_.count(fname) != 0 ?
      ImmutableLinkError() : this->target()->NewWritableFile(fname, result);
    return s;
  }

  virtual Status NewSequentialFile(const std::string& fname,
                                   SequentialFile** result) {
    Status s;
    *result = NULL;
    MutexLock lock(&mu_);
    s = link_table_.count(fname) != 0 ?
      target()->NewSequentialFile(link_table_[fname], result) :
      target()->NewSequentialFile(fname, result);
    return s;
  }

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     RandomAccessFile** result) {
    Status s;
    *result = NULL;
    MutexLock lock(&mu_);
    s = link_table_.count(fname) != 0 ?
      target()->NewRandomAccessFile(link_table_[fname], result) :
      target()->NewRandomAccessFile(fname, result);
    return s;
  }

  virtual Status GetFileSize(const std::string& fname,
                             uint64_t* file_size) {
    Status s;
    MutexLock lock(&mu_);
    s = link_table_.count(fname) != 0 ?
      target()->GetFileSize(link_table_[fname], file_size) :
      target()->GetFileSize(fname, file_size);
    return s;
  }

  virtual bool FileExists(const std::string& fname) {
    bool r;
    MutexLock lock(&mu_);
    r = link_table_.count(fname) != 0 ? true : target()->FileExists(fname);
    return r;
  }

 private:

  static Status ImmutableLinkError() {
    return Status::NotSupported("Links are read only");
  }

  port::Mutex mu_;
  // No copying allowed
  MemLinkEnv(const MemLinkEnv&);
  MemLinkEnv& operator=(const MemLinkEnv&);
  std::map<std::string, std::string> link_table_;
};

} // namespace

Env* NewMemLinkEnv(Env* src_env) { return new MemLinkEnv(src_env); }

} // namespace leveldb
