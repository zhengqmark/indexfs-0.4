// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/filename.h"
#include "env/obj_io.h"
#include "env/obj_set.h"
#include "env/env_impl.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <gflags/gflags.h>

// Pool for metadata persistence
DEFINE_string(rados_pool_name, "metadata", "set the rados pool for object storage");
// Object prefix (optional)
DEFINE_string(rados_obj_prefix, "indexfs_", "set the prefix for each rados object");
// Path to Ceph configuration
DEFINE_string(rados_conf_path, "/tmp/ceph.conf", "set the path to rados config file");

namespace leveldb {
namespace {

static const uint64_t kLogWriteBuffer = 2 * (1 << 16);
static const uint64_t kTableWriteBuffer = 2 * (1 << 16);
static const uint64_t kManifestWriteBuffer = 2 * (1 << 10);

class BufferedWritableFile: public WritableFile {
 private:
  size_t buffer_size_;
  std::string space_;
  WritableFile* file_;

  Status DoFlush() {
    Status s;
    if (!space_.empty()) {
      s = file_->Append(space_);
      if (s.ok()) {
        space_.clear();
      }
    }
    return s;
  }

 public:
  BufferedWritableFile(WritableFile* file, size_t buffer_size)
     : buffer_size_(buffer_size), file_(file) {
    space_.reserve(buffer_size_);
  }
  virtual ~BufferedWritableFile() {
    delete file_;
  }

  virtual Status Append(const Slice& data) {
    Status s;
    if (space_.size() + data.size() <= buffer_size_) {
      space_.append(data.data(), data.size());
    } else {
      if (!space_.empty()) {
        s = file_->Append(space_);
      }
      if (s.ok()) {
        space_.clear();
        if (data.size() <= buffer_size_) {
          space_.append(data.data(), data.size());
        } else {
          s = file_->Append(data);
        }
      }
    }
    return s;
  }

  virtual Status Close() {
    Status s = DoFlush();
    if (s.ok()) {
      s = file_->Close();
    }
    return s;
  }

  virtual Status Flush() {
    return Status::OK();
  }

  virtual Status Sync() {
    Status s = DoFlush();
    if (s.ok()) {
      s = file_->Sync();
    }
    return s;
  }
};

class RadosEnv : public Env {
 public:
  RadosEnv(const char* db_root, const char* db_home, bool read_only);

  virtual ~RadosEnv() {
    char msg[] = "Destroying RadosEnv\n";
    fwrite(msg, 1, sizeof(msg), stderr);
    abort();
  }

  virtual Status NewSequentialFile(const std::string& fname,
                                   SequentialFile** result) {
    Status s;
    FileType type = ResolveFileType(fname);
    s = OnRados(type) ? obj_env_->CreateSequentialObject(fname, result) :
        default_env_->NewSequentialFile(fname, result);
#   ifdef RADOS_DEBUG
      fprintf(stderr, "[RADOS] (%s) %s: %s\n", __func__,
          fname.c_str(), s.ToCodeString().c_str());
#   endif
    return s;
  }

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     RandomAccessFile** result) {
    Status s;
    FileType type = ResolveFileType(fname);
    s = OnRados(type) ? obj_env_->CreateRandomAccessObject(fname, result) :
        default_env_->NewRandomAccessFile(fname, result);
#   ifdef RADOS_DEBUG
      fprintf(stderr, "[RADOS] (%s) %s: %s\n", __func__,
          fname.c_str(), s.ToCodeString().c_str());
#   endif
    return s;
  }

  virtual Status NewWritableFile(const std::string& fname,
                                 WritableFile** result) {
    Status s;
    FileType type = ResolveFileType(fname);
    if (!OnRados(type)) {
      s = default_env_->NewWritableFile(fname, result);
    } else {
      s = obj_env_->CreateWritableObject(fname, result);
      if (type == kLogFile) {
        *result = new BufferedWritableFile(*result, kLogWriteBuffer);
      }
      if (type == kTableFile) {
        *result = new BufferedWritableFile(*result, kTableWriteBuffer);
      }
      if (type == kDescriptorFile) {
        *result = new BufferedWritableFile(*result, kManifestWriteBuffer);
      }
    }
#   ifdef RADOS_DEBUG
      fprintf(stderr, "[RADOS] (%s) %s: %s\n", __func__,
          fname.c_str(), s.ToCodeString().c_str());
#   endif
    return s;
  }

  virtual bool FileExists(const std::string& fname) {
    bool r;
    if (UnderRadosRoot(fname) &&
        OnRados(ResolveFileType(fname))) {
      r = obj_env_->HasObject(fname).ok();
    } else {
      // Set (a.k.a. directory) created in RADOS is also handled here
      r = default_env_->FileExists(fname);
    }
#   ifdef RADOS_DEBUG
      fprintf(stderr, "[RADOS] (%s) %s: %s\n", __func__,
          fname.c_str(), r ? "OK" : "Not Found");
#   endif
    return r;
  }

  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) {
    Status s;
    s = default_env_->GetChildren(dir, result);
    if (!s.ok()) {
      // It is all right for local directory listing to fail,
      // since there might not be a directory at local after all.
      result->clear();
    }
    if (UnderRadosRoot(dir)) {
      s = obj_env_->ListSet(dir, result);
    }
#   ifdef RADOS_DEBUG
      fprintf(stderr, "[RADOS] (%s) %s: %s\n", __func__,
          dir.c_str(), s.ToCodeString().c_str());
#   endif
    return s;
  }

  virtual Status DeleteFile(const std::string& fname) {
    Status s;
    FileType type = ResolveFileType(fname);
    s = OnRados(type) ? obj_env_->DeleteObject(fname) :
        default_env_->DeleteFile(fname);
#   ifdef RADOS_DEBUG
      fprintf(stderr, "[RADOS] (%s) %s: %s\n", __func__,
          fname.c_str(), s.ToCodeString().c_str());
#   endif
    return s;
  }

  virtual Status CreateDir(const std::string& dir) {
    Status s;
    if (UnderRadosRoot(dir) && dir != db_home_) {
      s = obj_env_->CreateSet(dir);
    }
    if (s.ok()) {
      s = default_env_->CreateDir(dir);
    }
#   ifdef RADOS_DEBUG
      fprintf(stderr, "[RADOS] (%s) %s: %s\n", __func__,
          dir.c_str(), s.ToCodeString().c_str());
#   endif
    return s;
  }

  virtual Status DeleteDir(const std::string& dir) {
    Status s;
    if (UnderRadosRoot(dir)) {
      s = obj_env_->DeleteSet(dir);
    }
    if (s.ok()) {
      Status s_ = default_env_->DeleteDir(dir);
      if (!s_.ok()) {
        fprintf(stderr, "Fail to delete local directory %s: %s\n",
            dir.c_str(), s_.ToString().c_str());
      }
    }
#   ifdef RADOS_DEBUG
      fprintf(stderr, "[RADOS] (%s) %s: %s\n", __func__,
          dir.c_str(), s.ToCodeString().c_str());
#   endif
    return s;
  }

  virtual Status GetFileSize(const std::string& fname, uint64_t* size) {
    Status s;
    FileType type = ResolveFileType(fname);
    s = OnRados(type) ? obj_env_->GetObjectSize(fname, size) :
        default_env_->GetFileSize(fname, size);
#   ifdef RADOS_DEBUG
      fprintf(stderr, "[RADOS] (%s) %s: %s\n", __func__,
          fname.c_str(), s.ToCodeString().c_str());
#   endif
    return s;
  }

  virtual Status CopyFile(const std::string& src,
                          const std::string& target) {
    Status s;
    std::pair<bool, bool> f(OnRados(ResolveFileType(src)),
                            OnRados(ResolveFileType(target)));
    if (!f.first && !f.second) {
      s = default_env_->CopyFile(src, target);
    } else if (f.first && f.second) {
      s = obj_env_->CopyObject(src, target);
    } else {
      s = Status::NotSupported(std::string(__func__));
    }
#   ifdef RADOS_DEBUG
      fprintf(stderr, "[RADOS] (%s) %s -> %s: %s\n", __func__,
          src.c_str(), target.c_str(), s.ToCodeString().c_str());
#   endif
    return s;
  }

  virtual Status SymlinkFile(const std::string& src,
                             const std::string& target) {
    Status s;
    std::pair<bool, bool> f(OnRados(ResolveFileType(src)),
                            OnRados(ResolveFileType(target)));
    if (!f.first && !f.second) {
      s = default_env_->SymlinkFile(src, target);
    } else {
      s = Status::NotSupported(std::string(__func__));
    }
#   ifdef RADOS_DEBUG
      fprintf(stderr, "[RADOS] (%s) %s -> %s: %s\n", __func__,
          src.c_str(), target.c_str(), s.ToCodeString().c_str());
#   endif
    return s;
  }

  virtual Status MigrateToRados(const std::string& src,
                                const std::string& target) {
    Status s;
    std::string content;
    s = ReadFileToString(default_env_, src, &content);
    if (s.ok()) {
      s = obj_env_->PutObject(target, content);
    }
    if (s.ok()) {
      s = default_env_->DeleteFile(src);
    }
#   ifdef RADOS_DEBUG
      fprintf(stderr, "[RADOS] (%s) %s -> %s: %s\n", __func__,
          src.c_str(), target.c_str(), s.ToCodeString().c_str());
#   endif
    return s;
  }

  virtual Status RenameFile(const std::string& src,
                            const std::string& target) {
    Status s;
    std::pair<bool, bool> f(OnRados(ResolveFileType(src)),
                            OnRados(ResolveFileType(target)));
    if (!f.first && !f.second) {
      s = default_env_->RenameFile(src, target);
    } else if (!f.first && f.second) {
      if (EndsWith(src, E_TMP)) {
        return MigrateToRados(src, target);
      } else {
        s = Status::NotSupported(std::string(__func__));
      }
    } else {
      s = Status::NotSupported(std::string(__func__));
    }
#   ifdef RADOS_DEBUG
      fprintf(stderr, "[RADOS] (%s) %s -> %s: %s\n", __func__,
          src.c_str(), target.c_str(), s.ToCodeString().c_str());
#   endif
    return s;
  }

  virtual Status LinkFile(const std::string& src,
                          const std::string& target) {
    Status s;
    std::pair<bool, bool> f(OnRados(ResolveFileType(src)),
                            OnRados(ResolveFileType(target)));
    if (!f.first && !f.second) {
      s = default_env_->LinkFile(src, target);
    } else {
      s = Status::NotSupported(std::string(__func__));
    }
#   ifdef RADOS_DEBUG
      fprintf(stderr, "[RADOS] (%s) %s -> %s: %s\n", __func__,
          src.c_str(), target.c_str(), s.ToCodeString().c_str());
#   endif
    return s;
  }

  virtual Status LockFile(const std::string& fname, FileLock** lock) {
    Status s;
    if (!OnRados(ResolveFileType(fname))) {
      s = default_env_->LockFile(fname, lock);
    } else {
      s = Status::NotSupported(std::string(__func__));
    }
    return s;
  }

  virtual Status UnlockFile(FileLock* lock) {
    return default_env_->UnlockFile(lock);
  }

  virtual void Schedule(void (*function)(void*), void* arg) {
    return default_env_->Schedule(function, arg);
  }

  virtual void StartThread(void (*function)(void* arg), void* arg) {
    return default_env_->StartThread(function, arg);
  }

  virtual Status GetTestDirectory(std::string* result) {
    return default_env_->GetTestDirectory(result);
  }

  virtual Status NewLogger(const std::string& fname, Logger** result) {
    return default_env_->NewLogger(fname, result);
  }

  virtual uint64_t NowMicros() {
    return default_env_->NowMicros();
  }

  virtual void SleepForMicroseconds(int micros) {
    return default_env_->SleepForMicroseconds(micros);
  }

 private:
  bool UnderRadosRoot(const std::string& name) {
    return StartsWith(name, db_root_) && name.length() > db_root_.length() + 1;
  }

  Env* default_env_;
  ObjEnv* obj_env_;
  std::string db_root_;
  std::string db_home_;
};

static IO* NewRadosIO() {
  return IO::RadosIO(FLAGS_rados_conf_path.c_str(), FLAGS_rados_pool_name.c_str());
}

RadosEnv::RadosEnv(const char* db_root,
                   const char* db_home,
                   bool read_only) :
                   default_env_(Env::Default()),
                   obj_env_(NULL),
                   db_root_(db_root),
                   db_home_(db_home) {
  if (!StartsWith(db_home_, db_root_)) {
    char msg[] = "Inconsistent DB home and root path\n";
    fwrite(msg, 1, sizeof(msg), stderr);
    abort();
  } else {
    ObjEnvFactory factory(NewRadosIO(), FLAGS_rados_obj_prefix);
    if (!read_only) {
      obj_env_ = factory.NewObjEnv(db_root_, db_home_);
    } else {
      obj_env_ = factory.NewReadOnlyObjEnv(db_root_, db_home_);
    }
  }
}

} // namespace

static Env* rados_env = NULL;
static pthread_once_t once = PTHREAD_ONCE_INIT;
namespace {
static const RadosOptions* rados_opt = NULL;
static void CreateRadosEnv() {
  assert(rados_env == NULL);
  rados_env = new RadosEnv(rados_opt->db_root, rados_opt->db_home, rados_opt->read_only);
}
}
Env* GetOrNewRadosEnv(const RadosOptions& options) {
  rados_opt = &options;
  pthread_once(&once, &CreateRadosEnv);
  return rados_env;
}

} // namespace leveldb
