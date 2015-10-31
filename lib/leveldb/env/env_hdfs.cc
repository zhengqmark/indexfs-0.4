// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <gflags/gflags.h>
#include <stdio.h>
#include <stdlib.h>
#include "hdfs.h"
#include "env/env_impl.h"
#include "leveldb/slice.h"
#include "leveldb/env.h"
#include "util/mutexlock.h"

// HDFS IP address
DEFINE_string(hdfs_ip, "127.0.0.1", "set HDFS IP address");
// HDFS port number
DEFINE_int32(hdfs_port, 8020, "set HDFS port number");

namespace leveldb {

namespace {

class HDFSSequentialFile: public SequentialFile {
 private:
  std::string filename_;
  hdfsFS hdfs_fs_;
  hdfsFile file_;

 public:
  HDFSSequentialFile(const std::string& filename,
                     hdfsFS hdfs_fs,
                     hdfsFile file)
      : filename_(filename), hdfs_fs_(hdfs_fs), file_(file) {
  }
  virtual ~HDFSSequentialFile() {
    if (file_ != NULL && hdfs_fs_ != NULL) {
      hdfsCloseFile(hdfs_fs_, file_);
    }
  }

  virtual Status Read(size_t n, Slice* result, char* scratch) {
    Status s;
    tSize r = hdfsRead(hdfs_fs_, file_,
                       scratch, static_cast<tSize>(n));
    *result = Slice(scratch, r);
    if (r < n) {
      if (r != -1) {
        // We leave status as ok if we hit the end of the file
      } else {
        // A partial read with an error: return a non-ok status
        s = Status::IOError("hdfsRead", filename_);
      }
    }
#   ifdef HDFS_IO_DEBUG
      fprintf(stderr, "[HDFS] (%s) %s\n", __func__, filename_.c_str());
#   endif
    return s;
  }

  virtual Status Skip(uint64_t n) {
    Status s;
    tOffset cur = hdfsTell(hdfs_fs_, file_);
    if (cur == -1) {
      s = Status::IOError("hdfsTell", filename_);
    }
    if (s.ok()) {
      int r = hdfsSeek(hdfs_fs_, file_,
                       cur + static_cast<tOffset>(n));
      if (r != 0) {
        s = Status::IOError("hdfsSeek", filename_);
      }
    }
#   ifdef HDFS_IO_DEBUG
      fprintf(stderr, "[HDFS] (%s) %s\n", __func__, filename_.c_str());
#   endif
    return s;
  }
};

class HDFSRandomAccessFile: public RandomAccessFile {
 private:
  std::string filename_;
  hdfsFS hdfs_fs_;
  hdfsFile file_;

 public:
  HDFSRandomAccessFile(const std::string& filename,
                       hdfsFS hdfs_fs,
                       hdfsFile file)
      : filename_(filename), hdfs_fs_(hdfs_fs), file_(file) {
  }
  virtual ~HDFSRandomAccessFile() {
    if (file_ != NULL && hdfs_fs_ != NULL) {
      hdfsCloseFile(hdfs_fs_, file_);
    }
  }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    Status s;
    tSize r = hdfsPread(hdfs_fs_, file_,
                        static_cast<tOffset>(offset),
                        scratch, static_cast<tSize>(n));
    *result = Slice(scratch, (r < 0) ? 0 : r);
    if (r < 0) {
      // An error: return a non-ok status
      s = Status::IOError("hdfsPread", filename_);
    }
#   ifdef HDFS_IO_DEBUG
      fprintf(stderr, "[HDFS] (%s) %s\n", __func__, filename_.c_str());
#   endif
    return s;
  }
};

class HDFSWritableFile : public WritableFile {
 private:
  std::string filename_;
  hdfsFS hdfs_fs_;
  hdfsFile file_;

 public:
  HDFSWritableFile(const std::string& filename,
                   hdfsFS hdfs_fs,
                   hdfsFile file)
      : filename_(filename), hdfs_fs_(hdfs_fs), file_(file){
  }
  virtual ~HDFSWritableFile() {
    if (file_ != NULL && hdfs_fs_ != NULL) {
      hdfsCloseFile(hdfs_fs_, file_);
    }
  }

  virtual Status Append(const Slice& data) {
    Status s;
    const char* src = data.data();
    tSize r = hdfsWrite(hdfs_fs_, file_, src,
                        static_cast<tSize>(data.size()));
    if (r < 0) {
      s = Status::IOError("hdfsWrite", filename_);
    }
#   ifdef HDFS_IO_DEBUG
      fprintf(stderr, "[HDFS] (%s) %s\n", __func__, filename_.c_str());
#   endif
    return s;
  }

  virtual Status Close() {
    Status s;
    if (hdfsCloseFile(hdfs_fs_, file_) < 0) {
      s = Status::IOError("hdfsClose", filename_);
    }
    file_ = NULL;
#   ifdef HDFS_IO_DEBUG
      fprintf(stderr, "[HDFS] (%s) %s\n", __func__, filename_.c_str());
#   endif
    return s;
  }

  virtual Status Flush() {
    // HDFS client auto flushes file data to data servers
    return Status::OK();
  }

  virtual Status Sync() {
    Status s;
    if (hdfsHFlush(hdfs_fs_, file_) < 0) {
      s = Status::IOError("hdfsHFlush", filename_);
    }
#   ifdef HDFS_IO_DEBUG
      fprintf(stderr, "[HDFS] (%s) %s\n", __func__, filename_.c_str());
#   endif
    return s;
  }
};

class HDFSEnv : public Env {
 public:
  HDFSEnv(const char* host, tPort port)
      : default_env_(Env::Default()) {
    hdfs_primary_fs_ = hdfsConnect(host, port);
  }

  virtual ~HDFSEnv() {
    hdfsDisconnect(hdfs_primary_fs_);
    char msg[] = "Destroying HDFSEnv\n";
    fwrite(msg, 1, sizeof(msg), stderr);
    abort();
  }

  virtual Status NewSequentialFile(const std::string& fname,
                                   SequentialFile** result) {
    Status s;
    if (!OnHDFS(fname)) {
      s = default_env_->NewSequentialFile(fname, result);
    } else {
      hdfsFile f = hdfsOpenFile(hdfs_primary_fs_,
                                fname.c_str(),
                                O_RDONLY, 0, 0, 0);
      if (f == NULL) {
        *result = NULL;
        s = Status::IOError("hdfsOpenFile", fname);
      } else {
        *result = new HDFSSequentialFile(fname, hdfs_primary_fs_, f);
      }
    }
#   ifdef HDFS_DEBUG
      fprintf(stderr, "[HDFS] (%s) %s\n", __func__, fname.c_str());
#   endif
    return s;
  }

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     RandomAccessFile** result) {
    Status s;
    if (!OnHDFS(fname)) {
      s = default_env_->NewRandomAccessFile(fname, result);
    } else {
      hdfsFile f = NULL;
      for (int i = 0; f == NULL && i < 3; ++i) {
        f = hdfsOpenFile(hdfs_primary_fs_, fname.c_str(),
                         O_RDONLY, 0, 0, 0);
      }
      if (f == NULL) {
        *result = NULL;
        s = Status::IOError("hdfsOpenFile", fname);
      } else {
        *result = new HDFSRandomAccessFile(fname, hdfs_primary_fs_, f);
      }
    }
#   ifdef HDFS_DEBUG
      fprintf(stderr, "[HDFS] (%s) %s\n", __func__, fname.c_str());
#   endif
    return s;
  }

  virtual Status NewWritableFile(const std::string& fname,
                                 WritableFile** result) {
    Status s;
    if (!OnHDFS(fname)) {
      s = default_env_->NewWritableFile(fname, result);
    } else {
      hdfsFile f = hdfsOpenFile(hdfs_primary_fs_,
                                fname.c_str(),
                                O_WRONLY, 0, 0, 0);
      if (f == NULL) {
        *result = NULL;
        s = Status::IOError("hdfsOpenFile", fname);
      } else {
        *result = new HDFSWritableFile(fname, hdfs_primary_fs_, f);
      }
    }
#   ifdef HDFS_DEBUG
      fprintf(stderr, "[HDFS] (%s) %s\n", __func__, fname.c_str());
#   endif
    return s;
  }

  virtual bool FileExists(const std::string& fname) {
    bool r;
    if (!OnHDFS(fname)) {
      r = default_env_->FileExists(fname);
    } else {
      r = hdfsExists(hdfs_primary_fs_, fname.c_str()) == 0;
    }
#   ifdef HDFS_DEBUG
      fprintf(stderr, "[HDFS] (%s) %s\n", __func__, fname.c_str());
#   endif
    return r;
  }

  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) {
    Status s;
    s = default_env_->GetChildren(dir, result);
    if (!s.ok()) {
      result->clear();
      s = Status::OK(); // Ignore local errors
    }
    hdfsFileInfo* infos = NULL;
    int num_entries = 0;
    infos = hdfsListDirectory(hdfs_primary_fs_,
                              dir.c_str(), &num_entries);
    if (num_entries > 0) {
      for (int i = 0; i < num_entries; ++i) {
        char* last_component = strrchr(infos[i].mName, '/');
        if (last_component != NULL) {
          result->push_back(last_component + 1);
        }
      }
    }
    if (infos != NULL) {
      hdfsFreeFileInfo(infos, num_entries);
    }
#   ifdef HDFS_DEBUG
      fprintf(stderr, "[HDFS] (%s) %s\n", __func__, dir.c_str());
#   endif
    return s;
  }

  virtual Status DeleteFile(const std::string& fname) {
    Status s;
    if (!OnHDFS(fname)) {
      s = default_env_->DeleteFile(fname);
    } else {
      int r = hdfsDelete(hdfs_primary_fs_, fname.c_str(), 0);
      if (r != 0) {
        s = Status::IOError("hdfsDelete", fname);
      }
    }
#   ifdef HDFS_DEBUG
      fprintf(stderr, "[HDFS] (%s) %s\n", __func__, fname.c_str());
#   endif
    return s;
  };

  virtual Status CreateDir(const std::string& name) {
    Status s;
    int r = hdfsCreateDirectory(hdfs_primary_fs_, name.c_str());
    if (r != 0) {
      s = Status::IOError("hdfsCreateDirectory", name);
    }
    if (s.ok()) {
      s = default_env_->CreateDir(name);
    }
#   ifdef HDFS_DEBUG
      fprintf(stderr, "[HDFS] (%s) %s\n", __func__, name.c_str());
#   endif
    return s;
  };

  virtual Status DeleteDir(const std::string& name) {
    Status s;
    int r = hdfsDelete(hdfs_primary_fs_, name.c_str(), 1);
    if (r != 0) {
      s = Status::IOError("hdfsDelete", name);
    }
    if (s.ok()) {
      Status s_ = default_env_->DeleteDir(name);
      if (!s_.ok()) {
        fprintf(stderr, "Cannot delete directory %s: %s\n", name.c_str(), s_.ToString().c_str());
      }
    }
#   ifdef HDFS_DEBUG
      fprintf(stderr, "[HDFS] (%s) %s\n", __func__, name.c_str());
#   endif
    return s;
  };

  virtual Status GetFileSize(const std::string& fname,
                             uint64_t* size) {
    Status s;
    if (!OnHDFS(fname)) {
      s = default_env_->GetFileSize(fname, size);
    } else {
      hdfsFileInfo* info = hdfsGetPathInfo(hdfs_primary_fs_,
                                           fname.c_str());
      if (info != NULL) {
        *size = info->mSize;
        hdfsFreeFileInfo(info, 1);
      } else {
        s = Status::IOError("hdfsGetPathInfo", fname);
      }
    }
#   ifdef HDFS_DEBUG
      fprintf(stderr, "[HDFS] (%s) %s\n", __func__, fname.c_str());
#   endif
    return s;
  }

  virtual Status CopyFile(const std::string& src,
                          const std::string& target) {
    Status s;
    if (!OnHDFS(src) && !OnHDFS(target)) {
      s = default_env_->CopyFile(src, target);
    } else {
      s = Status::IOError("Cannot copy within or across HDFS");
    }
#   ifdef HDFS_DEBUG
      fprintf(stderr, "[HDFS] (%s) %s -> %s\n", __func__, src.c_str(), target.c_str());
#   endif
    return s;
  }

  virtual Status SymlinkFile(const std::string& src,
                             const std::string& target) {
    Status s;
    if (!OnHDFS(src) && !OnHDFS(target)) {
      s = default_env_->SymlinkFile(src, target);
    } else {
      s = Status::IOError("Cannot link within or across HDFS");
    }
#   ifdef HDFS_DEBUG
      fprintf(stderr, "[HDFS] (%s) %s -> %s\n", __func__, src.c_str(), target.c_str());
#   endif
    return s;
  }

  virtual Status RenameFile(const std::string& src,
                            const std::string& target) {
    Status s;
    if (!OnHDFS(src) && !OnHDFS(target)) {
      s = default_env_->RenameFile(src, target);
    } else if (OnHDFS(src) && OnHDFS(target)) {
      int r = hdfsRename(hdfs_primary_fs_, src.c_str(), target.c_str());
      if (r < 0) {
        s = Status::IOError("hdfsRename", src + "->" + target);
      }
    } else {
      s = Status::IOError("Cannot rename across file systems");
    }
#   ifdef HDFS_DEBUG
      fprintf(stderr, "[HDFS] (%s) %s -> %s\n", __func__, src.c_str(), target.c_str());
#   endif
    return s;
  }

  virtual Status LinkFile(const std::string& src,
                          const std::string& target) {
    Status s;
    if (!OnHDFS(src) && !OnHDFS(target)) {
      s = default_env_->LinkFile(src, target);
    } else {
      s = Status::IOError("Cannot link within or across HDFS");
    }
#   ifdef HDFS_DEBUG
      fprintf(stderr, "[HDFS] (%s) %s -> %s\n", __func__, src.c_str(), target.c_str());
#   endif
    return s;
  }

  virtual Status LockFile(const std::string& fname, FileLock** lock) {
    Status s;
    if (!OnHDFS(fname)) {
      s = default_env_->LockFile(fname, lock);
    } else {
      s = Status::IOError("Cannot lock file within HDFS");
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
  Env* default_env_;
  hdfsFS hdfs_primary_fs_;
};

}  // namespace

static port::Mutex mtx;
static Env* hdfs_env = NULL;

Env* Env::HDFS_Env() {
  if (hdfs_env == NULL) {
    MutexLock lock(&mtx);
    if (hdfs_env == NULL) {
      hdfs_env = new HDFSEnv(FLAGS_hdfs_ip.c_str(), FLAGS_hdfs_port);
    }
  }
  return hdfs_env;
}

}  // namespace leveldb
