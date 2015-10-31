// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "env/obj_io.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <gflags/gflags.h>
#include <rados/librados.hpp>

// Check object existence before "opening a file"
DEFINE_bool(rados_read_check, true, "check object existence before reads");
// Ensure object existence before "creating a writable file"
DEFINE_bool(rados_write_check, true, "ensure object existence before writes");
// Discard existing object data before "creating a writable file"
DEFINE_bool(rados_truck_to_empty, true, "force truncate object before writes");
// Use asynchronous object I/O to support writable file
DEFINE_bool(rados_use_async_write, true, "use AIO to perform write operations");

namespace leveldb {
namespace {

// Import Ceph types
using ceph::buffer;
using ceph::bufferptr;
using ceph::bufferlist;

// Import RADOS specific types
using librados::AioCompletion;
using librados::Rados;
using librados::IoCtx;
using librados::ObjectReadOperation;
using librados::ObjectWriteOperation;


static Status IOError(const std::string& context, int err_number) {
  char sp[32];
  snprintf(sp, 32, "rados_err=%d", -1 * err_number);
  return Status::IOError(context, sp);
}

class RadosSequentialFile: public SequentialFile {
 private:
  uint64_t offset_;
  IoCtx io_;
  bool io_shared_;
  std::string filename_;

 public:
  RadosSequentialFile(const std::string& filename,
                      IoCtx io, bool io_shared)
      : offset_(0), io_(io), io_shared_(io_shared), filename_(filename) {
  }
  virtual ~RadosSequentialFile() {
    if (!io_shared_) {
      io_.close();
    }
  }

  virtual Status Read(size_t n, Slice* result, char* scratch) {
    Status s;

    bufferlist bl;
    bufferptr bp = buffer::create_static(n, scratch);
    bl.push_back(bp);

    int r = io_.read(filename_, bl, n, offset_);
    if (r >= 0) {
      assert(bl.length() <= n);
      if (bl.c_str() != scratch) {
        bl.copy(0, bl.length(), scratch);
      }
      r = bl.length();
    }

    if (r >= 0) {
      offset_ += r;
      *result = Slice(scratch, r);
    } else {
      s = IOError(__func__, r);
    }

    return s;
  }

  virtual Status Skip(uint64_t n) {
    offset_ += n;
    return Status::OK();
  }
};

class RadosRandomAccessFile: public RandomAccessFile {
 private:
  mutable IoCtx io_;
  bool io_shared_;
  std::string filename_;

 public:
  RadosRandomAccessFile(const std::string& filename,
                        IoCtx io, bool io_shared)
      : io_(io), io_shared_(io_shared), filename_(filename) {
  }
  virtual ~RadosRandomAccessFile() {
    if (!io_shared_) {
      io_.close();
    }
  }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    Status s;

    bufferlist bl;
    bufferptr bp = buffer::create_static(n, scratch);
    bl.push_back(bp);

    int r = io_.read(filename_, bl, n, offset);
    if (r >= 0) {
      assert(bl.length() <= n);
      if (bl.c_str() != scratch) {
        bl.copy(0, bl.length(), scratch);
      }
      r = bl.length();
    }

    if (r >= 0) {
      *result = Slice(scratch, r);
    } else {
      s = IOError(__func__, r);
    }

    return s;
  }
};

class RadosWritableFile : public WritableFile {
 private:
  IoCtx io_;
  bool io_shared_;
  std::string filename_;

 public:
  RadosWritableFile(const std::string& filename,
                    IoCtx io, bool io_shared)
      : io_(io), io_shared_(io_shared), filename_(filename) {
  }
  virtual ~RadosWritableFile() {
    if (!io_shared_) {
      io_.close();
    }
  }

  virtual Status Append(const Slice& data) {
    Status s;

    bufferlist bl;
    bl.append(data.data(), data.size());

    int r = io_.append(filename_, bl, data.size());
    if (r < 0) {
      s = IOError(__func__, r);
    }
#   ifdef RADOS_IO_DEBUG
      fprintf(stderr, "[RADOS] (%s) %s(%lu): %s\n", __func__,
          filename_.c_str(), data.size(), s.ToCodeString().c_str());
#   endif
    return s;
  }

  virtual Status Close() {
    Status s;
#   ifdef RADOS_IO_DEBUG
      fprintf(stderr, "[RADOS] (%s) %s: %s\n", __func__,
          filename_.c_str(), s.ToCodeString().c_str());
#   endif
    return s;
  }

  virtual Status Flush() {
    return Status::OK();
  }

  virtual Status Sync() {
    Status s;
#   ifdef RADOS_IO_DEBUG
      fprintf(stderr, "[RADOS] (%s) %s: %s\n", __func__,
          filename_.c_str(), s.ToCodeString().c_str());
#   endif
    return s;
  }
};

class RadosAsyncWritableFile : public WritableFile {
 private:
  IoCtx io_;
  bool io_shared_;
  std::string filename_;

 public:
  RadosAsyncWritableFile(const std::string& filename,
                         IoCtx io, bool io_shared)
      : io_(io), io_shared_(io_shared), filename_(filename) {
  }
  virtual ~RadosAsyncWritableFile() {
    if (!io_shared_) {
      io_.close();
    }
  }

  virtual Status Append(const Slice& data) {
    Status s;

    bufferlist bl;
    bl.append(data.data(), data.size());

    AioCompletion* comp = Rados::aio_create_completion();
    int r = io_.aio_append(filename_, comp, bl, data.size());

    if (r < 0) {
      s = IOError(__func__, r);
    }
#   ifdef RADOS_IO_DEBUG
      fprintf(stderr, "[RADOS] (%s) %s(%lu): %s\n", __func__,
          filename_.c_str(), data.size(), s.ToCodeString().c_str());
#   endif
    delete comp;
    return s;
  }

  virtual Status Close() {
    Status s;
#   ifdef RADOS_IO_DEBUG
      fprintf(stderr, "[RADOS] (%s) %s: %s\n", __func__,
          filename_.c_str(), s.ToCodeString().c_str());
#   endif
    return s;
  }

  virtual Status Flush() {
    return Status::OK();
  }

  virtual Status Sync() {
    Status s;
    int r = io_.aio_flush();
    if (r < 0) {
      s = IOError(__func__, r);
    }
#   ifdef RADOS_IO_DEBUG
      fprintf(stderr, "[RADOS] (%s) %s: %s\n", __func__,
          filename_.c_str(), s.ToCodeString().c_str());
#   endif
    return s;
  }
};

class RadosIO: virtual public IO {
 private:
  IoCtx io_ctx_;
  Rados rados_cluster_;

  static void RadosCall(const char* context, int ret) {
    if (ret != 0) {
      fprintf(stderr, "Fail to perform RADOS %s: err=%d\n", context, ret);
      abort();
    }
  }

 public:
  RadosIO(const char* conf_path,
          const char* pool_name) {
    RadosCall("init",
        rados_cluster_.init(NULL));
    RadosCall("conf_read_file",
        rados_cluster_.conf_read_file(conf_path));
    RadosCall("connect",
        rados_cluster_.connect());
    RadosCall("ioctx_create",
        rados_cluster_.ioctx_create(pool_name, io_ctx_));
  }

  virtual ~RadosIO() {
    fprintf(stderr, "Destroying global RADOS IO instance\n");
    abort();
  }

  bool Exists(const std::string& oname) {
    time_t mtime;
    uint64_t size;
    int r = io_ctx_.stat(oname, &size, &mtime);
    return r == 0 ? true : false;
  }

  Status PutObject(const std::string& oname, const Slice& data) {
    Status s;
    ObjectWriteOperation op;
    bufferlist bl;
    bl.append(data.data(), data.size());
    op.write_full(bl);
    int r = io_ctx_.operate(oname, &op);
    return r == 0 ? s : IOError(__func__, r);
  }

  Status DeleteObject(const std::string& oname) {
    Status s;
    ObjectWriteOperation op;
    op.remove();
    int r = io_ctx_.operate(oname, &op);
    return r == 0 ? s : IOError(__func__, r);
  }

  Status CopyObject(const std::string& src, const std::string& target) {
    Status s;
    ObjectWriteOperation op;
    op.copy_from(src, io_ctx_, 0);
    int r = io_ctx_.operate(target, &op);
    return r == 0 ? s : IOError(__func__, r);
  }

  Status NewWritableObject(const std::string& oname,
      WritableFile** object) {

    Status s;

    if (FLAGS_rados_truck_to_empty) {
      bufferlist bl;
      int r = io_ctx_.write_full(oname, bl);

      if (r < 0) {
        s = IOError(__func__, r);
      }
    }

    else if (FLAGS_rados_write_check) {
      int r = io_ctx_.create(oname, false);

      if (r < 0) {
        s = IOError(__func__, r);
      }
    }

    if (s.ok()) {
      if (FLAGS_rados_use_async_write) {
        IoCtx lo_ctx;
        lo_ctx.dup(io_ctx_);
        *object = new RadosAsyncWritableFile(oname, lo_ctx, false);
      } else {
        *object = new RadosWritableFile(oname, io_ctx_, true);
      }
    }

    return s;
  }

  Status NewSequentialObject(const std::string& oname,
      SequentialFile** object) {

    Status s;

    if (FLAGS_rados_read_check) {
      time_t mtime;
      uint64_t size;
      int r = io_ctx_.stat(oname, &size, &mtime);

      if (r < 0) {
        s = IOError(__func__, r);
      }
    }

    if (s.ok()) {
      *object = new RadosSequentialFile(oname, io_ctx_, true);
    }

    return s;
  }

  Status NewRandomAccessObject(const std::string& oname,
      RandomAccessFile** object) {

    Status s;

    if (FLAGS_rados_read_check) {
      time_t mtime;
      uint64_t size;
      int r = io_ctx_.stat(oname, &size, &mtime);

      if (r < 0) {
        s = IOError(__func__, r);
      }
    }

    if (s.ok()) {
      *object = new RadosRandomAccessFile(oname, io_ctx_, true);
    }

    return s;
  }

  Status GetObjectSize(const std::string& oname, uint64_t* size) {
    Status s;
    time_t mtime;
    int r = io_ctx_.stat(oname, size, &mtime);
    return r == 0 ? s : IOError(__func__, r);
  }
};

} // namespace

static pthread_once_t once = PTHREAD_ONCE_INIT;
static IO* rados_io = NULL;

static const char* conf_path = NULL;
static const char* pool_name = NULL;
static void InitRadosIO() { rados_io = new RadosIO(conf_path, pool_name); }

IO* IO::RadosIO(const char* _conf_path, const char* _pool_name) {
  conf_path = _conf_path;
  pool_name = _pool_name;
  pthread_once(&once, InitRadosIO);
  return rados_io;
}

} // namespace leveldb
