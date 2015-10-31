// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifdef PVFS
extern "C" {
#include <orange.h>
}
#endif

#include <string>
#include <sstream>
#include "io_client.h"

namespace indexfs { namespace mpi {

DEFINE_string(pvfs_mnt, "/m/pvfs", "OrangeFS mount point (OrangeFS only)");

#ifdef PVFS
namespace {

static
Status CreateErrorStatus(const char* msg, Path &path) {
  std::stringstream ss;
  ss << msg << ": " << path << ", " << strerror(errno);
  return Status::IOError(ss.str());
}

static
Status CreateErrStatusWeak(const char* msg, Path &path) {
  if (errno == EEXIST) {
    if (FLAGS_print_ops) {
      fprintf(stderr, "warning: %s already exists\n", path.c_str());
    }
    return Status::OK();
  }
  return CreateErrorStatus(msg, path); // Fall back to normal
}

// An IO client implementation that is backed by OrangeFS, or PVFS version 2.
// OrangeFS exposes two levels of user interface, with one at system level,
// and another user level. This implementation is linked with orangefs user
// level interface, which might be slower than the system level version.
//
class OrangeFSClient: public IOClient {
 public:

  OrangeFSClient()
    : IOClient() {
    buffer_.reserve(256);
    buffer_.append(FLAGS_pvfs_mnt);
    mp_size_ = buffer_.size();
  }

  virtual ~OrangeFSClient()    {                      }

  virtual Status NewFile           (   Path &path   );
  virtual Status MakeDirectory     (   Path &path   );
  virtual Status MakeDirectories   (   Path &path   );
  virtual Status SyncDirectory     (   Path &path   );
  virtual Status ResetMode         (   Path &path   );
  virtual Status GetAttr           (   Path &path   );
  virtual Status ListDirectory     (   Path &path   );
  virtual Status Remove            (   Path &path   );

  virtual Status Rename            (   Path &from    ,
                                       Path &dest   );

  virtual Status Init()        { return Status::OK() ;}
  virtual Status Dispose()     { return Status::OK() ;}

 protected:
  size_t mp_size_; // Length of the mount point path
  std::string buffer_; // A buffered path prefixed with the mount point

 private:
  // No copying allowed
  OrangeFSClient(const OrangeFSClient&);
  OrangeFSClient& operator=(const OrangeFSClient&);
};

Status OrangeFSClient::NewFile
  (Path &path) {
  buffer_.resize(mp_size_);
  buffer_.append(path);
  const char* filename = buffer_.c_str();
  if (FLAGS_print_ops) {
    printf("mknod %s ... ", filename);
  }
  int fno = pvfs_creat(filename, (S_IRWXU | S_IRWXG | S_IRWXO));
  Status s = fno < 0
    ? CreateErrorStatus("cannot create file", buffer_)
    : Status::OK();
  if (fno >= 0) {
    pvfs_close(fno);
  }
  if (FLAGS_print_ops) {
    printf("%s\n", s.ToString().c_str());
  }
  return s;
}

Status OrangeFSClient::MakeDirectory
  (Path &path) {
  buffer_.resize(mp_size_);
  buffer_.append(path);
  const char* dirname = buffer_.c_str();
  if (FLAGS_print_ops) {
    printf("mkdir %s ... ", dirname);
  }
  Status s = pvfs_mkdir(dirname, (S_IRWXU | S_IRWXG | S_IRWXO)) != 0
    ? CreateErrorStatus("cannot create directory", buffer_)
    : Status::OK();
  if (FLAGS_print_ops) {
    printf("%s\n", s.ToString().c_str());
  }
  return s;
}

static
inline size_t NextEntry(Path &path, size_t start) {
  size_t pos = path.find('/', start);
  return pos == std::string::npos ? path.size() : pos;
}

Status OrangeFSClient::MakeDirectories
  (Path &path) {
  buffer_.resize(mp_size_);
  buffer_.append(path);
  if (FLAGS_print_ops) {
    printf("mkdirs %s ... \n", buffer_.c_str());
  }
  std::string parent;
  const char* dirname = NULL;
  parent.reserve(buffer_.size());
  size_t entry = buffer_.rfind('/');
  size_t idx = mp_size_;
  while ((idx = NextEntry(buffer_, idx + 1)) <= entry) {
    parent = buffer_.substr(0, idx);
    dirname = parent.c_str();
    if (pvfs_access(dirname, F_OK) != 0) {
      if (errno == ENOENT) {
        if (FLAGS_print_ops) {
          printf("  mkdir %s ... ", dirname);
        }
        Status s = pvfs_mkdir(dirname, (S_IRWXU | S_IRWXG | S_IRWXO)) != 0
          ? CreateErrStatusWeak("cannot create directory", parent)
          : Status::OK();
        if (FLAGS_print_ops) {
          printf("%s\n", s.ToString().c_str());
        }
        if (!s.ok()) return s;
      } else {
        return CreateErrorStatus("cannot access entry", parent);
      }
    }
  } // end while
  dirname = buffer_.c_str();
  if (FLAGS_print_ops) {
    printf("  mkdir %s ... ", dirname);
  }
  Status s = pvfs_mkdir(dirname, (S_IRWXU | S_IRWXG | S_IRWXO)) != 0
    ? CreateErrStatusWeak("cannot create directory", buffer_)
    : Status::OK();
  if (FLAGS_print_ops) {
    printf("%s\n", s.ToString().c_str());
    printf("mkdirs done\n");
  }
  return s;
}

Status OrangeFSClient::SyncDirectory
  (Path &path) {
  return Status::OK(); // Nothing to do
}

Status OrangeFSClient::ResetMode
  (Path &path) {
  buffer_.resize(mp_size_);
  buffer_.append(path);
  const char* objname = buffer_.c_str();
  if (FLAGS_print_ops) {
    printf("chmod %s ... ", objname);
  }
  Status s = pvfs_chmod(objname, (S_IRWXU | S_IRWXG | S_IRWXO)) != 0
    ? CreateErrorStatus("cannot chmod on entry", buffer_)
    : Status::OK();
  if (FLAGS_print_ops) {
    printf("%s\n", s.ToString().c_str());
  }
  return s;
}

Status OrangeFSClient::Rename
  (Path &from, Path &dest) {
  return Status::Corruption("Op Not Supported", "Rename");
}

Status OrangeFSClient::Remove
  (Path &path) {
  buffer_.resize(mp_size_);
  buffer_.append(path);
  const char* objname = buffer_.c_str();
  if (FLAGS_print_ops) {
    printf("remove %s ... ", objname);
  }
  Status s = pvfs_unlink(objname) != 0
    ? CreateErrorStatus("cannot remove entry", buffer_)
    : Status::OK();
  if (FLAGS_print_ops) {
    printf("%s\n", s.ToString().c_str());
  }
  return s;
}

Status OrangeFSClient::GetAttr
  (Path &path) {
  buffer_.resize(mp_size_);
  buffer_.append(path);
  const char* objname = buffer_.c_str();
  if (FLAGS_print_ops) {
    printf("getattr %s ... ", objname);
  }
  struct stat stats;
  Status s = pvfs_stat(objname, &stats) != 0
    ? CreateErrorStatus("cannot getattr on entry", buffer_)
    : Status::OK();
  if (FLAGS_print_ops) {
    printf("%s\n", s.ToString().c_str());
  }
  return s;
}

Status OrangeFSClient::ListDirectory
  (Path &path) {
  return Status::Corruption("Op Not Supported", "ListDirectory");
}

} /* anonymous namepsace */
#endif

IOClient* IOClient::NewOrangeFSClient() {
#ifdef PVFS
  return new OrangeFSClient();
#else
  return NULL;
#endif
}

} /* namespace mpi */ } /* namespace indexfs */
