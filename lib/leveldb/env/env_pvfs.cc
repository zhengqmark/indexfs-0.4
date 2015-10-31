// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <map>
#include <deque>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#if defined(LEVELDB_PLATFORM_ANDROID)
#include <sys/stat.h>
#endif
#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/posix_logger.h"
#include "util/mutexlock.h"
#if defined(OS_LINUX)
#if defined(PVFS)
extern "C" {
#include "orange.h"
}

// #define LEVELDB_PVFS_VERBOSE // print each pvfs call
// #define PVFS_ENV_DEBUG // no concurrent read/write PVFS IO

namespace leveldb {

static int kLogBufferSize = 4 << 10;

namespace {
#if defined(PVFS_ENV_DEBUG)
static port::Mutex env_mu_;
#endif

#ifdef LEVELDB_PVFS_VERBOSE
static FILE* V_ = NULL;
#endif

static const std::string pvfs_prefix = "/m/pvfs";

static Status IOError(const std::string& context, int err_number) {
  return Status::IOError(context, strerror(err_number));
}

#define PVFS_VERBOSITY 1
void pvfsDebugLog(short verbosity, const char* format, ... ) {
  if(verbosity <= PVFS_VERBOSITY) {
    va_list args;
    va_start( args, format );
    vfprintf( stdout, format, args );
    va_end( args );
  }
}

bool stringEndsWith(const std::string &src, const std::string &suffix) {
  if (src.length() >= suffix.length()) {
    return src.compare(src.length()-suffix.length(),
                       suffix.length(),
                       suffix) == 0;
  } else {
    return false;
  }
}

bool stringStartsWith(const std::string &src, const std::string &prefix) {
  if (src.length() >= prefix.length()) {
    return src.compare(0,
                       prefix.length(),
                       prefix) == 0;
  } else {
    return false;
  }
}

bool lastComponentStartsWith(const std::string &src, const std::string &prefix) {
  if (src.length() >= prefix.length()) {
    size_t pos = src.find_last_of('/') + 1;
    return src.compare(pos,
                       prefix.length(),
                       prefix) == 0;
  } else {
    return false;
  }
}

static bool isRemote(const std::string& fname) {
  return fname.compare(0, pvfs_prefix.length(), pvfs_prefix) == 0;
}

std::string getPath(const std::string &fname) {
  return fname;
}

class PosixSequentialFile: public SequentialFile {
 private:
  std::string filename_;
  FILE* file_;

 public:
  PosixSequentialFile(const std::string& fname, FILE* f)
      : filename_(fname), file_(f) { }
  virtual ~PosixSequentialFile() { fclose(file_); }

  virtual Status Read(size_t n, Slice* result, char* scratch) {
    Status s;
    size_t r = fread_unlocked(scratch, 1, n, file_);
    *result = Slice(scratch, r);
    if (r < n) {
      if (feof(file_)) {
        // We leave status as ok if we hit the end of the file
      } else {
        // A partial read with an error: return a non-ok status
        s = IOError(filename_, errno);
      }
    }
    return s;
  }

  virtual Status Skip(uint64_t n) {
    if (fseek(file_, n, SEEK_CUR)) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }
};

class PosixWritableFile : public WritableFile {
 private:
  std::string filename_;
  int fd_;
  uint64_t file_offset_;  // Offset of base_ in file

 public:
  PosixWritableFile(const std::string& fname, int fd)
      : filename_(fname),
        fd_(fd),
        file_offset_(0) {
  }


  ~PosixWritableFile() {
    if (fd_ >= 0) {
      PosixWritableFile::Close();
    }
  }

  virtual Status Append(const Slice& data) {
    const char* src = data.data();
    size_t data_size = data.size();
    if (pwrite(fd_, src, data_size, file_offset_) < 0) {
      return IOError(filename_, errno);
    }
    file_offset_ += data_size;

    return Status::OK();
  }

  virtual Status Close() {
    Status s;
    if (close(fd_) < 0) {
      if (s.ok()) {
        s = IOError(filename_, errno);
      }
    }
    fd_ = -1;
    return s;
  }

  virtual Status Flush() {
    return Status::OK();
  }

  virtual Status Sync() {
    Status s;

    if (fsync(fd_) < 0) {
        s = IOError(filename_, errno);
    }

    return s;
  }
};


// pread() based random-access
class PosixRandomAccessFile: public RandomAccessFile {
 private:
  std::string filename_;
  int fd_;

 public:
  PosixRandomAccessFile(const std::string& fname, int fd)
      : filename_(fname), fd_(fd) { }

  virtual ~PosixRandomAccessFile() { close(fd_); }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    Status s;
    ssize_t r = pread(fd_, scratch, n, static_cast<off_t>(offset));
    *result = Slice(scratch, (r < 0) ? 0 : r);
    if (r < 0) {
      // An error: return a non-ok status
      s = IOError(filename_, errno);
    }
    return s;
  }
};

class PVFSSequentialFile: public SequentialFile {
 private:
  std::string filename_;
  int file_;

 public:
  PVFSSequentialFile(const std::string& fname, int f)
      : filename_(fname), file_(f) { }

  virtual ~PVFSSequentialFile() {
#ifdef LEVELDB_PVFS_VERBOSE
    fprintf(V_, "Close\t%s\n", filename_.c_str());
    fflush(V_);
#endif
    pvfs_close(file_);
  }

  virtual Status Read(size_t n, Slice* result, char* scratch) {
    Status s;
#if defined(PVFS_ENV_DEBUG)
    MutexLock ml(&env_mu_);
#endif
#ifdef LEVELDB_PVFS_VERBOSE
    fprintf(V_, "Read\t%s\t%lu\n",
      filename_.c_str(), n);
    fflush(V_);
#endif
    size_t r = pvfs_read(file_, scratch, n);
    *result = Slice(scratch, r);
    if (r < n) {
      if (r != -1) {
        // We leave status as ok if we hit the end of the file
      } else {
        // A partial read with an error: return a non-ok status
        s = IOError(filename_, errno);
      }
    }
    return s;
  }

  virtual Status Skip(uint64_t n) {
    Status s;
#ifdef LEVELDB_PVFS_VERBOSE
    fprintf(V_, "Lseek\t%s\t%lu\n",
      filename_.c_str(), n);
    fflush(V_);
#endif
    if (pvfs_lseek(file_, n, SEEK_CUR) < 0) {
      s = IOError(filename_, errno);
    }
    return s;
  }
};

// pread() based random-access
class PVFSRandomAccessFile: public RandomAccessFile {
 private:
  std::string filename_;
  int fd_;

 public:
  PVFSRandomAccessFile(const std::string& fname, int fd)
      : filename_(fname), fd_(fd) { }

  virtual ~PVFSRandomAccessFile() {
#ifdef LEVELDB_PVFS_VERBOSE
    fprintf(V_, "Close\t%s\n", filename_.c_str());
    fflush(V_);
#endif
    pvfs_close(fd_);
  }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    Status s;
#ifdef LEVELDB_PVFS_VERBOSE
    fprintf(V_, "Pread\t%s\t%lu\t%lu\n",
      filename_.c_str(), offset, n);
    fflush(V_);
#endif
    ssize_t r = pvfs_pread(fd_, scratch, n, static_cast<off_t>(offset));
    *result = Slice(scratch, (r < 0) ? 0 : r);
    if (r < 0) {
      // An error: return a non-ok status
      s = IOError(filename_, errno);
    }
    return s;
  }
};

class PVFSWritableFile : public WritableFile {
 private:
  std::string filename_;
  int fd_;
  char* buf_;
  size_t buf_size_;

 public:
  PVFSWritableFile(const std::string& fname, int fd)
      : filename_(fname),
        fd_(fd),
        buf_(new char[kLogBufferSize*2]),
        buf_size_(0) {
  }

  ~PVFSWritableFile() {
    if (fd_ >= 0) {
      PVFSWritableFile::Close();
    }
    if (buf_ != NULL) {
      delete [] buf_;
    }
  }

  virtual Status Append(const Slice& data) {
    const char* src = data.data();
    size_t data_size = data.size();
    Status s;
    if (data_size + buf_size_ > kLogBufferSize) {
      s = MyFlush();
      if (!s.ok()) return s;
      if (data_size >= kLogBufferSize) {
#if defined(PVFS_ENV_DEBUG)
    MutexLock ml(&env_mu_);
#endif

#ifdef LEVELDB_PVFS_VERBOSE
        fprintf(V_, "Write\t%s\t%lu\n",
          filename_.c_str(), data_size);
        fflush(V_);
#endif
        if (pvfs_write(fd_, src, data_size) < 0) {
          return IOError(filename_, errno);
        }
        return s;
      }
    }
    memcpy(buf_+buf_size_, src, data_size);
    buf_size_ += data_size;
    if (buf_size_ == kLogBufferSize)
      s = MyFlush();
    return s;
  }

  virtual Status Close() {
    Status s;
    s = MyFlush();
    if (s.ok()) {
#ifdef LEVELDB_PVFS_VERBOSE
      fprintf(V_, "Close\t%s\n", filename_.c_str());
      fflush(V_);
#endif
      if (pvfs_close(fd_) < 0) {
        s = IOError(filename_, errno);
      }
    }
    fd_ = -1;
    return s;
  }

  Status MyFlush() {
    if (buf_size_ > 0) {
#if defined(PVFS_ENV_DEBUG)
    MutexLock ml(&env_mu_);
#endif

#ifdef LEVELDB_PVFS_VERBOSE
      fprintf(V_, "Write\t%s\t%lu\n",
        filename_.c_str(), buf_size_);
      fflush(V_);
#endif
      if (pvfs_write(fd_, buf_, buf_size_) < 0) {
        return IOError(filename_, errno);
      }
      buf_size_ = 0;
    }
    return Status::OK();
  }

  virtual Status Flush() {
    return Status::OK();
  }

  virtual Status Sync() {
    Status s;
    s = MyFlush();
    if (s.ok()) {
#ifdef LEVELDB_PVFS_VERBOSE
      fprintf(V_, "Fsync\t%s\n", filename_.c_str());
      fflush(V_);
#endif
      if (pvfs_fsync(fd_) < 0) {
        s = IOError(filename_, errno);
      }
    }
    return s;
  }
};

static int LockOrUnlock(int fd, bool lock) {
  return 0;
}

class PosixFileLock : public FileLock {
 public:
  int fd_;
};

class PVFSEnv : public Env {

public:
  PVFSEnv();
  virtual ~PVFSEnv() {
    fprintf(stderr, "Destroying PVFSEnv::Default()\n");
    exit(1);
  }

  //Check if the file should be accessed from PVFS
  bool onPVFS(const std::string &fname) {
    bool on_pvfs = false;
    const std::string ext_dat = ".dat";
    const std::string ext_sst = ".sst";
    const std::string ext_log = ".log";
    const std::string prefix_des = "MANIFEST";

    on_pvfs = stringEndsWith(fname, ext_sst) ||
              stringEndsWith(fname, ext_dat) ||
              stringEndsWith(fname, ext_log);
    return on_pvfs;
  }

  virtual Status NewSequentialFile(const std::string& fname,
                                   SequentialFile** result) {
    if(onPVFS(fname)) {
      std::string filename = getPath(fname);
#ifdef LEVELDB_PVFS_VERBOSE
      fprintf(V_, "NewSequential\t%s\n", filename.c_str());
#endif
      int f = pvfs_open(filename.c_str(), O_RDONLY);
      if (f < 0) {
        *result = NULL;
        return IOError(fname, errno);
      } else {
        *result = new PVFSSequentialFile(fname, f);
        return Status::OK();
      }
    } else {
      FILE* f = fopen(fname.c_str(), "r");
      if (f == NULL) {
        *result = NULL;
        return IOError(fname, errno);
      } else {
        *result = new PosixSequentialFile(fname, f);
        return Status::OK();
      }
    }
  }

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     RandomAccessFile** result) {
    *result = NULL;
    Status s;

    if(onPVFS(fname)) {
#ifdef LEVELDB_PVFS_VERBOSE
      fprintf(V_, "NewRandom\t%s\n", fname.c_str());
#endif
      int new_file = pvfs_open(fname.c_str(), O_RDONLY);
      if (new_file < 0) {
        s = IOError(fname, errno);
      } /*else if (sizeof(void*) >= 8) {
        uint64_t size;
        s = GetFileSize(fname, &size);
        if (s.ok()) {
          void* base = mmap(NULL, size, PROT_READ, MAP_SHARED, new_file, 0);
          if (base != MAP_FAILED) {
            *result = new PVFSMmapReadableFile(fname, base, size);
          } else {
            s = IOError(fname, errno);
          }
        }
        close(new_file);
     }*/ else {
        *result = new PVFSRandomAccessFile(fname, new_file);
      }
    } else {
      int fd = open(fname.c_str(), O_RDONLY);
      if (fd < 0) {
        s = IOError(fname, errno);
      } else {
        *result = new PosixRandomAccessFile(fname, fd);
      }
    }
    return s;
  }

  virtual Status NewWritableFile(const std::string& fname,
                                 WritableFile** result) {
    Status s;

    if(onPVFS(fname)) {
      std::string filename = getPath(fname);
#ifdef LEVELDB_PVFS_VERBOSE
          fprintf(V_, "NewWritable\t%s\n", filename.c_str());
#endif
      int att = 64;
      while (att-- >= 0) {
        int fd = pvfs_open(filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) {
          *result = NULL;
          usleep(1000);
        } else {
          *result = new PVFSWritableFile(fname, fd);
          return Status::OK();
        }
      }
      s = IOError(fname, errno);
    } else {
      const int fd = open(fname.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
      if (fd < 0) {
        *result = NULL;
        s = IOError(fname, errno);
      } else {
        *result = new PosixWritableFile(fname, fd);
      }
    }
    return s;
  }

  virtual bool FileExists(const std::string& fname) {
    if(onPVFS(fname)) {
      std::string filename = getPath(fname);
      return (pvfs_access(filename.c_str(), F_OK) == 0);
    }
    return access(fname.c_str(), F_OK) == 0;
  }

  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) {
    result->clear();

    std::string dirname = getPath(dir);
    int dird = pvfs_open(dirname.c_str(), O_RDONLY);
    if (dird >= 0) {
      struct dirent entry;
      while (pvfs_readdir(dird, &entry, 1) > 0) {
        result->push_back(entry.d_name);
      }
      pvfs_close(dird);
    }

    DIR* d = opendir(dir.c_str());
    if (d != NULL) {
      struct dirent* entry;
      while ((entry = readdir(d)) != NULL) {
        result->push_back(entry->d_name);
      }
      closedir(d);
    }
    return Status::OK();
  }

  virtual Status DeleteFile(const std::string& fname) {
    Status result;
    if(onPVFS(fname)) {
      std::string filename = getPath(fname);
      if (pvfs_unlink(filename.c_str()) != 0) {
        result = IOError(fname, errno);
      }
    } else {
      if (unlink(fname.c_str()) != 0) {
        result = IOError(fname, errno);
      }
    }
    return result;
  };

  virtual Status CreateDir(const std::string& name) {
    Status result;
    std::string filename = getPath(name);
    if (pvfs_mkdir(filename.c_str(), 0755) != 0) {
      result = IOError(filename, errno);
    }
    if (mkdir(name.c_str(), 0755) != 0) {
      result = IOError(name, errno);
    }
    return result;
  };

  virtual Status DeleteDir(const std::string& name) {
    Status result;
    std::string filename = getPath(name);
    if (pvfs_rmdir(filename.c_str()) != 0) {
      result = IOError(filename, errno);
    }
    if (rmdir(name.c_str()) != 0) {
      result = IOError(name, errno);
    }
    return result;
  };

  virtual Status GetFileSize(const std::string& fname, uint64_t* size) {
    Status s;
    if(onPVFS(fname)) {
      struct stat sbuf;
      if (pvfs_stat(fname.c_str(), &sbuf) != 0) {
        *size = sbuf.st_size;
      } else {
        *size = 0;
        s = IOError(fname, errno);
      }
    } else {
      struct stat sbuf;
      if (stat(fname.c_str(), &sbuf) != 0) {
        *size = 0;
        s = IOError(fname, errno);
      } else {
        *size = sbuf.st_size;
      }
    }
    return s;
  }

  virtual Status CopyFile(const std::string& src, const std::string& target) {
    if(onPVFS(src) || onPVFS(target)) {
      fprintf(stdout, "PVFS: Copy File src:%s target:%s not implemented\n",
              src.c_str(), target.c_str());
      exit(0);
    }

    Status result;
    int r_fd, w_fd;
    if ((r_fd = open(src.c_str(), O_RDONLY)) < 0) {
      result = IOError(src, errno);
      return result;
    }
    if ((w_fd = open(target.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644)) < 0) {
      result = IOError(target, errno);
      return result;
    }

#if (defined(OS_LINUX))
    int p[2];
    pipe(p);
    while(splice(p[0], 0, w_fd, 0, splice(r_fd, 0, p[1], 0, 4096, 0), 0) > 0);
#else
    char buf[4096];
    ssize_t len;
    while ((len = read(r_fd, buf, 4096)) > 0) {
      write(w_fd, buf, len);
    }
#endif

    close(r_fd);
    close(w_fd);

    return result;
  }

  virtual Status SymlinkFile(const std::string& src, const std::string& target)
  {
    if (!onPVFS(src) && !onPVFS(target)) {
      Status s;
      if (symlink(src.c_str(), target.c_str()) != 0) {
        s = IOError(src, errno);
      }
      return s;
    } else {
      return Status::IOError(src, "Cannot symlink across file systems");
    }
  }

  virtual Status RenameFile(const std::string& src, const std::string& target) {
    Status s;
    if(onPVFS(src) && onPVFS(target)) {
      if (pvfs_rename(src.c_str(), target.c_str()) < 0) {
        s = IOError(src, errno);
      }
    } else if (!onPVFS(src) && !onPVFS(target)) {
      if (rename(src.c_str(), target.c_str()) != 0) {
        s = IOError(src, errno);
      }
    } else {
      s = Status::IOError(src, "Cannot rename across file systems");
    }
    return s;
  }

  virtual Status LinkFile(const std::string& src, const std::string& target) {
    if(onPVFS(src) || onPVFS(target)) {
      fprintf(stdout, "PVFS: Link File src:%s target:%s. Not supported\n",
              src.c_str(), target.c_str());
      exit(0);
    }
    Status result;
    if (link(src.c_str(), target.c_str()) != 0) {
      result = IOError(src, errno);
    }
    return result;
  }

  virtual Status LockFile(const std::string& fname, FileLock** lock) {
    if(onPVFS(fname)) {
      fprintf(stdout, "PVFS: Lock File %s. Not supported\n", fname.c_str());
      exit(0);
    }
    *lock = NULL;
    Status result;
    int fd = open(fname.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
      result = IOError(fname, errno);
    } else if (LockOrUnlock(fd, true) == -1) {
      result = IOError("lock " + fname, errno);
      close(fd);
    } else {
      PosixFileLock* my_lock = new PosixFileLock;
      my_lock->fd_ = fd;
      *lock = my_lock;
    }
    return result;
  }

  virtual Status UnlockFile(FileLock* lock) {
    PosixFileLock* my_lock = reinterpret_cast<PosixFileLock*>(lock);
    Status result;
    if (LockOrUnlock(my_lock->fd_, false) == -1) {
      result = IOError("unlock", errno);
    }
    close(my_lock->fd_);
    delete my_lock;
    return result;
  }

  virtual void Schedule(void (*function)(void*), void* arg);

  virtual void StartThread(void (*function)(void* arg), void* arg);

  virtual Status GetTestDirectory(std::string* result) {
    const char* env = getenv("TEST_TMPDIR");
    if (env && env[0] != '\0') {
      *result = env;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "/tmp/leveldbtest-%d", int(geteuid()));
      *result = buf;
    }
    // Directory may already exist
    CreateDir(*result);
    return Status::OK();
  }

  static uint64_t gettid() {
    pthread_t tid = pthread_self();
    uint64_t thread_id = 0;
    memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
    return thread_id;
  }

  virtual Status NewLogger(const std::string& fname, Logger** result) {
    FILE* f = fopen(fname.c_str(), "w");
    if (f == NULL) {
      *result = NULL;
      return IOError(fname, errno);
    } else {
      *result = new PosixLogger(f, &PVFSEnv::gettid);
      return Status::OK();
    }
  }

  virtual uint64_t NowMicros() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
  }

  virtual void SleepForMicroseconds(int micros) {
    struct timespec req, rem;
    req.tv_sec = micros / 1000000;
    req.tv_nsec = (micros % 1000000) * 1000;
    nanosleep(&req, &rem);
    //usleep(micros);
  }

 private:
  void PthreadCall(const char* label, int result) {
    if (result != 0) {
      fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
      exit(1);
    }
  }

  // BGThread() is the body of the background thread
  void BGThread();
  static void* BGThreadWrapper(void* arg) {
    reinterpret_cast<PVFSEnv*>(arg)->BGThread();
    return NULL;
  }

  size_t page_size_;
  pthread_mutex_t mu_;
  pthread_cond_t bgsignal_;
  pthread_t bgthread_;
  bool started_bgthread_;

  // Entry per Schedule() call
  struct BGItem { void* arg; void (*function)(void*); };
  typedef std::deque<BGItem> BGQueue;
  BGQueue queue_;
};

PVFSEnv::PVFSEnv() : page_size_(getpagesize()),
                     started_bgthread_(false) {
  PthreadCall("mutex_init", pthread_mutex_init(&mu_, NULL));
  PthreadCall("cvar_init", pthread_cond_init(&bgsignal_, NULL));
}

void PVFSEnv::Schedule(void (*function)(void*), void* arg) {
  PthreadCall("lock", pthread_mutex_lock(&mu_));

  // Start background thread if necessary
  if (!started_bgthread_) {
    started_bgthread_ = true;
    PthreadCall(
        "create thread",
        pthread_create(&bgthread_, NULL,  &PVFSEnv::BGThreadWrapper, this));
  }

  // If the queue is currently empty, the background thread may currently be
  // waiting.
  if (queue_.empty()) {
    PthreadCall("signal", pthread_cond_signal(&bgsignal_));
  }

  // Add to priority queue
  queue_.push_back(BGItem());
  queue_.back().function = function;
  queue_.back().arg = arg;

  PthreadCall("unlock", pthread_mutex_unlock(&mu_));
}

void PVFSEnv::BGThread() {
  while (true) {
    // Wait until there is an item that is ready to run
    PthreadCall("lock", pthread_mutex_lock(&mu_));
    while (queue_.empty()) {
      PthreadCall("wait", pthread_cond_wait(&bgsignal_, &mu_));
    }

    void (*function)(void*) = queue_.front().function;
    void* arg = queue_.front().arg;
    queue_.pop_front();

    PthreadCall("unlock", pthread_mutex_unlock(&mu_));
    (*function)(arg);
  }
}

namespace {
struct StartThreadState {
  void (*user_function)(void*);
  void* arg;
};
}
static void* StartThreadWrapper(void* arg) {
  StartThreadState* state = reinterpret_cast<StartThreadState*>(arg);
  state->user_function(state->arg);
  delete state;
  return NULL;
}

void PVFSEnv::StartThread(void (*function)(void* arg), void* arg) {
  pthread_t t;
  StartThreadState* state = new StartThreadState;
  state->user_function = function;
  state->arg = arg;
  PthreadCall("start thread",
              pthread_create(&t, NULL,  &StartThreadWrapper, state));
}

}  // namespace

static pthread_once_t once = PTHREAD_ONCE_INIT;

static Env* pvfs_env;
static void InitPVFSEnv() {
  pvfs_env = new PVFSEnv();
#ifdef LEVELDB_PVFS_VERBOSE
  char name[128];
  snprintf(name, 128, "/tmp/leveldb_pvfs.log.%d", getpid());
  V_ = fopen(name, "w");
  if (V_ == NULL) {
    V_ = stderr;
  }
#endif
}

Env* Env::PVFSEnv(int log_buffer_size) {
  kLogBufferSize = log_buffer_size;
  pthread_once(&once, InitPVFSEnv);
  return pvfs_env;
}

}  // namespace leveldb

#endif // if defined PVFS
#endif // if defined OS_LINUX
