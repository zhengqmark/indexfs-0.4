// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <map>
#include <stdlib.h>
#include <unistd.h>

#include "io_task.h"
#include <gflags/gflags.h>

namespace indexfs { namespace mpi {

DEFINE_int32(dirs,
    0, "Total number of directories to create");
DEFINE_int32(files,
    0, "Total number of files to create");
DEFINE_bool(share_dirs,
    false, "Set to enable clients to create files in each other's directories");
DEFINE_bool(local_stat,
    true, "Set to false to enable random stats over all files within the namespace");
DEFINE_string(prefix,
    "prefix", "The prefix for each directory entry");
DEFINE_bool(flush_buffer,
     true, "Always flush write buffer after bulk insertion");

namespace {

class TreeTest: public IOTask {

  static inline
  void MakeDirectory(IOClient* IO, IOListener* L, int dno) {
    Status s = IO->MakeDirectory(dno, FLAGS_prefix);
    if (!s.ok()) {
      if (L != NULL) {
        L->IOFailed("mkdir");
      }
      throw IOError(dno, "mkdir", s.ToString());
    }
    if (L != NULL) {
      L->IOPerformed("mkdir");
    }
  }

  static inline
  void CreateFile(IOClient* IO, IOListener* L, int dno, int fno) {
    Status s = IO->NewFile(dno, fno, FLAGS_prefix);
    if (!s.ok()) {
      if (L != NULL) {
        L->IOFailed("mknod");
      }
      throw IOError(dno, fno, "mknod", s.ToString());
    }
    if (L != NULL) {
      L->IOPerformed("mknod");
    }
  }

  static inline
  void GetAttr(IOClient* IO, IOListener* L, int dno, int fno) {
    Status s = IO->GetAttr(dno, fno, FLAGS_prefix);
    if (!s.ok()) {
      if (L != NULL) {
        L->IOFailed("getattr");
      }
      throw IOError(dno, fno, "getattr", s.ToString());
    }
    if (L != NULL) {
      L->IOPerformed("getattr");
    }
  }

  static inline
  void FlushBuffer(IOClient* IO, IOListener* L) {
    Status s = IO->FlushWriteBuffer();
    if (!s.ok()) {
      if (L != NULL) {
        L->IOFailed("flush");
      }
      throw IOError("flush", s.ToString());
    }
    if (L != NULL) {
      L->IOPerformed("flush");
    }
  }

  int PrintSettings() {
    return printf("Test Settings:\n"
      "  total dirs -> %d\n"
      "  total files -> %d\n"
      "  total processes -> %d\n"
      "  share_dirs -> %s\n"
      "  local_stat -> %s\n"
      "  batch_creates -> %s\n"
      "  bulk_insert -> %s\n"
      "  backend_fs -> %s\n"
      "  run prepare phase -> %s\n"
      "  run main phase -> %s\n"
      "  run clean phase -> %s\n"
      "  ignore_errors -> %s\n"
      "  log_file -> %s\n"
      "  run_id -> %s\n"
      "  run_prefix -> %s\n",
      num_dirs_,
      num_files_,
      comm_sz_,
      GetBoolString(FLAGS_share_dirs),
      GetBoolString(FLAGS_local_stat),
      GetBoolString(FLAGS_batch_creates),
      GetBoolString(FLAGS_bulk_insert),
      FLAGS_fs.c_str(),
      GetBoolString(FLAGS_enable_prepare),
      GetBoolString(FLAGS_enable_main),
      GetBoolString(FLAGS_enable_clean),
      GetBoolString(FLAGS_ignore_errors),
      FLAGS_log_file.c_str(),
      FLAGS_run_id.c_str(),
      FLAGS_prefix.c_str());
  }

# ifndef NDEBUG
  // file identifier -> parent directory
  std::map<int, int> file2dir_;
  // file identifier -> number of getattr requests
  std::map<int, int> file2nstats_;
# endif

  int num_dirs_; // Total number of directories to create
  int num_files_; // Total number of files to create

 public:

  TreeTest(int my_rank, int comm_sz)
    : IOTask(my_rank, comm_sz)
    , num_dirs_(FLAGS_dirs), num_files_(FLAGS_files) {
  }

  virtual void Prepare() {
    Status s = IO_->Init();
    if (!s.ok()) {
      throw IOError("init", s.ToString());
    }
    if (FLAGS_enable_prepare) {
      IOMeasurements::EnableMonitoring(IO_, false);
      if (FLAGS_share_dirs && FLAGS_bulk_insert) {
        for (int i = 0; i < num_dirs_; i++) {
          try {
            MakeDirectory(IO_, listener_, i);
          } catch (IOError &err) {
            if (!FLAGS_ignore_errors)
              throw err;
          }
        }
      } else {
        for (int i = my_rank_; i < num_dirs_; i += comm_sz_) {
          try {
            MakeDirectory(IO_, listener_, i);
          } catch (IOError &err) {
            if (!FLAGS_ignore_errors)
              throw err;
          }
        }
      }
      IOMeasurements::EnableMonitoring(IO_, true);
    }
  }

  virtual void Run() {
    if (FLAGS_enable_main) {
      IOMeasurements::Reset(IO_);
      for (int i = my_rank_; i < num_files_; i += comm_sz_) {
        int d = rand() % num_dirs_;
        if (!FLAGS_share_dirs) {
          d = my_rank_ + d - (d % comm_sz_);
          if (d >= num_dirs_)
            d = my_rank_;
        }
        try {
          CreateFile(IO_, listener_, d, i);
#         ifndef NDEBUG
          file2dir_.insert(std::make_pair(i, d));
          file2nstats_.insert(std::make_pair(i, 0));
#         endif
        } catch (IOError &err) {
          if (!FLAGS_ignore_errors)
            throw err;
        }
      }
      fprintf(LOG_, "== Main Phase Performance Data ==\n\n");
      IOMeasurements::PrintMeasurements(IO_, LOG_);
    }
  }

  virtual void PostRun() {
    if (FLAGS_enable_main) {
      if (FLAGS_flush_buffer) {
        try {
          FlushBuffer(IO_, listener_);
        } catch (IOError &err) {
          if (!FLAGS_ignore_errors)
            throw err;
        }
      }
    }
  }

  virtual void Clean() {
    if (FLAGS_enable_clean) {
      if (num_dirs_ == 1 && FLAGS_share_dirs) {
        IOMeasurements::Reset(IO_);
        for (int i = my_rank_; i < num_files_; i += comm_sz_) {
          int f;
          if (FLAGS_local_stat) {
            int g = num_files_ / comm_sz_;
            int r = num_files_ % comm_sz_;
            if (r != 0) {
              if (my_rank_ < r) {
                g++;
              }
            }
            // each client random stat's their own files
            f = (rand() % g) * comm_sz_ + my_rank_;
          } else {
            f = rand() % num_files_;
          }
          try {
#           ifndef NDEBUG
            if (file2dir_.count(f) != 1 || file2dir_[f] != 0
                    || file2nstats_.count(f) != 1) {
              abort();
            }
            file2nstats_[f]++;
#           endif
            GetAttr(IO_, listener_, 0, f);
          } catch (IOError &err) {
            if (!FLAGS_ignore_errors)
              throw err;
          }
        }
        fprintf(LOG_, "== Clean Phase Performance Data ==\n\n");
        IOMeasurements::PrintMeasurements(IO_, LOG_);
      }
#     ifndef NDEBUG
      int num_0 = 0;
      int num_1 = 0;
      int num_2 = 0;
      int num_3 = 0;
      int num_n = 0;
      std::map<int, int>::iterator it;
      for (it = file2nstats_.begin(); it != file2nstats_.end(); ++it) {
        int num = it->second;
        if (num == 0) {
          num_0++;
        } else if (num == 1) {
          num_1++;
        } else if (num == 2) {
          num_2++;
        } else if (num == 3) {
          num_3++;
        } else {
          num_n++;
        }
      }
      fprintf(LOG_, "****** DEBUG ******\n");
      fprintf(LOG_, "Number of file stat'd %d times: %d\n", 0, num_0);
      fprintf(LOG_, "Number of file stat'd %d times: %d\n", 1, num_1);
      fprintf(LOG_, "Number of file stat'd %d times: %d\n", 2, num_2);
      fprintf(LOG_, "Number of file stat'd %d times: %d\n", 3, num_3);
      fprintf(LOG_, "Number of file stat'd more than %d times: %d\n", 4, num_n);
#     endif
    }
  }

  virtual bool CheckPrecondition() {
    if (IO_ == NULL || LOG_ == NULL) {
      return false; // err has already been printed elsewhere
    }
    if (num_dirs_ <= 0) {
      my_rank_ == 0 ? fprintf(stderr, "%s! (%s)\n",
        "fail to specify the total number of directories to create",
        "use --dirs=xx to specify") : 0;
      return false;
    }
    if (num_files_ <= 0) {
      my_rank_ == 0 ? fprintf(stderr, "%s! (%s)\n",
        "fail to specify the total number of files to create",
        "use --files=xx to specify") : 0;
      return false;
    }
    if (!FLAGS_share_dirs) {
      if (num_dirs_ < comm_sz_) {
        my_rank_ == 0 ? fprintf(stderr, "%s and %s! (%s)\n",
          "number of directories is less than the number of processes",
          "share_dirs is not enabled",
          "use --share_dirs to enable") : 0;
        return false;
      }
    }
    if (num_files_ < num_dirs_) {
      my_rank_ == 0 ? fprintf(stderr, "warning: %s\n",
        "number of files to create is less than the number of directories to create") : 0;
    }
    my_rank_ == 0 ? PrintSettings() : 0;
    // All will check, yet only the zeroth process will do the printing
    return true;
  }
};

} /* anonymous namespace */

IOTask* IOTaskFactory::GetTreeTestTask(int my_rank, int comm_sz) {
  return new TreeTest(my_rank, comm_sz);
}

} /* namespace mpi */ } /* namespace indexfs */
