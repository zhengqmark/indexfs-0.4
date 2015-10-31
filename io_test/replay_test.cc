// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <fstream>
#include <sstream>

#include <map>
#include <vector>
#include "io_task.h"
#include "gzstream.h"
#include <gflags/gflags.h>

namespace indexfs { namespace mpi {

DEFINE_string(root_dir,
    "/root", "The root dir for this particular test run");
DEFINE_string(init_log,
    "", "A list of directories to make to bootstrap an exp");
DEFINE_string(prepare_log,
    "", "A list of files to create to setup a certain test env");
DEFINE_string(verify_log,
    "", "A list of files to check to verify a certain test env");
DEFINE_string(replay_log,
    "", "Trace for ops constituting the main benchmark");
DEFINE_bool(check,
    false, "Check pre-conditions before performing each IO operation");
DEFINE_bool(read_only,
    false, "Do not update metadata -- issue read operation instead");

// For merging multiple logs into one replay list
//
DEFINE_int32(num_parts, 0, "Total number of partitions");
DEFINE_int32(win_size, 200, "Windows size");

namespace {

// Adapting common HDFS IO operations to our own IO client
// interface.
//
struct IOAdaptor {

  IOClient* IO_;

  IOAdaptor(IOClient* IO)
    : IO_(IO) {
  }

  // To make things easy, all OP here take 2 path parameters
  // no matter what
  //
  Status IO_open            (Path &path, Path &path2);
  Status IO_mkdirs          (Path &path, Path &path2);
  Status IO_mkdir           (Path &path, Path &path2);
  Status IO_create          (Path &path, Path &path2);
  Status IO_delete          (Path &path, Path &path2);
  Status IO_rename          (Path &path, Path &path2);
  Status IO_listStatus      (Path &path, Path &path2);
  Status IO_setOwner        (Path &path, Path &path2);
  Status IO_setPermission   (Path &path, Path &path2);
  Status IO_setReplication  (Path &path, Path &path2);

};

class Op {
 public:
  virtual ~Op() {}
  virtual void Exec(IOListener* L) = 0;

 protected:
  Op(IOClient* IO, const std::string &p, const std::string &p2)
    : IO_(IO), path_(p), path2_(p2) {
  }
  IOClient* IO_;
  std::string path_;
  std::string path2_;

 private:
  // No copying allowed
  Op(const Op&);
  Op& operator=(const Op&);
};

typedef Op*(*OpFactory)
  (IOClient* IO, const std::string &p, const std::string &p2);
typedef std::map<std::string, OpFactory> OpTable;

static OpTable ops;

static
bool RegisterOp(const char* name, OpFactory factory) {
  return ops.insert(std::make_pair(name, factory)).second;
}

static
OpFactory GetOpFactory(const std::string &op_name) {
  OpTable::iterator it = ops.find(op_name);
  if (it == ops.end())
    return NULL;
  return it->second;
}

#if 0
/////////////////////////////////////////////////////////////////////
static
size_t NextEntry(Path &path, size_t start) {
  size_t pos = path.find('/', start);
  return pos == std::string::npos ? path.size() : pos;
}

static
Status CreateParents(IOClient* IO, Path &path) {
  std::string buffer;
  buffer.reserve(path.size());
  size_t entry = path.rfind('/');

  size_t parent = 0;
  while ((parent = NextEntry(path, parent + 1)) <= entry ) {
    buffer = path.substr(0, parent);
    Status s = IO->GetAttr(buffer);
    if (s.IsNotFound()) {
      s = IO->MakeDirectory(buffer);
      if (!s.ok()) return s;
    } else if (!s.ok()) return s;
  }
  return Status::OK();
}
/////////////////////////////////////////////////////////////////////
#endif

#define REGISTER_OP(op)                                                    \
  struct Op_##op: public Op {                                              \
    virtual void Exec(IOListener* L) {                                     \
      IOAdaptor ada(IO_);                                                  \
      Status s = ada.IO_##op(path_, path2_);                               \
      if (!s.ok()) {                                                       \
        if (L != NULL) L->IOFailed(#op);                                   \
        throw IOError(path_, #op, s.ToString());                           \
      }                                                                    \
      if (L != NULL) {                                                     \
        L->IOPerformed(#op);                                               \
      }                                                                    \
    }                                                                      \
    Op_##op(IOClient* IO, const std::string &p, const std::string &p2)     \
      : Op(IO, p, p2) {                                                    \
    }                                                                      \
  };                                                                       \
  static Op* New_Op_##op                                                   \
    (IOClient* IO, const std::string &p, const std::string &p2) {          \
    return new Op_##op(IO, p, p2);                                         \
  }                                                                        \
  bool r_reg_op_##op = RegisterOp(#op, &New_Op_##op);                      \
  Status IOAdaptor::IO_##op(Path &path, Path &path2)

//////////////////////////////////////////////////////////////////////////////////
// IO OPERATION IMPLEMENTATION
//

REGISTER_OP(open) {
//  if (FLAGS_check) {
//    Status s = IO_->GetAttr(path);
//    if (s.IsNotFound()) return Status::OK();
//    return s;
//  }
/************************************************
  int r = rand() % 500;
  if (r == 0) {
    size_t idx = path.rfind('/');
    std::string parent = path.substr(0, idx);
    IO_->ResetMode(parent);
  }
*************************************************/
  return IO_->GetAttr(path);
}

REGISTER_OP(create) {
//  if (FLAGS_check) {
//    CreateParents(IO_, path);
//    Status s = IO_->GetAttr(path);
//    if (s.ok()) return s;
//    if (!s.IsNotFound()) return s;
//  }
//  return IO_->NewFile(path);
  return IO_->MakeDirectory(path);
}

REGISTER_OP(delete) {
//  if (FLAGS_check) {
//    Status s = IO_->GetAttr(path);
//    if (s.IsNotFound()) return Status::OK();
//    if (!s.ok()) return s;
//  }
//  return IO_->Remove(path);
  return IO_->GetAttr(path);
}

REGISTER_OP(rename) {
//  if (FLAGS_check) {
//    Status s = IO_->GetAttr(path);
//    if (s.IsNotFound()) return Status::OK();
//    if (!s.ok()) return s;
//    CreateParents(IO_, path2);
//    s = IO_->GetAttr(path2);
//    if (s.ok()) return s;
//    if (!s.IsNotFound()) return s;
//  }
//  return IO_->Rename(path, path2);
  if (FLAGS_read_only) {
    IO_->GetAttr(path);
  } else {
    IO_->ResetMode(path);
  }
  return IO_->MakeDirectory(path2);
}

REGISTER_OP(mkdir) {
//  if (FLAGS_check) {
//    CreateParents(IO_, path);
//    Status s = IO_->GetAttr(path);
//    if (s.ok()) return s;
//    if (!s.IsNotFound()) return s;
//  }
  Status s = IO_->MakeDirectory(path);
  if (!s.IsIOError()) {
    return s;
  }
  return Status::OK();
}

REGISTER_OP(mkdirs) {
//  if (FLAGS_check) {
//    CreateParents(IO_, path);
//    Status s = IO_->GetAttr(path);
//    if (s.ok()) return s;
//    if (!s.IsNotFound()) return s;
//  }
//  Status s = IO_->MakeDirectory(path);
//  if (s.ok()) {
//    return Status::OK();
//  }
  return IO_->MakeDirectories(path);
}

REGISTER_OP(listStatus) {
//  if (FLAGS_check) {
//    Status s = IO_->GetAttr(path);
//    if (s.IsNotFound()) return Status::OK();
//    return s;
//  }
/*************************************************
  int r = rand() % 1000;
  if (r == 0) {
    size_t idx = path.rfind('/');
    std::string parent = path.substr(0, idx);
    IO_->ResetMode(parent);
  }
*************************************************/
  return IO_->GetAttr(path);
}

REGISTER_OP(setOwner) {
//  if (FLAGS_check) {
//    Status s = IO_->GetAttr(path);
//    if (s.IsNotFound()) return Status::OK();
//    if (!s.ok()) return s;
//  }
  if (FLAGS_read_only) {
    return IO_->GetAttr(path);
  }
  return IO_->ResetMode(path);
}

REGISTER_OP(setPermission) {
//  if (FLAGS_check) {
//    Status s = IO_->GetAttr(path);
//    if (s.IsNotFound()) return Status::OK();
//    if (!s.ok()) return s;
//  }
  if (FLAGS_read_only) {
    return IO_->GetAttr(path);
  }
  return IO_->ResetMode(path);
}

REGISTER_OP(setReplication) {
//  if (FLAGS_check) {
//    Status s = IO_->GetAttr(path);
//    if (s.IsNotFound()) return Status::OK();
//    return s;
//  }
  return IO_->GetAttr(path);
}

#undef REGISTER_OP

//////////////////////////////////////////////////////////////////////////////////
// REPLAY TEST IMPLEMENTATION
//

class ReplayTest: public IOTask {

  static
  void OpenTrace(igzstream &fs,
    const std::string &path, int rank) {
    std::stringstream ss;
    ss << path << "." << rank << "." << "gz";
    std::string full_path = ss.str();
    fs.open(full_path.c_str(), std::ios::in);
    if (!fs.good()) {
      fprintf(stderr, "cannot open trace log: %s\n", full_path.c_str());
    }
  }

  static
  void OpenTraces(std::vector<igzstream*> &fs_lst,
    const std::string &path, int rank, int comm) {
    for (int i = rank; i < FLAGS_num_parts; i += comm) {
      igzstream* fs = new igzstream();
      OpenTrace((*fs), path, i);
      fs_lst.push_back(fs);
    }
  }

  inline static
  bool OpenedAllReplayLogs(std::vector<igzstream*> &fs_lst) {
    for (size_t i = 0; i < fs_lst.size(); i++) {
      if (!fs_lst[i]->good()) {
        return false;
      }
    }
    return true;
  }

  inline static
  bool LoadedAllReplayLogs(std::vector<igzstream*> &fs_lst) {
    for (size_t i = 0; i < fs_lst.size(); i++) {
      if (!fs_lst[i]->eof() && fs_lst[i]->good()) {
        return false;
      }
    }
    return true;
  }

  void FreeOpList(std::vector<Op*> &op_list) {
    for (size_t i = 0; i < op_list.size(); i++) {
      delete op_list[i];
    }
    op_list.clear();
  }

  void BuildOpList(const char* op, std::vector<Op*> &op_list,
    igzstream &log_file, const std::string &log_path) {
    std::string line, path;
    line.reserve(128);
    path.reserve(128);
    op_list.reserve(65536);
    path.append(FLAGS_root_dir);
    size_t root_size = path.size();

    OpFactory f = GetOpFactory(op);
    while ((log_file >> line) != NULL) {
      path.resize(root_size);
      if (line.at(0) != '/') {
        path.append("/");
      }
      path.append(line);
      op_list.push_back((*f)(IO_, path, ""));
    }
    printf("# Proc %d loaded %lu ops\n", my_rank_, op_list.size());
  }

  void BuildReplayList() {
    std::string line;
    std::string op, path, path2;

    op.reserve(32);
    line.reserve(256);
    path.reserve(128);
    path2.reserve(128);
    replay_list_.reserve(65536);
    path.append(FLAGS_root_dir);
    path2.append(FLAGS_root_dir);
    size_t root_size = path2.size();

    std::string empty;
    while (!LoadedAllReplayLogs(replay_logs_)) {
      for (size_t idx = 0; idx < replay_logs_.size(); idx++) {
        if (replay_logs_[idx]->eof() || !replay_logs_[idx]->good()) {
          continue; // skip this log
        }
        for (int i = 0; i < FLAGS_win_size; i++) {
          if (std::getline(*replay_logs_[idx], line) == NULL) {
            break; // reach end-of-file
          }
          line.append("\t"); // Using a tailing sentinel element
          size_t op_pos = line.find('\t');
          size_t path_pos = line.find('\t', op_pos + 1);
          op = line.substr(0, op_pos);
          path.resize(root_size);
          path.append(line.substr(op_pos + 1, path_pos - op_pos - 1));
          path2.resize(root_size);
          path2.append(line.substr(path_pos + 1));
          if (path2.size() > root_size) {
            path2.erase(path2.length() - 1);
          }
          OpFactory f = GetOpFactory(op);
          if (f == NULL) {
            fprintf(stderr, "warning: unknown IO op: %s\n", op.c_str());
            continue; // No matching Op found
          }
          if (path2.size() > root_size) {
            replay_list_.push_back((*f)(IO_, path, path2));
          } else {
            replay_list_.push_back((*f)(IO_, path, empty));
          }
        }
      }
    }
    printf("# Proc %d loaded %lu ops\n", my_rank_, replay_list_.size());
  }

  int PrintSettings() {
    return printf("Test Settings:\n"
      "  total processes -> %d\n"
      "  backend_fs -> %s\n"
      "  bulk_insert -> %s\n"
      "  ignore_errors -> %s\n"
      "  init_log -> %s\n"
      "  prepare_log -> %s\n"
      "  verify_log -> %s\n"
      "  replay_log -> %s\n"
      "  num_parts -> %d\n"
      "  win_size -> %d\n"
      "  root_dir -> %s\n"
      "  detailed_perf_output -> %s\n",
      comm_sz_,
      FLAGS_fs.c_str(),
      GetBoolString(FLAGS_bulk_insert),
      GetBoolString(FLAGS_ignore_errors),
      FLAGS_init_log.c_str(),
      FLAGS_prepare_log.c_str(),
      FLAGS_verify_log.c_str(),
      FLAGS_replay_log.c_str(),
      FLAGS_num_parts,
      FLAGS_win_size,
      FLAGS_root_dir.c_str(),
      FLAGS_log_file.c_str());
  }

  bool init_;
  igzstream init_log_;
  std::vector<Op*> init_list_;

  bool prepare_;
  igzstream prepare_log_;
  std::vector<Op*> prepare_list_;

  bool verify_;
  igzstream verify_log_;
  std::vector<Op*> verify_list_;

  bool replay_;
  std::vector<igzstream*> replay_logs_;
  std::vector<Op*> replay_list_;

 public:

  virtual ~ReplayTest() {
    if (init_log_.good()) {
      init_log_.close();
    }
    if (prepare_log_.good()) {
      prepare_log_.close();
    }
    if (verify_log_.good()) {
      verify_log_.close();
    }
    for (size_t i = 0; i < replay_logs_.size(); i++) {
      if (replay_logs_[i]->good()) {
        replay_logs_[i]->close();
        delete replay_logs_[i];
      }
    }
    replay_logs_.clear();
  }

  ReplayTest(int my_rank, int comm_sz) : IOTask(my_rank, comm_sz),
    init_(false), prepare_(false), verify_(false), replay_(false) {
    if (FLAGS_num_parts == 0) {
      FLAGS_num_parts = comm_sz;
    }
    if (!FLAGS_init_log.empty()) {
      init_ = true;
      OpenTrace(init_log_, FLAGS_init_log, my_rank);
    }
    if (!FLAGS_prepare_log.empty()) {
      prepare_ = true;
      OpenTrace(prepare_log_, FLAGS_prepare_log, my_rank);
    }
    if (!FLAGS_verify_log.empty()) {
      verify_ = true;
      OpenTrace(verify_log_, FLAGS_verify_log, my_rank);
    }
    if (!FLAGS_replay_log.empty()) {
      replay_ = true;
      OpenTraces(replay_logs_, FLAGS_replay_log, my_rank, comm_sz);
    }
  }

  virtual void Prepare() {
    Status s = IO_->Init();
    if (!s.ok()) {
      throw IOError("init", s.ToString());
    }
    if (init_) {
      OpFactory f = GetOpFactory("mkdirs");
      init_list_.push_back((*f)(IO_, FLAGS_root_dir, ""));
      BuildOpList("mkdirs", init_list_, init_log_, FLAGS_init_log);
    }
    if (prepare_) {
      BuildOpList("mkdir", prepare_list_, prepare_log_, FLAGS_prepare_log);
    }
    if (verify_) {
      BuildOpList("listStatus", verify_list_, verify_log_, FLAGS_verify_log);
    }
    if (replay_) {
      BuildReplayList();
    }
  }

  virtual void Run() {
    IOMeasurements::Reset(IO_);
    if (init_) {
      for (size_t i = 0; i < init_list_.size(); i++) {
        init_list_[i]->Exec(listener_);
      }
    }
    if (prepare_) {
      for (size_t i = 0; i < prepare_list_.size(); i++) {
        prepare_list_[i]->Exec(listener_);
      }
    }
    if (verify_) {
      for (size_t i = 0; i < verify_list_.size(); i++) {
        verify_list_[i]->Exec(listener_);
      }
    }
    if (replay_) {
      for (size_t i = 0; i < replay_list_.size(); i++) {
        try {
          replay_list_[i]->Exec(listener_);
        } catch (IOError &err) {
          if (!FLAGS_ignore_errors)
            throw err;
        }
      }
    }
    fprintf(LOG_, "== Main Phase Performance Data ==\n\n");
    IOMeasurements::PrintMeasurements(IO_, LOG_);
  }

  virtual void Clean() {
    FreeOpList(init_list_);
    FreeOpList(prepare_list_);
    FreeOpList(verify_list_);
    FreeOpList(replay_list_);
  }

  virtual bool CheckPrecondition() {
    if (IO_ == NULL || LOG_ == NULL) {
      return false; // err has already been printed elsewhere
    }
    if (init_ && !init_log_.good()) {
      return false; // err has already been printed elsewhere
    }
    if (prepare_ && !prepare_log_.good()) {
      return false; // err has already been printed elsewhere
    }
    if (verify_ && !verify_log_.good()) {
      return false; // err has already been printed elsewhere
    }
    if (replay_ && !OpenedAllReplayLogs(replay_logs_)) {
      return false; // err has already been printed elsewhere
    }
    if (!init_ && !prepare_ && !verify_ && !replay_) {
      my_rank_ == 0
        ? fprintf(stderr, "No trace file of any category is specified\n")
        : 0;
      return false;
    }
    my_rank_ == 0 ? PrintSettings() : 0;
    return true;
  }

};

} /* anonymous namepsace */

IOTask* IOTaskFactory::GetReplayTestTask(int my_rank, int comm_sz) {
  return new ReplayTest(my_rank, comm_sz);
}

} /* namespace mpi */ } /* namespace indexfs */
