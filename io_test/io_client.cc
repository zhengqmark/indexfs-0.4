// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <mpi.h>
#include <time.h>
#include <sstream>
#include <gflags/gflags.h>

#include "io_client.h"
#include "leveldb/util/socket.h"
#include "leveldb/util/histogram.h"

namespace indexfs { namespace mpi {

IOClient::~IOClient() {
  // empty
}

DEFINE_bool(print_ops, false, "Log all operations performed to stdout");
DEFINE_string(tsdb_ip, "127.0.0.1", "TSDB's IP address");
DEFINE_int32(tsdb_port, 10600, "TSDB's UDP port");

DEFINE_bool(per_op, false, "send Performance data per certain number of ops");
DEFINE_int32(perf_num_ops, 5000, "Send performance data per such number of ops");
DEFINE_bool(per_sec, true, "Send Performance data per certain number of seconds");
DEFINE_int32(perf_num_secs, 5, "Send performance data per such number of seconds");

namespace {

using ::leveldb::Histogram;
using ::leveldb::UDPSocket;
using ::leveldb::SocketException;

static
inline void AddOpCounts(std::stringstream &ss,
  int now, int numops, const char** names, int* counters,
    int rank, const std::string &runid) {
  for (int i = 0; i < numops; i++) {
    ss << "idxfs.bench." << names[i] << "_num "
       << now << " " << counters[i]
       << " rank=" << rank
       << " run=" << runid << "\n";
  }
}

static
inline void AddSuccOps(std::stringstream &ss,
  int now, int ops, int rank, const std::string &runid) {
  ss << "idxfs.bench.succ_ops "
     << now << " " << ops
     << " rank=" << rank
     << " run=" << runid << "\n";
}

static
inline void AddErrOps(std::stringstream &ss,
  int now, int ops, int rank, const std::string &runid) {
  ss << "idxfs.bench.err_ops "
     << now << " " << ops
     << " rank=" << rank
     << " run=" << runid << "\n";
}

static
inline void AddAvgLatency(std::stringstream &ss,
  int now, double sum, int ops, int rank, const std::string &runid) {
  if (ops > 0) {
    ss << "idxfs.bench.avg_latency "
       << now << " " << sum / ops
       << " rank=" << rank
       << " run=" << runid << "\n";
  }
}

static
void SendPerfData(const std::string &data) {
  UDPSocket sock;
  try {
    sock.sendTo(data.data(), data.length(),
        FLAGS_tsdb_ip, (unsigned short) FLAGS_tsdb_port);
  } catch (SocketException &e) {
    fprintf(stderr, "cannot send perf data to TSDB: %s\n", e.what());
  }
}

class MetaClient: public IOClient {
 public:

  virtual Status Init();
  virtual Status Dispose();

  virtual ~MetaClient() {
    delete cli_;
  }
  explicit MetaClient(IOClient* cli, int rank, const std::string &id)
    : IOClient(), rank_(rank), id_(id), cli_(cli) {
    mon_ = true;
    last_sent_ = 0;
    res_sum_ = ops_ = err_ops_ = 0;
    last_res_sum_ = last_ops_ = last_err_ops_ = 0;
  }

  virtual void Noop() {
    cli_->Noop();
  }

  virtual Status FlushWriteBuffer() {
    return cli_->FlushWriteBuffer();
  }

  void PublishPerfData() {
    std::stringstream ss;

    int now = time(NULL);
    AddOpCounts(ss, now, kNumOps, kOpNames, counters_, rank_, id_);
    AddSuccOps(ss, now, ops_, rank_, id_);
    AddErrOps(ss, now, err_ops_, rank_, id_);
    AddAvgLatency(ss, now, res_sum_ - last_res_sum_, ops_ - last_ops_ , rank_, id_);

    last_ops_ = ops_;
    last_err_ops_ = err_ops_;
    last_res_sum_ = res_sum_;

    SendPerfData(ss.str()); // Send perf data to TSDB or its local agent
  }

#define MONITORED_OP_1ARG(op, arg_type)                              \
  virtual Status op(arg_type arg) {                                  \
    if (mon_) {                                                      \
      double start, finish, dura;                                    \
      start = MPI_Wtime();                                           \
      Status s = cli_->op(arg);                                      \
      finish = MPI_Wtime();                                          \
      dura = (finish - start) * 1000 * 1000;                         \
      if (!s.ok()) {                                                 \
        err_ops_++;                                                  \
      } else {                                                       \
        ops_++;                                                      \
        res_sum_ += dura;                                            \
        counters_[k##op]++;                                          \
        latencies_[kOpCategoryIndex[k##op]].Add(dura);               \
      }                                                              \
      if (FLAGS_per_sec) {                                           \
        if (finish - last_sent_ >= FLAGS_perf_num_secs) {            \
          PublishPerfData();                                         \
          last_sent_ = finish;                                       \
        }                                                            \
      }                                                              \
      else if (FLAGS_per_op) {                                       \
        if ((ops_ + err_ops_) % FLAGS_perf_num_ops == 0) {           \
          PublishPerfData();                                         \
        }                                                            \
      }                                                              \
      return s;                                                      \
    }                                                                \
    return cli_->op(arg);                                            \
  }

#define MONITORED_OP_2ARG(op, arg_type1, arg_type2)                  \
  virtual Status op(arg_type1 arg1, arg_type2 arg2) {                \
    if (mon_) {                                                      \
      double start, finish, dura;                                    \
      start = MPI_Wtime();                                           \
      Status s = cli_->op(arg1, arg2);                               \
      finish = MPI_Wtime();                                          \
      dura = (finish - start) * 1000 * 1000;                         \
      if (!s.ok()) {                                                 \
        err_ops_++;                                                  \
      } else {                                                       \
        ops_++;                                                      \
        res_sum_ += dura;                                            \
        counters_[k##op]++;                                          \
        latencies_[kOpCategoryIndex[k##op]].Add(dura);               \
      }                                                              \
      if (FLAGS_per_sec) {                                           \
        if (finish - last_sent_ >= FLAGS_perf_num_secs) {            \
          PublishPerfData();                                         \
          last_sent_ = finish;                                       \
        }                                                            \
      }                                                              \
      else if (FLAGS_per_op) {                                       \
        if ((ops_ + err_ops_) % FLAGS_perf_num_ops == 0) {           \
          PublishPerfData();                                         \
        }                                                            \
      }                                                              \
      return s;                                                      \
    }                                                                \
    return cli_->op(arg1, arg2);                                     \
  }

  // WRAP ALL IO OPS TO GATHER PERF DATA //

  MONITORED_OP_1ARG(NewFile          ,Path&)
  MONITORED_OP_1ARG(MakeDirectory    ,Path&)
  MONITORED_OP_1ARG(MakeDirectories  ,Path&)
  MONITORED_OP_1ARG(SyncDirectory    ,Path&)
  MONITORED_OP_1ARG(ResetMode        ,Path&)
  MONITORED_OP_1ARG(GetAttr          ,Path&)
  MONITORED_OP_1ARG(ListDirectory    ,Path&)
  MONITORED_OP_1ARG(Remove           ,Path&)

  MONITORED_OP_2ARG(Rename           ,Path&  ,Path&)

  // IO MEASUREMENT INTERFACE //

  void EnableMonitoring(bool enable);
  void Reset();
  void __PrintMeasurements__(FILE* output); // max, min, avg and latency histogram

 private:

  // All Recoginized Operation Categories
  enum { kMDRead, kMDWrite, kIO, kNumCategories };
  // All supported Operations
  enum { kMakeDirectory, kMakeDirectories, kSyncDirectory, kNewFile,
    kResetMode, kRename, kGetAttr, kListDirectory, kRemove, kNumOps };

  static const char* kOpNames[kNumOps];
  static const char* kCategoryNames[kNumCategories];
  static const int kOpCategoryIndex[kNumOps];

  int ops_;
  int err_ops_;
  double res_sum_;

  int last_ops_;
  int last_err_ops_;
  double last_res_sum_;
  double last_sent_; // the last time we published performance data

  bool mon_; // true iff monitoring is enabled
  int rank_; // an unique rank identifying a specific client process
  std::string id_; // an unique string identifier for a particular test/benchmark run
  Histogram latencies_[kNumCategories];
  int counters_[kNumOps];
  IOClient* cli_; // The real IO client being composited

  // No copying allowed
  MetaClient(const MetaClient&);
  MetaClient& operator=(const MetaClient&);
};

const char* MetaClient::kOpNames[kNumOps] = {
  "mkdir", "mkdirs", "fsyncdir", "mknod",
  "chmod", "rename", "getattr", "readdir", "remove"
};

const char* MetaClient::kCategoryNames[kNumCategories] = {
  "Read" /* Metadata Reads */, "Write" /* Metadata Updates */, "I/O" /* Data */
};

const int MetaClient::kOpCategoryIndex[kNumOps] = {
  kMDWrite /* MakeDirectory*/, kMDWrite /* MakeDirectories */, kMDWrite /* SyncDirectory */,
  kMDWrite /* NewFile */, kMDWrite /* ResetMode */, kMDWrite /* Rename */, kMDRead /* GetAttr */,
  kMDRead /* ListDirectory */, kMDWrite /* Remove*/
};

Status MetaClient::Init() {
  return cli_->Init();
}

Status MetaClient::Dispose() {
  return cli_->Dispose();
}

void MetaClient::EnableMonitoring(bool enable) {
  mon_ = enable;
}

void MetaClient::Reset() {
  memset(counters_, 0, kNumOps * sizeof(int));
  for (int i = 0; i < kNumCategories; i++) {
    latencies_[i].Clear();
  }
  last_sent_ = MPI_Wtime();
  res_sum_ = ops_ = err_ops_ = 0;
  last_res_sum_ = last_ops_ = last_err_ops_ = 0;
}

void MetaClient::__PrintMeasurements__(FILE* output) {
  fprintf(output, ">>>>>> Client's Self-Measurements >>> \n\n");
  cli_->PrintMeasurements(output);
  fprintf(output, "\n\n\n");
  // --------------------------------------------------------- //
  fprintf(output, ">>>>>> IO Client Measurements >>>>>>> \n\n");
  for (int i = 0; i < kNumCategories; i++) {
    fprintf(output, "== Latencies for %s ops:\n", kCategoryNames[i]);
    fprintf(output, "%s\n", latencies_[i].ToString().c_str());
  }
  fprintf(output, "== Total I/O Ops Issued: %d ", ops_ + err_ops_);
  fprintf(output, "(Succ ops: %d, Error ops: %d)\n\n", ops_, err_ops_);
}

} /* anonymous namespace */

Status IOClient::MakeDirectory(int dno, const std::string &prefix) {
  std::stringstream ss;
  ss << "/d_" << prefix << dno;
  return MakeDirectory(ss.str());
}

Status IOClient::SyncDirectory(int dno, const std::string &prefix) {
  std::stringstream ss;
  ss << "/d_" << prefix << dno;
  return SyncDirectory(ss.str());
}

Status IOClient::NewFile(int dno, int fno, const std::string &prefix) {
  std::stringstream ss;
  ss << "/d_" << prefix << dno << "/f_" << prefix << fno;
  return NewFile(ss.str());
}

Status IOClient::GetAttr(int dno, int fno, const std::string &prefix) {
  std::stringstream ss;
  ss << "/d_" << prefix << dno << "/f_" << prefix << fno;
  return GetAttr(ss.str());
}

//////////////////////////////////////////////////////////////////////////////////
// IO MEASUREMENT INTERFACE
//

void IOMeasurements::EnableMonitoring(IOClient* cli, bool enable) {
  MetaClient* meta_cli = reinterpret_cast<MetaClient*>(cli);
  meta_cli->EnableMonitoring(enable);
}

void IOMeasurements::Reset(IOClient* cli) {
  MetaClient* meta_cli = reinterpret_cast<MetaClient*>(cli);
  meta_cli->Reset();
}

void IOMeasurements::PrintMeasurements(IOClient* cli, FILE* output) {
  MetaClient* meta_cli = reinterpret_cast<MetaClient*>(cli);
  meta_cli->__PrintMeasurements__(output);
}

//////////////////////////////////////////////////////////////////////////////////
// IO-CLIENT FACTORY
//

IOClient* IOClientFactory::GetLocalFSClient(int rank, const std::string &id) {
  return new MetaClient(IOClient::NewLocalFSClient(), rank, id);
}

IOClient* IOClientFactory::GetOrangeFSClient(int rank, const std::string &id) {
  return new MetaClient(IOClient::NewOrangeFSClient(), rank, id);
}

IOClient* IOClientFactory::GetIndexFSClient(int rank, int size, const std::string &id) {
  return new MetaClient(IOClient::NewIndexFSClient(rank, size), rank, id);
}

} /* namespace mpi */ } /* namespace indexfs */
