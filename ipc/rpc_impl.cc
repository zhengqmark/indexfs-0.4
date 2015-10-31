// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "ipc/rpc.h"
#include "ipc/rpc_impl.h"

#ifdef IDXFS_RPC_NOBLOCKING
DEFINE_int32(rpc_io_threads, 4, "Set the number of I/O threads");
DEFINE_int32(rpc_worker_threads, 4, "Set the number of worker threads");
#endif

DEFINE_int32(rpc_retries, 0, "Set the number of retries after a failed call");
DEFINE_int32(rpc_conn_attempts, 1, "Set the max number of TCP connection attempts");

namespace indexfs {

FTCliRepWrapper::~FTCliRepWrapper() {
  if (client_ != NULL) {
    if (client_->IsReady()) {
      client_->Close();
    }
    delete client_;
  }
}

FTCliRepWrapper::FTCliRepWrapper(
    int srv_id, Config* conf, MemberSet* member_set)
  : client_(NULL)
  , srv_id_(srv_id)
  , conf_(conf)
  , env_(Env::Default())
  , member_set_(member_set) {
}

namespace {
static inline
int GetBackoffDuration(int attempt) {
  DLOG_ASSERT(attempt >= 1);
  if (attempt <= 1) {
    return 0;
  }
  return (1 << (attempt - 2)) * 1000 * 1000;
}
}

// Dispose the existing client and create a new one.
//
void FTCliRepWrapper::PrepareClient() {
  if (client_ != NULL) {
    if (client_->IsReady()) {
      client_->Close();
    }
    delete client_;
  }
  client_ = new CliRep(ip_, port_);
}

// Retrieve the latest address of the target server.
//
void FTCliRepWrapper::RetrieveServerAddress() {
  DLOG_ASSERT(member_set_ != NULL);
  Status s = member_set_->FindServer(srv_id_, &ip_, &port_);
  CHECK(s.ok())
      << "Fail to retrieve the address of "
      << "server " << srv_id_ << ": " << s.ToString();
  DLOG(INFO) << "RPC setting address [server=" << srv_id_ << "]"
      << ": ip=" << ip_ << ", port=" << port_;
}

// Reconnect the target server by
// disposing the existing RPC client and creating a new one.
//
Status FTCliRepWrapper::Reconnect(int max_attempts) {
  Status s;
  int attempt = 0;
  s = Status::IOError(Slice());
  while (!s.ok() && ++attempt <= max_attempts) {
    int msecs = GetBackoffDuration(attempt);
    if (msecs > 0) {
      env_->SleepForMicroseconds(msecs);
    }
    PrepareClient();
    s = client_->Open();
  }
  DLOG(INFO) << "RPC connecting to server"
      << " [server=" << srv_id_ << "]"
      << ": total_attempts=" << std::min(attempt, max_attempts)
      << ", result=" << s.ToCodeString();
  return s;
}

// Attempt to rebuild connection to a logic server by loading the latest
// physical server address and try connecting to it.
//
Status FTCliRepWrapper::ReestabilishConnectionToServer() {
  Status s;
  LOG_IF(WARNING, client_ != NULL)
      << "RPC connection not found or lost"
      << " [server=" << srv_id_ << "]: ip=" << ip_ << ", port=" << port_;
  int attempt = 0;
  s = Status::IOError(Slice());
  while (!s.ok() && ++attempt <= FLAGS_rpc_conn_attempts) {
    RetrieveServerAddress();
    s = Reconnect(attempt);
  }
  LOG_IF(INFO, s.ok())
      << "RPC connection reestablished"
      << " [server=" << srv_id_ << "]: ip=" << ip_ << ", port=" << port_;
  return s;
}

Status FTCliRepWrapper::Open() {
  DLOG_ASSERT(client_ == NULL);
  RetrieveServerAddress();
  Status s = Reconnect(1);
  LOG_IF(INFO, s.ok())
      << "RPC connection established"
      << " [server=" << srv_id_ << "]: ip=" << ip_ << ", port=" << port_;
  return s;
}

Status FTCliRepWrapper::Shutdown() {
  if (client_ != NULL && client_->IsReady()) {
    client_->Close();
  }
  return Status::OK();
}

// This must work, otherwise we are screwed.
//
void FTCliRepWrapper::RecoverConnectionToServer() {
  Status s = ReestabilishConnectionToServer();
  if (!s.ok()) throw TTransportException(s.ToString());
}

// -------------------------------------------------------------
// Fault-tolerant RCP Implementation
// -------------------------------------------------------------

#define EXEC_EXIT() return
#define EXEC_ABORT() abort()
#define EXEC_RETURN(r) return r

#define EXEC_WITH_RETRY_TRY()                              \
  int retry = -1;                                          \
  while (++retry <= FLAGS_rpc_retries) {                   \
    try                                                    \

#define EXEC_WITH_RETRY_CATCH()                            \
    catch(TTransportException &ex) {                       \
      if (retry == FLAGS_rpc_retries) {                    \
        throw ex;                                          \
      }                                                    \
      Status s = ReestabilishConnectionToServer();         \
      if (!s.ok()) throw ex;                               \
    }                                                      \
  }                                                        \

#ifdef IDXFS_RPC_DEBUG
#define RPC_TRACE(rpc_name)                                \
  DLOG_EVERY_N(INFO, 1) << ">> RPC [" << rpc_name << "]"   \
             << "(" << google::COUNTER << ")"              \
             << ": target server=" << srv_id_              \
             << " (" << ip_ << ":" << port_ << ")"         \

#else
#define RPC_TRACE(rpc_name)                                \
  true ? (void) 0 : (void) (DLOG(INFO) << rpc_name, 0)     \

#endif

// -------------------------------------------------------------

void FTCliRepWrapper::Ping() {
  RPC_TRACE(__func__);
  EXEC_WITH_RETRY_TRY() {
    GetInternalStub()->Ping();
    EXEC_EXIT();
  }
  EXEC_WITH_RETRY_CATCH();
}

void FTCliRepWrapper::FlushDB() {
  RPC_TRACE(__func__);
  EXEC_WITH_RETRY_TRY() {
    GetInternalStub()->FlushDB();
    EXEC_EXIT();
  }
  EXEC_WITH_RETRY_CATCH();
}

void FTCliRepWrapper::Mknod(const OID& obj_id, const int16_t perm) {
  RPC_TRACE(__func__);
  EXEC_WITH_RETRY_TRY() {
    GetInternalStub()->Mknod(obj_id, perm);
    EXEC_EXIT();
  }
  EXEC_WITH_RETRY_CATCH();
}

void FTCliRepWrapper::Mknod_Bulk(const OIDS& obj_ids, const int16_t perm) {
  RPC_TRACE(__func__);
  EXEC_WITH_RETRY_TRY() {
    GetInternalStub()->Mknod_Bulk(obj_ids, perm);
    EXEC_EXIT();
  }
  EXEC_WITH_RETRY_CATCH();
}

bool FTCliRepWrapper::Chmod(const OID& obj_id, const int16_t perm) {
  bool r;
  RPC_TRACE(__func__);
  EXEC_WITH_RETRY_TRY() {
    r = GetInternalStub()->Chmod(obj_id, perm);
    EXEC_RETURN(r);
  }
  EXEC_WITH_RETRY_CATCH();
  EXEC_ABORT(); // Unreachable code
}

bool FTCliRepWrapper::Chown(const OID& obj_id,
        const int16_t uid, const int16_t gid) {
  bool r;
  RPC_TRACE(__func__);
  EXEC_WITH_RETRY_TRY() {
    r = GetInternalStub()->Chown(obj_id, uid, gid);
    EXEC_RETURN(r);
  }
  EXEC_WITH_RETRY_CATCH();
  EXEC_ABORT(); // Unreachable code
}

void FTCliRepWrapper::Mkdir(const OID& obj_id,
        const int16_t perm,
        const int16_t hint_server1, const int16_t hint_server2) {
  RPC_TRACE(__func__);
  EXEC_WITH_RETRY_TRY() {
    GetInternalStub()->Mkdir(obj_id, perm, hint_server1, hint_server2);
    EXEC_EXIT();
  }
  EXEC_WITH_RETRY_CATCH();
}

void FTCliRepWrapper::Mkdir_Presplit(const OID& obj_id,
        const int16_t perm,
        const int16_t hint_server1, const int16_t hint_server2) {
  RPC_TRACE(__func__);
  EXEC_WITH_RETRY_TRY() {
    GetInternalStub()->Mkdir_Presplit(obj_id, perm, hint_server1, hint_server2);
    EXEC_EXIT();
  }
  EXEC_WITH_RETRY_CATCH();
}

void FTCliRepWrapper::CreateZeroth(const int64_t dir_id,
        const int16_t zeroth_server) {
  RPC_TRACE(__func__);
  EXEC_WITH_RETRY_TRY() {
    GetInternalStub()->CreateZeroth(dir_id, zeroth_server);
    EXEC_EXIT();
  }
  EXEC_WITH_RETRY_CATCH();
}

void FTCliRepWrapper::Renew(LookupInfo& _return, const OID& obj_id) {
  RPC_TRACE(__func__);
  EXEC_WITH_RETRY_TRY() {
    GetInternalStub()->Renew(_return, obj_id);
    EXEC_EXIT();
  }
  EXEC_WITH_RETRY_CATCH();
}

void FTCliRepWrapper::Access(LookupInfo& _return, const OID& obj_id) {
  RPC_TRACE(__func__);
  EXEC_WITH_RETRY_TRY() {
    GetInternalStub()->Access(_return, obj_id);
    EXEC_EXIT();
  }
  EXEC_WITH_RETRY_CATCH();
}

void FTCliRepWrapper::Getattr(StatInfo& _return, const OID& obj_id) {
  RPC_TRACE(__func__);
  EXEC_WITH_RETRY_TRY() {
    GetInternalStub()->Getattr(_return, obj_id);
    EXEC_EXIT();
  }
  EXEC_WITH_RETRY_CATCH();
}

void FTCliRepWrapper::UpdateBitmap(const int64_t dir_id,
        const std::string& dmap_data) {
  RPC_TRACE(__func__);
  EXEC_WITH_RETRY_TRY() {
    GetInternalStub()->UpdateBitmap(dir_id, dmap_data);
    EXEC_EXIT();
  }
  EXEC_WITH_RETRY_CATCH();
}

void FTCliRepWrapper::ReadBitmap(std::string& _return, const int64_t dir_id) {
  RPC_TRACE(__func__);
  EXEC_WITH_RETRY_TRY() {
    GetInternalStub()->ReadBitmap(_return, dir_id);
    EXEC_EXIT();
  }
  EXEC_WITH_RETRY_CATCH();
}

void FTCliRepWrapper::Readdir(EntryList& _return,
        const int64_t dir_id, const int16_t index) {
  RPC_TRACE(__func__);
  EXEC_WITH_RETRY_TRY() {
    GetInternalStub()->Readdir(_return, dir_id, index);
    EXEC_EXIT();
  }
  EXEC_WITH_RETRY_CATCH();
}

void FTCliRepWrapper::InsertSplit(const int64_t dir_id,
        const int16_t parent_index, const int16_t child_index,
        const std::string& path_split_files,
        const std::string& dmap_data,
        const int64_t min_seq, const int64_t max_seq, const int64_t num_entries) {
  RPC_TRACE(__func__);
  EXEC_WITH_RETRY_TRY() {
    GetInternalStub()->InsertSplit(dir_id,
            parent_index, child_index,
            path_split_files, dmap_data, min_seq, max_seq, num_entries);
    EXEC_EXIT();
  }
  EXEC_WITH_RETRY_CATCH();
}

} /* namespace indexfs */
