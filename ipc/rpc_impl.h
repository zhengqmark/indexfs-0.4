// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_IPC_RPCIMPL_H_
#define _INDEXFS_IPC_RPCIMPL_H_

#include "ipc/membset.h"
#include "ipc/rpc.h"
#include "ipc/rpc_types.h"

// -------------------------------------------------------------
// Internal RPC Implementation
// -------------------------------------------------------------

#ifdef IDXFS_RPC_NOBLOCKING
#include <gflags/gflags.h>
DECLARE_int32(rpc_io_threads);
DECLARE_int32(rpc_worker_threads);
#endif

namespace indexfs {

// An internal abstraction representing a client-side RPC stub
// associated with a remote RPC server.
// Each client-side RPC stub can only be opened once --- any stub opened
// unsuccessfully should be garbage collected and replaced with a new one.
//
class CliRep {

  // No copying allowed
  CliRep(const CliRep&);
  CliRep& operator=(const CliRep&);

  bool alive_, opened_;
  shared_ptr<TSocket> socket_;
  shared_ptr<TTransport> transport_;
  shared_ptr<TProtocol> protocol_;
  scoped_ptr<MetadataIndexServiceIf> stub_;

 public:

  CliRep(const std::string& ip, int port)
    : socket_(new TSocket(ip, port))
#   ifdef IDXFS_RPC_NOBLOCKING
    , transport_(new TFramedTransport(socket_))
#   else
    , transport_(new TBufferedTransport(socket_))
#   endif
    , protocol_(new TBinaryProtocol(transport_))
    , stub_(new MetadataIndexServiceClient(protocol_)) {
    alive_ = opened_ = false;
  }

  void Close() {
    DLOG_ASSERT(alive_);
    try {
      transport_->close();
    } catch (TTransportException &tx) {
      LOG(WARNING) << "Fail to close socket: " << tx.what();
    }
    alive_ = false;
  }

  Status Open() {
    DLOG_ASSERT(!opened_ && !alive_);
    opened_ = true;
    try {
      transport_->open();
    } catch (TTransportException &tx) {
      LOG(ERROR) << "Fail to open socket: " << tx.what();
      return Status::IOError(Slice());
    }
    alive_ = true;
    return Status::OK();
  }

  bool IsReady() { return opened_ && alive_; }
  MetadataIndexServiceIf* GetClientStub() { return stub_.get(); }
};

// An internal abstraction representing a RPC server exposing a set of RPC
// endpoints. Each server instance is bind to a local TCP port,
// and is associated with a RPC handler, which implements those RPC endpoints.
// This handler is automatically disposed when its parent RPC server is destroyed.
//
#ifdef IDXFS_RPC_NOBLOCKING
class SrvRep {

  // No copying allowed
  SrvRep(const SrvRep&);
  SrvRep& operator=(const SrvRep&);

  scoped_ptr<TNonblockingServer> server_;
  shared_ptr<MetadataIndexServiceIf> handler_;
  shared_ptr<MetadataIndexServiceProcessor> processor_;
  shared_ptr<ThreadManager> thread_manager_;
  shared_ptr<PosixThreadFactory> thread_factory_;

 public:

  void Start() {
    server_->serve();
  }

  void Stop() {
    server_->stop();
  }

  SrvRep(MetadataIndexServiceIf* handler, int port)
    : handler_(handler)
    , processor_(new MetadataIndexServiceProcessor(handler_))
    , thread_factory_(new PosixThreadFactory()) {
    thread_manager_ = ThreadManager::newSimpleThreadManager(FLAGS_rpc_worker_threads);
    thread_manager_->threadFactory(thread_factory_);
    thread_manager_->start();
    server_.reset(new TNonblockingServer(processor_, port));
    server_->setThreadManager(thread_manager_);
    server_->setNumIOThreads(FLAGS_rpc_io_threads);
  }

};
#else
class SrvRep {

  // No copying allowed
  SrvRep(const SrvRep&);
  SrvRep& operator=(const SrvRep&);

  scoped_ptr<TServer> server_;
  shared_ptr<MetadataIndexServiceIf> handler_;
  shared_ptr<MetadataIndexServiceProcessor> processor_;
  shared_ptr<TServerTransport> socket_;
  shared_ptr<TProtocolFactory> protocol_factory_;
  shared_ptr<TTransportFactory> transport_factory_;

 public:

  void Start() {
    server_->serve();
  }

  void Stop() {
    server_->stop();
  }

  SrvRep(MetadataIndexServiceIf* handler, int port)
    : handler_(handler)
    , processor_(new MetadataIndexServiceProcessor(handler_))
    , socket_(new TServerSocket(port))
    , protocol_factory_(new TBinaryProtocolFactory())
    , transport_factory_(new TBufferedTransportFactory()) {
    server_.reset(new TThreadedServer(
        processor_, socket_, transport_factory_, protocol_factory_));
  }

};
#endif

// An augmented client-side RPC implementation with automatic detection
// and recovery from failed RPC servers. RPC servers are considered failed
// if a client can no longer send packages to or receive data from those servers.
// This may be caused by network partitions, server crashes, or hardware outage.
//
class FTCliRepWrapper: virtual public MetadataIndexServiceIf {

  // No copying allowed
  FTCliRepWrapper(const FTCliRepWrapper&);
  FTCliRepWrapper& operator=(const FTCliRepWrapper&);

  CliRep* client_; // the real client being adapted
  int srv_id_;
  Config* conf_;
  Env* env_;
  MemberSet* member_set_;

  // The last server address used to initialize the client
  int port_;
  std::string ip_;

  MetadataIndexServiceIf* GetInternalStub() {
    DLOG_ASSERT(client_ != NULL);
    DLOG_ASSERT(client_->IsReady());
    MetadataIndexServiceIf* stub = client_->GetClientStub();
    DLOG_ASSERT(stub != NULL);
    return stub;
  }

  void PrepareClient(); // allocate new internal client
  void RetrieveServerAddress(); // get latest address from membership service
  Status Reconnect(int max_attempts);
  Status ReestabilishConnectionToServer();

 public:

  virtual ~FTCliRepWrapper();
  FTCliRepWrapper(int srv_id, Config* conf, MemberSet* member_set);

  // Open the underlying TCP connection, if possible, to the server.
  Status Open();

  // Close the underlying TCP connection, if exists, to the target server.
  Status Shutdown();

  // Reconnect to the target server, otherwise throw TTransportException.
  // Note that the current TCP connection will be automatically disposed if
  // it has been opened successfully in the past.
  void RecoverConnectionToServer();

  // True iff the underlying TCP connection was opened successfully
  // at the time it was opened.
  // Note that there is no promise that this TCP connection is still working.
  bool IsReady() { return client_ != NULL && client_->IsReady(); }

  // -------------------------------------------------------------
  // Add auto-recovery to the following RPC functions
  // -------------------------------------------------------------

  void Ping();
  void FlushDB();

  void Access(LookupInfo& _return, const OID& obj_id);
  void Renew(LookupInfo& _return, const OID& obj_id);
  void Getattr(StatInfo& _return, const OID& obj_id);
  void Readdir(EntryList& _return, const int64_t dir_id, const int16_t index);

  void Mknod(const OID& obj_id, const int16_t perm);
  void Mknod_Bulk(const OIDS& obj_ids, const int16_t perm);
  void Mkdir(const OID& obj_id, const int16_t perm,
      const int16_t hint_server1, const int16_t hint_server2);
  void Mkdir_Presplit(const OID& obj_id, const int16_t perm,
      const int16_t hint_server1, const int16_t hint_server2);
  bool Chmod(const OID& obj_id, const int16_t perm);
  bool Chown(const OID& obj_id, const int16_t uid, const int16_t gid);

  void CreateZeroth(const int64_t dir_id, const int16_t zeroth_server);
  void ReadBitmap(std::string& _return, const int64_t dir_id);
  void UpdateBitmap(const int64_t dir_id, const std::string& dmap_data);

  void InsertSplit(const int64_t dir_id,
      const int16_t parent_index, const int16_t child_index,
      const std::string& path_split_files, const std::string& dmap_data,
      const int64_t min_seq, const int64_t max_seq, const int64_t num_entries);
};

} /* namespace indexfs */

#endif /* _INDEXFS_IPC_RPCIMPL_H_ */
