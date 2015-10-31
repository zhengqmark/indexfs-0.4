// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rpc.h"
#include "rpc_impl.h"

namespace indexfs {

// -------------------------------------------------------------
// RPC Implementation
// -------------------------------------------------------------

RPC::~RPC() {
  if (clients_ != NULL) {
    for (int i = 0; i < TotolServers(); i++) {
      if (clients_[i] != NULL) {
        delete clients_[i];
        clients_[i] = NULL;
      }
    }
    delete [] clients_;
  }
  if (mtxes_ != NULL) delete [] mtxes_;
}

Status RPC::Init() {
  for (int i = 0; i < TotolServers(); i++) {
    if (clients_[i] != NULL) {
      Status s = clients_[i]->Open();
      LOG_IF(WARNING, !s.ok())
          << "Fail to open RPC client #" << i
          << " (" << s.ToCodeString() << "), will retry later";
      DLOG(INFO) << "RPC client #" << i << " initialized";
    }
  }
  return Status::OK();
}

Status RPC::Shutdown() {
  for (int i = 0; i < TotolServers(); i++) {
    if (clients_[i] != NULL) {
      clients_[i]->Shutdown();
      DLOG(INFO) << "RPC client #" << i << " closed";
    }
  }
  return Status::OK();
}

inline bool RPC::IsServerLocal(int srv_id) {
  return conf_->GetSrvId() == srv_id;
}

FTCliRepWrapper* RPC::CreateClientFor(int srv_id) {
  DLOG_ASSERT(member_set_ != NULL);
  return new FTCliRepWrapper(srv_id, conf_, member_set_);
}

FTCliRepWrapper* RPC::CreateClientIfNotLocal(int srv_id) {
  if (self_ == NULL) {
    return CreateClientFor(srv_id);
  }
  return IsServerLocal(srv_id) ? NULL : CreateClientFor(srv_id);
}

// Retrieves the advisory lock associated with a given RPC client.
//
Mutex* RPC::GetMutex(int srv_id) {
  DLOG_ASSERT(srv_id >= 0);
  DLOG_ASSERT(srv_id < conf_->GetSrvNum());
  return mtxes_ + srv_id;
}

// Retrieves the "service" of a given server. Returns the local server handle if
// possible, otherwise returns a RPC client stub associated with the remote server.
// Re-establishes the TCP connection to the server if previous attempts failed.
//
Status RPC::GetMetadataService(int srv_id, MetadataIndexServiceIf** _return) {
  *_return = NULL;
  DLOG_ASSERT(srv_id >= 0);
  DLOG_ASSERT(srv_id < conf_->GetSrvNum());
  if (self_ != NULL && IsServerLocal(srv_id)) {
    DLOG_ASSERT(clients_[srv_id] == NULL);
    *_return = self_;
    return Status::OK();
  }
  DLOG_ASSERT(clients_[srv_id] != NULL);
  if (!clients_[srv_id]->IsReady()) {
    clients_[srv_id]->RecoverConnectionToServer();
    DLOG_ASSERT(clients_[srv_id]->IsReady());
    DLOG(INFO) << "RPC client #" << srv_id << " re-initialized";
  }
  *_return = clients_[srv_id];
  return Status::OK();
}

// Returns the "RPC stub" associated with a given server.
// Throws TTransportException if that server cannot be reached at the moment.
//
MetadataIndexServiceIf* RPC::GetClient(int srv_id) {
  MetadataIndexServiceIf* _return;
  Status s = GetMetadataService(srv_id, &_return);
  DLOG_ASSERT(s.ok());
  return _return;
}

// -------------------------------------------------------------
// RPC Client Implementation
// -------------------------------------------------------------

RPC_Client::~RPC_Client() {
  if (client_ != NULL) {
    delete client_;
  }
}

// Initialize the client by trying opening the underlying TCP connection to the
// target server. If the connection cannot be made at the moment, the failure
// will be remembered internally and will not be exposed to the caller. Failed
// connection will be automatically recovered, if possible, when any one of the
// RPC functions is invoked in the future.
//
Status RPC_Client::Init() {
  DLOG_ASSERT(client_ != NULL);
  Status s = client_->Open();
  DLOG_IF(ERROR, !s.ok())
      << "Fail to open RPC client #" << srv_id_
      << ": " << s.ToString() << ", will retry later";
  DLOG(INFO) << "RPC client #" << srv_id_ << " initialized";
  return Status::OK();
}

// Close the underlying TCP connection, if there is one, to the server. It is
// safe to shutdown a client even if the client has never been initialized.
//
Status RPC_Client::Shutdown() {
  DLOG_ASSERT(client_ != NULL);
  client_->Shutdown();
  DLOG(INFO) << "RPC client #" << srv_id_ << " closed";
  return Status::OK();
}

FTCliRepWrapper* RPC_Client::CreateInternalClient() {
  DLOG_ASSERT(member_set_ != NULL);
  return new FTCliRepWrapper(srv_id_, conf_, member_set_);
}

// Returns a RPC client stub associated with the remote server.
// Re-establishes the TCP connection to the server if previous attempts failed.
//
Status RPC_Client::GetMetadataService(MetadataIndexServiceIf** _return) {
  *_return = NULL;
  DLOG_ASSERT(client_ != NULL);
  if (!client_->IsReady()) {
    client_->RecoverConnectionToServer();
    DLOG_ASSERT(client_->IsReady());
    DLOG(INFO) << "RPC client #" << srv_id_ << " re-initialized";
  }
  *_return = client_;
  return Status::OK();
}

// Returns the "RPC stub" associated with the remote server.
// Throws TTransportException if that server cannot be reached at the moment.
//
MetadataIndexServiceIf* RPC_Client::operator->() {
  MetadataIndexServiceIf* _return;
  Status s = GetMetadataService(&_return);
  DLOG_ASSERT(s.ok());
  return _return;
}

// -------------------------------------------------------------
// RPC Server Implementation
// -------------------------------------------------------------

RPC_Server::~RPC_Server() {
  if (server_ != NULL) {
    delete server_;
  }
}

// Interrupt the server and stop its service.
//
void RPC_Server::Stop() {
  DLOG_ASSERT(server_ != NULL);
  server_->Stop();
}

// Ask the server to start listening to client requests.
// This call should and will never return.
//
void RPC_Server::RunForever() {
  DLOG_ASSERT(server_ != NULL);
  server_->Start();
}

// Creates an internal RPC server using pre-specified server configurations.
//
SrvRep* RPC_Server::CreateInteralServer() {
  DLOG_ASSERT(handler_ != NULL);
  DLOG_ASSERT(conf_->GetSrvId() >= 0);
  return new SrvRep(handler_, conf_->GetSrvPort(conf_->GetSrvId()));
}

} /* namespace indexfs */
