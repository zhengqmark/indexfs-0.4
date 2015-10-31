// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_IPC_RPCIFACE_H_
#define _INDEXFS_IPC_RPCIFACE_H_

#include "common/common.h"
#include "common/config.h"
#include "common/logging.h"

#include "ipc/membset.h"
#include "thrift/MetadataIndexService.h"

namespace indexfs {

// Internal RPC implementations
//
class SrvRep;
class CliRep;
class FTCliRepWrapper;

class RPC {
 public:

  Status Init();
  Status Shutdown();

  static RPC* CreateRPC(Config* conf) {
    return new RPC(conf);
  }

  int TotolServers() { return member_set_->TotalServers(); }

  virtual ~RPC();
  Mutex* GetMutex(int srv_id);
  MetadataIndexServiceIf* GetClient(int srv_id);
  Status GetMetadataService(int srv_id, MetadataIndexServiceIf** _return);

 private:

  explicit RPC(Config* conf, MetadataIndexServiceIf* self = NULL)
    : conf_(conf)
    , self_(self) {
    member_set_ = CreateStaticMemberSet(conf_);
    int total_server = member_set_->TotalServers();
    mtxes_ = new Mutex[total_server];
    clients_ = new FTCliRepWrapper*[total_server];
    for (int i = 0; i < total_server; i++) {
      clients_[i] = CreateClientIfNotLocal(i);
    }
  }

  Config* conf_;
  MemberSet* member_set_;
  MetadataIndexServiceIf* self_;

  // Advisory lock for RPC client sharing in MT contexts
  Mutex* mtxes_;

  FTCliRepWrapper** clients_;
  bool IsServerLocal(int srv_id);
  FTCliRepWrapper* CreateClientFor(int srv_id);
  FTCliRepWrapper* CreateClientIfNotLocal(int srv_id);

  // No copy allowed
  RPC(const RPC&);
  RPC& operator=(const RPC&);
};

class RPC_Client {
 public:

  Status Init();
  Status Shutdown();

  RPC_Client(Config* conf, int srv_id)
    : conf_(conf), srv_id_(srv_id), member_set_(NULL), client_(NULL) {
    member_set_ = CreateStaticMemberSet(conf_);
    client_ = CreateInternalClient();
  }

  virtual ~RPC_Client();

  MetadataIndexServiceIf* operator->();
  Status GetMetadataService(MetadataIndexServiceIf** _return);

 private:

  Config* conf_;
  int srv_id_;
  MemberSet* member_set_;

  FTCliRepWrapper* client_;
  FTCliRepWrapper* CreateInternalClient();

  // No copying allowed
  RPC_Client(const RPC_Client&);
  RPC_Client& operator=(const RPC_Client&);
};

class RPC_Server {
 public:

  void Stop();
  void RunForever();

  RPC_Server(Config* conf, MetadataIndexServiceIf* handler)
    : conf_(conf), handler_(handler) {
    server_ = CreateInteralServer();
  }

  virtual ~RPC_Server();

 private:

  Config* conf_;
  MetadataIndexServiceIf* handler_;

  // Server implementation
  SrvRep* server_;
  SrvRep* CreateInteralServer();

  // No copy allowed
  RPC_Server(const RPC_Server&);
  RPC_Server& operator=(const RPC_Server&);
};

} /* namespace indexfs */

#endif /* _INDEXFS_IPC_RPCIFACE_H_ */
