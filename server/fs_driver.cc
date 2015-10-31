// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <vector>

#include "ipc/rpc.h"
#include "server/fs_driver.h"
#include "server/index_server.h"

namespace indexfs {

namespace {
static const char* kOpNames[kNumSrvOps] = {
  "access",
  "renew",
  "getattr",
  "mknod",
  "mkdir",
  "createentry",
  "createzeroth",
  "chmod",
  "chown",
  "remove",
  "rename",
  "readdir",
  "readbitmap",
  "updatebitmap",
  "split",
  "insertsplit",
  "open",
  "read",
  "write",
  "close",
};
}

Monitor* CreateMonitorForServer(int server_id) {
  return new Monitor(kOpNames, kNumSrvOps, server_id);
}


namespace {
static
void PrepareDirectory(Env* env,
                      const std::string& dir_name) {
  Status s;
  if (!env->FileExists(dir_name)) {
    s = env->CreateDir(dir_name);
  }
  bool dir_exists = env->FileExists(dir_name);
  CHECK(dir_exists) << "Fail to create dir: " << s.ToString();
}
}

class IndexFSDriver: virtual public ServerDriver {
 public:

  explicit IndexFSDriver(Env* env, Config* config)
    : ServerDriver(env, config),
      rpc_(NULL), rpc_srv_(NULL), index_ctx_(NULL), index_srv_(NULL),
      monitor_(NULL), monitor_thread_(NULL) {
  }

  virtual ~IndexFSDriver() {
    delete monitor_thread_;
    delete index_srv_;
    delete index_ctx_;
    delete monitor_;
    delete rpc_;
    delete rpc_srv_;
  }

  void Start() {
    DLOG_ASSERT(monitor_thread_ != NULL);
    monitor_thread_->Start();
    DLOG_ASSERT(rpc_srv_ != NULL);
    rpc_srv_->RunForever();
  }

  void Shutdown() {
    DLOG_ASSERT(rpc_srv_ != NULL);
    rpc_srv_->Stop();
    DLOG_ASSERT(monitor_thread_ != NULL);
    monitor_thread_->Shutdown();
  }

  void PrepareContext() {
    PrepareDirectory(env_, config_->GetFileDir());
    PrepareDirectory(env_, config_->GetDBRootDir());
    PrepareDirectory(env_, config_->GetDBHomeDir());
    PrepareDirectory(env_, config_->GetDBSplitDir());
    DLOG_ASSERT(index_ctx_ == NULL);
    index_ctx_ = new IndexContext(env_, config_);
    Status s = index_ctx_->Open();
    CHECK(s.ok()) << "Fail to create index context: " << s.ToString();
    DLOG(INFO) << "IndexFS server context initialized";
  }

  void OpenServer() {
    DLOG_ASSERT(index_ctx_ != NULL && monitor_ != NULL);
    DLOG_ASSERT(rpc_ == NULL);
    rpc_ = RPC::CreateRPC(config_);
    DLOG_ASSERT(index_srv_ == NULL);
    index_srv_ = new IndexServer(index_ctx_, monitor_, rpc_);
    DLOG_ASSERT(rpc_srv_ == NULL);
    rpc_srv_ = new RPC_Server(config_, index_srv_);
    DLOG(INFO) << "IndexFS is ready for service, listening to incoming clients ... ";
  }

  void SetupMonitoring() {
    DLOG_ASSERT(monitor_ == NULL);
    monitor_ = CreateMonitorForServer(config_->GetSrvId());
    DLOG_ASSERT(monitor_thread_ == NULL);
    monitor_thread_ = new MonitorThread(monitor_);
  }

 private:
  RPC* rpc_;
  RPC_Server* rpc_srv_;
  IndexContext* index_ctx_;
  IndexServer* index_srv_;
  Monitor* monitor_;
  MonitorThread* monitor_thread_;

  // No copying allowed
  IndexFSDriver(const IndexFSDriver&);
  IndexFSDriver& operator=(const IndexFSDriver&);
};


ServerDriver::ServerDriver(Env* env, Config* config) :
  env_(env), config_(config) {
}

ServerDriver* ServerDriver::NewIndexFSDriver(Env* env, Config* config) {
  return new IndexFSDriver(env, config);
}

} // namespace indexfs
