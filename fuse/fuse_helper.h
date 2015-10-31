// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdio.h>
#include <pthread.h>

#include "client/client.h"
#include "common/config.h"
#include "common/logging.h"

namespace indexfs {

DEFINE_string(logfn, "libclient", "");
DEFINE_string(configfn, GetDefaultConfigFileName(), "");
DEFINE_string(srvlstfn, GetDefaultServerListFileName(), "");

#ifdef HDFS
DEFINE_string(hconfigfn, GetDefaultHDFSConfigFileName(), "");
#endif

class IDXClientManager {
 public:

  IDXClientManager() {
    Init();
    pthread_key_create(&cli_key, NULL);
  }
  virtual ~IDXClientManager() {
    Dispose();
  }

  Status GetThreadLoadClient(Client** cliptr) {
    Client* client = (Client*) pthread_getspecific(cli_key);

    if (client != NULL) {
      (*cliptr) = client;
      return Status::OK();
    }

    ClientFactory* factory = GetDefaultClientFactory();
    client = factory->GetClient(LoadClientConfig());
    delete factory;

    Status s = client->Init();
    if (!s.ok()) {
      return s;
    }
    (*cliptr) = client;
    pthread_setspecific(cli_key, client);
    return Status::OK();
  }

 private:

  void Init() {
    Logger::Initialize(NULL);
  }

  void Dispose() {
    Logger::Shutdown();
  }

  pthread_key_t cli_key;
};

} /* namespace indexfs */
