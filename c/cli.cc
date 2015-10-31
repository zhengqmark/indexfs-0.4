// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "c/cli.h"

#include <time.h>
#include <stdio.h>
#include <fcntl.h>

#include "client/client.h"
#include "common/config.h"
#include "common/logging.h"

using ::indexfs::Slice;
using ::indexfs::Config;
using ::indexfs::Logger;
using ::indexfs::StatInfo;
using ::indexfs::Status;
using ::indexfs::Client;
using ::indexfs::ClientFactory;
using ::indexfs::LoadClientConfig;
using ::indexfs::ParseCommandLineFlags;

extern "C" {

namespace {
static
int CheckErrors(const char* func,
                const Status& s) {
  if (!s.ok()) {
    LOG(ERROR) << "(" << func << ") - " << s.ToString();
    return -1;
  }
  return 0;
}
}

void idxfs_parse_arguments(int *argc, char*** argv) {
  ParseCommandLineFlags(argc, argv, true);
}

void idxfs_log_init() {
  Logger::Initialize(NULL);
}

void idxfs_log_close() {
  Logger::Shutdown();
}

int idxfs_mknod(cli_t* cli, const char* path, mode_t mode) {
  Status s;
  std::string p = path;
  Client* client = (Client*) cli->rep;
  s = client->Mknod(p, mode);
  return CheckErrors(__func__, s);
}

int idxfs_mkdir(cli_t* cli, const char* path, mode_t mode) {
  Status s;
  std::string p = path;
  Client* client = (Client*) cli->rep;
  s = client->Mkdir(p, mode);
  return CheckErrors(__func__, s);
}

int idxfs_rmdir(cli_t* cli, const char* path) {
  return CheckErrors(__func__, Status::Corruption(Slice()));
}

int idxfs_delete(cli_t* cli, const char* path) {
  return CheckErrors(__func__, Status::Corruption(Slice()));
}

int idxfs_exists(cli_t* cli, const char* path) {
  Status s;
  std::string p = path;
  Client* client = (Client*) cli->rep;
  StatInfo info;
  s = client->Getattr(p, &info);
  return CheckErrors(__func__, s);
}

int idxfs_dir_exists(cli_t* cli, const char* path) {
  Status s;
  std::string p = path;
  Client* client = (Client*) cli->rep;
  s = client->AccessDir(p);
  return CheckErrors(__func__, s);
}

int idxfs_getinfo(cli_t* cli, const char* path, info_t* _return) {
  Status s;
  std::string p = path;
  Client* client = (Client*) cli->rep;
  StatInfo info;
  s = client->Getattr(p, &info);
  if (s.ok()) {
    _return->mode = info.mode;
    _return->size = info.size;
    _return->uid = info.uid;
    _return->gid = info.gid;
    _return->mtime = info.mtime;
    _return->ctime = info.ctime;
    _return->is_dir = S_ISDIR(info.mode);
  }
  return CheckErrors(__func__, s);
}

int idxfs_chmod(cli_t* cli, const char* path,
                mode_t mode, bool* is_dir) {
  Status s;
  std::string p = path;
  Client* client = (Client*) cli->rep;
  s = client->Chmod(p, mode, is_dir);
  return CheckErrors(__func__, s);
}

int idxfs_chown(cli_t* cli, const char* path,
                uid_t owner, gid_t group, bool* is_dir) {
  Status s;
  std::string p = path;
  Client* client = (Client*) cli->rep;
  s = client->Chown(p, owner, group, is_dir);
  return CheckErrors(__func__, s);
}

int idxfs_readdir(cli_t* cli, const char* path,
                  readdir_handler_t handler, void* arg) {
  Status s;
  std::string p = path;
  Client* client = (Client*) cli->rep;
  std::vector<std::string> names;
  s = client->ReadDir(p, &names);
  if (s.ok() && handler != NULL) {
    std::vector<std::string>::iterator it = names.begin();
    for (; it != names.end(); ++it) {
      (*handler)(it->c_str(), arg);
    }
  }
  return CheckErrors(__func__, s);
}

namespace {
static
Config* CreateConfig(conf_t* conf) {
  if (conf == NULL) {
    return LoadClientConfig(-1, -1);
  }
  std::vector<std::string> servers;
  return LoadClientConfig(-1, -1, servers,
          conf->config_file == NULL ? "" : conf->config_file,
          conf->server_list == NULL ? "" : conf->server_list);
}
}

int idxfs_destroy_client(cli_t* cli) {
  Status s;
  if (cli != NULL) {
    Client* client = (Client*) cli->rep;
    s = client->Dispose();
    delete client;
    delete cli;
  }
  return CheckErrors(__func__, s);
}

int idxfs_create_client(conf_t* conf, cli_t** _return) {
  Status s;
  *_return = NULL;
  Config* config = CreateConfig(conf);
  Client* client = ClientFactory::GetClient(config);
  s = client->Init();
  if (s.ok()) {
    cli_t* cli = new cli_t;
    cli->rep = client;
    *_return = cli;
  } else {
    delete client;
  }
  return CheckErrors(__func__, s);
}

} /* end extern "C" */
