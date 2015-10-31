// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_COMMON_CONFIG_H_
#define _INDEXFS_COMMON_CONFIG_H_

#include "common/common.h"
#include "common/options.h"

#include <map>
#include <vector>
#include <string>

#include <string.h>
#include <stdlib.h>
#include <gflags/gflags.h>

namespace indexfs {

// Instance type.
//
enum InstanceType {
  kServer,
  kClient,
  kBatchClient,
};

// The main configuration interface shared by both clients and servers
//
class Config {

  explicit Config(InstanceType type);

  // Unique instance id representing either a server id
  // or a batch client id, or -1 if running as a regular client.
  //
  int instance_id_;

  // Total number of instances (either client or server)
  //
  int num_instances_;

  // Instance type.
  //
  InstanceType instance_type_;

  // Local machine host name
  //
  std::string host_name_;

  // Local machine IP addresses
  // We assume our local machines may have multiple NICs,
  // so we must consider all of them
  //
  std::vector<std::string> ip_addrs_;

  // Server address list
  //
  std::vector<std::pair<std::string, int> > srv_addrs_;

  // Data directory for large files
  //
  std::string file_dir_;

  // Root directory for METADATA persistence
  //
  std::string db_root_;

  // Home directory for LevelDB
  //
  std::string db_home_;

  // Temporary directory for transient files
  // generated during dynamic directory splitting
  //
  std::string db_split_;

  // A set of SSTable-holding directories collectively
  // representing an immutable file system namespace populated by a
  // previous job or workflow phase.
  //
  std::pair<std::string, int> db_data_;

 public:

  virtual ~Config() { }

  // Initializes an empty config object for servers.
  //
  static Config* CreateServerConfig();

  // Initializes an empty config object for clients.
  //
  static Config* CreateClientConfig();

  // Initializes an empty config object for batch clients.
  //
  static Config* CreateBatchClientConfig();

  // Creates a configuration object for server testing. All configuration items
  // are deterministically assigned and no configuration files will be read.
  //
  static Config* CreateServerTestingConfig(const std::string &srv_dir="/tmp");

  // Returns the server id, or -1 for non-server instances.
  //
  int GetSrvId() { return IsServer() ? instance_id_ : -1; }

  // Returns the client id, or -1 if not running as a normal client.
  //
  int GetClientId() { return IsClient() ? instance_id_ : -1; }

  // Returns the batch client id, or -1 if not running as a batch client.
  //
  int GetBatchClientId() { return IsBatchClient() ? instance_id_ : -1; }

  // Returns the number of clients, or -1 if not known.
  //
  int GetNumClients() { return IsClient() || IsBatchClient() ? num_instances_ : -1; }

  // Returns true iff running as a server.
  //
  bool IsServer() { return instance_type_ == kServer; }

  // Returns true iff running as a regular client.
  //
  bool IsClient() { return instance_type_ == kClient; }

  // Returns true iff running as a batch client.
  //
  bool IsBatchClient() { return instance_type_ == kBatchClient; }

  // Returns the host name of the local machine.
  //
  const std::string& GetHostname() { return host_name_; }

  // Returns the total number of servers
  //
  int GetSrvNum() { return srv_addrs_.size(); }

  // Returns the default port number.
  //
  int GetDefaultSrvPort() { return DEFAULT_SRV_PORT; }

  // Returns the IP address of a given server.
  //
  const std::string& GetSrvIP(int srv_id) { return srv_addrs_[srv_id].first; }

  // Returns the port number of a given server.
  //
  int GetSrvPort(int srv_id) { return srv_addrs_[srv_id].second; }

  // Returns the IP address and port number of a given server.
  //
  const std::pair<std::string, int>& GetSrvAddr(int srv_id) { return srv_addrs_[srv_id]; }

  // Returns the storage directory for user file data
  //
  const std::string& GetFileDir() { return file_dir_; }

  // Returns the root directory for LevelDB
  //
  const std::string& GetDBRootDir() { return db_root_; }

  // Returns the home directory for LevelDB
  //
  const std::string& GetDBHomeDir() { return db_home_; }

  // Returns the temporary directory for directory splitting
  //
  const std::string& GetDBSplitDir() { return db_split_; }

  // Returns true iff we have an existing namespace to load first
  //
  bool HasOldData() { return db_data_.second > 0; }

  // Returns a set of data directories holding previous DB states.
  //
  const std::pair<std::string, int>& GetDBDataDirs() { return db_data_; }

  // Returns the threshold for directory splitting
  //
  int GetSplitThreshold() {
    const char* env = getenv("FS_DIR_SPLIT_THR");
    int result = ( env != NULL ? atoi(env) : DEFAULT_DIR_SPLIT_THR );
    return result > 0 ? result : DEFAULT_DIR_SPLIT_THR;
  }

  // Returns the max number of entries that could be bulk inserted into
  // a shadow namespace.
  //
  int GetBulkSize() {
    const char* env = getenv("FS_BULK_SIZE");
    int result = ( env != NULL ? atoi(env) : DEFAULT_BULK_SIZE );
    return result > 0 ? result : DEFAULT_BULK_SIZE;
  }

  // Returns the max number of directories that could be bulk inserted into
  // a shadow namespace.
  //
  int GetDirBulkSize() {
    const char* env = getenv("FS_DIR_BULK_SIZE");
    int result = ( env != NULL ? atoi(env) : DEFAULT_DIR_BULK_SIZE );
    return result > 0 ? result : DEFAULT_DIR_BULK_SIZE;
  }

  // Returns the size of the directory mapping cache.
  //
  int GetDirMappingCacheSize() {
    const char* env = getenv("FS_DMAP_CACHE_SIZE");
    int result = ( env != NULL ? atoi(env) : DEFAULT_DMAP_CACHE_SIZE );
    return result > 0 ? result : DEFAULT_DMAP_CACHE_SIZE;
  }

  // Returns the size of the directory entry cache.
  //
  int GetDirEntryCacheSize() {
    const char* env = getenv("FS_DENT_CACHE_SIZE");
    int result = ( env != NULL ? atoi(env) : DEFAULT_DENT_CACHE_SIZE );
    return result > 0 ? result : DEFAULT_DENT_CACHE_SIZE;
  }

  Status VerifyInstanceInfoAndServerList();
  Status LoadNetworkInfo();
  Status LoadServerList(const char* server_list);
  Status LoadOptionsFromFile(const char* config_file);

  Status SetClientInfo(int cli_id, int num_clis);
  Status SetBatchClientInfo(int cli_id, int num_clis);
  Status SetSrvId(int srv_id);
  Status SetServers(const std::vector<std::string>& servers);
  Status SetServers(const std::vector<std::pair<std::string, int> >& servers);
};

} /* namespace indexfs */

// file name for the main log file
DECLARE_string(logfn);

// file name for the configuration file
DECLARE_string(configfn);

// file name for the server list file
DECLARE_string(srvlstfn);

namespace indexfs {

/*---------------------------------------------------
 * Command Line Helpers
 * --------------------------------------------------
 */

extern const char* GetLogFileName();
extern const char* GetDefaultLogDir();
extern const char* GetDefaultLogFileName();

extern const char* GetConfigFileName();
extern const char* GetDefaultConfigFileName();

extern const char* GetServerListFileName();
extern const char* GetDefaultServerListFileName();

/*---------------------------------------------------
 * System Environment
 * --------------------------------------------------
 */

// Retrieves the underlying system environment representing different
// operating systems and object storage systems such as POSIX, HDFS, and PVFS.
//
extern Env* GetSystemEnv(Config* config);

/*---------------------------------------------------
 * Main Interface
 * --------------------------------------------------
 */

// This function should be called at the server's bootstrapping phase.
// It will load important system options from a set of configuration files
// whose locations are determined by a set of command line arguments or
// environmental variables. Callers of this function can optionally choose to
// explicitly specify the server ID and an initial set of member servers,
// bypassing or superseding the configuration files.
//
extern Config* LoadServerConfig(
    int srv_id = -1,
    const std::vector<std::pair<std::string, int> >& servers
        = std::vector<std::pair<std::string, int> >());

// This function should be called at the client's bootstrapping phase.
// It will load important system options from a set of configuration files
// whose locations are either given as arguments here or specified by a set of
// command-line arguments or environmental variables. Callers of this
// function can optionally choose to explicitly specify an initial set of
// member servers, bypassing or superseding the configuration files.
//
extern Config* LoadClientConfig(int cli_id, int num_clis,
    const std::vector<std::string>& servers = std::vector<std::string>(),
    const std::string& server_list = std::string(),
    const std::string& config_file = std::string());

// This function should be called at the client's bootstrapping phase.
// It will load important system options from a set of configuration files
// whose locations are either given as arguments here or specified by a set of
// command-line arguments or environmental variables. The caller must
// specify an unique id for each batch client. In addition, Callers of this
// function can optionally choose to explicitly specify an initial set of
// member servers, bypassing or superseding the configuration files.
//
extern Config* LoadBatchClientConfig(int cli_id, int num_clis,
    const std::vector<std::string>& servers = std::vector<std::string>(),
    const std::string& server_list = std::string(),
    const std::string& config_file = std::string());

} /* namespace indexfs */

#endif /* _INDEXFS_COMMON_CONFIG_H_ */
