// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdlib.h>
#include <unistd.h>

#include "common/config.h"
#include "common/logging.h"
#include "common/scanner.h"
#include "common/network.h"

namespace indexfs {

namespace {
// A list of IP strings
typedef const std::vector<std::string> IPList;
// A list of IP/port pairs
typedef const std::vector<std::pair<std::string, int> > IPPortList;
}

// Default log file name
static const char* DEFAULT_LOG_FILE = "indexfs";
// Default log directory
static const char* DEFAULT_LOG_DIR = "/tmp/indexfs/logs";
// Default server list file
static const char* DEFAULT_SERVER_LIST = "/tmp/indexfs/servers";
// Default configuration file
static const char* DEFAULT_CONFIG_FILE = "/tmp/indexfs/config";
// Legacy server list file used in old releases
static const char* LEGACY_SERVER_LIST = "/tmp/giga_conf";
// Legacy configuration file used in old releases
static const char* LEGACY_CONFIG_FILE = "/tmp/idxfs_conf";

// Create a new configuration object for servers
//
Config* Config::CreateServerConfig() {
  return new Config(kServer);
}

// Create a new configuration object for clients
//
Config* Config::CreateClientConfig() {
  return new Config(kClient);
}

// Create a new configuration object for batch clients
//
Config* Config::CreateBatchClientConfig() {
  return new Config(kBatchClient);
}

// Create a fresh configuration object.
//
Config::Config(InstanceType type) : instance_id_(-1), num_instances_(-1), instance_type_(type) {
}

/*---------------------------------------------------
 * Configuration
 * --------------------------------------------------
 */

Status Config::LoadNetworkInfo() {
  Status s;
  s = FetchHostname(&host_name_);
  if (!s.ok()) {
    return s;
  }
  s = GetHostIPAddrs(&ip_addrs_);
  if (!s.ok()) {
    return s;
  }
  if (IsServer()) {
    IPList::iterator it = ip_addrs_.begin();
    for (; it != ip_addrs_.end(); it++) {
      DLOG(INFO)<< "Local IP: " << *it;
    }
    DLOG(INFO)<< "Local host name: " << host_name_;
  }
  return s;
}

// Reset the instance id unconditionally.
//
Status Config::SetSrvId(int srv_id) {
  Status s;
  instance_id_ = srv_id;
  DLOG_ASSERT(IsServer());
  return s;
}

// Set client id and number of clients.
//
Status Config::SetClientInfo(int cli_id, int num_clis) {
  Status s;
  instance_id_ = cli_id;
  num_instances_ = num_clis;
  DLOG_ASSERT(IsClient());
  return s;
}

// Set batch client id and number of batch clients.
//
Status Config::SetBatchClientInfo(int cli_id, int num_clis) {
  Status s;
  instance_id_ = cli_id;
  num_instances_ = num_clis;
  DLOG_ASSERT(IsBatchClient());
  return s;
}

// Directly set the member servers by injecting a list of servers into the configuration
// object. No action will be taken if the provided server list is empty, otherwise,
// the original set of member servers will be overridden in its entirety.
// In addition to setting the servers, we will, if necessary, try to figure out our
// own server ID by comparing IP addresses of the local machine
// with IP addresses on the server list.
//
Status Config::SetServers(IPList& servers) {
  if (!servers.empty()) {
    srv_addrs_.clear();
    IPList::const_iterator it = servers.begin();
    for (; it != servers.end(); it++) {
      const std::string &ip = *it;
      srv_addrs_.push_back(std::make_pair(ip, GetDefaultSrvPort()));
      if (IsServer()) {
        for (size_t i = 0; instance_id_ < 0 && i < ip_addrs_.size(); i++) {
          if (ip == ip_addrs_[i]) {
            instance_id_ = srv_addrs_.size() - 1;
          }
        }
      }
    }
  }
  return Status::OK();
}

// Directly set the member servers by injecting a list of servers into the configuration
// object. No action will be taken if the provided server list is empty, otherwise,
// the original set of member servers will be overridden in its entirety.
// In addition to setting the servers, we will, if necessary, try to figure out our
// own server ID by comparing IP addresses of the local machine
// with IP addresses on the server list.
//
Status Config::SetServers(IPPortList& servers) {
  if (!servers.empty()) {
    srv_addrs_.clear();
    IPPortList::const_iterator it = servers.begin();
    for (; it != servers.end(); it++) {
      const std::string &ip = it->first;
      srv_addrs_.push_back(std::make_pair(ip, it->second));
      if (IsServer()) {
        for (size_t i = 0; instance_id_ < 0 && i < ip_addrs_.size(); i++) {
          if (ip == ip_addrs_[i]) {
            instance_id_ = srv_addrs_.size() - 1;
          }
        }
      }
    }
  }
  return Status::OK();
}

// Retrieve a fixed set of member servers by loading their IP addresses and port numbers
// from a user-specified server list file. If we already have a set of member servers, then
// the provided server list will be ignored and no server will be loaded.
// In addition to setting the servers, we will, if necessary, try to figure out our
// own server ID by comparing IP addresses of the local machine
// with IP addresses on the server list.
//
Status Config::LoadServerList(const char* server_list) {
  if (srv_addrs_.empty()) {
    DLOG_ASSERT(server_list != NULL);
    Scanner scanner(server_list);
    if (!scanner.IsOpen()) {
      return Status::IOError("Cannot open file", std::string(server_list));
    }
    std::string ip, port;
    while (scanner.HasNextLine()) {
      if (scanner.NextServerAddress(&ip, &port)) {
        srv_addrs_.push_back(std::make_pair(ip,
                port.empty() ? GetDefaultSrvPort() : atoi(port.c_str())));
        if (IsServer()) {
          for (size_t i = 0; instance_id_ < 0 && i < ip_addrs_.size(); i++) {
            if (ip == ip_addrs_[i]) {
              instance_id_ = srv_addrs_.size() - 1;
            }
          }
        }
      }
    }
    if (srv_addrs_.empty()) {
      return Status::Corruption("Empty server list", std::string(server_list));
    }
  }
  return Status::OK();
}

Status Config::VerifyInstanceInfoAndServerList() {
  Status s;
  switch (instance_type_) {
    case kClient:
      break;
    case kServer:
      num_instances_ = srv_addrs_.size();
      if (instance_id_ < 0 ||
          instance_id_ >= num_instances_) {
        s = Status::Corruption("Invalid server id");
      }
      break;
    case kBatchClient:
      if (instance_id_ < 0) {
        s = Status::Corruption("Invalid batch client id");
      }
      if (num_instances_ < 0) {
        s = Status::Corruption("Invalid total number of batch clients");
      }
      break;
    default:
      s = Status::Corruption("Unknown instance type");
  }
  if (s.ok() && IsServer()) {
    DLOG_ASSERT(num_instances_ == srv_addrs_.size());
    IPPortList::iterator it = srv_addrs_.begin();
    for (; it != srv_addrs_.end(); it++) {
      int i = it - srv_addrs_.begin();
      if (!IsServer() || instance_id_ != i) {
        DLOG(INFO)<< "Server " << i << ": " << it->first << ":" << it->second;
      } else {
        DLOG(INFO)<< "Server " << i << ": " << it->first << ":" << it->second << " (me)";
      }
    }
  }
  return s;
}

namespace {
DEFINE_string(file_dir, "", "set large file directory");
DEFINE_string(db_root, "", "set DB root directory");
DEFINE_string(db_split, "", "set DB split directory");
DEFINE_string(db_home, "", "Set DB home directory");
DEFINE_string(db_home_str, "", "Set DB home directory string");
DEFINE_string(db_data, "", "Set DB data directory");
DEFINE_string(db_data_str, "", "Set DB data directory string");
DEFINE_int32(db_data_partitions, -1, "set number of partitions for DB data directory");
}

static void LoadFromFile(const char* config_file,
                         std::map<std::string, std::string>* confs) {
  if (config_file != NULL) {
    Scanner scanner(config_file);
    if (scanner.IsOpen()) {
      std::string key, value;
      while (scanner.HasNextLine()) {
        if (scanner.NextKeyValue(&key, &value)) {
          (*confs)[key] = value;
        }
      }
    }
  }
}

static std::string GetDBHomeString(Config* config) {
  if (!(FLAGS_db_home_str.empty())) {
    return FLAGS_db_home_str;
  }
  if (config->IsServer()) {
    return "s";
  }
  if (config->IsBatchClient()) {
    return "bk";
  }
  return "tmp";
}

static std::string GetDBDataString(Config* config) {
  if (!(FLAGS_db_data_str.empty())) {
    return FLAGS_db_data_str;
  }
  return "l";
}

Status Config::LoadOptionsFromFile(const char* config_file) {
  std::map<std::string, std::string> confs;
  LoadFromFile(config_file, &confs);

  file_dir_ = FLAGS_file_dir;
  if (file_dir_.empty()) {
    file_dir_ = confs["file_dir"];
    if (file_dir_.empty()) {
      return Status::NotFound("Missing option", "file_dir");
    }
  }
  db_root_ = FLAGS_db_root;
  if (db_root_.empty()) {
    if (!confs["leveldb_dir"].empty()) {
      db_root_ = confs["leveldb_dir"];
    }
    if (!confs["db_root"].empty()) {
      db_root_ = confs["db_root"];
    }
    if (db_root_.empty()) {
      return Status::NotFound("Missing option", "leveldb_dir");
    }
  }

  db_split_ = FLAGS_db_split;
  if (db_split_.empty()) {
    if (!confs["split_dir"].empty()) {
      db_split_ = confs["split_dir"];
    }
    if (!confs["db_split"].empty()) {
      db_split_ = confs["db_split"];
    }
    if (db_split_.empty()) {
      db_split_ = db_root_;
    }
  }
  std::string db_home = FLAGS_db_home;
  if (db_home.empty()) {
    db_home = confs["db_home"];
    if (db_home.empty()) {
      db_home = db_root_ + "/" + GetDBHomeString(this);
    }
  }
  std::stringstream ss;
  ss << db_home;
  if (instance_id_ >= 0) {
    ss << instance_id_;
  }
  db_home_ = ss.str();
  std::string db_data = FLAGS_db_data;
  if (db_data.empty()) {
    db_data = confs["data_dir"];
    if (db_data.empty()) {
      db_data = db_root_ + "/" + GetDBDataString(this);
    }
  }
  db_data_ = std::make_pair(db_data, FLAGS_db_data_partitions);

  if (IsServer()) {
    DLOG(INFO)<< "DB root directory: " << db_root_;
    DLOG(INFO)<< "DB split directory: " << db_split_;
    DLOG(INFO)<< "DB home directory: " << db_home_;
    if (!HasOldData()) {
      DLOG(INFO) << "Previous DB data directory: N/A";
    } else {
      DLOG(INFO) << "Previous DB data directory: "
              << db_data_.first << "[0-" << db_data_.second - 1 << "]";
    }
  }
  return Status::OK();
}

/*---------------------------------------------------
 * Main Interface
 * --------------------------------------------------
 */

namespace {
static inline
void CheckErrors(const Status& s) {
  CHECK(s.ok()) << s.ToString();
}
}

// Create and prepare a configuration object for servers.
//
Config* LoadServerConfig(int srv_id, IPPortList& servers) {
  Config* srv_conf = Config::CreateServerConfig();
  CheckErrors(srv_conf->SetSrvId(srv_id));

  CheckErrors(srv_conf->LoadNetworkInfo());
  CheckErrors(srv_conf->SetServers(servers));
  CheckErrors(srv_conf->LoadServerList(GetServerListFileName()));
  CheckErrors(srv_conf->VerifyInstanceInfoAndServerList());

  CheckErrors(srv_conf->LoadOptionsFromFile(GetConfigFileName()));
  Logger::FlushLogFiles(); // To ease debugging
  return srv_conf;
}

// Create and prepare a configuration object for clients.
//
Config* LoadClientConfig(int cli_id,
                         int num_clis,
                         IPList& servers,
                         const std::string& server_list,
                         const std::string& config_file) {
  Config* cli_conf = Config::CreateClientConfig();
  CheckErrors(cli_conf->SetClientInfo(cli_id, num_clis));

  if (!server_list.empty())
    FLAGS_srvlstfn = server_list;
  if (!config_file.empty())
    FLAGS_configfn = config_file;

  CheckErrors(cli_conf->LoadNetworkInfo());
  CheckErrors(cli_conf->SetServers(servers));
  CheckErrors(cli_conf->LoadServerList(GetServerListFileName()));
  CheckErrors(cli_conf->VerifyInstanceInfoAndServerList());

  CheckErrors(cli_conf->LoadOptionsFromFile(GetConfigFileName()));
  Logger::FlushLogFiles(); // To ease debugging
  return cli_conf;
}

// Create and prepare a configuration object for batch clients.
//
Config* LoadBatchClientConfig(int cli_id,
                              int num_clis,
                              IPList& servers,
                              const std::string& server_list,
                              const std::string& config_file) {
  Config* cli_conf = Config::CreateBatchClientConfig();
  CheckErrors(cli_conf->SetBatchClientInfo(cli_id, num_clis));

  if (!server_list.empty())
    FLAGS_srvlstfn = server_list;
  if (!config_file.empty())
    FLAGS_configfn = config_file;

  CheckErrors(cli_conf->LoadNetworkInfo());
  CheckErrors(cli_conf->SetServers(servers));
  CheckErrors(cli_conf->LoadServerList(GetServerListFileName()));
  CheckErrors(cli_conf->VerifyInstanceInfoAndServerList());

  CheckErrors(cli_conf->LoadOptionsFromFile(GetConfigFileName()));
  Logger::FlushLogFiles(); // To ease debugging
  return cli_conf;
}

// Attempting to figure out the name of the log file. Try the command
// line argument first, then environmental variables, and then resort
// to a hard-coded default log file name.
//
const char* GetLogFileName() {
  if (!FLAGS_logfn.empty()) {
    return FLAGS_logfn.c_str();
  }
  const char* env = getenv("IDXFS_LOG_NAME");
  if (env != NULL) {
    return env;
  }
  LOG(INFO)<< "No log file name specified -- use \"" << DEFAULT_LOG_FILE << "\" by default";
  return DEFAULT_LOG_FILE;
}

// Trying to figure out the path of the configuration file. Consult
// the command line argument first, then environmental variables, and
// then resort to a hard-coded legacy file path, if possible.
//
const char* GetConfigFileName() {
  if (!FLAGS_configfn.empty()) {
    if (access(FLAGS_configfn.c_str(), R_OK) == 0) {
      return FLAGS_configfn.c_str();
    }
    LOG(INFO)<< "No config file found at "<< FLAGS_configfn;
  }
  const char* env = getenv("IDXFS_CONFIG_FILE");
  if (env != NULL) {
    if (access(env, R_OK) == 0) {
      return env;
    }
    LOG(INFO) << "No config file found at " << env;
  }
  if (access(LEGACY_CONFIG_FILE, R_OK) == 0) {
    return LEGACY_CONFIG_FILE;
  } else {
    LOG(INFO) << "No config file found at" << LEGACY_CONFIG_FILE;
  }
  // No configuration file found
  // All configurations go to the command line
  return NULL;
}

// Trying to figure out the path of the server list file. Consult
// the command line argument first, then environmental variables, and
// then resort to a hard-coded legacy file path, if possible.
//
const char* GetServerListFileName() {
  if (!FLAGS_srvlstfn.empty()) {
    if (access(FLAGS_srvlstfn.c_str(), R_OK) == 0) {
      return FLAGS_srvlstfn.c_str();
    }
    LOG(INFO)<< "No server list found at " << FLAGS_srvlstfn;
  }
  const char* env = getenv("IDXFS_SERVER_LIST");
  if (env != NULL) {
    if (access(env, R_OK) == 0) {
      return env;
    }
    LOG(INFO) << "No server list found at " << FLAGS_srvlstfn;
  }
  if (access(LEGACY_SERVER_LIST, R_OK) == 0) {
    return LEGACY_SERVER_LIST;
  } else {
    LOG(INFO) << "No server list found at " << LEGACY_SERVER_LIST;
  }
  LOG(ERROR) << "Fail to locate server list, will commit suicide now!";
  exit(EXIT_FAILURE);
}

// The default log directory used to reset
// glog's default log directory, which is often "/tmp".
// NB: glog will not try to create any parent directories of its log files.
// Make sure they exist before glog attempts to create any log files.
//
const char* GetDefaultLogDir() {
  return DEFAULT_LOG_DIR;
}

// The default name of the log file to be generated by IndexFS.
//
const char* GetDefaultLogFileName() {
  return DEFAULT_LOG_FILE;
}

// The default location of the configuration file.
//
const char* GetDefaultConfigFileName() {
  return DEFAULT_CONFIG_FILE;
}

// The default location of the server list file.
//
const char* GetDefaultServerListFileName() {
  return DEFAULT_SERVER_LIST;
}

// Create a new configuration object for servers testing.
//
Config* Config::CreateServerTestingConfig(const std::string& srv_dir) {
  Config* config = new Config(kServer);
  config->instance_id_ = 0;
  config->host_name_ = "localhost";
  config->file_dir_ = srv_dir + "/file";
  config->db_root_ = srv_dir + "/leveldb";
  config->db_home_ = config->db_root_ + "/l0";
  config->db_split_ = config->db_root_ + "/split";
  config->ip_addrs_.push_back("127.0.0.1");
  config->srv_addrs_.push_back(std::make_pair("127.0.0.1", config->GetDefaultSrvPort()));
  return config;
}

// Returns the global system environment object according to
// runtime configuration and compile-time backend switch.
//
Env* GetSystemEnv(Config* config) {
  Env* env = Env::Default();
  if (config->IsServer() || config->IsBatchClient()) {
#   ifdef PVFS
      env = GetOrNewPvfsEnv();
#   endif
#   ifdef HDFS
      env = GetOrNewHdfsEnv();
#   endif
#   ifdef RADOS
      using leveldb::RadosOptions;
      RadosOptions opt;
      opt.read_only = false;
      opt.db_root = config->GetDBRootDir().c_str();
      opt.db_home = config->GetDBHomeDir().c_str();
      env = GetOrNewRadosEnv(opt);
#   endif
  }
  return env;
}

} /* namespace indexfs */

// Log file name
DEFINE_string(logfn, indexfs::DEFAULT_LOG_FILE, "set log file name");
// Server list
DEFINE_string(srvlstfn, indexfs::DEFAULT_SERVER_LIST, "set server list file");
// Configuration file
DEFINE_string(configfn, indexfs::DEFAULT_CONFIG_FILE, "set server config file");
