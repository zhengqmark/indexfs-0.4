// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "ipc/membset.h"

namespace indexfs {

struct StaticMemberSet: virtual public MemberSet {
  Config* conf_;
  virtual ~StaticMemberSet() { }
  StaticMemberSet(Config* conf) : conf_(conf) { }
  virtual Status Init() { return Status::OK(); }
  virtual int TotalServers() { return conf_->GetSrvNum(); }
  virtual Status FindServer(int srv_id, std::string *ip, int *port);
};

MemberSet* CreateStaticMemberSet(Config* conf) {
  DLOG_ASSERT(conf != NULL);
  return new StaticMemberSet(conf);
}

Status StaticMemberSet::FindServer(int srv_id, std::string *ip, int *port) {
  DLOG_ASSERT(srv_id >= 0 && srv_id < TotalServers());
  const std::pair<std::string, int>& addr = conf_->GetSrvAddr(srv_id);
  *ip = addr.first;
  *port = addr.second;
  return Status::OK();
}

} /* namespace indexfs */
