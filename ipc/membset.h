// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_COMM_MEMBSET_H_
#define _INDEXFS_COMM_MEMBSET_H_

#include "common/common.h"
#include "common/config.h"
#include "common/logging.h"

namespace indexfs {

struct MemberSet {
  virtual ~MemberSet() { }
  virtual Status Init() = 0;
  virtual int TotalServers() = 0;
  virtual Status FindServer(int srv_id, std::string *ip, int *port) = 0;
};

extern MemberSet* CreateZKMemberSet(Config* conf);

extern MemberSet* CreateStaticMemberSet(Config* conf);

} /* namespace indexfs */

#endif /* _INDEXFS_COMM_MEMBSET_H_ */
