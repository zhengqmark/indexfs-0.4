// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "common/config.h"
#include "common/logging.h"
#include "client/libclient.h"

// libclient's C based interface has already been fully
// self-implemented in the client module. Here we only need to
// deal with certain gflag and glog related issues  :-)

namespace indexfs {

DEFINE_string(configfn, "", /* empty */
    "please use ENV 'IDXFS_CONFIG_FILE' to set this option");

DEFINE_string(srvlstfn, "", /* empty */
    "please use ENV 'IDXFS_SERVER_LIST' to set this option");

#ifdef HDFS
DEFINE_string(hconfigfn, "", /* empty */
    "please use ENV 'IDXFS_HDFS_CONFIG_FILE' to set this option");
#endif

} /* namespace indexfs */

// GFlags has been effectively disabled, since md_test has its own command
// line parsing logic, integrate that with gflag could be pain and error-prone
