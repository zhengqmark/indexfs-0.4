// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_SERVER_FSERROR_H_
#define _INDEXFS_SERVER_FSERROR_H_

#include "common/dirguard.h"

namespace indexfs {


#define SERVER_INTERNAL_ERROR(err_msg) \
  ServerInternalError sie;             \
  sie.message = err_msg;               \
  throw sie;                           \


extern void MaybeThrowException(const Status& status);
extern void MaybeThrowRedirectException(const DirGuard& dir_guard,
        int obj_idx, int my_rank);
extern void MaybeThrowUnknownDirException(const DirGuard::DirData& dir_data);

} // namespace indexfs

#endif /* _INDEXFS_SERVER_FSERROR_H_ */
