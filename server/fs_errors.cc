// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "common/logging.h"
#include "server/fs_errors.h"
#include "thrift/indexfs_types.h"

namespace indexfs {

namespace {
static void
ThrowExceptionByStatus(const Status& err) {
  DLOG_ASSERT(!err.ok());
  if (err.IsNotFound())
    throw FileNotFoundException();
  if (err.IsAlreadyExists())
    throw FileAlreadyExistsException();
  if (err.IsInvalidArgument())
    throw WrongServerError();
  if (err.IsIOError()) {
    IOError ioe;
    ioe.message = err.ToString();
    throw ioe;
  }
  if (err.IsCorruption()) {
    ServerInternalError sie;
    sie.message = err.ToString();
    throw sie;
  }
  LOG(FATAL) << "Unknown server error: " << err.ToString();
}
}

void MaybeThrowException(const Status& status) {
  if (!status.ok()) {
    ThrowExceptionByStatus(status);
  }
}

void MaybeThrowRedirectException(const DirGuard& dir_guard,
        int obj_idx, int my_rank) {
  if (dir_guard.ToServer(obj_idx) != my_rank) {
    ServerRedirectionException srv_re;
    dir_guard.PutDirIndex(&srv_re.dmap_data);
    throw srv_re;
  }
}

void MaybeThrowUnknownDirException(const DirGuard::DirData& dir_data) {
  if (DirGuard::Empty(dir_data)) {
    throw UnrecognizedDirectoryError();
  }
}

} // namespace indexfs
