// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "client/client_impl.h"

namespace indexfs {

Status ClientImpl::Close(FileHandle* handle) {
  return Status::Corruption("Not implemented");
}

Status ClientImpl::Open(const std::string& path, int mode, FileHandle** handle) {
  return Status::Corruption("Not implemented");
}

} // namespace indexfs
