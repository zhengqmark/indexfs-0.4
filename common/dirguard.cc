// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "common/dirguard.h"

namespace indexfs {

DirGuard::~DirGuard() {
  ctrl_table_->Release(dir_data_.first);
  index_cache_->Release(dir_data_.second);
}

DirGuard::DirGuard(const DirData& dir_data) : dir_data_(dir_data) {
  ctrl_table_ = dir_data_.first->GetTable();
  dir_idx_ = dir_data_.second->index;
  index_cache_ = dir_data_.second->GetCache();
}

} // namespace indexfs
