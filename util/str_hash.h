// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_UTILS_STR_HASH_H
#define _INDEXFS_UTILS_STR_HASH_H

#include <stdlib.h>
#include <stdint.h>

namespace indexfs {
    
uint32_t GetStrHash(const char* data, size_t n, uint32_t seed);

} // namespace indexfs

#endif /* _INDEXFS_UTILS_STR_HASH_H */
