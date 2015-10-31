// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_METADB_OPTIONS_H_
#define _INDEXFS_METADB_OPTIONS_H_

#include "common/common.h"
#include "common/config.h"

#include <sstream>

namespace indexfs {
namespace mdb {

// Callback handler that MetaDB will use to initialize its
// internal storage options.
//
typedef void (*OptionInitializer)
    (Options *options, Config *config, Env *env);

// Initializes LevelDB options with user-configurable settings.
//
extern void
DefaultLevelDBOptionInitializer(Options *options, Config *config, Env *env);
extern void
BatchClientLevelDBOptionInitializer(Options *options, Config *config, Env *env);

} /* namespace mdb */
} /* namespace indexfs */

#endif /* _INDEXFS_METADB_OPTIONS_H_ */
