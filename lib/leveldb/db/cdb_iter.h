// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_CDB_ITER_H_
#define STORAGE_LEVELDB_DB_CDB_ITER_H_

#include <stdint.h>
#include "db/column_db.h"

namespace leveldb {

// Return a new iterator that converts internal keys (yielded by
// "*internal_iter") that were live at the specified "sequence" number
// into appropriate user keys.
extern Iterator* NewColumnDBIterator(
    const ReadOptions& opttions,
    ColumnDB* db,
    Iterator* internal_iter);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_CDB_ITER_H_
