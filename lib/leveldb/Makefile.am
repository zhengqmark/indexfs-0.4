## Copyright (c) 2014 The IndexFS Authors. All rights reserved.
## Use of this source code is governed by a BSD-style license that can be
## found in the LICENSE file. See the AUTHORS file for names of contributors.

## -------------------------------------------------------------------------
## C/CXX Flags
## -------------------------------------------------------------------------

COMM_FLAGS =
COMM_FLAGS += "-I$(top_srcdir)/lib/leveldb/include"
COMM_FLAGS += $(BACKEND_FLAGS) $(SNAPPY_FLAGS)
COMM_FLAGS += $(PLATFORM) -DLEVELDB_PLATFORM_POSIX

AM_CFLAGS = $(COMM_FLAGS) $(EXTRA_CFLAGS)
AM_CXXFLAGS = $(COMM_FLAGS) $(EXTRA_CFLAGS)

## -------------------------------------------------------------------------
## LevelDB Ex/Interal Headers
## -------------------------------------------------------------------------

# Headers.
noinst_HEADERS =
noinst_HEADERS += include/leveldb/cache.h
# noinst_HEADERS += include/leveldb/c.h
noinst_HEADERS += include/leveldb/comparator.h
noinst_HEADERS += include/leveldb/db.h
noinst_HEADERS += include/leveldb/env.h
noinst_HEADERS += include/leveldb/filter_policy.h
noinst_HEADERS += include/leveldb/iterator.h
noinst_HEADERS += include/leveldb/options.h
noinst_HEADERS += include/leveldb/slice.h
noinst_HEADERS += include/leveldb/status.h
noinst_HEADERS += include/leveldb/table_builder.h
noinst_HEADERS += include/leveldb/table.h
noinst_HEADERS += include/leveldb/write_batch.h
noinst_HEADERS += include/leveldb/env/env_posix.h
noinst_HEADERS += include/leveldb/env/env_memlink.h
noinst_HEADERS += include/leveldb/env/env_wrapper.h

# Internal headers.
noinst_HEADERS += db/builder.h
noinst_HEADERS += db/dbformat.h
noinst_HEADERS += db/db_impl.h
noinst_HEADERS += db/db_iter.h
noinst_HEADERS += db/filename.h
noinst_HEADERS += db/log_format.h
noinst_HEADERS += db/log_reader.h
noinst_HEADERS += db/log_writer.h
noinst_HEADERS += db/memtable.h
noinst_HEADERS += db/skiplist.h
noinst_HEADERS += db/snapshot.h
noinst_HEADERS += db/table_cache.h
noinst_HEADERS += db/version_edit.h
noinst_HEADERS += db/version_set.h
noinst_HEADERS += db/write_batch_internal.h
noinst_HEADERS += table/block_builder.h
noinst_HEADERS += table/block.h
noinst_HEADERS += table/filter_block.h
noinst_HEADERS += table/format.h
noinst_HEADERS += table/iterator_wrapper.h
noinst_HEADERS += table/merger.h
noinst_HEADERS += table/two_level_iterator.h
noinst_HEADERS += util/arena.h
noinst_HEADERS += util/coding.h
noinst_HEADERS += util/crc32c.h
noinst_HEADERS += util/hash.h
noinst_HEADERS += util/histogram.h
noinst_HEADERS += util/logging.h
noinst_HEADERS += util/mutexlock.h
noinst_HEADERS += util/posix_logger.h
noinst_HEADERS += util/random.h
noinst_HEADERS += env/obj_io.h
noinst_HEADERS += env/obj_set.h
noinst_HEADERS += env/io_logger.h
noinst_HEADERS += env/env_impl.h
noinst_HEADERS += port/win/stdint.h
noinst_HEADERS += port/port.h
noinst_HEADERS += port/port_posix.h
noinst_HEADERS += port/atomic_pointer.h

# Additional headers.
# noinst_HEADERS += db/cdb_iter.h
# noinst_HEADERS += db/column_db.h
# noinst_HEADERS += db/data_cache.h
# noinst_HEADERS += db/membuf.h
noinst_HEADERS += util/monitor.h
noinst_HEADERS += util/socket.h

## -------------------------------------------------------------------------
## libleveldb
## -------------------------------------------------------------------------

# Lib to build.
noinst_LTLIBRARIES = libleveldb.la

libleveldb_la_SOURCES =
libleveldb_la_SOURCES += db/builder.cc
# libleveldb_la_SOURCES += db/c.cc
# libleveldb_la_SOURCES += db/cdb_iter.cc
# libleveldb_la_SOURCES += db/column_db.cc
# libleveldb_la_SOURCES += db/data_cache.cc
libleveldb_la_SOURCES += db/dbformat.cc
libleveldb_la_SOURCES += db/db_impl.cc
libleveldb_la_SOURCES += db/db_iter.cc
libleveldb_la_SOURCES += db/filename.cc
libleveldb_la_SOURCES += db/log_reader.cc
libleveldb_la_SOURCES += db/log_writer.cc
libleveldb_la_SOURCES += db/memtable.cc
libleveldb_la_SOURCES += db/repair.cc
libleveldb_la_SOURCES += db/table_cache.cc
libleveldb_la_SOURCES += db/version_edit.cc
libleveldb_la_SOURCES += db/version_set.cc
libleveldb_la_SOURCES += db/write_batch.cc
libleveldb_la_SOURCES += table/block_builder.cc
libleveldb_la_SOURCES += table/block.cc
libleveldb_la_SOURCES += table/filter_block.cc
libleveldb_la_SOURCES += table/format.cc
libleveldb_la_SOURCES += table/iterator.cc
libleveldb_la_SOURCES += table/merger.cc
libleveldb_la_SOURCES += table/table_builder.cc
libleveldb_la_SOURCES += table/table.cc
libleveldb_la_SOURCES += table/two_level_iterator.cc
libleveldb_la_SOURCES += util/arena.cc
libleveldb_la_SOURCES += util/bloom.cc
libleveldb_la_SOURCES += util/cache.cc
libleveldb_la_SOURCES += util/coding.cc
libleveldb_la_SOURCES += util/comparator.cc
libleveldb_la_SOURCES += util/crc32c.cc
libleveldb_la_SOURCES += util/filter_policy.cc
libleveldb_la_SOURCES += util/hash.cc
libleveldb_la_SOURCES += util/histogram.cc
libleveldb_la_SOURCES += util/logging.cc
libleveldb_la_SOURCES += util/monitor.cc
libleveldb_la_SOURCES += util/options.cc
libleveldb_la_SOURCES += util/socket.cc
libleveldb_la_SOURCES += util/status.cc
libleveldb_la_SOURCES += util/zigzag.cc
libleveldb_la_SOURCES += env/env.cc
libleveldb_la_SOURCES += env/env_posix.cc
libleveldb_la_SOURCES += env/env_memlink.cc
libleveldb_la_SOURCES += env/obj_set.cc
libleveldb_la_SOURCES += env/obj_io.cc
libleveldb_la_SOURCES += env/obj_path.cc
libleveldb_la_SOURCES += env/posix_io.cc
libleveldb_la_SOURCES += env/io_logger.cc
libleveldb_la_SOURCES += port/port_posix.cc

## -------------------------------------------------------------------------
## Backend Switch
## -------------------------------------------------------------------------

if BACKEND_HDFS
libleveldb_la_SOURCES += env/env_hdfs.cc
noinst_HEADERS += include/leveldb/env/env_hdfs.h
endif

if BACKEND_PVFS2
libleveldb_la_SOURCES += env/env_pvfs.cc
noinst_HEADERS += include/leveldb/env/env_pvfs.h
endif

if BACKEND_RADOS
libleveldb_la_SOURCES += env/rados_io.cc
libleveldb_la_SOURCES += env/env_rados.cc
noinst_HEADERS += include/leveldb/env/env_rados.h
endif

## -------------------------------------------------------------------------
## Unit Test
## -------------------------------------------------------------------------

noinst_LTLIBRARIES += libleveldbt.la

libleveldbt_la_SOURCES =
libleveldbt_la_SOURCES += util/testharness.cc
libleveldbt_la_SOURCES += util/testutil.cc

nobase_bin_PROGRAMS =

nobase_bin_PROGRAMS += set_test
set_test_LDADD =
set_test_LDADD += libleveldbt.la
set_test_LDADD += libleveldb.la
set_test_SOURCES = env/set_test.cc

nobase_bin_PROGRAMS += logger_test
logger_test_LDADD =
logger_test_LDADD += libleveldbt.la
logger_test_LDADD += libleveldb.la
logger_test_SOURCES = env/logger_test.cc

nobase_bin_PROGRAMS += posix_test
posix_test_LDADD =
posix_test_LDADD += libleveldbt.la
posix_test_LDADD += libleveldb.la
posix_test_SOURCES = env/posix_test.cc

if BACKEND_RADOS
nobase_bin_PROGRAMS += rados_test
rados_test_LDADD =
rados_test_LDADD += libleveldbt.la
rados_test_LDADD += libleveldb.la
rados_test_SOURCES = env/rados_test.cc
endif

nobase_bin_PROGRAMS += version_edit_test
version_edit_test_LDADD =
version_edit_test_LDADD += libleveldbt.la
version_edit_test_LDADD += libleveldb.la
version_edit_test_SOURCES = db/version_edit_test.cc

## -------------------------------------------------------------------------
