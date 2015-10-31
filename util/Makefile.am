# Copyright (c) 2014 The IndexFS Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.

## -------------------------------------------------------------------------
## C/CXX Flags
## -------------------------------------------------------------------------

COMM_FLAGS =
COMM_FLAGS += $(BACKEND_FLAGS) $(SNAPPY_FLAGS)
COMM_FLAGS += $(PLATFORM) -DLEVELDB_PLATFORM_POSIX

AM_CFLAGS = $(EXTRA_INCLUDES) $(COMM_FLAGS) $(EXTRA_CFLAGS)
AM_CXXFLAGS = $(EXTRA_INCLUDES) $(COMM_FLAGS) $(EXTRA_CFLAGS)

## -------------------------------------------------------------------------
## Headers
## -------------------------------------------------------------------------

noinst_HEADERS =

# monitor headers
noinst_HEADERS += exec_srv.h
noinst_HEADERS += monitor.h
noinst_HEADERS += monitor_thread.h
noinst_HEADERS += leveldb_types.h
noinst_HEADERS += leveldb_io.h
noinst_HEADERS += leveldb_reader.h

## -------------------------------------------------------------------------
## Static Lib
## -------------------------------------------------------------------------

noinst_LTLIBRARIES =

noinst_LTLIBRARIES += libutil_idxfs.la

libutil_idxfs_la_SOURCES =
libutil_idxfs_la_SOURCES += exec_srv.cc
libutil_idxfs_la_SOURCES += monitor.cc
libutil_idxfs_la_SOURCES += monitor_thread.cc
libutil_idxfs_la_SOURCES += leveldb_io.cc

## -------------------------------------------------------------------------
## Test Programs
## -------------------------------------------------------------------------

nobase_bin_PROGRAMS =
nobase_bin_PROGRAMS += monitor_test
nobase_bin_PROGRAMS += leveldb_test

monitor_test_SOURCES = monitor_test.cc

monitor_test_LDADD =
monitor_test_LDADD += libutil_idxfs.la
monitor_test_LDADD += $(top_builddir)/common/libtest_idxfs.la
monitor_test_LDADD += $(top_builddir)/common/libcommon_idxfs.la
monitor_test_LDADD += $(top_builddir)/lib/leveldb/libleveldb.la


leveldb_test_SOURCES = leveldb_test.cc

leveldb_test_LDADD =
leveldb_test_LDADD += libutil_idxfs.la
leveldb_test_LDADD += $(top_builddir)/common/libtest_idxfs.la
leveldb_test_LDADD += $(top_builddir)/common/libcommon_idxfs.la
leveldb_test_LDADD += $(top_builddir)/lib/leveldb/libleveldb.la

## -------------------------------------------------------------------------
