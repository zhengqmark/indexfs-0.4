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
noinst_HEADERS += fs_driver.h
noinst_HEADERS += fs_errors.h
noinst_HEADERS += index_ctx.h
noinst_HEADERS += index_server.h
noinst_HEADERS += bulk_insert.h

## -------------------------------------------------------------------------
## Test Programs
## -------------------------------------------------------------------------

nobase_bin_PROGRAMS =

nobase_bin_PROGRAMS += server_test

server_test_SOURCES =
server_test_SOURCES += fs_driver.cc
server_test_SOURCES += fs_errors.cc
server_test_SOURCES += index_ctx.cc
server_test_SOURCES += index_server.cc
server_test_SOURCES += bulk_insert.cc
server_test_SOURCES += server_test.cc

server_test_LDADD =
server_test_LDADD += $(top_builddir)/metadb/libmetadb_idxfs.la
server_test_LDADD += $(top_builddir)/util/libutil_idxfs.la
server_test_LDADD += $(top_builddir)/ipc/libipc_idxfs.la
server_test_LDADD += $(top_builddir)/common/libtest_idxfs.la
server_test_LDADD += $(top_builddir)/common/libcommon_idxfs.la
server_test_LDADD += $(top_builddir)/thrift/libthrift_idxfs.la
server_test_LDADD += $(top_builddir)/lib/leveldb/libleveldb.la

## -------------------------------------------------------------------------
## IndexFS Server
## -------------------------------------------------------------------------

nobase_bin_PROGRAMS += indexfs_server

indexfs_server_SOURCES =
indexfs_server_SOURCES += fs_driver.cc
indexfs_server_SOURCES += fs_errors.cc
indexfs_server_SOURCES += index_ctx.cc
indexfs_server_SOURCES += index_server.cc
indexfs_server_SOURCES += bulk_insert.cc
indexfs_server_SOURCES += server_main.cc

indexfs_server_LDADD =
indexfs_server_LDADD += $(top_builddir)/metadb/libmetadb_idxfs.la
indexfs_server_LDADD += $(top_builddir)/util/libutil_idxfs.la
indexfs_server_LDADD += $(top_builddir)/ipc/libipc_idxfs.la
indexfs_server_LDADD += $(top_builddir)/common/libcommon_idxfs.la
indexfs_server_LDADD += $(top_builddir)/thrift/libthrift_idxfs.la
indexfs_server_LDADD += $(top_builddir)/lib/leveldb/libleveldb.la

## -------------------------------------------------------------------------
