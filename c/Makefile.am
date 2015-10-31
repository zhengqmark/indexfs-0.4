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
noinst_HEADERS += cli.h
noinst_HEADERS += cli_types.h
noinst_HEADERS += libclient.h

## -------------------------------------------------------------------------
## C Interface
## -------------------------------------------------------------------------

noinst_LTLIBRARIES = libclient_c_idxfs.la

libclient_c_idxfs_la_SOURCES = cli.cc

nobase_lib_LTLIBRARIES =

nobase_lib_LTLIBRARIES += libindexfs_st.la

libindexfs_st_la_SOURCES = cli_st.cc

libindexfs_st_la_LIBADD =
libindexfs_st_la_LIBADD += libclient_c_idxfs.la
libindexfs_st_la_LIBADD += $(top_builddir)/client/libclient_idxfs.la
libindexfs_st_la_LIBADD += $(top_builddir)/ipc/libipc_idxfs.la
libindexfs_st_la_LIBADD += $(top_builddir)/common/libcommon_idxfs.la
libindexfs_st_la_LIBADD += $(top_builddir)/thrift/libthrift_idxfs.la
libindexfs_st_la_LIBADD += $(top_builddir)/lib/leveldb/libleveldb.la

libindexfs_st_la_LDFLAGS = -release $(INDEXFS_VERSION)

nobase_lib_LTLIBRARIES += libindexfs_mt.la

libindexfs_mt_la_SOURCES = cli_mt.cc

libindexfs_mt_la_LIBADD =
libindexfs_mt_la_LIBADD += libclient_c_idxfs.la
libindexfs_mt_la_LIBADD += $(top_builddir)/client/libclient_idxfs.la
libindexfs_mt_la_LIBADD += $(top_builddir)/ipc/libipc_idxfs.la
libindexfs_mt_la_LIBADD += $(top_builddir)/common/libcommon_idxfs.la
libindexfs_mt_la_LIBADD += $(top_builddir)/thrift/libthrift_idxfs.la
libindexfs_mt_la_LIBADD += $(top_builddir)/lib/leveldb/libleveldb.la

libindexfs_mt_la_LDFLAGS = -release $(INDEXFS_VERSION)

## -------------------------------------------------------------------------
## Test Programs
## -------------------------------------------------------------------------

nobase_bin_PROGRAMS = libclient_test

libclient_test_SOURCES = libclient_test.cc

## -------------------------------------------------------------------------
