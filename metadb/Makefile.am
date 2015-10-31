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
noinst_HEADERS += metadb.h
noinst_HEADERS += metadb_io.h
noinst_HEADERS += fstat.h
noinst_HEADERS += dbtypes.h
noinst_HEADERS += dboptions.h

## -------------------------------------------------------------------------
## Static Lib
## -------------------------------------------------------------------------

noinst_LTLIBRARIES = libmetadb_idxfs.la

libmetadb_idxfs_la_SOURCES =
libmetadb_idxfs_la_SOURCES += dbtypes.cc
libmetadb_idxfs_la_SOURCES += dboptions.cc
libmetadb_idxfs_la_SOURCES += metadb.cc
libmetadb_idxfs_la_SOURCES += metadb_reader.cc
libmetadb_idxfs_la_SOURCES += metadb_writer.cc

## -------------------------------------------------------------------------
## Test Programs
## -------------------------------------------------------------------------

nobase_bin_PROGRAMS =
nobase_bin_PROGRAMS += fstat_test
nobase_bin_PROGRAMS += dbtypes_test
nobase_bin_PROGRAMS += metadb_test

fstat_test_SOURCES = fstat_test.cc

fstat_test_LDADD =
fstat_test_LDADD += $(top_builddir)/common/libtest_idxfs.la
fstat_test_LDADD += $(top_builddir)/common/libcommon_idxfs.la
fstat_test_LDADD += $(top_builddir)/lib/leveldb/libleveldb.la

dbtypes_test_SOURCES = dbtypes_test.cc

dbtypes_test_LDADD =
dbtypes_test_LDADD += libmetadb_idxfs.la
dbtypes_test_LDADD += $(top_builddir)/common/libtest_idxfs.la
dbtypes_test_LDADD += $(top_builddir)/common/libcommon_idxfs.la
dbtypes_test_LDADD += $(top_builddir)/lib/leveldb/libleveldb.la

metadb_test_SOURCES =
metadb_test_SOURCES += metadb_test.cc

metadb_test_LDADD =
metadb_test_LDADD += libmetadb_idxfs.la
metadb_test_LDADD += $(top_builddir)/util/libutil_idxfs.la
metadb_test_LDADD += $(top_builddir)/common/libtest_idxfs.la
metadb_test_LDADD += $(top_builddir)/common/libcommon_idxfs.la
metadb_test_LDADD += $(top_builddir)/thrift/libthrift_idxfs.la
metadb_test_LDADD += $(top_builddir)/lib/leveldb/libleveldb.la

## -------------------------------------------------------------------------
