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

# common headers
noinst_HEADERS += common.h
noinst_HEADERS += config.h
noinst_HEADERS += logging.h
noinst_HEADERS += dirctrl.h
noinst_HEADERS += dirguard.h
noinst_HEADERS += dirlock.h
noinst_HEADERS += didxcache.h
noinst_HEADERS += leasectrl.h
noinst_HEADERS += lookupcache.h
noinst_HEADERS += gigaidx.h
noinst_HEADERS += scanner.h
noinst_HEADERS += network.h
noinst_HEADERS += unit_test.h
noinst_HEADERS += murmurhash3.h

# helper headers
noinst_HEADERS += options.h
noinst_HEADERS += counter.h
noinst_HEADERS += ../util/str_hash.h

## -------------------------------------------------------------------------
## Static Lib
## -------------------------------------------------------------------------

noinst_LTLIBRARIES =

noinst_LTLIBRARIES += libcommon_idxfs.la

libcommon_idxfs_la_SOURCES =
libcommon_idxfs_la_SOURCES += murmurhash3.cc
libcommon_idxfs_la_SOURCES += config.cc
libcommon_idxfs_la_SOURCES += logging.cc
libcommon_idxfs_la_SOURCES += dirctrl.cc
libcommon_idxfs_la_SOURCES += dirguard.cc
libcommon_idxfs_la_SOURCES += didxcache.cc
libcommon_idxfs_la_SOURCES += leasectrl.cc
libcommon_idxfs_la_SOURCES += lookupcache.cc
libcommon_idxfs_la_SOURCES += gigaidx.cc
libcommon_idxfs_la_SOURCES += scanner.cc
libcommon_idxfs_la_SOURCES += network.cc
libcommon_idxfs_la_SOURCES += ../util/str_hash.cc

noinst_LTLIBRARIES += libtest_idxfs.la

libtest_idxfs_la_SOURCES =
libtest_idxfs_la_SOURCES += unit_test.cc

## -------------------------------------------------------------------------
## Test Programs
## -------------------------------------------------------------------------

nobase_bin_PROGRAMS =
nobase_bin_PROGRAMS += network_test
nobase_bin_PROGRAMS += bitmap_test
nobase_bin_PROGRAMS += dirctrl_test
nobase_bin_PROGRAMS += didxcache_test

bitmap_test_SOURCES = gigaidx_test.cc
bitmap_test_LDADD =
bitmap_test_LDADD += libtest_idxfs.la
bitmap_test_LDADD += libcommon_idxfs.la
bitmap_test_LDADD += $(top_builddir)/lib/leveldb/libleveldb.la

dirctrl_test_SOURCES = dirctrl_test.cc
dirctrl_test_LDADD =
dirctrl_test_LDADD += libtest_idxfs.la
dirctrl_test_LDADD += libcommon_idxfs.la
dirctrl_test_LDADD += $(top_builddir)/lib/leveldb/libleveldb.la

didxcache_test_SOURCES = didxcache_test.cc
didxcache_test_LDADD =
didxcache_test_LDADD += libtest_idxfs.la
didxcache_test_LDADD += libcommon_idxfs.la
didxcache_test_LDADD += $(top_builddir)/lib/leveldb/libleveldb.la

network_test_SOURCES = network_test.cc
network_test_LDADD =
network_test_LDADD += libcommon_idxfs.la
network_test_LDADD += $(top_builddir)/lib/leveldb/libleveldb.la

## -------------------------------------------------------------------------
