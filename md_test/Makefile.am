# Copyright (c) 2014 The IndexFS Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.

## -------------------------------------------------------------------------
## C/CXX Flags
## -------------------------------------------------------------------------

CC = $(MPICC)
CXX = $(MPICXX)

COMM_FLAGS =
COMM_FLAGS += -D_HAS_IDXFS
COMM_FLAGS += $(BACKEND_FLAGS) $(SNAPPY_FLAGS)
COMM_FLAGS += $(PLATFORM) -DLEVELDB_PLATFORM_POSIX

AM_CFLAGS = $(EXTRA_INCLUDES) $(COMM_FLAGS) $(EXTRA_CFLAGS)
AM_CXXFLAGS = $(EXTRA_INCLUDES) $(COMM_FLAGS) $(EXTRA_CFLAGS)

## -------------------------------------------------------------------------
## BulkIn Flags
## -------------------------------------------------------------------------

BULKIN_FLAGS =

BULKIN_FLAGS += -DIDXFS_USE_CITYHASH
# BULKIN_FLAGS += -DIDXFS_USE_SHA1
# BULKIN_FLAGS += -DIDXFS_USE_SHA1_HEX

BULKIN_FLAGS += -DIDXFS_USE_SMALL_STAT
# BULKIN_FLAGS += -DIDXFS_USE_SYS_STAT

if BACKEND_HDFS
BULKIN_FLAGS += -DIDXFS_USE_HDFS
endif

if BACKEND_PVFS2
BULKIN_FLAGS += -DIDXFS_USE_PVFS
endif

# BULKIN_FLAGS += -DIDXFS_IO_SYNC
# BULKIN_FLAGS += -DIDXFS_DISABLE_WAL
# BULKIN_FLAGS += -DIDXFS_CHECK_BY_GET
# BULKIN_FLAGS += -DIDXFS_COUNT_KV_SIZE

BULKIN_FLAGS += -DIDXFS_NAME_CHECK
BULKIN_FLAGS += -DIDXFS_USE_BLOOM_FILTER

## -------------------------------------------------------------------------
## Headers
## -------------------------------------------------------------------------

noinst_HEADERS =
noinst_HEADERS += city.h
noinst_HEADERS += filestat.h

## -------------------------------------------------------------------------
## MD - TEST
## -------------------------------------------------------------------------

noinst_PROGRAMS =
noinst_PROGRAMS += mdtest_nobk
noinst_PROGRAMS += mdtest_bkin
noinst_PROGRAMS += mdtest_posix

if BACKEND_HDFS
noinst_PROGRAMS += mdtest_hdfs
endif

# -- original mdtest
mdtest_posix_SOURCES = md_test.c
mdtest_posix_CFLAGS = $(EXTRA_CFLAGS)

# -- mdtest on hdfs
mdtest_hdfs_SOURCES =
mdtest_hdfs_SOURCES += md_test.c
mdtest_hdfs_SOURCES += hdfs_client.cc

mdtest_hdfs_CFLAGS = -I$(top_srcdir) -D_HAS_IDXFS $(EXTRA_CFLAGS)
mdtest_hdfs_CXXFLAGS = -I$(top_srcdir) -D_HAS_IDXFS $(EXTRA_CFLAGS)

# -- mdtest on indexfs without bulk insertion
mdtest_nobk_SOURCES =
mdtest_nobk_SOURCES += md_test.c
mdtest_nobk_SOURCES += indexfs_client.cc

mdtest_nobk_LDADD =
mdtest_nobk_LDADD += $(top_builddir)/client/libclient_c_idxfs.la
mdtest_nobk_LDADD += $(top_builddir)/client/libclient_idxfs.la
mdtest_nobk_LDADD += $(top_builddir)/backends/libbackends_idxfs.la
mdtest_nobk_LDADD += $(top_builddir)/communication/librpc_idxfs.la
mdtest_nobk_LDADD += $(top_builddir)/common/libcommon_idxfs.la
mdtest_nobk_LDADD += $(top_builddir)/thrift/libthrift_idxfs.la
mdtest_nobk_LDADD += $(top_builddir)/lib/leveldb/libleveldb.la

# -- mdtest on indexfs with bulk insertion
mdtest_bkin_CFLAGS = $(AM_CFLAGS) $(BULKIN_FLAGS)
mdtest_bkin_CXXFLAGS = $(AM_CXXFLAGS) $(BULKIN_FLAGS)

mdtest_bkin_SOURCES =
mdtest_bkin_SOURCES += city.cc
mdtest_bkin_SOURCES += md_test.c
mdtest_bkin_SOURCES += leveldb_client.cc

mdtest_bkin_LDADD =
mdtest_bkin_LDADD += $(top_builddir)/common/libcommon_idxfs.la
mdtest_bkin_LDADD += $(top_builddir)/lib/leveldb/libleveldb.la

## -------------------------------------------------------------------------
