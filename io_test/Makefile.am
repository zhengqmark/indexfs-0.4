# Copyright (c) 2014 The IndexFS Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.

## -------------------------------------------------------------------------
## C/CXX Flags
## -------------------------------------------------------------------------

CXX = $(MPICXX)

COMM_FLAGS =
COMM_FLAGS += $(BACKEND_FLAGS) $(SNAPPY_FLAGS)
COMM_FLAGS += $(PLATFORM) -DLEVELDB_PLATFORM_POSIX

AM_CFLAGS = $(EXTRA_INCLUDES) $(COMM_FLAGS) $(EXTRA_CFLAGS)
AM_CXXFLAGS = $(EXTRA_INCLUDES) $(COMM_FLAGS) $(EXTRA_CFLAGS)

## -------------------------------------------------------------------------
## Headers
## -------------------------------------------------------------------------

noinst_HEADERS =
noinst_HEADERS += io_client.h
noinst_HEADERS += io_task.h
noinst_HEADERS += gzstream.h

## -------------------------------------------------------------------------
## Static Lib
## -------------------------------------------------------------------------

noinst_LIBRARIES = libioclient_idxfs.a

libioclient_idxfs_a_SOURCES =
libioclient_idxfs_a_SOURCES += io_client.cc
libioclient_idxfs_a_SOURCES += localfs_client.cc
libioclient_idxfs_a_SOURCES += indexfs_client.cc
libioclient_idxfs_a_SOURCES += orangefs_client.cc

## -------------------------------------------------------------------------
## MPI IOTEST
## -------------------------------------------------------------------------

noinst_PROGRAMS = io_driver

io_driver_SOURCES =
io_driver_SOURCES += gzstream.cc
io_driver_SOURCES += io_task.cc
io_driver_SOURCES += tree_test.cc
io_driver_SOURCES += replay_test.cc
io_driver_SOURCES += cache_test.cc
io_driver_SOURCES += rpc_test.cc
io_driver_SOURCES += sstcomp_test.cc
io_driver_SOURCES += io_driver.cc

io_driver_LDADD =
io_driver_LDADD += libioclient_idxfs.a
io_driver_LDADD += $(top_builddir)/client/libbatchclient_idxfs.la
io_driver_LDADD += $(top_builddir)/client/libclient_idxfs.la
io_driver_LDADD += $(top_builddir)/metadb/libmetadb_idxfs.la
io_driver_LDADD += $(top_builddir)/util/libutil_idxfs.la
io_driver_LDADD += $(top_builddir)/ipc/libipc_idxfs.la
io_driver_LDADD += $(top_builddir)/common/libcommon_idxfs.la
io_driver_LDADD += $(top_builddir)/thrift/libthrift_idxfs.la
io_driver_LDADD += $(top_builddir)/lib/leveldb/libleveldb.la

## -------------------------------------------------------------------------
