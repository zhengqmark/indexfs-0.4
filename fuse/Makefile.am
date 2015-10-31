# Copyright (c) 2014 The IndexFS Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.

## -------------------------------------------------------------------------
## C/CXX Flags
## -------------------------------------------------------------------------

COMM_FLAGS = -D_FILE_OFFSET_BITS=64
COMM_FLAGS += $(BACKEND_FLAGS) $(SNAPPY_FLAGS)
COMM_FLAGS += $(PLATFORM) -DLEVELDB_PLATFORM_POSIX

AM_CFLAGS = $(EXTRA_INCLUDES) $(COMM_FLAGS) $(EXTRA_CFLAGS)
AM_CXXFLAGS = $(EXTRA_INCLUDES) $(COMM_FLAGS) $(EXTRA_CFLAGS)

## -------------------------------------------------------------------------
## Headers
## -------------------------------------------------------------------------

noinst_HEADERS =
noinst_HEADERS += fuse_helper.h

## -------------------------------------------------------------------------
## Fuse Client
## -------------------------------------------------------------------------

nobase_bin_PROGRAMS = fuse_main

fuse_main_SOURCES = fuse_main.cc

fuse_main_LDADD =
fuse_main_LDADD += $(top_builddir)/client/libclient_idxfs.la
fuse_main_LDADD += $(top_builddir)/backends/libbackends_idxfs.la
fuse_main_LDADD += $(top_builddir)/communication/librpc_idxfs.la
fuse_main_LDADD += $(top_builddir)/common/libcommon_idxfs.la
fuse_main_LDADD += $(top_builddir)/thrift/libthrift_idxfs.la
fuse_main_LDADD += $(top_builddir)/lib/leveldb/libleveldb.la

## -------------------------------------------------------------------------
