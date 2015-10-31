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
## Required Libs
## -------------------------------------------------------------------------

LOCAL_LDADD =
LOCAL_LDADD += $(top_builddir)/c/libclient_c_idxfs.la
LOCAL_LDADD += $(top_builddir)/client/libclient_idxfs.la
LOCAL_LDADD += $(top_builddir)/ipc/libipc_idxfs.la
LOCAL_LDADD += $(top_builddir)/common/libcommon_idxfs.la
LOCAL_LDADD += $(top_builddir)/thrift/libthrift_idxfs.la
LOCAL_LDADD += $(top_builddir)/lib/leveldb/libleveldb.la

## -------------------------------------------------------------------------
## IndexFS Client Tools
## -------------------------------------------------------------------------

nobase_bin_PROGRAMS =
nobase_bin_PROGRAMS += mkdir
nobase_bin_PROGRAMS += mknod
nobase_bin_PROGRAMS += chmod
nobase_bin_PROGRAMS += unlink
nobase_bin_PROGRAMS += getattr
nobase_bin_PROGRAMS += readdir

## ---------------------------------------

mkdir_SOURCES   = mkdir.cc
mkdir_LDADD     = $(LOCAL_LDADD)
mknod_SOURCES   = mknod.cc
mknod_LDADD     = $(LOCAL_LDADD)
chmod_SOURCES   = chmod.cc
chmod_LDADD     = $(LOCAL_LDADD)
unlink_SOURCES  = unlink.cc
unlink_LDADD    = $(LOCAL_LDADD)
getattr_SOURCES = getattr.cc
getattr_LDADD   = $(LOCAL_LDADD)
readdir_SOURCES = readdir.cc
readdir_LDADD   = $(LOCAL_LDADD)

## -------------------------------------------------------------------------
