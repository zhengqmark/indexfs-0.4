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
noinst_HEADERS += membset.h
noinst_HEADERS += rpc.h
noinst_HEADERS += rpc_impl.h
noinst_HEADERS += rpc_types.h

## -------------------------------------------------------------------------
## Static Lib
## -------------------------------------------------------------------------

noinst_LTLIBRARIES = libipc_idxfs.la

libipc_idxfs_la_SOURCES =
libipc_idxfs_la_SOURCES += membset.cc
libipc_idxfs_la_SOURCES += rpc.cc
libipc_idxfs_la_SOURCES += rpc_impl.cc

## -------------------------------------------------------------------------
