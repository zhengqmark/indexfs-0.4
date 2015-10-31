# Copyright (c) 2014 The IndexFS Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.

## -------------------------------------------------------------------------
## C/CXX Flags
## -------------------------------------------------------------------------

COMM_FLAGS =
COMM_FLAGS += -DNBULK_INSERT
COMM_FLAGS += $(BACKEND_FLAGS) $(SNAPPY_FLAGS)
COMM_FLAGS += $(PLATFORM) -DLEVELDB_PLATFORM_POSIX

AM_CFLAGS = $(EXTRA_INCLUDES) $(COMM_FLAGS) $(EXTRA_CFLAGS)
AM_CXXFLAGS = $(EXTRA_INCLUDES) $(COMM_FLAGS) $(EXTRA_CFLAGS)

## -------------------------------------------------------------------------
## Headers
## -------------------------------------------------------------------------

noinst_HEADERS =
noinst_HEADERS += rpc_exec.h
noinst_HEADERS += client.h
noinst_HEADERS += client_impl.h
noinst_HEADERS += batch_client.h

## -------------------------------------------------------------------------
## Static Lib
## -------------------------------------------------------------------------

noinst_LTLIBRARIES =
noinst_LTLIBRARIES += libclient_idxfs.la

libclient_idxfs_la_SOURCES =
libclient_idxfs_la_SOURCES += client.cc
libclient_idxfs_la_SOURCES += client_impl.cc
libclient_idxfs_la_SOURCES += file_io.cc
libclient_idxfs_la_SOURCES += mkdir_split.cc
libclient_idxfs_la_SOURCES += mknod_buf.cc
libclient_idxfs_la_SOURCES += read_dir.cc
libclient_idxfs_la_SOURCES += resolve_path.cc

noinst_LTLIBRARIES += libbatchclient_idxfs.la

libbatchclient_idxfs_la_SOURCES =
libbatchclient_idxfs_la_SOURCES += batch_ctrl.cc
libbatchclient_idxfs_la_SOURCES += batch_client.cc

## -------------------------------------------------------------------------
