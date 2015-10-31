# Copyright (c) 2014 The IndexFS Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.

## -------------------------------------------------------------------------
## C/CXX Flags
## -------------------------------------------------------------------------

AM_CFLAGS = $(EXTRA_CFLAGS)
AM_CXXFLAGS = $(EXTRA_CFLAGS)

## -------------------------------------------------------------------------
## Headers
## -------------------------------------------------------------------------

nodist_noinst_HEADERS =
nodist_noinst_HEADERS += indexfs_types.h
nodist_noinst_HEADERS += indexfs_constants.h
nodist_noinst_HEADERS += MetadataIndexService.h

## -------------------------------------------------------------------------
## Static Lib
## -------------------------------------------------------------------------

noinst_LTLIBRARIES = libthrift_idxfs.la

nodist_libthrift_idxfs_la_SOURCES =
nodist_libthrift_idxfs_la_SOURCES += indexfs_types.cpp
nodist_libthrift_idxfs_la_SOURCES += indexfs_constants.cpp
nodist_libthrift_idxfs_la_SOURCES += MetadataIndexService.cpp

## -------------------------------------------------------------------------

EXTRA_DIST = indexfs.thrift

BUILT_SOURCES =
BUILT_SOURCES += indexfs_types.cpp

indexfs_types.cpp: indexfs.thrift
	$(THRIFT) -gen cpp -out ./ $(top_srcdir)/thrift/indexfs.thrift && sleep 1

CLEANFILES =

CLEANFILES += indexfs_types.h
CLEANFILES += indexfs_constants.h
CLEANFILES += MetadataIndexService.h

CLEANFILES += indexfs_types.cpp
CLEANFILES += indexfs_constants.cpp
CLEANFILES += MetadataIndexService.cpp

## -------------------------------------------------------------------------
