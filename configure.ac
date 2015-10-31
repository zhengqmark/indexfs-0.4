# Copyright (c) 2014 The IndexFS Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.

## -------------------------------------------------------------------
## Bootstrap Checks
## -------------------------------------------------------------------

AC_PREREQ([2.68])
AC_INIT([indexfs], [0.4.0])
AC_USE_SYSTEM_EXTENSIONS
AM_INIT_AUTOMAKE([foreign subdir-objects -Wall])
AM_PROG_AR
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])
LT_PREREQ([2.4])
LT_INIT

AC_CONFIG_SRCDIR([server/index_server.cc])
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_MACRO_DIR([m4])

CFLAGS=${CFLAGS:-"-O2 -DNDEBUG"}
CXXFLAGS=${CXXFLAGS:-"-O2 -DNDEBUG"}

AC_PROG_CXX
AC_PROG_CC
AC_PROG_LN_S
AC_PROG_INSTALL
AC_PROG_MAKE_SET

AC_LANG([C++])
LT_LANG([C++])

## AC_CHECK_HEADER_STDBOOL # not supported util 2.69
AC_CHECK_HEADERS([netinet/in.h])

AC_C_INLINE
AC_TYPE_INT16_T
AC_TYPE_INT32_T
AC_TYPE_INT64_T
AC_TYPE_INT8_T
AC_TYPE_MODE_T
AC_TYPE_OFF_T
AC_TYPE_PID_T
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T
AC_TYPE_UID_T
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T
AC_TYPE_UINT8_T

AC_FUNC_ERROR_AT_LINE
AC_FUNC_MALLOC
AC_FUNC_MMAP
AC_FUNC_LSTAT_FOLLOWS_SLASHED_SYMLINK

AC_SYS_LARGEFILE
AX_MPI([mpi_detect=yes], [mpi_detect=false])

## -------------------------------------------------------------------
## Checks for Local Operating System
## -------------------------------------------------------------------

AC_MSG_CHECKING([target os])
TARGET_OS=$(uname -s)
case "$TARGET_OS" in
  Darwin)
    PLATFORM="-DOS_MACOSX"
    ;;
  Linux)
    PLATFORM="-DOS_LINUX"
    ;;
  SunOS)
    PLATFORM="-DOS_SOLARIS"
    AC_MSG_ERROR(["SunOS is not currently supported :-("])
    ;;
  FreeBSD)
    PLATFORM="-DOS_FREEBSD"
    AC_MSG_ERROR(["FreeBSD is not currently supported :-("])
    ;;
  NetBSD)
    PLATFORM="-DOS_NETBSD"
    AC_MSG_ERROR(["NetBSD is not currently supported :-("])
    ;;
  OpenBSD)
    PLATFORM="-DOS_OPENBSD"
    AC_MSG_ERROR(["OpenBSD is not currently supported :-("])
    ;;
  *)
    AC_MSG_ERROR([
---------------------------------------------------
Unknown operating system:
  $TARGET_OS
Please switch to a LINUX, UNIX, or BSD-based OS.
---------------------------------------------------])
    ;;
esac
AC_SUBST([PLATFORM])
AC_MSG_RESULT([$TARGET_OS])

## -------------------------------------------------------------------
## Checks for Extra C/CXX Flags
## -------------------------------------------------------------------

EXTRA_CFLAGS=""
AX_CHECK_COMPILE_FLAG([-std=c++0x],
                      [EXTRA_CFLAGS="${EXTRA_CFLAGS} -std=c++0x"],,)
AX_CHECK_COMPILE_FLAG([-pthread],
                      [EXTRA_CFLAGS="${EXTRA_CFLAGS} -pthread"],,)
AX_CHECK_COMPILE_FLAG([-fno-builtin-memcmp],
                      [EXTRA_CFLAGS="${EXTRA_CFLAGS} -fno-builtin-memcmp"],,)
AX_CHECK_COMPILE_FLAG([-fno-builtin-memmove],
                      [EXTRA_CFLAGS="${EXTRA_CFLAGS} -fno-builtin-memmove"],,)
EXTRA_INCLUDES='-I$(top_srcdir) -I$(top_srcdir)/lib -I$(top_srcdir)/lib/leveldb -I$(top_srcdir)/lib/leveldb/include'
AC_SUBST([EXTRA_CFLAGS])
AC_SUBST([EXTRA_INCLUDES])

## -------------------------------------------------------------------
## Checks for common libs
## -------------------------------------------------------------------

AC_CHECK_LIB([stdc++], [main], [],
             [AC_MSG_ERROR([
---------------------------------------------------
libstdc++.so not found
which is required by IndexFS
please install it and try again.
---------------------------------------------------])])
AC_CHECK_LIB([m], [sin], [],
             [AC_MSG_ERROR([
---------------------------------------------------
libm.so not found
which is required by IndexFS
please install it and try again.
---------------------------------------------------])])
AC_CHECK_LIB([z], [gzopen], [],
             [AC_MSG_ERROR([
---------------------------------------------------
libz.so not found
which is required by IndexFS
please install it and try again.
---------------------------------------------------])])
AC_CHECK_LIB([dl], [dlopen], [],
             [AC_MSG_ERROR([
---------------------------------------------------
libdl.so not found
which is required by IndexFS
please install it and try again.
---------------------------------------------------])])
AC_CHECK_LIB([pthread], [pthread_create], [],
             [AC_MSG_ERROR([
---------------------------------------------------
libpthread.so not found
which is required by IndexFS
please install it and try again.
---------------------------------------------------])])

## -------------------------------------------------------------------
## Checks for libs
## -------------------------------------------------------------------

AC_CHECK_LIB([glog], [main], [],
             [AC_MSG_ERROR([
---------------------------------------------------
libglog.so not found
which is required by IndexFS
please install it and try again.
---------------------------------------------------])])
AC_CHECK_LIB([gflags], [main], [],
             [AC_MSG_ERROR([
---------------------------------------------------
libgflags.so not found
which is required by IndexFS
please install it and try again.
---------------------------------------------------])])
AC_CHECK_LIB([event], [event_dispatch], [],
             [AC_MSG_ERROR([
---------------------------------------------------
libevent.so not found
which is required by IndexFS
please install it and try again.
---------------------------------------------------])])
AC_CHECK_LIB([thrift], [main], [],
             [AC_MSG_ERROR([
---------------------------------------------------
libthrift.so not found
which is required by IndexFS
please install it and try again.
---------------------------------------------------])])
AC_CHECK_LIB([thriftnb], [main], [],
             [AC_MSG_ERROR([
---------------------------------------------------
libthriftnb.so not found
which is required by IndexFS
please install it and try again.
---------------------------------------------------])])

## -------------------------------------------------------------------
## Checks for headers
## -------------------------------------------------------------------

AC_CHECK_HEADERS([gflags/gflags.h], [],
                 [AC_MSG_ERROR([
---------------------------------------------------
gflags header files missing
---------------------------------------------------])])
AC_CHECK_HEADERS([glog/logging.h], [],
                 [AC_MSG_ERROR([
---------------------------------------------------
glog header files missing
---------------------------------------------------])])
AC_CHECK_HEADERS([thrift/TProcessor.h \
                  thrift/protocol/TProtocol.h \
                  thrift/transport/TTransport.h \
                  thrift/concurrency/ThreadManager.h \
                  thrift/server/TServer.h], [],
                 [AC_MSG_ERROR([
---------------------------------------------------
thrift header files missing
---------------------------------------------------])])

## -------------------------------------------------------------------
## Checks for Thrift generator
## -------------------------------------------------------------------

AC_PATH_PROG([THRIFT], [thrift])
if test x"${THRIFT}" = "x"; then
  AC_MSG_ERROR([
---------------------------------------------------
thrift not found
which is required by IndexFS to generate
RPC related source files
please install thrift and try again.
---------------------------------------------------])
fi
AC_SUBST([THRIFT])

## -------------------------------------------------------------------
## Checks for Java VM
## -------------------------------------------------------------------

java=no
AC_ARG_WITH([java],
            [AS_HELP_STRING([--with-java], [set the path to your jdk home])],
            [JAVA_HOME=${withval}], [JAVA_HOME=${JAVA_HOME}])
if test x"${JAVA_HOME}" = "xyes" -o x"${JAVA_HOME}" = "x"; then
  for jvm in "${HOME}/usr/sunjdk" "${HOME}/usr/openjdk" "/usr/local/sunjdk" "/usr/local/openjdk"
  do
    if test -e ${jvm}/bin/javac; then
      JAVA_HOME="${jvm}"
      break
    fi
  done
  if test x"${JAVA_HOME}" = "xyes"; then
    AC_MSG_ERROR([
---------------------------------------------------
Java have to be explicitly specified
  with '--with-java=JAVA_HOME'
JDK is required for IndexFS to work with HDFS.
---------------------------------------------------])
  fi
fi
JVM_FLAGS=""
JVM_LIBS=""
JVM_LDFLAGS=""
if test x"${JAVA_HOME}" != "x" -a x"${JAVA_HOME}" != "xno"; then
  AC_CHECK_FILES([${JAVA_HOME}/include/jni.h
                  ${JAVA_HOME}/include/linux/jni_md.h
                  ${JAVA_HOME}/jre/lib/amd64/libjava.so
                  ${JAVA_HOME}/jre/lib/amd64/server/libjvm.so],
                 [], [AC_MSG_ERROR([
---------------------------------------------------
The given JAVA_HOME:
  ${JAVA_HOME}
appears to be corrupted (Are you using a JRE?)
---------------------------------------------------])])
  java=yes
  JVM_FLAGS="-I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux"
  JVM_LDFLAGS="-L${JAVA_HOME}/jre/lib/amd64 -L${JAVA_HOME}/jre/lib/amd64/server"
  JVM_LIBS="-ljvm"
  AC_MSG_NOTICE([accept JAVA_HOME=${JAVA_HOME}])
fi
AM_CONDITIONAL([JDK_INSTALLED], [test x"${java}" = "xyes"])

## -------------------------------------------------------------------
## Checks for HDFS
## -------------------------------------------------------------------

hadoop=no
AC_ARG_WITH([hadoop],
            [AS_HELP_STRING([--with-hadoop], [build indexfs with hdfs])],
            [HADOOP_HOME=${withval}], [HADOOP_HOME=${HADOOP_HOME}])
if test x"${HADOOP_HOME}" = "xyes"; then
  for hh in "${HOME}/usr/hadoop2" "/usr/local/hadoop2"
  do
    if test -e ${hh}/bin/hdfs; then
      HADOOP_HOME="${hh}"
      break
    fi
  done
  if test x"${HADOOP_HOME}" = "xyes"; then
    AC_MSG_ERROR([
---------------------------------------------------
HADOOP have to be explicitly specified
  with '--with-hadoop=HADOOP_HOME'
HADOOP is required for IndexFS to work with HDFS.
---------------------------------------------------])
  fi
fi
HDFS_FLAGS=""
HDFS_LIBS=""
HDFS_LDFLAGS=""
if test x"${HADOOP_HOME}" != "x" -a x"${HADOOP_HOME}" != "xno"; then
  if test x"${java}" = "xno"; then
    AC_MSG_ERROR([
---------------------------------------------------
HADOOP cannot be enabled without a JDK
please specify JDK using '--with-java=JAVA_HOME'
---------------------------------------------------])
  fi
  AC_CHECK_FILES([${HADOOP_HOME}/include/hdfs.h
                  ${HADOOP_HOME}/lib/native/libhdfs.so],
                 [], [AC_MSG_ERROR([
---------------------------------------------------
The given HADOOP_HOME:
  ${HADOOP_HOME}
appears to be corrupted (Perhaps due to a typo?)
---------------------------------------------------])])
  hadoop=yes
  HDFS_FLAGS="-I${HADOOP_HOME}/include"
  HDFS_LDFLAGS="-L${HADOOP_HOME}/lib/native"
  HDFS_LIBS="-lhdfs"
  AC_MSG_NOTICE([accept HADOOP_HOME=${HADOOP_HOME}])
fi
AM_CONDITIONAL([BACKEND_HDFS], [test x"${hadoop}" = "xyes"])

## -------------------------------------------------------------------
## Checks for PVFS
## -------------------------------------------------------------------

AC_ARG_WITH([pvfs2],
            [AS_HELP_STRING([--with-pvfs2],
              [build indexfs with pvfs2])],
            [pvfs2=${withval}], [pvfs2=no])
PVFS2_LIBS=""
if test x"${pvfs2}" != "xno"; then
  AC_CHECK_HEADER([pvfs2.h],
                  [], [AC_MSG_ERROR([
---------------------------------------------------
pvfs2.h not found
which is required for IndexFS to work with PVFS2
please install pvfs2 and try again.
---------------------------------------------------])])
  AC_CHECK_LIB([pvfs2], [pvfs_fsync],
               [], [AC_MSG_ERROR([
---------------------------------------------------
libpvfs2.so not found
which is required for IndexFS to work with PVFS2
please install pvfs2 and try again.
---------------------------------------------------])])
  AC_CHECK_LIB([crypto], [CRYPTO_new_ex_data],
               [], [AC_MSG_ERROR([
---------------------------------------------------
libcrypto.so not found
which is required by PVFS2 client module
please install ssl/openssl and try again.
---------------------------------------------------])])
  AC_CHECK_LIB([ssl], [SSL_library_init],
               [], [AC_MSG_ERROR([
---------------------------------------------------
libssl.so not found
which is required by PVFS2 client module
please install ssl/openssl and try again.
---------------------------------------------------])])
  PVFS2_LIBS="-lpvfs2 -lssl -lcrypto"
fi
AC_SUBST([PVFS2_LIBS])
AM_CONDITIONAL([BACKEND_PVFS2], [test x"${pvfs2}" != "xno"])

## -------------------------------------------------------------------
## Checks for RADOS
## -------------------------------------------------------------------

rados=no
AC_ARG_WITH([rados],
            [AS_HELP_STRING([--with-rados],
              [build indexfs with rados])],
            [rados=${withval}], [rados=no])
if test x"${rados}" != "xno"; then
  AC_CHECK_HEADER([rados/librados.h],
                  [], [AC_MSG_ERROR([
---------------------------------------------------
librados.h not found
which is required for indexfs to work with rados
please install librados and try again.
---------------------------------------------------])])
  AC_CHECK_LIB([rados], [rados_create],
               [], [AC_MSG_ERROR([
---------------------------------------------------
librados.so not found
which is required for indexfs to work with rados
please install librados and try again.
---------------------------------------------------])])
fi
AM_CONDITIONAL([BACKEND_RADOS], [test x"${rados}" != "xno"])

## -------------------------------------------------------------------
## Checks for IO Tests
## -------------------------------------------------------------------

AC_ARG_ENABLE([iotests],
              [AS_HELP_STRING([--enable-iotests],
                              [build iotests @<:@default: auto@:>@])],
              [iotests=${enableval}], [iotests=auto])
if test x"${iotests}" = "xyes"; then
  if test x"${mpi_detect}" != "xyes"; then
    AC_MSG_ERROR([mpi not installed])
  fi
fi
if test x"${mpi_detect}" = "xyes"; then
  if test x"${iotests}" != "xno"; then
    iotests=yes
  fi
fi
AM_CONDITIONAL([BUILD_IOTESTS], [test x"${iotests}" = "xyes"])

## -------------------------------------------------------------------
## Checks for MD Tests
## -------------------------------------------------------------------

AC_ARG_ENABLE([mdtests],
              [AS_HELP_STRING([--enable-mdtests],
                              [build mdtests @<:@default: auto@:>@])],
              [mdtests=${enableval}], [mdtests=auto])
if test x"${mdtests}" = "xyes"; then
  if test x"${mpi_detect}" != "xyes"; then
    AC_MSG_ERROR([mpi not installed])
  fi
fi
if test x"${mpi_detect}" = "xyes"; then
  if test x"${mdtests}" != "xno"; then
    mdtests=yes
  fi
fi
AM_CONDITIONAL([BUILD_MDTESTS], [test x"${mdtests}" = "xyes"])

## -------------------------------------------------------------------
## Checks for FUSE
## -------------------------------------------------------------------

fuse_detect_hdr=yes
fuse_detect_lib=yes
CPPFLAGS_BACKUP="${CPPFLAGS}"
CPPFLAGS="${CPPFLAGS_BACKUP} -D_FILE_OFFSET_BITS=64"
AC_CHECK_HEADERS([fuse.h], [], [fuse_detect_hdr=no])
AC_CHECK_LIB([fuse], [fuse_main], [], [fuse_detect_lib=no])
CPPFLAGS="${CPPFLAGS_BACKUP}"
AC_ARG_ENABLE([fuse],
              [AS_HELP_STRING([--enable-fuse],
                              [build FUSE Client @<:@default: auto@:>@])],
              [fuse=${enableval}], [fuse=auto])
if test x"${fuse}" = "xyes"; then
  if test x"${fuse_detect_hdr}" != "xyes"; then
    AC_MSG_ERROR([fuse.h not found])
  fi
  if test x"${fuse_detect_lib}" != "xyes"; then
    AC_MSG_ERROR([fuse.so not found])
  fi
fi
if test x"${fuse_detect_hdr}" = "xyes" -a x"${fuse_detect_lib}" = "xyes"; then
  if test x"${fuse}" != "xno"; then
    fuse=yes
  fi
fi
AM_CONDITIONAL([BUILD_FUSE], [test x"${fuse}" = "xyes"])

## -------------------------------------------------------------------
## Checks for Zookeeper
## -------------------------------------------------------------------

zk_detect_hdr=yes
zk_detect_lib=yes
AC_CHECK_HEADERS([zookeeper/zookeeper.h], [], [zk_detect_hdr=no])
AC_CHECK_LIB([zookeeper_mt], [zookeeper_init], [], [zk_detect_lib=no])
AC_ARG_ENABLE([zookeeper],
              [AS_HELP_STRING([--enable-zookeeper],
                              [built with Zookeeper @<:@default: auto@:>@])],
              [zk=${enableval}], [zk=auto])
if test x"${zk}" = "xyes"; then
  if test x"${zk_detect_hdr}" != "xyes"; then
    AC_MSG_ERROR([zookeeper.h not found])
  fi
  if test x"${zk_detect_lib}" != "xyes"; then
    AC_MSG_ERROR([libzookeeper_mt.so not found])
  fi
fi
if test x"${zk_detect_hdr}" = "xyes" -a x"${zk_detect_lib}" = "xyes"; then
  if test x"${zk}" != "xno"; then
    zk=yes
  fi
fi
AM_CONDITIONAL([BUILD_ZK], [test x"${zk}" = "xyes"])

## -------------------------------------------------------------------
## Checks for Snappy
## -------------------------------------------------------------------

snappy_detect_hdr=yes
snappy_detect_lib=yes
AC_CHECK_HEADERS([snappy.h], [], [snappy_detect_hdr=no])
AC_CHECK_LIB([snappy], [snappy_compress], [], [snappy_detect_lib=no])
AC_ARG_ENABLE([snappy],
              [AS_HELP_STRING([--enable-snappy],
                              [build with Snappy @<:@default: auto@:>@])],
              [snappy=${enableval}], [snappy=auto])
if test x"${snappy}" = "xyes"; then
  if test x"${snappy_detect_hdr}" != "xyes"; then
    AC_MSG_ERROR([snappy.h not found])
  fi
  if test x"${snappy_detect_lib}" != "xyes"; then
    AC_MSG_ERROR([libsnappy.so not found])
  fi
fi
SNAPPY_FLAGS=""
if test x"${snappy_detect_hdr}" = "xyes" -a x"${snappy_detect_lib}" = "xyes"; then
  if test x"${snappy}" != "xno"; then
    SNAPPY_FLAGS="-DSNAPPY"
  fi
fi
AC_SUBST([SNAPPY_FLAGS])

## -------------------------------------------------------------------
## Setup Version Number
## -------------------------------------------------------------------

INDEXFS_VERSION=${PACKAGE_VERSION}
AC_SUBST([INDEXFS_VERSION])

## -------------------------------------------------------------------
## Backend Checks
## -------------------------------------------------------------------

if test x"${hadoop}" != "xno" -a \
  x"${pvfs2}" != "xno"; then
  AC_MSG_ERROR([
---------------------------------------------------
Only 1 specific backend can be enabled
---------------------------------------------------])
fi
##### HDFS #####
if test x"${hadoop}" != "xno"; then
  BACKEND_FLAGS="-DHDFS"
  CFLAGS="$CFLAGS $HDFS_FLAGS $JVM_FLAGS"
  CXXFLAGS="$CXXFLAGS $HDFS_FLAGS $JVM_FLAGS"
  LDFLAGS="$LDFLAGS $HDFS_LDFLAGS $JVM_LDFLAGS"
  LIBS="$LIBS $HDFS_LIBS $JVM_LIBS"
  AC_DEFINE([__HDFS__], [],
            [use HDFS as the underlying file system])
fi
##### PVFS #####
if test x"${pvfs2}" != "xno"; then
  BACKEND_FLAGS="-DPVFS"
  AC_DEFINE([__PVFS__], [],
            [use PVFS as the underlying file system])
fi
##### RADOS #####
if test x"${rados}" != "xno"; then
  BACKEND_FLAGS="-DRADOS"
  AC_DEFINE([__RADOS__], [],
            [use RADOS as the underlying file system])
fi
##### POSIX #####
if test x"${hadoop}" = "xno" -a x"${pvfs2}" = "xno" -a x"${rados}" = "xno"; then
  BACKEND_FLAGS="-DNFS"
  AC_DEFINE([__NFS__], [],
            [use NFS as the underlying file system])
fi
AC_SUBST([BACKEND_FLAGS])

## -------------------------------------------------------------------
## Generate Makefiles
## -------------------------------------------------------------------

AC_CONFIG_FILES([Makefile
                 lib/leveldb/Makefile
                 thrift/Makefile
                 common/Makefile
                 util/Makefile
                 metadb/Makefile
                 ipc/Makefile
                 server/Makefile
                 client/Makefile
                 c/Makefile
                 bin/Makefile
                 io_test/Makefile])
AC_OUTPUT

AC_MSG_NOTICE([
Using IndexFS : ${INDEXFS_VERSION}])

if test x"${hadoop}" != "xno"; then
  AC_MSG_NOTICE([
---------------------------------------------------
IndexFS Building Plan

Summary:
  JAVA_HOME:
    ${JAVA_HOME}
  HADOOP_HOME:
    ${HADOOP_HOME}
  Bulk Insertion: Off
  IndexFS will be built upon HDFS version 2.x
---------------------------------------------------])
fi
if test x"${pvfs2}" != "xno"; then
  AC_MSG_NOTICE([
---------------------------------------------------
IndexFS Building Plan

Summary:
  Bulk Insertion: Off
  IndexFS will be built upon PVFS2
---------------------------------------------------])
fi
if test x"${rados}" != "xno"; then
  AC_MSG_NOTICE([
---------------------------------------------------
IndexFS Building Plan

Summary:
  Bulk Insertion: Off
  IndexFS will be built upon RADOS
---------------------------------------------------])
fi
if test x"${hadoop}" = "xno" -a x"${pvfs2}" = "xno" -a x"${rados}" = "xno"; then
  AC_MSG_NOTICE([
---------------------------------------------------
IndexFS Building Plan

Summary:
  Bulk Insertion: Off
  IndexFS will be built upon POSIX
---------------------------------------------------])
fi

## -------------------------------------------------------------------
## END CONFIGURATION
## -------------------------------------------------------------------
