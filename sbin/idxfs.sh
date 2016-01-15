#!/bin/bash
#
# Copyright (c) 2014-2016 Carnegie Mellon University.
#
# All rights reserved.
#
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.
#

if test $# -lt 1
then
  echo "== Usage: $0 [mkdir|mknod|getattr|chmod|readdir|backend] <file_path>"
  exit 1
fi

me=$0
INDEXFS_HOME=$(cd -P -- `dirname $me`/.. && pwd -P)
INDEXFS_CONF_DIR=${INDEXFS_CONF_DIR:-"$INDEXFS_HOME/etc/indexfs-lo"}

# check the location of the build directory
INDEXFS_BASE=$INDEXFS_HOME

if test -d $INDEXFS_HOME/build
then
  INDEXFS_BASE=$INDEXFS_HOME/build
fi

is_nfs()
{
  cat $INDEXFS_BASE/config.h \
    | grep "#define __NFS__" &>/dev/null
  if test $? = 0
  then
    echo __NFS__ && return 0
  fi
  return 1
}

is_hdfs()
{
  cat $INDEXFS_BASE/config.h \
    | grep "#define __HDFS__" &>/dev/null
  if test $? = 0
  then
    echo __HDFS__ && return 0
  fi
  return 1
}

is_rados()
{
  cat $INDEXFS_BASE/config.h \
    | grep "#define __RADOS__" &>/dev/null
  if test $? = 0
  then
    echo __RADOS__ && return 0
  fi
  return 1
} 

get_indexfs_backend()
{
  is_nfs || is_hdfs || is_rados || exit 1
}

prepare_runtime()
{
  if test x"`get_indexfs_backend`" = x"__HDFS__"
  then
    LD_PATH=`$INDEXFS_HOME/sbin/hdfs.sh ldpath`
    export LD_LIBRARY_PATH=$LD_PATH
    export LIBHDFS_OPTS="-Djava.library.path=$LD_PATH"
    export CLASSPATH=`$INDEXFS_HOME/sbin/hdfs.sh classpath`
  fi
}

# execute command
COMMAND=$1
case $COMMAND in
  mkdir)
    prepare_runtime
    $INDEXFS_BASE/bin/mkdir \
      --configfn=$INDEXFS_CONF_DIR/indexfs_conf \
      --srvlstfn=$INDEXFS_CONF_DIR/server_list "$2"
    ;;
  mknod)
    prepare_runtime
    $INDEXFS_BASE/bin/mknod \
      --configfn=$INDEXFS_CONF_DIR/indexfs_conf \
      --srvlstfn=$INDEXFS_CONF_DIR/server_list "$2"
    ;;
  chmod)
    prepare_runtime
    $INDEXFS_BASE/bin/chmod \
      --configfn=$INDEXFS_CONF_DIR/indexfs_conf \
      --srvlstfn=$INDEXFS_CONF_DIR/server_list "$2"
    ;;
  getattr)
    prepare_runtime
    $INDEXFS_BASE/bin/getattr \
      --configfn=$INDEXFS_CONF_DIR/indexfs_conf \
      --srvlstfn=$INDEXFS_CONF_DIR/server_list "$2"
    ;;
  readdir)
    prepare_runtime
    $INDEXFS_BASE/bin/readdir \
      --configfn=$INDEXFS_CONF_DIR/indexfs_conf \
      --srvlstfn=$INDEXFS_CONF_DIR/server_list "$2"
    ;;
  backend)
    get_indexfs_backend
    ;;
  *)
    echo "Unrecognized command '$COMMAND' - oops"
    exit 1
    ;;
esac

exit 0
