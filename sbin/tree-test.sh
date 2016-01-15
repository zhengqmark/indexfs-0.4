#!/bin/bash
#
# Copyright (c) 2014 The IndexFS Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.
#
# Please run this script at the indexfs's home directory:
#   > sbin/tree-test.sh <num_of_clients> <run_type>
#
# This launches an MPI-based performance test on an indexfs setup.
# Root privilege is neither required nor recommended to run this script.
#
# MPI is required to run this script:
#   > mpirun -version
#

me=$0
INDEXFS_HOME=$(cd -P -- `dirname $me`/.. && pwd -P)
INDEXFS_CONF_FILE="/tmp/idxfs_conf"
INDEXFS_SERVER_LIST="/tmp/giga_conf"
INDEXFS_RUN_TYPE=${2-"$INDEXFS_RUN_TYPE"}
if test -z "$INDEXFS_RUN_TYPE"; then INDEXFS_RUN_TYPE="regular"; fi
INDEXFS_RUN_PREFIX=${INDEXFS_RUN_PREFIX="`date +%s`"}

# check the location of the build directory
INDEXFS_BUILD=$INDEXFS_HOME

if test -d $INDEXFS_HOME/build
then
  INDEXFS_BUILD=$INDEXFS_HOME/build
fi

# check our test binary
if test ! -e $INDEXFS_BUILD/io_test/io_driver
then
  echo "Cannot find the test binary -- oops"
  echo "It is supposed to be found at $INDEXFS_BUILD/io_test/io_driver"
  exit 1
fi

# check server list
if test ! -e $INDEXFS_SERVER_LIST
then
  echo "Cannot find indexfs server list -- oops"
  echo "It is supposed to be found at $INDEXFS_SERVER_LIST"
  exit 1
fi

# check indexfs configuration file
if test ! -e $INDEXFS_CONF_FILE
then
  echo "Cannot find indexfs configuration file -- oops"
  echo "It is supposed to be found at $INDEXFS_CONF_FILE"
  exit 1
fi

INDEXFS_ROOT=${INDEXFS_ROOT:-"/tmp/indexfs"}
INDEXFS_RUN=$INDEXFS_ROOT/run
INDEXFS_OUTPUT=$INDEXFS_RUN/tree_test
INDEXFS_BACKEND=`$INDEXFS_HOME/sbin/idxfs.sh backend`

# prepare test directories
rm -rf $INDEXFS_OUTPUT/*
mkdir -p $INDEXFS_OUTPUT

# prepare indexfs-hdfs runtime if necessary
if test x"$INDEXFS_BACKEND" = x"__HDFS__"
then
  LD_PATH=`$INDEXFS_HOME/sbin/hdfs.sh ldpath`
  if test -n "$LD_LIBRARY_PATH"
  then
    LD_PATH=$LD_PATH:$LD_LIBRARY_PATH
  fi
  export LD_LIBRARY_PATH=$LD_PATH
  export LIBHDFS_OPTS="-Djava.library.path=$LD_PATH"
  export CLASSPATH=`$INDEXFS_HOME/sbin/hdfs.sh classpath`
fi

# use mpich if possible
which mpiexec.mpich
if test $? -eq 0
then
  MPIEXEC=mpiexec.mpich
else
  MPIEXEC=mpiexec
fi

# advanced tree test settings
NUM_CLIENTS=${1-"2"}
FILE_ROOT="$INDEXFS_ROOT/_DATA_"
DB_ROOT="$INDEXFS_ROOT/_META_"
case $INDEXFS_RUN_TYPE in
  batch-mknod)
    extra_opts="--batch_creates=true"
    ;;
  no-write)
    extra_opts="--enable_prepare=false --enable_main=false"
    ;;
  bulk-insert)
    extra_opts="--enable_clean=false --bulk_insert=true"
    ;;
  read-old)
    extra_opts="--enable_main=false --bulk_insert=true --local_stat=false"
    extra_opts="$extra_opts --db_home_str=c --db_data_str=l --db_data_partitions=$NUM_CLIENTS"
    ;;
  *)
    ;;
esac

$MPIEXEC -np $NUM_CLIENTS $INDEXFS_BUILD/io_test/io_driver \
  --prefix=$INDEXFS_RUN_PREFIX \
  --task=tree \
  --dirs=1 \
  --files=8000 \
  --share_dirs \
  --ignore_errors=false \
  --file_dir=$FILE_ROOT \
  --db_root=$DB_ROOT \
  --configfn=$INDEXFS_CONF_FILE \
  --srvlstfn=$INDEXFS_SERVER_LIST \
  --log_dir=$INDEXFS_OUTPUT \
  --log_file=$INDEXFS_OUTPUT/perf_report \
  $extra_opts

exit 0
