#!/bin/bash
#
# Copyright (c) 2014-2016 Carnegie Mellon University.
#
# All rights reserved.
#
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.
#
# Please run this script at the indexfs's home directory:
#   > sbin/start-cluster.sh
#
# This starts an indexfs cluster consisting of multiple indexfs server
# instances running on a set of machines. If indexfs has been built with
# a 3rd-party backend such as HDFS or RADOS, then that backend
# will also be created to server as the underlying storage infrastructure.
# Root privilege is neither required nor recommended to run this script.
#
# Before using this script, please prepare required config files at
# etc/indexfs/indexfs_conf and etc/indexfs/server_list.
# These files are distributed with default settings along with the source code.
# 
# Note that please use only IPv4 addresses in the server list file, as we
# currently do not support hostname or IPv6 addresses.
#

me=$0
INDEXFS_HOME=$(cd -P -- `dirname $me`/.. && pwd -P)
INDEXFS_CONF_DIR=${INDEXFS_CONF_DIR:-"$INDEXFS_HOME/etc/indexfs"}

# check indexfs backend type
INDEXFS_BACKEND=`$INDEXFS_HOME/sbin/idxfs.sh backend`
if test -z "$INDEXFS_BACKEND"
then
  echo "Cannot determine indexfs backend --oops"
  exit 1
fi
case "$INDEXFS_BACKEND" in
  __NFS__)
    echo "Using native POXIS as the storage backend"
    ;;
  __HDFS__)
    echo "Using HDFS as the storage backend"
    ;;
  __RADOS__)
    echo "Using RADOS as the storage backend"
    ;;
  *)
    echo "Unexpected backend '$INDEXFS_BACKEND'"
    exit 1
    ;;
esac

# check if we have the required server list
if test ! -e "$INDEXFS_CONF_DIR/server_list"
then
  echo "Cannot find our server list file -- oops"
  echo "It is supposed to be found at $INDEXFS_CONF_DIR/server_list"
  exit 1
fi

# check if we have the required configuration files
if test ! -e "$INDEXFS_CONF_DIR/indexfs_conf"
then
  echo "Cannot find our indexfs config file -- oops"
  echo "It is supposed to be found at $INDEXFS_CONF_DIR/indexfs_conf"
  exit 1
fi

# check the location of the build directory
INDEXFS_BUILD=$INDEXFS_HOME
if test -d $INDEXFS_HOME/build
then
  INDEXFS_BUILD=$INDEXFS_HOME/build
fi

# check the existence of our indexfs server binary
if test ! -e $INDEXFS_BUILD/indexfs_server
then
  echo "Cannot find the indexfs server binary -- oops"
  echo "It is supposed to be found at $INDEXFS_BUILD/indexfs_server"
  exit 1
fi

gen_fsid() {
  fsid=$(cat /dev/urandom \
    | tr -dc 'a-zA-Z0-9' \
    | fold -w 32 \
    | head -n 1)
  export fsid=$fsid
}

# prepare cluster root
# note: this must be a shared path in distributed settings
INDEXFS_ROOT=${INDEXFS_ROOT-"/tmp/indexfs"}
rm -rf $INDEXFS_ROOT/*
mkdir -p $INDEXFS_ROOT || exit 1
# check accesses
gen_fsid
echo $fsid > $INDEXFS_ROOT/__fsid__ || exit 1

# boot hdfs is necessary
# node: this only works in stand-alone mode
if test x"$INDEXFS_BACKEND" = x"__HDFS__"
then
  # check hdfs status
  echo "Cheking hdfs ..."
  $INDEXFS_HOME/sbin/hdfs.sh check || exit 1

  # bootstrap hdfs first
  echo "Starting hdfs at `hostname -s` ..."
  $INDEXFS_HOME/sbin/hdfs.sh kill &>/dev/null
  $INDEXFS_HOME/sbin/hdfs.sh start || exit 1 # will remove old data
  sleep 5

  echo "Setup hdfs ..."
  $INDEXFS_HOME/sbin/hdfs.sh mkdir $INDEXFS_RUN || exit 1
fi

# boot rados if necessary
# note: this only works in stand-alone mode
if test x"$INDEXFS_BACKEND" = x"__RADOS__"
then
  # check rados status
  echo "Checking rados ..."
  $INDEXFS_HOME/sbin/rados.sh check || exit 1

  # initialize rados cluster
  echo "Starting rados at `hostname -s` ..."
  $INDEXFS_HOME/sbin/rados.sh kill &>/dev/null
  $INDEXFS_HOME/sbin/rados.sh start || exit 1 # will remove old data
  sleep 5

  # check rados health
  echo "Checking rados cluster status ..."
  $INDEXFS_HOME/sbin/rados.sh status || exit 1
fi

report_error() {
  echo "Fail to start indexfs server at $1"
  echo "Abort!"
  exit 1
}

# boot indexfs cluster
for srv_addr in \
  $(cat $INDEXFS_CONF_DIR/server_list)
do
  INDEXFS_ID=$((${INDEXFS_ID:-"-1"} + 1)) # starts from 0
  INDEXFS_RUN=$INDEXFS_ROOT/run/s$INDEXFS_ID
  ssh $(echo $srv_addr | cut -d':' -f1) "env \
      INDEXFS_ID=$INDEXFS_ID \
      INDEXFS_CONF_DIR=$INDEXFS_CONF_DIR \
      INDEXFS_ROOT=$INDEXFS_ROOT \
      INDEXFS_RUN=$INDEXFS_RUN \
      INDEXFS_BUILD=$INDEXFS_BUILD \
  $INDEXFS_HOME/sbin/start-idxfs.sh $srv_addr" || report_error $srv_addr
done

exit 0
