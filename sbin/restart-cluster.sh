#!/bin/bash
#
# Copyright (c) 2014 The IndexFS Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.
#
# Please run this script at the indexfs's home directory:
#   > sbin/restart-cluster.sh
#
# This restarts the indexfs cluster currently running on the local machine.
#

me=$0
INDEXFS_HOME=$(cd -P -- `dirname $me`/.. && pwd -P)
INDEXFS_CONF_DIR=${INDEXFS_CONF_DIR:-"$INDEXFS_HOME/etc/indexfs-lo"}

# check the location of the build directory
INDEXFS_BASE=$INDEXFS_HOME
if test -d "$INDEXFS_HOME/build"
then
  INDEXFS_BASE=$INDEXFS_HOME/build
fi

# check the existence of our indexfs server binary
if test ! -e "$INDEXFS_BASE/indexfs_server"
then
  echo "Cannot find the indexfs server binary -- oops"
  echo "It is supposed to be found at $INDEXFS_BASE/indexfs_server"
  exit 1
fi

# check cluster root
INDEXFS_ROOT=${INDEXFS_ROOT-"/tmp/indexfs"}
if test ! -d "$INDEXFS_ROOT"
then
  echo "Root directory not found -- oops"
  echo "It is supposed to be found at $INDEXFS_ROOT"
  exit 1
fi

report_error() {
  echo "Fail to restart indexfs server at $1"
  echo "Abort!"
  exit 1
}

# reboot indexfs cluster
for srv_addr in \
  $(cat $INDEXFS_CONF_DIR/server_list)
do
  INDEXFS_ID=$((${INDEXFS_ID:-"-1"} + 1))
  INDEXFS_RUN=$INDEXFS_ROOT/run/s$INDEXFS_ID
  env INDEXFS_ID=$INDEXFS_ID \
      INDEXFS_CONF_DIR=$INDEXFS_CONF_DIR \
      INDEXFS_ROOT=$INDEXFS_ROOT \
      INDEXFS_RUN=$INDEXFS_RUN \
      INDEXFS_BASE=$INDEXFS_BASE \
  $INDEXFS_HOME/sbin/start-idxfs.sh $srv_addr restart || report_error $srv_addr
done

exit 0
