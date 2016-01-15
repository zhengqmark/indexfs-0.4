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
#   > sbin/stop-cluster.sh
#
# Use this script to shutdown an indexfs cluster running on
# a set of machines.
#
# Root privilege is neither required nor recommended to run this script.  
#

me=$0
INDEXFS_HOME=$(cd -P -- `dirname $me`/.. && pwd -P)
INDEXFS_ROOT=${INDEXFS_ROOT:-"/tmp/indexfs"}
INDEXFS_CONF_DIR=${INDEXFS_CONF_DIR:-"$INDEXFS_HOME/etc/indexfs"}

report_error() {
  echo "Fail to stop indexfs server at $1"
}

# shutdown indexfs cluster
for srv_addr in \
  $(cat $INDEXFS_CONF_DIR/server_list)
do
  INDEXFS_ID=$((${INDEXFS_ID:-"-1"} + 1))
  INDEXFS_RUN=$INDEXFS_ROOT/run/s$INDEXFS_ID
  ssh $(echo $srv_addr | cut -d':' -f1) "env \
      INDEXFS_ID=$INDEXFS_ID \
      INDEXFS_ROOT=$INDEXFS_ROOT \
      INDEXFS_RUN=$INDEXFS_RUN \
  $INDEXFS_HOME/sbin/stop-idxfs.sh $srv_addr" || report_error $srv_addr
done

# stop backend storage service if necessary
# note: this only works in stand-alone mode
INDEXFS_BACKEND=`$INDEXFS_HOME/sbin/idxfs.sh backend`
if test -z "$INDEXFS_BACKEND"
then
  echo "Cannot determine indexfs backend -- oops"
  exit 1
fi
case "$INDEXFS_BACKEND" in
  __NFS__)
    ;;
  __HDFS__)
    echo "Stopping HDFS ..."
    $INDEXFS_HOME/sbin/hdfs.sh kill
    ;;
  __RADOS__)
    echo "Stopping RADOS ..."
    $INDEXFS_HOME/sbin/rados.sh kill
    ;;
  *)
    echo "Unknown backend: $INDEXFS_BACKEND"
    ;;
esac

exit 0
