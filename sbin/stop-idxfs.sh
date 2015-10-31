#!/bin/bash
#
# Copyright (c) 2014 The IndexFS Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.
#
# Use this script to shutdown an indexfs server instance.
#
# Root privilege is neither required nor recommended to run this script.  
#

me=$0
INDEXFS_HOME=$(cd -P -- `dirname $me`/.. && pwd -P)

INDEXFS_ROOT=${INDEXFS_ROOT:-"/tmp/indexfs"}
INDEXFS_ID=${INDEXFS_ID:-"0"}
INDEXFS_RUN=${INDEXFS_RUN:-"$INDEXFS_ROOT/run/s$INDEXFS_ID"}
INDEXFS_PID_FILE=$INDEXFS_RUN/indexfs_server.pid.$INDEXFS_ID

srv_addr=${1-"`hostname -s`"}
echo "Stopping indexfs server $INDEXFS_ID at $srv_addr ... "

# shutdown indexfs server
if test -e $INDEXFS_PID_FILE
then
  pid=`cat $INDEXFS_PID_FILE`
  kill -9 $pid &>/dev/null; rm -f $INDEXFS_PID_FILE
else
  echo "No running indexfs server instance found at $srv_addr"
fi

exit 0
