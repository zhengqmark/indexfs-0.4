#!/bin/bash
#
# Copyright (c) 2014 The IndexFS Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.
#
# Use this script to start or restart an indexfs server instance.
#
# Root privilege is neither required nor recommended to run this script. 
#

me=$0
cmd=${2-"start"}
INDEXFS_HOME=$(cd -P -- `dirname $me`/.. && pwd -P)
INDEXFS_BASE=${INDEXFS_BASE:-"$INDEXFS_HOME/build"}
INDEXFS_CONF_DIR=${INDEXFS_CONF_DIR:-"$INDEXFS_HOME/etc/indexfs-lo"}
INDEXFS_BACKEND=`$INDEXFS_HOME/sbin/idxfs.sh backend`

# check indexfs backend
if test -z "$INDEXFS_BACKEND"
then
  echo "Cannot determine indexfs backend -- oops"
  exit 1
fi

INDEXFS_ID=${INDEXFS_ID:-"0"}
INDEXFS_ROOT=${INDEXFS_ROOT:-"/tmp/indexfs"}
INDEXFS_RUN=${INDEXFS_RUN:-"$INDEXFS_ROOT/run/s$INDEXFS_ID"}
INDEXFS_LOGS=$INDEXFS_RUN/logs
INDEXFS_OLD_LOGS=$INDEXFS_RUN/old_logs
INDEXFS_PID_FILE=$INDEXFS_RUN/indexfs_server.pid.$INDEXFS_ID

# check running instances
case "$cmd" in
  start)
    if test -e "$INDEXFS_PID_FILE"
    then
      echo "Killing existing indexfs server ..."
      pid=$(cat $INDEXFS_PID_FILE)
      kill -9 $pid && sleep 1; rm -f $INDEXFS_PID_FILE
    fi
    ;;
  restart)
    if test -e "$INDEXFS_PID_FILE"
    then
      echo "Stopping indexfs server ..."
      pid=$(cat $INDEXFS_PID_FILE)
      kill $pid && sleep 2; rm -f $INDEXFS_PID_FILE
    fi
    ;;
  *)
    exit 1
    ;;
esac

# prepare runtime env if necessary
if test x"$INDEXFS_BACKEND" = x"__HDFS__"
then
  LD_PATH=`$INDEXFS_HOME/sbin/hdfs.sh ldpath`
  if test -n "$LD_LIBRARY_PATH"
  then
    LD_PATH="$LD_PATH:$LD_LIBRARY_PATH"
  fi
  export LD_LIBRARY_PATH=$LD_PATH
  export LIBHDFS_OPTS="-Djava.library.path=$LD_PATH"
  export CLASSPATH=`$INDEXFS_HOME/sbin/hdfs.sh classpath`
fi

# prepare server directories
if test -d "$INDEXFS_LOGS"
then
  rm -rf $INDEXFS_OLD_LOGS
  mkdir -p $INDEXFS_RUN && mv \
    $INDEXFS_LOGS $INDEXFS_OLD_LOGS || exit 1
fi
mkdir -p $INDEXFS_RUN $INDEXFS_LOGS || exit 1

srv_addr=${1-"`hostname -s`"}
echo "Starting indexfs server $INDEXFS_ID at $srv_addr, logging to $INDEXFS_LOGS ..."

# start indexfs server
nohup $INDEXFS_BASE/indexfs_server \
    --srvid="$INDEXFS_ID" \
    --log_dir="$INDEXFS_LOGS" \
    --db_root="$INDEXFS_ROOT/leveldb" \
    --configfn="$INDEXFS_CONF_DIR/indexfs_conf" \
    --srvlstfn="$INDEXFS_CONF_DIR/server_list" \
  1>$INDEXFS_LOGS/indexfs_server.STDOUT 2>$INDEXFS_LOGS/indexfs_server.STDERR </dev/null &

echo "$!" | tee $INDEXFS_PID_FILE &>/dev/null

exit 0
