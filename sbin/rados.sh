#!/bin/bash
#
# Copyright (c) 2014 The IndexFS Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.

if test $# -lt 1
then
  echo "== Usage: $0 [start|stop|status|kill|check|watch|ls|stat]"
  exit 1
fi

me=$0
INDEXFS_HOME=$(cd -P -- `dirname $me`/.. && pwd -P)
RADOS_CONF_DIR=$INDEXFS_HOME/etc/rados-lo
CEPH_CONF=$RADOS_CONF_DIR/ceph.conf
RADOS_RUN=/tmp/indexfs-rados

prepare_rados()
{
  sudo rm -rf $RADOS_RUN/*
  sudo mkdir -p $RADOS_RUN $RADOS_RUN/mon
  sudo mkdir -p $RADOS_RUN/osd/ceph-0 $RADOS_RUN/osd/ceph-1 $RADOS_RUN/osd/ceph-2
  sudo rm -f /tmp/ceph.conf
  sudo ln -fs $CEPH_CONF /tmp/ceph.conf
}

fix_pools()
{
  for pool in 'metadata'
  do
    sudo ceph -c $CEPH_CONF osd pool set $pool min_size 1
    sudo ceph -c $CEPH_CONF osd pool set $pool size 1
  done
  for pool in 'data' 'rbd'
  do
    sudo ceph -c $CEPH_CONF osd pool delete $pool $pool --yes-i-really-really-mean-it
  done
}

kill_rados()
{
  sudo killall -9 ceph-mon
  sudo killall -9 ceph-osd
}

force_kill_rados()
{
  sudo killall -9 ceph-mon &>/dev/null
  sudo killall -9 ceph-osd &>/dev/null
}

COMMAND=$1
case $COMMAND in
  ls)
    sudo rados -c $CEPH_CONF -p metadata ls
    ;;
  stat)
    sudo rados -c $CEPH_CONF -p metadata stat $2
    ;;
  check)
    sudo ceph --version
    ;;
  watch)
    sudo ceph -c $CEPH_CONF -w
    ;;
  status)
    sudo ceph -c $CEPH_CONF -s
    ;;
  start)
    force_kill_rados
    prepare_rados
    sudo mkcephfs -a -c $CEPH_CONF --no-copy-conf || exit 1
    sudo ceph-mon -i a -c $CEPH_CONF || exit 1
    fix_pools
    sudo ceph-osd -i 0 -c $CEPH_CONF || exit 1
    sudo ceph-osd -i 1 -c $CEPH_CONF || exit 1
    sudo ceph-osd -i 2 -c $CEPH_CONF || exit 1
    ;;
  stop|kill)
    kill_rados
    ;;
  *)
    echo "Unrecognized command '$COMMAND' -- oops"
    exit 1
    ;;
esac

exit 0
