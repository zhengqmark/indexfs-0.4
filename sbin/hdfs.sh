#!/bin/bash
#
# Copyright (c) 2014-2016 Carnegie Mellon University.
#
# All rights reserved.
#
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.
#
# Please set the hadoop and sunjdk path below:
#
# JAVA_HOME=${JAVA_HOME}
# HADOOP_HOME=${HADOOP_HOME}
#

if test $# -lt 1
then
  echo "== Usage: $0 [start|stop|kill|check|mkdir|rm|ls|classpath|ldpath] <hdfs_path>"
  exit 1
fi

me=$0
INDEXFS_HOME=$(cd -P -- `dirname $me`/.. && pwd -P)
HADOOP_CONF_DIR=$INDEXFS_HOME/etc/hdfs-lo
export HADOOP_CONF_DIR=$HADOOP_CONF_DIR

# check JAVA_HOME configuration
if test "x$JAVA_HOME" = "x"
then
  for openjdk in "/usr/local/openjdk" "$HOME/usr/openjdk"
  do
    if test -e $openjdk/jre/bin/java
    then
      JAVA_HOME=$openjdk
    fi
  done
  for sunjdk in "/usr/local/sunjdk" "$HOME/usr/sunjdk"
  do
    if test -e $sunjdk/jre/bin/java
    then
      JAVA_HOME=$sunjdk
    fi
  done
fi
if test "x$JAVA_HOME" = "x"
then
  echo "JAVA_HOME not set -- oops" && exit 1
fi
export JAVA_HOME=$JAVA_HOME

# check HADOOP_HOME configuration
if test "x$HADOOP_HOME" = "x"
then
  for hadoop_base in "/usr/local/hadoop2" "$HOME/usr/hadoop2"
  do
    if test -e $hadoop_base/bin/hadoop
    then
      HADOOP_HOME="$hadoop_base"
    fi
  done
fi
if test "x$HADOOP_HOME" = "x"
then
  echo "HADOOP_HOME not set -- oops" && exit 1
fi
export HADOOP_HOME=$HADOOP_HOME

prepare-hdfs()
{
# HDFS runtime root
INDEXFS_HADOOP_RUN="/tmp/indexfs-hadoop"

rm -rf $INDEXFS_HADOOP_RUN/*

INDEXFS_HADOOP_LOGS=$INDEXFS_HADOOP_RUN/logs
INDEXFS_HADOOP_NAME_DIR=$INDEXFS_HADOOP_RUN/dfs-name
INDEXFS_HADOOP_DATA_DIR=$INDEXFS_HADOOP_RUN/dfs-data

mkdir -p $INDEXFS_HADOOP_RUN
mkdir -p $INDEXFS_HADOOP_LOGS $INDEXFS_HADOOP_NAME_DIR $INDEXFS_HADOOP_DATA_DIR
}

fetch-hdfs-classpath()
{
echo -n "$HADOOP_CONF_DIR:"

COMM=$(find \
    $HADOOP_HOME/share/hadoop/common -name '*.jar')
HDFS=$(find \
    $HADOOP_HOME/share/hadoop/hdfs -name '*.jar')

echo -en "$COMM\n$HDFS" | sort -u \
    | grep -v 'sources.jar' | grep -v 'tests.jar' | tr '\n' ':'
echo "." # add the current dir to finish the job
}

fetch-hdfs-ldpath()
{
HDFS_LD_PATH=$HADOOP_HOME/lib/native
SUNJDK_LD_PATH=$JAVA_HOME/jre/lib/amd64/server:$JAVA_HOME/jre/lib/amd64

echo "$HDFS_LD_PATH:$SUNJDK_LD_PATH"

}

force-kill-hdfs()
{
for hdfs_proc in \
  "namenode" "datanode"
do
  for pid in $(ps aux | grep $hdfs_proc \
                      | grep -v grep \
                      | tr -s ' ' \
                      | cut -d' ' -f2)
  do
    kill -9 $pid
  done
done

}

COMMAND=$1
case $COMMAND in
  mkdir)
    $HADOOP_HOME/bin/hadoop fs -mkdir -p "$2" || exit 1
    $HADOOP_HOME/bin/hadoop fs -chmod 777 "$2" || exit 1
    ;;
  rm)
    $HADOOP_HOME/bin/hadoop fs -rm -r "$2" || exit 1
    ;;
  ls)
    $HADOOP_HOME/bin/hadoop fs -ls -h "$2" || exit 1
    ;;
  start)
    prepare-hdfs
    $HADOOP_HOME/bin/hdfs namenode -format || exit 1
    $HADOOP_HOME/sbin/hadoop-daemon.sh --script hdfs start namenode || exit 1
    $HADOOP_HOME/sbin/hadoop-daemon.sh --script hdfs start datanode || exit 1
    ;;
  stop)
    $HADOOP_HOME/sbin/hadoop-daemon.sh --script hdfs stop namenode || exit 1
    $HADOOP_HOME/sbin/hadoop-daemon.sh --script hdfs stop datanode || exit 1
    force-kill-hdfs
    ;;
  kill)
    force-kill-hdfs
    ;;
  check)
    $HADOOP_HOME/bin/hadoop version && $HADOOP_HOME/bin/hadoop checknative || exit 1
    ;;
  ldpath)
    fetch-hdfs-ldpath
    ;;
  classpath)
    fetch-hdfs-classpath
    ;;
  *)
    echo "Unrecognized command '$COMMAND' -- oops"
    exit 1
    ;;
esac

# Tested on Hadoop 2.4.1 and 2.5.2 #

exit 0
