#!/bin/bash -x

me=$0
cd `dirname $me`/.. && pwd
./stop-cluster.sh && ./start-cluster.sh
num_clients=2
export INDEXFS_RUN_PREFIX="a"
sleep 3
export INDEXFS_RUN_TYPE="regular"
./tree-test.sh "$num_clients"
sleep 3
./restart-cluster.sh
sleep 3
export INDEXFS_RUN_TYPE="no-write"
./tree-test.sh "$num_clients"

exit 0
