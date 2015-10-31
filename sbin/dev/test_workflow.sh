#!/bin/bash -x

me=$0
cd `dirname $me`/.. && pwd
./stop-cluster.sh && ./start-cluster.sh
num_clients=2
export INDEXFS_RUN_PREFIX="a"
export INDEXFS_RUN_TYPE="bulk-insert"
./tree-test.sh "$num_clients"
sleep 5
mpiexec -np "$((2 * num_clients))" ../build/io_driver --task=sstcomp \
  --fs=localfs --sst_root=/tmp/indexfs/leveldb \
  --sst_input=/tmp/indexfs/leveldb/bk --sst_output=/tmp/indexfs/leveldb/l
sleep 5
export INDEXFS_RUN_TYPE="read-old"
./tree-test.sh "$num_clients"

exit 0
