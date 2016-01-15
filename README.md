IndexFS - 0.4.x
===============

IndexFS is designed as file system middleware layered on top of an
existing cluster file system deployment to improve metadata performance
as well as small file operation efficiency of the original file system.
IndexFS reuses the data path of the underlying file system and packs
directory entries, file attributes, and small file data into a set of
large, immutable, log-structured, and indexed, data structures
(**_SSTables_**) that are stored in the underlying file system. Our
experiments show that IndexFS is able to our-perform existing solutions
such as `PVFS`, `Lustre`, and `HDFS`, by as much as orders of magnitude.

The following is a guide describing how to install and run IndexFS on
your local Linux machine. Please visit our project home at
http://www.pdl.cmu.edu/indexfs for more information. Please also note that
the current implementation of IndexFS is not of production quality
and is recommended to be used for research purpose only. Thanks a lot.

INDEXFS INSTALLATION GUIDE
==========================

1. System Prerequisites
2. Build from Source
3. IndexFS in Standalone Mode

SYSTEM PREREQUISITES
--------------------

IndexFS depends on `gflags-2.0`, `glog-0.3.3+`, and `thrift-0.9.1+`.
In order to build IndexFS from its source, you will also need a C++
building system such as `GUN` including `gcc`, `g++`, `make`,
`autoconf`, `automake`, and `libtool`.

In addition, some benchmarks that IndexFS uses to evaluate system
performance are build with `MPI` -- at least one implementation of MPI
(such as `MPICH`) should be present for these benchmarks to run.

To help ease IndexFS deployment and avoid dependency issues, IndexFS
provides gflags, glog, and thrift source packages along with its src
code. System administrators may directly use these packages to build
and install these required IndexFS dependencies.

#### STEP-BY-STEP INSTRUCTIONS

##### INSTALL SYSTEM PACKAGES

* **Ubuntu**

        sudo apt-get install gcc g++ make flex bison
        sudo apt-get install autoconf automake libtool pkg-config
        sudo apt-get install zlib1g-dev libsnappy-dev
        sudo apt-get install libboost-all-dev libevent-dev libssl-dev
        sudo apt-get install libfuse-dev libmpich-dev mpich pdsh

##### Build & Install Depends

Use GNU standard building process to build and install `gflags`,
`glog`, and `thrift`, in that order.

* **To build gflags and glog**:

        ./configure && make && sudo make install

NB: thrift's automake scripts have several known bugs, which will
cause both make and make install to fail. However, those errors are
not vital in terms of building and installation. Just ignore them
and life is still good.

* **To build thrift**:

        ./configure || make || sudo make install || exit 0

BUILD INDEXFS FROM SOURCE
-------------------------

##### Build IndexFS

IndexFS also follows GNU standard building process. For your
convenience, IndexFS provides `bootstrap.sh` which does this
automatically for you.

* **To build IndexFS**:
  
        ./bootstrap.sh

NB: you don't have to install IndexFS into your system. Our scripts
will not assume IndexFS binaries to be accessible from your system path.

INDEXFS IN STANDALONE MODE
--------------------------

##### Run standalone IndexFS

Running IndexFS in standalone mode is a quick way to test if IndexFS
has been successfully built.

By being _standalone_, we mean running one single IndexFS (metadata)
server instance and multiple client processes at one single machine.
So everything is in one box.

* **To start IndexFS server**:

        $INDEXFS_HOME/sbin/start-idxfs.sh

* **To start IndexFS clients (processes) and run tests**:

        $INDEXFS_HOME/sbin/tree-test.sh

* **To stop IndexFS server**:

        $INDEXFS_HOME/sbin/stop-idxfs.sh

In the above scripts, IndexFS server will be started as a daemon
running in the background. It's pid will be remembered at
**/tmp/indexfs/metadata_server.pid**.

A simple MPI-based test will be performed against IndexFS in terms of
its metadata path. The test will fork 2 client processes to
collectively create and stat 1600 files under 1 single shared
directory. This test is expected to conclude in less than 1 second.
