#!/bin/sh
#
#-----------------------------------------------------------------
# Uncomment exactly one of the lines labelled (A), (B), and (C) below
# to switch between compilation modes.
#
OPT="-O2 -DNDEBUG"           # (A) Production use (optimized mode)
# OPT="-g2"                  # (B) Debug mode, w/ full line-level debugging symbols
# OPT="-g2 -DNDEBUG"         # (C) Profiling mode: opt, but w/debugging symbols
#-----------------------------------------------------------------
#
#-----------------------------------------------------------------
# Uncomment exactly one of the lines labelled (A), (B), (C), and (D) below
# to enable PVFS2, HDFS, or other bindings.
#
FS_OPT=""                    # (A) Build IndexFS upon POSIX
# FS_OPT="--with-hadoop"     # (B) Build IndexFS upon HDFS
# FS_OPT="--with-pvfs2"      # (C) Build IndexFS upon PVFS2
# FS_OPT="--with-rados"      # (D) Build IndexFS upon RADOS
#-----------------------------------------------------------------

# OPT="$OPT -Wall -Wno-sign-compare"

# MORE_OPT1="-DIDXFS_EXTRA_SCALE -DIDXFS_VIRTUAL_SERVERS"
# MORE_OPT2="-DIDXFS_RPC_NOBLOCKING"
# MORE_OPT3="-DIDXFS_RPC_DEBUG"
# MORE_OPT4="-DLEVELDB_VERSION_EDIT_DEBUG"
OPT="$OPT $MORE_OPT1 $MORE_OPT2 $MORE_OPT3 $MORE_OPT4 -DIDXFS_ENABLE_COMPRESSION"

which mpicc.mpich &>/dev/null
if test $? -eq 0
then
  MPI_OPT="MPICC=mpicc.mpich MPICXX=mpicxx.mpich"
fi

mkdir -p build && cd build || exit 1
../configure CFLAGS="${OPT}" CXXFLAGS="${OPT}" ${MPI_OPT} ${FS_OPT} || exit 1
make -j8 --no-print-directory || exit 1

exit 0
