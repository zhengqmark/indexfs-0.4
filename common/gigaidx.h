// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_GIGA_DIRINDEX_H_
#define _INDEXFS_GIGA_DIRINDEX_H_

#include "common/config.h"

namespace indexfs {

class DirIndex;

// A policy holds global directory indexing parameters that can be shared
// among all directory indices. These include:
//   1) Current number of servers
//   2) Max number of virtual servers allowed
// We currently do not support dynamic changes of the total number of
// servers. However, we do support re-starting indexfs with distinct sets
// of servers. In fact, one can start an indexfs cluster using an initial number
// of servers, stop the cluster, and then restart it using a different number of
// servers. In these cases, we expect indexfs to rebalance itself so that each new
// indexfs server will get an equal load compared to other indexfs servers.
//
class DirIndexPolicy {
 private:
  int num_servers_;
  int max_vs_;

 public:
  // Return the current number of servers.
  int NumServers() {
    return num_servers_;
  }

  // Return the maximum number of virtual servers allowed.
  int MaxNumVirtualServers() {
    return max_vs_;
  }

  // Retrieve the system-wide directory index policy.
  // The returned object belongs to the system and should never
  // be disposed.
  //
  static DirIndexPolicy* Default(Config* config);

  static DirIndexPolicy* TEST_NewPolicy(int num_servers, int max_vs);

  // Load a previous DirIndex from an existing in-memory image.
  DirIndex* RecoverDirIndex(const Slice& dmap_data);

  // Initialize a new DirIndex for the given directory.
  DirIndex* NewDirIndex(int64_t dir_id, int16_t zeroth_server);
};

class DirIndex {
 private:
  // The max radix present in the bitmap
  int MaxRadix() const;

  // The highest index present in the bitmap
  int HighestIndex() const;

  // Reconstruct an index using the given in-memory representation.
  DirIndex(const Slice& dmap_data, DirIndexPolicy* policy);

  // Create a new index using the given settings.
  DirIndex(int64_t dir_id, int16_t zeroth_server, DirIndexPolicy* policy);

  friend class DirIndexPolicy;
  DirIndexPolicy* policy_;
  struct Rep;
  Rep* rep_;
  // No copying allowed
  DirIndex(const DirIndex&);
  DirIndex& operator=(const DirIndex&);

 public:

  // Discard the current index and override it with another in-memory image.
  void TEST_Reset(const Slice& dmap_data);

  // Update the index by merging another index of the same directory.
  void Update(const Slice& dmap_data);

  // Update the index by merging another index of the same directory.
  void Update(const DirIndex& dir_idx);

  // Return the server responsible for the given partition.
  int GetServerForIndex(int index) const;

  // Return the partition responsible for the given file.
  int GetIndex(const std::string& fname) const;

  // Return the server responsible for the given file.
  int SelectServer(const std::string& fname) const;

  // Return the status of the given bit.
  bool GetBit(int index) const;

  // Set the bit at the given index.
  void SetBit(int index);

  // Set all bits within the given range (both inclusive), [start, end] <- 1.
  void TEST_SetBits(int end, int start = 0);

  // Clear the bit at the given index.
  void TEST_UnsetBit(int index);

  // Revert all bits and roll back to the initial state.
  void TEST_RevertAll();

  // Return true if the given partition can be further divided.
  bool IsSplittable(int index) const;

  // Return the next child partition for the given parent partition.
  int NewIndexForSplitting(int index) const;

  // Return the INODE number of the directory being indexed.
  int64_t FetchDirId() const;

  // Return the zeroth server of the directory being indexed.
  int16_t FetchZerothServer() const;

  // Return the internal bitmap radix of the index.
  int16_t FetchBitmapRadix() const;

  // Return the in-memory representation of this index.
  Slice ToSlice() const;

  // Return true if the given hash will belong to the given child partition.
  static bool ToBeMigrated(int index, const char* hash);

  // Return the hash value of the given file.
  static size_t GetNameHash(const std::string& fname, char* hash);

  // Return the server responsible for a given index.
  static int MapIndexToServer(int index, int zeroth_server, int num_servers);
};

} /* namespace indexfs */

#endif /* _INDEXFS_GIGA_DIRINDEX_H_ */
