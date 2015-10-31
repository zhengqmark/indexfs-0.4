// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <string.h>
#include <math.h>
#include <sstream>
#include <algorithm>
#include <gflags/gflags.h>

#include "common/common.h"
#include "common/gigaidx.h"
#include "common/logging.h"
#include "common/murmurhash3.h"

namespace indexfs {

namespace {

// Number of bytes in each name hash.
static const size_t kHashSize = 8;
// The constant magic number in every bitmap.
static const int16_t kMagicNum = 65527;

// Smallest bitmap radix.
static const int kMinRadix = 0;
#ifndef IDXFS_EXTRA_SCALE
// Largest bitmap radix.
static const int kMaxRadix = 8;
// Max radix for virtual servers.
static const int kVirRadix = 8;
#else
// Largest bitmap radix.
static const int kMaxRadix = 14;
// Max radix for virtual servers.
static const int kVirRadix = 12;
#endif
// Max number of partitions.
static const int kMaxPartitions = 1 << kMaxRadix;
// Default number of virtual servers.
static const int kNumVirtualServers = 1 << kVirRadix;

DEFINE_int32(num_virtual_servers, kNumVirtualServers, "Set the number of virtual servers");

// Number of bits in an unsigned 8-bit integer.
static const int kUnitSize = 8; // 8 bits
// Initial size of the bitmap.
static const int kInitBitmapSize = 32; // 32 bytes = 256 bits
// Max size in bytes of the bitmap for each directory.
static const int kMaxBitmapSize = kMaxPartitions / kUnitSize; // 32 bytes or 8 KB

// -------------------------------------------------------------
// Internal Helper Methods
// -------------------------------------------------------------

static inline
int ToLog2(int val) {
  return static_cast<int>(floor(log2(val)));
}

// Bit constants for fast bitwise calculation.
//
static
uint8_t kBitConsts[] = {
  1 << 0, 1 << 1, 1 << 2, 1 << 3, 1 << 4, 1 << 5, 1 << 6, 1 << 7
};

// Reverse the bits in a byte with 4 operations.
//
static inline
uint8_t Reverse(uint8_t byte) {
  return ((byte * 0x80200802ULL) & 0x0884422110ULL) * 0x0101010101ULL >> 32;
}

// The number of bits necessary to hold the given index.
//
// ------------------------
//   sample input/output
// ------------------------
//   0           -->  0
//   1           -->  1
//   2,3         -->  2
//   4,5,6,7     -->  3
//   128,129,255 -->  8
// ------------------------
//
static inline
int GetRadix(int index) {
  DLOG_ASSERT(index >= 0 && index < kMaxPartitions);
  return index == 0 ? 0 : 1 + ToLog2(index);
}

// Return the child index for a given index at a given radix.
//
// ------------------------
//   sample input/output
// ------------------------
//   i=0,   r=0 -->  1
//   i=1,   r=1 -->  3
//   i=3,   r=2 -->  7
//   i=7,   r=3 -->  15
//   i=127, r=7 -->  255
// ------------------------
//
static inline
int GetChildIndex(int index, int radix) {
  DLOG_ASSERT(index >= 0 && index < kMaxPartitions / 2);
  DLOG_ASSERT(radix >= kMinRadix && radix < kMaxRadix);
  return index + ( 1 << radix );
}

// Deduce the parent index from a specified child index.
//
// ------------------------
//   sample input/output
// ------------------------
//   0,1,2,4,128 -->  0
//   3,5,9,129   -->  1
//   6,10,130    -->  2
//   7,11,131    -->  3
//   255         -->  127
// ------------------------
//
static inline
int GetParentIndex(int index) {
  DLOG_ASSERT(index >= 0 && index < kMaxPartitions);
  return index == 0 ? 0 : index - ( 1 << ToLog2(index) );
}

// Current implementation uses 16-byte (128-bit) Murmur hash.
// However, any hash function producing at least 2-byte (16-bit) hash
// strings should in theory work for this bitmap too, for we currently
// only want to support as many as 65536 partitions per directory.
//
static inline
void MurmurHash(const std::string &buffer, char (&hash)[16]) {
  MurmurHash3_x64_128(buffer.c_str(), buffer.length(), 0, hash);
}

// Use the first "radix" number of bits from the hash to get the index,
// which is illustrated below.
//
// |<---------------  hash  --------------->|
// [ - 1st  byte - ][ - 2nd  byte - ][] .. []  << hash (input)
// |<---- radix bits ------>|
//
// [ - 2nd  byte - ][ - 1st  byte - ]
// [x x x x 4 3 2 1][8 7 6 5 4 3 2 1] << index (output)
//         |<---- radix bits ------>|
//
static
int ComputeIndexFromHash(const char *hash, int radix) {
  int index = 0;

  DLOG_ASSERT(radix >= kMinRadix);
  DLOG_ASSERT(radix <= kMaxRadix);

  int idx;
  int num_bytes = radix / kUnitSize;
  for (idx = 0; idx < num_bytes; idx++) {
    index += ( Reverse(hash[idx]) ) << (idx * kUnitSize);
  }
  int num_bits = radix % kUnitSize;
  if (num_bits > 0) {
    index += ( Reverse(hash[idx]) & ((1 << num_bits) - 1) ) << (idx * kUnitSize);
  }

  DLOG_ASSERT(index >= 0 && index < kMaxPartitions);
  return index;
}

} // namespace

// -------------------------------------------------------------
// Bitmap Policy Implementation
// -------------------------------------------------------------


DirIndexPolicy* DirIndexPolicy::Default(Config* config) {
  DirIndexPolicy* result = new DirIndexPolicy();
  result->num_servers_ = config->GetSrvNum();
# ifndef IDXFS_VIRTUAL_SERVERS
  result->max_vs_ = result->num_servers_;
# else
  result->max_vs_ = std::max(FLAGS_num_virtual_servers, config->GetSrvNum());
# endif
  DLOG_ASSERT(result->max_vs_ <= kMaxPartitions);
  return result;
}

DirIndexPolicy* DirIndexPolicy::TEST_NewPolicy(int num_servers, int max_vs) {
  DirIndexPolicy* result = new DirIndexPolicy();
  result->num_servers_ = num_servers;
  result->max_vs_ = max_vs;
  DLOG_ASSERT(result->max_vs_ <= kMaxPartitions);
  return result;
}

// Load a previous DirIndex from an existing in-memory image.
DirIndex* DirIndexPolicy::RecoverDirIndex(const Slice& dmap_data) {
  return new DirIndex(dmap_data, this);
}

// Initialize a new DirIndex for the given directory.
DirIndex* DirIndexPolicy::NewDirIndex(int64_t dir_id, int16_t zeroth_server) {
  return new DirIndex(dir_id, zeroth_server, this);
}

// -------------------------------------------------------------
// Bitmap Representation
// -------------------------------------------------------------

struct DirIndex::Rep {

  // Establish new state from the beginning
  Rep(int64_t dir_id, int16_t zeroth_server);

  // Rebuild the state from existing in-memory image
  Rep(const Slice& dmap_data);

  virtual ~Rep() {
    if (space_ != NULL) {
#     ifndef IDXFS_EXTRA_SCALE
        DLOG_ASSERT(space_ == static_space_);
#     endif
      if (space_ != static_space_) {
        delete [] space_;
      }
    }
  }

  Slice ToSlice() const {
    if (ExtractRadix() == 0) {
      return Slice(space_, kHeadSize);
    } else {
      return Slice(space_, kHeadSize + bitmap_size_);
    }
  }

  int HighestBit() const {
    int idx = bitmap_size_ - 1;
    for (; idx >= 0 && ( bitmap_[idx] ) == 0; --idx);
    DLOG_ASSERT(idx >= 0);
    int off = kUnitSize - 1;
    for (; off >= 0 && ( bitmap_[idx] & kBitConsts[off] ) == 0; --off);
    DLOG_ASSERT(off >= 0);
    return off + (idx * kUnitSize);
  }

  bool CheckBit(int index) const {
    DLOG_ASSERT(index >= 0 && index < kMaxPartitions);
    if (index < bitmap_size_ * kUnitSize) {
      return 0 !=
          ( bitmap_[index / kUnitSize] & kBitConsts[index % kUnitSize] );
    } else {
      return false;
    }
  }

  void TEST_TurnOffBit(int index) {
    DLOG_ASSERT(index >= 0 && index < kMaxPartitions);
    if (index < bitmap_size_ * kUnitSize) {
      // We will not try to shrink memory when bits are turned off.
      bitmap_[index / kUnitSize] &= ( ~kBitConsts[index % kUnitSize] );
      // Update radix if necessary
      if (GetRadix(index) == ExtractRadix()) {
        SetRadix(GetRadix(HighestBit()));
      }
    } else {
      // Do nothing
    }
  }

  void TurnOnBit(int index) {
    DLOG_ASSERT(index >= 0 && index < bitmap_size_ * 2 * kUnitSize);

    if (index < bitmap_size_ * kUnitSize) {
      // Update radix if necessary
      int r = GetRadix(index);
      if (ExtractRadix() < r) {
        DLOG_ASSERT(ExtractRadix() == r - 1);
        SetRadix(r);
      }
      DLOG_ASSERT(r <= ExtractRadix());

      bitmap_[index / kUnitSize] |= ( kBitConsts[index % kUnitSize] );
      DLOG_ASSERT(ExtractRadix() == GetRadix(HighestBit()));
      DLOG_ASSERT(ExtractRadix() <= GetRadix(kUnitSize * ExtractBitmapSize() - 1));

    } else {
      // Expand bitmap space first
      ScaleUp(2);
      SetBitmapSize(bitmap_size_);
      DLOG_ASSERT(index < bitmap_size_ * kUnitSize);

      // Always update radix
      int r = GetRadix(index);
      DLOG_ASSERT(ExtractRadix() == r - 1);
      SetRadix(r);
      DLOG_ASSERT(r == ExtractRadix());

      bitmap_[index / kUnitSize] |= ( kBitConsts[index % kUnitSize] );
      DLOG_ASSERT(ExtractRadix() == GetRadix(HighestBit()));
      DLOG_ASSERT(ExtractRadix() == GetRadix(kUnitSize * ExtractBitmapSize() - 1));
    }
  }

  void Merge(const Rep& other) {
    DLOG_ASSERT(ExtractDirId() == other.ExtractDirId());
    DLOG_ASSERT(ExtractZerothServer() == other.ExtractZerothServer());
    DLOG_ASSERT(ExtractMagicNum() == other.ExtractMagicNum());

    // Ensure bitmap size
    int new_size = std::max(bitmap_size_, other.bitmap_size_);
    ScaleToSize(new_size);
    SetBitmapSize(new_size);

    // Merge bitmap
    SetRadix(std::max(ExtractRadix(), other.ExtractRadix()));
    int i = 0;
    while (i < bitmap_size_ && i < other.bitmap_size_) {
      bitmap_[i] |= other.bitmap_[i];
      i++;
    }

    // Integrity checks
    DLOG_ASSERT(ExtractRadix() == GetRadix(HighestBit()));
    DLOG_ASSERT(ExtractRadix() <= GetRadix(kUnitSize * ExtractBitmapSize() - 1));
  }

  int64_t ExtractDirId() const { return DecodeFixed64(space_); }
  int16_t ExtractZerothServer() const { return DecodeFixed16(space_ + 8); }
  int16_t ExtractRadix() const { return DecodeFixed16(space_ + 10); }
  int16_t ExtractBitmapSize() const { return DecodeFixed16(space_ + 12); }
  int16_t ExtractMagicNum() const { return DecodeFixed16(space_ + 14); }

 private:
  static const int kHeadSize = 16; // 16 bytes
  static const int kSmallIndexSize = kHeadSize + kInitBitmapSize;

  // Head Format:
  // -----------------------------------------------
  // [0-7] - 8 bytes -> directory id
  // [8-9] - 2 bytes -> zeroth server
  // [10-11] - 2 bytes -> current bitmap radix
  // [12-13] - 2 bytes -> current bitmap size
  // [14-15] - 2 bytes -> magic number
  // -----------------------------------------------
  char* space_;

  // Transient state
  char* bitmap_;
  int16_t bitmap_size_;

  // Try not to allocate heap space for small indices.
  char static_space_[kSmallIndexSize];

  // Reset in-memory representation.
  void ResetRep(char* new_space, int16_t new_size) {
    if (space_ != NULL) {
      if (space_ != static_space_) {
        delete [] space_;
      }
    }
    space_ = new_space;
    bitmap_ = new_space + kHeadSize;
    bitmap_size_ = new_size;
    DLOG_ASSERT(bitmap_size_ <= kMaxBitmapSize);
  }

  // Expand in-memory space to accommodate more bits
  void ScaleUp(int factor) {
    DLOG_ASSERT(factor > 1);
    size_t old_size = kHeadSize + bitmap_size_;
    size_t new_size = kHeadSize + bitmap_size_ * factor;
    char* new_space = new char[new_size];
    memcpy(new_space, space_, old_size);
    memset(new_space + old_size, 0, new_size - old_size);
    ResetRep(new_space, bitmap_size_ * factor);
  }

  void ScaleToSize(int size) {
    if (bitmap_size_ < size) {
      DLOG_ASSERT(size % bitmap_size_ == 0);
      ScaleUp(size / bitmap_size_);
    }
  }

  void SetDirId(int64_t dir_id) { EncodeFixed64(space_, dir_id); }
  void SetZerothServer(int16_t server_id) { EncodeFixed16(space_ + 8, server_id); }
  void SetRadix(int16_t radix) { EncodeFixed16(space_ + 10, radix); }
  void SetBitmapSize(int16_t map_size) { EncodeFixed16(space_ + 12, map_size); }
  void SetMagicNum(int16_t magic_number) { EncodeFixed16(space_ + 14, magic_number); }

  // No copying allowed
  Rep(const Rep&);
  Rep& operator=(const Rep&);
};

DirIndex::Rep::Rep(int64_t dir_id, int16_t zeroth_server)
    : space_(NULL), bitmap_(NULL), bitmap_size_(0) {
  // Always use the static memory for new indices
  ResetRep(static_space_, kInitBitmapSize);
  SetDirId(dir_id);
  SetZerothServer(zeroth_server);
  SetMagicNum(kMagicNum);
  SetRadix(0);
  SetBitmapSize(kInitBitmapSize);

  // Initialize bitmap
  memset(bitmap_, 0, kInitBitmapSize);
  TurnOnBit(0);
  DLOG_ASSERT(GetRadix(HighestBit()) == ExtractRadix());
}

DirIndex::Rep::Rep(const Slice& dmap_data)
    : space_(NULL), bitmap_(NULL), bitmap_size_(0) {
  if (dmap_data.size() <= kSmallIndexSize) {
    // Case 1: small directory index with a limited bitmap
    memcpy(static_space_, dmap_data.data(), dmap_data.size());
    ResetRep(static_space_, kInitBitmapSize);
    DLOG_ASSERT(ExtractBitmapSize() == bitmap_size_);

    // Case 2: empty directory index with a single root partition
    if (dmap_data.size() == kHeadSize) {
      // Rebuild the deterministic in-memory state for this special case
      memset(bitmap_, 0, bitmap_size_);
      TurnOnBit(0);
      DLOG_ASSERT(ExtractRadix() == 0);
    } else {
      DLOG_ASSERT(dmap_data.size() == kSmallIndexSize);
    }

  } else {
    // Case 3: regular directory index with a potentially large bitmap
    char* new_space = new char[dmap_data.size()];
    memcpy(new_space, dmap_data.data(), dmap_data.size());
    ResetRep(new_space, dmap_data.size() - kHeadSize);
    DLOG_ASSERT(ExtractBitmapSize() == bitmap_size_);
  }

  // Extra index integrity checks
  DLOG_ASSERT(CheckBit(0));
  DLOG_ASSERT(ExtractMagicNum() == kMagicNum);
  DLOG_ASSERT(ExtractRadix() == GetRadix(HighestBit()));
  DLOG_ASSERT(ExtractRadix() <= GetRadix(kUnitSize * ExtractBitmapSize() - 1));
}

// -------------------------------------------------------------
// Bitmap Implementation
// -------------------------------------------------------------

// Create a new directory index from the beginning.
//
DirIndex::DirIndex(int64_t dir_id, int16_t zeroth_server, DirIndexPolicy* policy)
    : policy_(policy), rep_(NULL) {
  rep_ = new Rep(dir_id, zeroth_server);
}

// Initialize a new directory index using an existing piece
// of memory.
//
DirIndex::DirIndex(const Slice& dmap_data, DirIndexPolicy* policy)
  : policy_(policy), rep_(NULL) {
  rep_ = new Rep(dmap_data);
}

// Update the directory index by merging another directory index
// for the same directory.
//
void DirIndex::Update(const Slice& dmap_data) {
  rep_->Merge(Rep(dmap_data));
}

// Update the directory index by merging another directory index
// for the same directory.
//
void DirIndex::Update(const DirIndex& dir_idx) {
  rep_->Merge(*dir_idx.rep_);
}

// Override the directory index by directly copying a piece of memory
// into the memory of the index.
//
void DirIndex::TEST_Reset(const Slice& dmap_data) {
  delete rep_;
  rep_ = new Rep(dmap_data);
}

// Activate the given partition.
// Mark this partition as exiting.
//
void DirIndex::SetBit(int index) {
  DLOG_ASSERT(index >= 0 && index < policy_->MaxNumVirtualServers());
  rep_->TurnOnBit(index);
}

// Set all bits within the given range
//
void DirIndex::TEST_SetBits(int end, int start) {
  DLOG_ASSERT(rep_->CheckBit(0));
  for (int i = start; i <= end; ++i) {
    if (!rep_->CheckBit(i)) {
      rep_->TurnOnBit(i);
    }
  }
}

// Deactivate the given partition.
// Mark this partition as non-existing.
//
void DirIndex::TEST_UnsetBit(int index) {
  DLOG_ASSERT(index >= 0 && index < policy_->MaxNumVirtualServers());
  rep_->TEST_TurnOffBit(index);
}


// Clear all bits, except the zeroth bit.
//
void DirIndex::TEST_RevertAll() {
  for (int i = rep_->HighestBit(); i > 0; --i) {
    if (rep_->CheckBit(i)) {
      rep_->TEST_TurnOffBit(i);
    }
  }
  DLOG_ASSERT(rep_->CheckBit(0));
  DLOG_ASSERT(rep_->ExtractRadix() == 0);
}

// -------------------------------------------------------------
// Bitmap Query Interface
// -------------------------------------------------------------

int DirIndex::MaxRadix() const {
  return GetRadix(HighestIndex());
}

// Return the highest index present in the bitmap.
// This index marks the last partition in the sequence of all
// existing partitions of a particular directory.
//
int DirIndex::HighestIndex() const {
  return rep_->HighestBit();
}

// Return true if the partition marked by the specified index
// can be further divided to generate a child partition.
// This depends a lot on the current status of the directory mapping.
// Note that non-existing partitions are always non-splittable.
//
// Current Implementation does not consider the actual number of
// servers. Only the constant max number of virtual servers are considered.
// This, however, makes it flexible enough to facilitate virtual servers.
//
bool DirIndex::IsSplittable(int index) const {
  if (!rep_->CheckBit(index)) {
    return false;
  }
  int new_index = index;
  int radix = GetRadix(new_index);
  while (radix < kMaxRadix) {
    new_index = GetChildIndex(index, radix++);
    if (!rep_->CheckBit(new_index)) {
      return new_index < policy_->MaxNumVirtualServers();
    }
    DLOG_ASSERT(radix == GetRadix(new_index));
  }
  return false;
}

// Return the next available child index for a given parent index.
// The parent index must mark an existing partition and must
// be splittable in the first place.
//
int DirIndex::NewIndexForSplitting(int index) const {
  DLOG_ASSERT(IsSplittable(index));
  int new_index = index;
  int radix = GetRadix(index);
  while (rep_->CheckBit(new_index)) {
    new_index = GetChildIndex(index, radix++);
  }
  DLOG_ASSERT(new_index != index && new_index < kMaxPartitions);
  DLOG_ASSERT(new_index != index && new_index < policy_->MaxNumVirtualServers());
  return new_index;
}

// Figure out the partition responsible for the given file,
// according the the current directory mapping status and the hash
// value of the file name.
//
int DirIndex::GetIndex(const std::string& fname) const {
  DLOG_ASSERT(rep_->CheckBit(0));
  char hash[kHashSize];
  GetNameHash(fname, hash);
  DLOG_ASSERT(rep_->ExtractRadix() == MaxRadix());
  int index = ComputeIndexFromHash(hash, rep_->ExtractRadix());
  while (!rep_->CheckBit(index)) {
    index = GetParentIndex(index);
  }
  DLOG_ASSERT(index >= 0 && index < policy_->MaxNumVirtualServers());
  DLOG_ASSERT(index >= 0 && index < kMaxPartitions && rep_->CheckBit(index));
  return index;
}

// Pickup a member partition server to hold the given file,
// according to the current directory mapping status and the hash of
// that file name. Only servers currently holding a partition can be
// selected to accommodate new files.
//
int DirIndex::SelectServer(const std::string& fname) const {
  return GetServerForIndex( GetIndex(fname) );
}

// Return true if a file represented by the specified hash will be
// migrated to the given child partition once its parent partition splits.
// The given index marks this child partition. It is easy to deduce the
// parent partition from a child partition.
//
bool DirIndex::ToBeMigrated(int index, const char* hash) {
  return ComputeIndexFromHash(hash, GetRadix(index)) == index;
}

// Calculate the hash for a given string.
// Return the size of the hash.
//
size_t DirIndex::GetNameHash(const std::string& fname, char* hash) {
  char murmur_hash[16];
  MurmurHash(fname, murmur_hash);
  memcpy(hash, murmur_hash, kHashSize);
  return kHashSize;
}

// Return true if the bit at the given index is set.
// Return false otherwise.
//
bool DirIndex::GetBit(int index) const {
  return rep_->CheckBit(index);
}

// Return the INODE number of the directory being indexed.
//
int64_t DirIndex::FetchDirId() const {
  return rep_->ExtractDirId();
}

// Return the zeroth server of the directory being indexed.
//
int16_t DirIndex::FetchZerothServer() const {
  return rep_->ExtractZerothServer();
}

// Return the internal bitmap radix of the index.
//
int16_t DirIndex::FetchBitmapRadix() const {
  return rep_->ExtractRadix();
}

// Return the in-memory representation of this index.
//
Slice DirIndex::ToSlice() const {
  return rep_->ToSlice();
}

// Return the server responsible for a specific partition.
//
int DirIndex::GetServerForIndex(int index) const {
  DLOG_ASSERT(index >= 0 && index < policy_->MaxNumVirtualServers());
  int num_servers = policy_->NumServers();
  return MapIndexToServer(index, U16INT(FetchZerothServer()), num_servers);
}

// Return the server responsible for a specific partition.
//
int DirIndex::MapIndexToServer(int index, int zeroth_server, int num_servers) {
  return (index + zeroth_server) % num_servers;
}

} /* namespace indexfs */
