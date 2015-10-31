// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_METADB_FSTAT_H_
#define _INDEXFS_METADB_FSTAT_H_

#include <time.h>
#include <endian.h>
#include <stdint.h>

namespace indexfs {
namespace mdb {

class FileStat {

 private:
  // -----------------------------------------
  int64_t  file_ino_;        /*  8 bytes */
  int64_t  file_size_;       /*  8 bytes */
  int32_t  file_mode_;       /*  4 bytes */
  int16_t  file_status_;     /*  2 bytes */
  int16_t  zeroth_server_;   /*  2 bytes */
  int32_t  user_id_;         /*  4 bytes */
  int32_t  group_id_;        /*  4 bytes */
  timespec change_time_;     /* 16 bytes */
  timespec modify_time_;     /* 16 bytes */
  // -----------------------------------------
                             /* 64 bytes */
  // -----------------------------------------

 public:

  void SetInodeNo(int64_t inode_no) {
    file_ino_ = htole64(inode_no);
  }

  void SetFileSize(int64_t file_size) {
    file_size_ = htole64(file_size);
  }

  void SetFileMode(int32_t file_mode) {
    file_mode_ = htole32(file_mode);
  }

  void SetFileStatus(int16_t file_status) {
    file_status_ = htole16(file_status);
  }

  void SetZerothServer(int16_t zeroth_server) {
    zeroth_server_ = htole16(zeroth_server);
  }

  void SetUserId(int32_t user_id) {
    user_id_ = htole32(user_id);
  }

  void SetGroupId(int32_t group_id) {
    group_id_ = htole32(group_id);
  }

  void SetChangeTime(int64_t time_in_secs) {
    change_time_.tv_nsec = 0;
    change_time_.tv_sec = htole64(time_in_secs);
  }

  void SetModifyTime(int64_t time_in_secs) {
    modify_time_.tv_nsec = 0;
    modify_time_.tv_sec = htole64(time_in_secs);
  }

  void SetTime(int64_t time_in_secs) {
    change_time_.tv_nsec = modify_time_.tv_nsec = 0;
    change_time_.tv_sec = modify_time_.tv_sec = htole64(time_in_secs);
  }

  int64_t InodeNo() const { return le64toh(file_ino_); }
  int64_t FileSize() const { return le64toh(file_size_); }
  int32_t FileMode() const { return le32toh(file_mode_); }
  int16_t FileStatus() const { return le16toh(file_status_); }
  int16_t ZerothServer() const { return le16toh(zeroth_server_); }
  int32_t UserId() const { return le32toh(user_id_); }
  int32_t GroupId() const { return le32toh(group_id_); }
  int64_t ChangeTime() const { return le64toh(change_time_.tv_sec); }
  int64_t ModifyTime() const { return le64toh(modify_time_.tv_sec); }
};

} /* namespace mdb */
} /* namespace indexfs */

#endif /* _INDEXFS_METADB_FSTAT_H_ */
