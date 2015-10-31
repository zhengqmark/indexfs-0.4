# Copyright (c) 2014 The IndexFS Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.

namespace cpp indexfs
namespace java edu.cmu.pdl.indexfs.rpc

// ---------------------------------------------------------------
// IndexFS types
// ---------------------------------------------------------------

typedef i16 TNumServer
typedef i64 TInodeID

struct OID {
  1: required i16 path_depth
  2: required i64 dir_id
  3: required string obj_name
}

struct OIDS {
  1: required i16 path_depth
  2: required i64 dir_id
  3: required list<string> obj_names
}

struct StatInfo {
  1: required i32 mode
  2: required i16 uid
  3: required i16 gid
  4: required i64 size
  5: required i64 mtime
  6: required i64 ctime
  7: required i64 id
  8: required i16 zeroth_server
  9: required bool is_embedded
}

struct LookupInfo {
  1: required i64 id
  2: required i16 zeroth_server
  3: required i16 perm
  4: required i16 uid
  5: required i16 gid
  6: required i64 lease_due
}

struct EntryList {
  1: required string dmap_data
  2: required list<string> entries
}

struct ScanResult {
  1: required list<string> entries
  2: required string end_key
  3: required i16 end_partition
  4: required i16 more_entries
  5: required string dmap_data
}

struct ScanPlusResult {
  1: required list<string> names
  2: required list<StatInfo> entries
  3: required string end_key
  4: required i16 end_partition
  5: required i16 more_entries
  6: required string dmap_data
}

struct OpenResult {
  1: required bool is_embedded
  2: required string data
}

struct ReadResult {
  1: required bool is_embedded
  2: required string data
}

struct WriteResult {
  1: required bool is_embedded
  2: required string link
  3: required string data
}

// struct LeaseInfo {
//   1: i64 timeout
//   2: TInodeID next_inode
//   3: TNumServer next_zeroth_server
//   4: i32 max_dirs
// }

// ---------------------------------------------------------------
// IndexFS exceptions
// ---------------------------------------------------------------

exception FileNotFoundException {
  // NO additional payload needed
}

exception FileAlreadyExistsException {
  // NO additional payload needed
}

exception UnrecognizedDirectoryError {
  // NO additional payload needed
}

exception WrongServerError {
  // NO additional payload needed
}

exception DirectoryExpectedError {
  // NO additional payload needed
}

exception FileExpectedError {
  // NO additional payload needed
}

exception IOError {
  1: required string message
}

exception ServerInternalError {
  1: required string message
}

exception IllegalPathException {
  // NO additional payload needed
}

exception ServerRedirectionException {
  1: required string dmap_data
}

exception ParentPathNotFoundException {
  1: required string parent_path
}

exception ParentPathNotDirectoryException {
  1: required string parent_path
}

// ---------------------------------------------------------------
// IndexFS RPC interface
// ---------------------------------------------------------------

service MetadataIndexService {

void Ping()
  throws (1: ServerInternalError srv_error)

void FlushDB()
  throws (1: IOError io_error,
          2: ServerInternalError srv_error)

LookupInfo Access(1: OID obj_id)
  throws (1: UnrecognizedDirectoryError unknown_dir,
          2: ServerRedirectionException srv_redirect,
          3: FileNotFoundException not_found,
          4: DirectoryExpectedError not_a_dir,
          5: IOError io_error,
          6: ServerInternalError srv_error)

LookupInfo Renew(1: OID obj_id)
  throws (1: UnrecognizedDirectoryError unknown_dir,
          2: ServerRedirectionException srv_redirect,
          3: FileNotFoundException not_found,
          4: DirectoryExpectedError not_a_dir,
          5: IOError io_error,
          6: ServerInternalError srv_error)

StatInfo Getattr(1: OID obj_id)
  throws (1: UnrecognizedDirectoryError unknown_dir,
          2: ServerRedirectionException srv_redirect,
          3: FileNotFoundException not_found,
          4: IOError io_error,
          5: ServerInternalError srv_error)

void Mknod(1: OID obj_id, 2: i16 perm)
  throws (1: UnrecognizedDirectoryError unknown_dir,
          2: ServerRedirectionException srv_redirect,
          3: FileAlreadyExistsException file_exists,
          4: IOError io_error,
          5: ServerInternalError srv_error)

void Mknod_Bulk(1: OIDS obj_ids, 2: i16 perm)
  throws (1: UnrecognizedDirectoryError unknown_dir,
          2: ServerRedirectionException srv_redirect,
          3: FileAlreadyExistsException file_exists,
          4: IOError io_error,
          5: ServerInternalError srv_error)

void Mkdir(1: OID obj_id, 2: i16 perm, 3: i16 hint_server1, 4: i16 hint_server2)
  throws (1: UnrecognizedDirectoryError unknown_dir,
          2: ServerRedirectionException srv_redirect,
          3: FileAlreadyExistsException file_exists,
          4: IOError io_error,
          5: ServerInternalError srv_error)

void Mkdir_Presplit(1: OID obj_id, 2: i16 perm, 3: i16 hint_server1, 4: i16 hint_server2)
  throws (1: UnrecognizedDirectoryError unknown_dir,
          2: ServerRedirectionException srv_redirect,
          3: FileAlreadyExistsException file_exists,
          4: IOError io_error,
          5: ServerInternalError srv_error)

bool Chmod(1: OID obj_id, 2: i16 perm)
  throws (1: UnrecognizedDirectoryError unknown_dir,
          2: ServerRedirectionException srv_redirect,
          3: FileNotFoundException not_found,
          4: IOError io_error,
          5: ServerInternalError srv_error)

bool Chown(1: OID obj_id, 2: i16 uid, 3: i16 gid)
  throws (1: UnrecognizedDirectoryError unknown_dir,
          2: ServerRedirectionException srv_redirect,
          3: FileNotFoundException not_found,
          4: IOError io_error,
          5: ServerInternalError srv_error)

void CreateZeroth(1: i64 dir_id, 2: i16 zeroth_server)
  throws (1: WrongServerError wrong_srv,
          2: FileAlreadyExistsException file_exists,
          3: IOError io_error,
          4: ServerInternalError srv_error)

EntryList Readdir(1: i64 dir_id, 2: i16 index)
  throws (1: UnrecognizedDirectoryError unknown_dir,
          2: IOError io_error,
          3: ServerInternalError srv_error)

string ReadBitmap(1: i64 dir_id)
  throws (1: UnrecognizedDirectoryError unknown_dir,
          2: IOError io_error,
          3: ServerInternalError srv_error)

void UpdateBitmap(1: i64 dir_id, 2: string dmap_data)
  throws (1: UnrecognizedDirectoryError unknown_dir,
          2: IOError io_error,
          3: ServerInternalError srv_error)

void InsertSplit(1: i64 dir_id, 2: i16 parent_index, 3: i16 child_index,
                 4: string path_split_files, 5: string dmap_data,
                 6: i64 min_seq, 7: i64 max_seq, 8: i64 num_entries)
  throws (1: WrongServerError wrong_srv,
          2: FileAlreadyExistsException file_exists,
          3: IOError io_error,
          4: ServerInternalError srv_error)

}
