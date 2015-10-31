// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_C_LIBCLIENT_H_
#define _INDEXFS_C_LIBCLIENT_H_

#include "c/cli_types.h"

#ifdef __cplusplus
extern "C" {
#endif

// Initialize a new client context.
// Note that this function can only be called once (per thread).
//
extern int IDX_Init(conf_t* config);

// Shutdown the client context.
//
extern int IDX_Destroy();

// Create a file at the given path.
//
extern int IDX_Create(const char* path, mode_t mode);

// Create a file at the given path.
//
extern int IDX_Mknod(const char* path, mode_t mode);

// Make a new directory at the given path.
//
extern int IDX_Mkdir(const char* path, mode_t mode);

// Remove an empty directory identified by the given path.
//
extern int IDX_Rmdir(const char* path);

// Delete a file identified by the given path.
//
extern int IDX_Unlink(const char* path);

// Update the permission bits of the specified file or directory.
//
extern int IDX_Chmod(const char* path, mode_t mode, info_t* info);

// Update the ownership of the specified file or directory.
//
extern int IDX_Chown(const char* path, uid_t owner, gid_t group, info_t* info);

// Check if the given file or directory exists.
//
extern int IDX_Access(const char* path);

// Check if the given directory exists.
//
extern int IDX_AccessDir(const char* path);

// List entries in a directory.
//
extern int IDX_Readdir(const char* path, readdir_handler_t handler, void* arg);

// Get the attributes of the given file.
//
extern int IDX_Getattr(const char* path, info_t* buf);

// Open a file at the specified path and return its file descriptor.
//
extern int IDX_Open(const char* path, int flags, int* fd);

// Close the file associated with the given file descriptor.
//
extern int IDX_Close(int fd);

// Request a synchronization on the given file.
//
extern int IDX_Fsync(int fd);

// Perform an un-buffered read on the given file.
//
extern int IDX_Read(int fd, void* buf, size_t size);

// Perform an un-buffered write on the given file.
//
extern int IDX_Write(int fd, const void* buf, size_t size);

// Perform an un-buffered read on the given file at the given offset.
//
extern int IDX_Pread(int fd, void* buf, off_t offset, size_t size);

// Perform an un-buffered write on the given file at the given offset.
//
extern int IDX_Pwrite(int fd, const void* buf, off_t offset, size_t size);

#ifdef __cplusplus
}  /* end extern "C" */
#endif

#endif /* _INDEXFS_C_LIBCLIENT_H_ */
