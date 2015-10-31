// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_C_CLI_INTERFACE_H_
#define _INDEXFS_C_CLI_INTERFACE_H_

#include "c/cli_types.h"

#ifdef __cplusplus
extern "C" {
#endif

// Absorb command line arguments
//
extern void idxfs_parse_arguments(int *argc, char*** argv);

// Initialize the indexfs log system.
//
extern void idxfs_log_init();

// Close the indexfs log system.
//
extern void idxfs_log_close();

// Create a new indexfs client instance.
//
extern int idxfs_create_client(conf_t* conf, cli_t** cli);

// Dispose an existing indexfs client instance.
//
extern int idxfs_destroy_client(cli_t* cli);

// Create a new file at the given path.
//
extern int idxfs_mknod(cli_t* cli, const char* path, mode_t mode);

// Make a new directory at the given path.
//
extern int idxfs_mkdir(cli_t* cli, const char* path, mode_t mode);

// Remove an empty directory identified by the given path.
//
extern int idxfs_rmdir(cli_t* cli, const char* path);

// Delete a file identified by the given path.
//
extern int idxfs_delete(cli_t* cli, const char* path);

// Check if the given file or directory exists.
//
extern int idxfs_exists(cli_t* cli, const char* path);

// Check if the given directory exists.
//
extern int idxfs_dir_exists(cli_t* cli, const char* path);

// List entries in a directory.
//
extern int idxfs_readdir(cli_t* cli, const char* path,
                         readdir_handler_t handler, void* arg);

// Update the permission bits of the specified file or directory.
//
extern int idxfs_chmod(cli_t* cli, const char* path,
                       mode_t mode, bool* is_dir);

// Update the ownership of the specified file or directory.
//
extern int idxfs_chown(cli_t* cli, const char* path,
                       uid_t owner, gid_t group, bool* is_dir);

// Get the attributes of the given file.
//
extern int idxfs_getinfo(cli_t* cli, const char* path, info_t* info);

#ifdef __cplusplus
}  /* end extern "C" */
#endif

#endif /* _INDEXFS_C_CLI_INTERFACE_H_ */
