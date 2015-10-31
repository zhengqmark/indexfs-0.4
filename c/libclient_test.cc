// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>

namespace indexfs { namespace test {

typedef struct {
  int mode;
  int uid;
  int gid;
  int size;
  int mtime;
  int ctime;
  int is_dir;
} info_t;
typedef int (*idx_getattr_t)(const char* path, info_t* info);
typedef int (*idx_access_t)(const char* path);
typedef int (*idx_init_t)(void* config);
typedef int (*idx_destroy_t)();
typedef void (*readdir_handler_t)
    (const char* name, void* arg);
typedef int (*idx_readdir_t)
    (const char* path, readdir_handler_t handle, void* arg);
typedef int (*idx_mknod_t)(const char* path, int mode);
typedef int (*idx_mkdir_t)(const char* path, int mode);
typedef int (*idx_chmod_t)(const char* path, int mode);

#define ASSERT_TRUE(c)                                   \
  if (!(c)) {                                            \
    fprintf(stderr, "Assertion failed: %s\n", #c);       \
    abort();                                             \
  }

static const char* INDEXFS_LIB_NAME = "libindexfs.so";

namespace {
struct DylibTest {
  void* handle;
  void LoadFuncs();
  DylibTest() {
    handle = dlopen(INDEXFS_LIB_NAME, RTLD_NOW);
  }
  ~DylibTest() {
    dlclose(handle);
  }
  idx_init_t idx_init;
  idx_destroy_t idx_destroy;
  idx_mknod_t idx_mknod;
  idx_mkdir_t idx_mkdir;
  idx_chmod_t idx_chmod;
  idx_getattr_t idx_getattr;
  idx_access_t idx_access;
  idx_readdir_t idx_readdir;
};
void DylibTest::LoadFuncs() {
  ASSERT_TRUE(handle != NULL);
  idx_init = (idx_init_t) dlsym(handle, "IDX_Init");
  ASSERT_TRUE(idx_init != NULL);
  idx_destroy = (idx_destroy_t) dlsym(handle, "IDX_Destroy");
  ASSERT_TRUE(idx_destroy != NULL);
  idx_mknod = (idx_mknod_t) dlsym(handle, "IDX_Mknod");
  ASSERT_TRUE(idx_mknod != NULL);
  idx_mkdir = (idx_mkdir_t) dlsym(handle, "IDX_Mkdir");
  ASSERT_TRUE(idx_mkdir != NULL);
  idx_chmod = (idx_chmod_t) dlsym(handle, "IDX_Chmod");
  ASSERT_TRUE(idx_chmod != NULL);
  idx_getattr = (idx_getattr_t) dlsym(handle, "IDX_Getattr");
  ASSERT_TRUE(idx_getattr != NULL);
  idx_access = (idx_access_t) dlsym(handle, "IDX_Access");
  ASSERT_TRUE(idx_access != NULL);
  idx_readdir = (idx_readdir_t) dlsym(handle, "IDX_Readdir");
  ASSERT_TRUE(idx_readdir != NULL);
}
}

static void PrintName(const char* name, void* arg) {
  fprintf(stderr, "%s\n", name);
}

int RunAllTests() {
  DylibTest test;
  test.LoadFuncs();
  info_t info;
  ASSERT_TRUE(test.idx_init(NULL) == 0);
  ASSERT_TRUE(test.idx_mknod("/f1", -1) == 0);
  ASSERT_TRUE(test.idx_access("/f1") == 0);
  ASSERT_TRUE(test.idx_chmod("/f1", -1) == 0);
  ASSERT_TRUE(test.idx_getattr("/f1", &info) == 0);
  ASSERT_TRUE(info.is_dir == 0);
  ASSERT_TRUE(test.idx_mkdir("/d1", -1) == 0);
  ASSERT_TRUE(test.idx_access("/d1") == 0);
  ASSERT_TRUE(test.idx_chmod("/d1", -1) == 0);
  ASSERT_TRUE(test.idx_getattr("/d1", &info) == 0);
  ASSERT_TRUE(info.is_dir != 0);
  ASSERT_TRUE(test.idx_readdir("/", PrintName, NULL) == 0);
  ASSERT_TRUE(test.idx_destroy() == 0);
  return 0;
}

} // namespace test
} // namespace indexfs

int main(int argc, char* argv[]) {
  return indexfs::test::RunAllTests();
}
