// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <string>
#include <vector>
#include <string.h>
#include <stdlib.h>

#include "common/unit_test.h"

namespace indexfs {
namespace test {

namespace {
struct Test {
  const char* base;
  const char* name;
  void (*func)();
};
static std::vector<Test>* tests;

struct LogGuard {
  LogGuard()  {
    ::indexfs::Logger::Initialize(NULL);
  }
  ~LogGuard() { ::indexfs::Logger::Shutdown(); }
};
}

bool RegisterTest(const char* base, const char* name, void (*func)()) {
  if (tests == NULL) {
    tests = new std::vector<Test>;
  }
  Test t;
  t.base = base;
  t.name = name;
  t.func = func;
  tests->push_back(t);
  return true;
}

int RunAllTests() {
  LogGuard log_guard;
  const char* matcher = getenv("INDEXFS_TESTS");

  int num = 0;
  if (tests != NULL) {
    for (size_t i = 0; i < tests->size(); i++) {
      const Test& t = (*tests)[i];
      if (matcher != NULL) {
        std::string name = t.base;
        name.push_back('.');
        name.append(t.name);
        if (strstr(name.c_str(), matcher) == NULL) {
          continue;
        }
      }
      fprintf(stderr, "==== Test %s.%s\n", t.base, t.name);
      (*t.func)();
      ++num;
    }
  }
  fprintf(stderr, "==== PASSED %d tests\n", num);
  return 0;
}

std::string TmpDir() {
  std::string dir;
  Status s = Env::Default()->GetTestDirectory(&dir);
  ASSERT_TRUE(s.ok()) << s.ToString();
  return dir;
}

int RandomSeed() {
  const char* env = getenv("TEST_RANDOM_SEED");
  int result = (env != NULL ? atoi(env) : 301);
  if (result <= 0) {
    result = 301;
  }
  return result;
}

void CreateDirectory(Env* env, const std::string &dirname) {
  Status s = env->CreateDir(dirname);
  if (!s.ok()) {
    fprintf(stderr, "Cannot create directory: %s\n", s.ToString().c_str());
    fprintf(stderr, "System will exit\n");
    exit(-1);
  }
}

} /* namespace test */
} /* namespace indexfs */
