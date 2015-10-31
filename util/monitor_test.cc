// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/monitor.h"
#include "common/unit_test.h"

namespace indexfs { namespace test {

namespace {
static const int kRank = 0;
enum {
  op1, op2, kNumOps
};
static const char* kOpNames[kNumOps] = { "op_1", "op_2" };
struct MonitorTest {
  Monitor monitor_;
  MonitorTest() : monitor_(kOpNames, kNumOps, kRank, 0) { }
};
}

TEST(MonitorTest, Counting) {
  monitor_.AddMetric(op1, 100);
  monitor_.AddMetric(op1, 200);
  monitor_.AddMetric(op2, 100);
  monitor_.AddMetric(op2, 200);
  monitor_.AddMetric(op1, 300);
  ASSERT_EQ(3, monitor_.TEST_GetCount(op1));
  ASSERT_EQ(2, monitor_.TEST_GetCount(op2));
  ASSERT_EQ(5, monitor_.TEST_GetTotalCount());
}

TEST(MonitorTest, Window) {
  {
    std::stringstream ss;
    monitor_.GetCurrentStatus(&ss);
  }
  monitor_.AddMetric(op1, 1000);
  monitor_.AddMetric(op2, 2000);
  ASSERT_EQ(2, monitor_.TEST_GetTotalCount());
  ASSERT_EQ(1000, monitor_.TEST_GetMaxLatency(op1));
  ASSERT_EQ(2000, monitor_.TEST_GetMaxLatency(op2));
  {
    std::stringstream ss;
    monitor_.GetCurrentStatus(&ss);
  }
  monitor_.AddMetric(op1, 100);
  monitor_.AddMetric(op2, 200);
  ASSERT_EQ(4, monitor_.TEST_GetTotalCount());
  ASSERT_EQ(100, monitor_.TEST_GetMaxLatency(op1));
  ASSERT_EQ(200, monitor_.TEST_GetMaxLatency(op2));
}

TEST(MonitorTest, StatusReport) {
  monitor_.AddMetric(op1, 100);
  monitor_.AddMetric(op1, 200);
  monitor_.AddMetric(op2, 100);
  monitor_.AddMetric(op2, 200);
  monitor_.AddMetric(op1, 300);
  std::stringstream ss;
  monitor_.GetCurrentStatus(&ss);
  fprintf(stderr, "TSDB Report:\n");
  fprintf(stderr, "-------------\n");
  fprintf(stderr, "%s\n", ss.str().c_str());
  fprintf(stderr, "-------------\n");
}

} // namespace test
} // namespace indexfs


// Test Driver
int main(int argc, char* argv[]) {
  return indexfs::test::RunAllTests();
}
