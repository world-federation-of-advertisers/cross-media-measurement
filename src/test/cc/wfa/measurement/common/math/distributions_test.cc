// Copyright 2020 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "wfa/measurement/common/math/distributions.h"

#include <unordered_map>

#include "absl/status/statusor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/test/cc/testutil/status_macros.h"
#include "util/status_macros.h"

namespace wfa::measurement::common::math {
namespace {

// Create a random number with twoSidedGeometricDistribution using the
// decentralized mechanism, i.e., as the summation of N PolyaDiff.
absl::StatusOr<int64_t> GetTwoSidedGeometricDistributedRandomNumber(
    DistributedGeometricRandomComponentOptions options) {
  int64_t result = 0;
  for (size_t i = 0; i < options.num; ++i) {
    ASSIGN_OR_RETURN(int64_t temp,
                     GetDistributedGeometricRandomComponent(options));
    result += temp;
  }
  return result;
}

TEST(IndividualComponent, MeanMaxMinShouldBeCorrect) {
  int64_t shift_offset = 20;
  int64_t truncate_threshold = 10;

  double sum = 0.0;
  int64_t min_value = 1000;
  int64_t max_value = 0;

  size_t num_trials = 100000;
  for (size_t i = 0; i < num_trials; ++i) {
    ASSERT_OK_AND_ASSIGN(int64_t temp,
                         GetDistributedGeometricRandomComponent(
                             {.num = 3,
                              .p = 0.6,
                              .truncate_threshold = truncate_threshold,
                              .shift_offset = shift_offset}));
    sum += temp;
    min_value = std::min(min_value, temp);
    max_value = std::max(max_value, temp);
  }
  // Mean should be equal to shift_offset.
  EXPECT_NEAR(sum / num_trials, shift_offset, 0.05);
  // Max should be equal to shift_offset + truncate_threshold
  EXPECT_EQ(max_value, shift_offset + truncate_threshold);
  // Min should be equal to shift_offset - truncate_threshold
  EXPECT_EQ(min_value, shift_offset - truncate_threshold);
}

TEST(GlobalSummation, ProbabilityMassFunctionShouldBeCorrect) {
  double p = 0.6;
  int64_t num = 3;                  // 3 contributors
  int64_t shift_offset = 10;        // Individual offset
  int64_t truncate_threshold = 10;  // The value should be reasonably large.
  int64_t total_offset = num * shift_offset;
  int64_t min_output = total_offset - truncate_threshold * num;
  int64_t max_output = total_offset + truncate_threshold * num;

  size_t num_trials = 100000;
  std::unordered_map<int64_t, size_t> frequency_distribution;
  for (size_t i = 0; i < num_trials; ++i) {
    ASSERT_OK_AND_ASSIGN(int64_t temp,
                         GetTwoSidedGeometricDistributedRandomNumber(
                             {.num = num,
                              .p = p,
                              .truncate_threshold = truncate_threshold,
                              .shift_offset = shift_offset}));
    ASSERT_GE(temp, min_output);
    ASSERT_LE(temp, max_output);
    ++frequency_distribution[temp];
  }
  for (int64_t x = min_output; x <= max_output; ++x) {
    double probability =
        static_cast<double>(frequency_distribution[x]) / num_trials;
    double expected_probability =
        (1 - p) / (1 + p) * std::pow(p, std::abs(x - total_offset));
    EXPECT_NEAR(probability, expected_probability, 0.01);
  }
}

}  // namespace
}  // namespace wfa::measurement::common::math
