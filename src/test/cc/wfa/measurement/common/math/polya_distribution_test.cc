// Copyright 2020 The Measurement System Authors
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

#include "wfa/measurement/common/math/polya_distribution.h"

#include <unordered_map>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace wfa::measurement::common::math {
namespace {

// Create a random number with twoSidedGeometricDistribution using the
// decentralized mechanism, i.e., as the summation of the diff of N pairs of
// polya random variables.
int64_t getTwoSidedGeometricDistributedRandomNumber(size_t num, double r) {
  int64_t result = 0;
  for (size_t i = 0; i < num; ++i) {
    result += getPolyaRandomVariable(1.0 / num, r) -
              getPolyaRandomVariable(1.0 / num, r);
  }
  return result;
}

TEST(DistributedGeometric, MeanShouldBeAlmostZero) {
  double sum = 0.0;
  size_t n = 100000;
  for (size_t i = 0; i < n; ++i) {
    sum += getTwoSidedGeometricDistributedRandomNumber(3, 0.6);
  }
  EXPECT_NEAR(sum / n, 0.0, 0.05);
}

TEST(DistributedGeometric, ProbabilityMassFunctionShouldBeCorrect) {
  size_t total_number = 100000;
  std::unordered_map<int64_t, size_t> frequency_distribution;
  double r = 0.6;
  for (size_t i = 0; i < total_number; ++i) {
    frequency_distribution[getTwoSidedGeometricDistributedRandomNumber(3, r)]++;
  }

  for (const auto& [number, frequency] : frequency_distribution) {
    double probability = static_cast<double>(frequency) / total_number;
    double expected_probability =
        (1 - r) / (1 + r) * std::pow(r, std::abs(number));
    EXPECT_NEAR(probability, expected_probability, 0.01);
  }
}

}  // namespace
}  // namespace wfa::measurement::common::math
