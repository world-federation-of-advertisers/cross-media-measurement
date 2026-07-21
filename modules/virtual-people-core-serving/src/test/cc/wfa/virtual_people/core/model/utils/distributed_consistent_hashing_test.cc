// Copyright 2021 The Cross-Media Measurement Authors
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

#include "wfa/virtual_people/core/model/utils/distributed_consistent_hashing.h"

#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/memory/memory.h"
#include "absl/status/statusor.h"
#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace wfa_virtual_people {
namespace {

using ::testing::DoubleNear;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;
using ::wfa::StatusIs;

constexpr int kSeedNumber = 10000;

TEST(DistributedConsistentHashingTest, TestEmptyDistribution) {
  std::vector<DistributionChoice> distribution;
  EXPECT_THAT(
      DistributedConsistentHashing::Build(std::move(distribution)).status(),
      StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(DistributedConsistentHashingTest, TestZeroProbabilitiesSum) {
  std::vector<DistributionChoice> distribution(
      {DistributionChoice({0, 0}), DistributionChoice({1, 0})});
  EXPECT_THAT(
      DistributedConsistentHashing::Build(std::move(distribution)).status(),
      StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(DistributedConsistentHashingTest, TestNegativeProbability) {
  std::vector<DistributionChoice> distribution({DistributionChoice({0, -1})});
  EXPECT_THAT(
      DistributedConsistentHashing::Build(std::move(distribution)).status(),
      StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(DistributedConsistentHashingTest, TestOutputDistribution) {
  // Distribution:
  // choice_id probability
  // 0         0.4
  // 1         0.2
  // 2         0.2
  // 3         0.2
  std::vector<DistributionChoice> distribution(
      {DistributionChoice({0, 0.4}), DistributionChoice({1, 0.2}),
       DistributionChoice({2, 0.2}), DistributionChoice({3, 0.2})});
  ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DistributedConsistentHashing> hashing,
      DistributedConsistentHashing::Build(std::move(distribution)));
  absl::flat_hash_map<int32_t, int32_t> output_counts;
  for (int seed = 0; seed < kSeedNumber; ++seed) {
    int32_t output = hashing->Hash(std::to_string(seed));
    ++output_counts[output];
  }
  // Compare to the exact results to make sure C++ and Kotlin implementations
  // have same output.
  EXPECT_THAT(output_counts,
              UnorderedElementsAre(Pair(0, 4032), Pair(1, 2012), Pair(2, 1996),
                                   Pair(3, 1960)));
}

TEST(DistributedConsistentHashingTest, TestNotNormalized) {
  // Distribution:
  // choice_id probability_before_normalized
  // 0         0.8
  // 1         0.4
  // 2         0.4
  // 3         0.4
  std::vector<DistributionChoice> distribution(
      {DistributionChoice({0, 0.8}), DistributionChoice({1, 0.4}),
       DistributionChoice({2, 0.4}), DistributionChoice({3, 0.4})});
  EXPECT_THAT(
      DistributedConsistentHashing::Build(std::move(distribution)).status(),
      StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(DistributedConsistentHashingTest, TestZeroProbability) {
  // Distribution:
  // choice_id probability
  // 0         0
  // 1         1
  std::vector<DistributionChoice> distribution(
      {DistributionChoice({0, 0}), DistributionChoice({1, 1})});
  ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DistributedConsistentHashing> hashing,
      DistributedConsistentHashing::Build(std::move(distribution)));
  for (int seed = 0; seed < kSeedNumber; ++seed) {
    EXPECT_EQ(hashing->Hash(std::to_string(seed)), 1);
  }
}

TEST(DistributedConsistentHashingTest,
     TestOutputDistributionNonConsecutiveChoiceId) {
  // Distribution:
  // choice_id probability
  // 0         0.4
  // 2         0.2
  // 4         0.2
  // 6         0.2
  std::vector<DistributionChoice> distribution(
      {DistributionChoice({0, 0.4}), DistributionChoice({2, 0.2}),
       DistributionChoice({4, 0.2}), DistributionChoice({6, 0.2})});
  ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DistributedConsistentHashing> hashing,
      DistributedConsistentHashing::Build(std::move(distribution)));
  absl::flat_hash_map<int32_t, int32_t> output_counts;
  for (int seed = 0; seed < kSeedNumber; ++seed) {
    int32_t output = hashing->Hash(std::to_string(seed));
    ++output_counts[output];
  }
  // Compare to the exact results to make sure C++ and Kotlin implementations
  // have same output.
  EXPECT_THAT(output_counts,
              UnorderedElementsAre(Pair(0, 4049), Pair(2, 1961), Pair(4, 2028),
                                   Pair(6, 1962)));
}

TEST(DistributedConsistentHashingTest, TestOutputChangeCount) {
  // Distributions:
  // choice_id probability_1 probability_2
  // 0         0.4           0.2
  // 1         0.2           0.2
  // 2         0.2           0.2
  // 3         0.2           0.4
  std::vector<DistributionChoice> distribution_1(
      {DistributionChoice({0, 0.4}), DistributionChoice({1, 0.2}),
       DistributionChoice({2, 0.2}), DistributionChoice({3, 0.2})});
  ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DistributedConsistentHashing> hashing_1,
      DistributedConsistentHashing::Build(std::move(distribution_1)));
  std::vector<DistributionChoice> distribution_2(
      {DistributionChoice({0, 0.2}), DistributionChoice({1, 0.2}),
       DistributionChoice({2, 0.2}), DistributionChoice({3, 0.4})});
  ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DistributedConsistentHashing> hashing_2,
      DistributedConsistentHashing::Build(std::move(distribution_2)));

  int diff_output_count = 0;
  for (int seed = 0; seed < kSeedNumber; ++seed) {
    int32_t output_1 = hashing_1->Hash(std::to_string(seed));
    int32_t output_2 = hashing_2->Hash(std::to_string(seed));
    if (output_1 != output_2) {
      ++diff_output_count;
    }
  }
  // The number of outputs different between 2 hashings is guaranteed to be less
  // than L1 distance of the 2 distributions, which is 40% of total counts here.
  EXPECT_LE(diff_output_count, kSeedNumber * 0.4);
}

}  // namespace
}  // namespace wfa_virtual_people
