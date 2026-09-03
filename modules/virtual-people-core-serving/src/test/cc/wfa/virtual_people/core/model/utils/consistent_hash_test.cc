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

#include "wfa/virtual_people/core/model/utils/consistent_hash.h"

#include <limits>
#include <random>
#include <vector>

#include "absl/random/random.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace wfa_virtual_people {
namespace {
using ::testing::Each;
using ::testing::Ge;

constexpr int kKeyNumber = 1000;
constexpr int32_t kMaxBuckets = 1 << 16;

MATCHER(IsConsistentJumpHashing, "") {
  if (arg.empty()) return true;

  if (arg[0] != 0) {
    *result_listener << "first element is nonzero: " << arg[0];
    return false;
  }

  for (int i = 1; i < arg.size(); ++i) {
    if (arg[i] != arg[i - 1] && arg[i] != i) {
      *result_listener << "element " << arg[i] << " at index " << i
                       << " is neither " << arg[i - 1] << " nor " << i;
      return false;
    }
  }

  return true;
}

// When using the same key, for any number of buckets n > 1, one of the
// following must be satisfied
// * JumpConsistentHash(key, n) == JumpConsistentHash(key, n - 1)
// * JumpConsistentHash(key, n) == n - 1
void CheckCorrectnessForOneKey(const uint64_t key, const int32_t max_buckets) {
  std::vector<int32_t> outputs;
  outputs.reserve(max_buckets);
  for (int32_t num_buckets = 1; num_buckets <= max_buckets; ++num_buckets) {
    outputs.push_back(JumpConsistentHash(key, num_buckets));
  }
  ASSERT_THAT(outputs, IsConsistentJumpHashing())
      << "with key " << key << " and max_buckets " << max_buckets;
}

uint64_t RandomKey() {
  static absl::BitGen bitgen;
  return absl::Uniform(bitgen, std::numeric_limits<std::uint64_t>::min(),
                       std::numeric_limits<std::uint64_t>::max());
}

TEST(JumpConsistentHashTest, CheckCorrectnessOfExamples) {
  EXPECT_EQ(93, JumpConsistentHash(1000ULL, 1000));
  EXPECT_EQ(31613, JumpConsistentHash(1000ULL, kMaxBuckets));
}

TEST(JumpConsistentHashTest, TestCorrectness) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<std::uint64_t> distrib(
      std::numeric_limits<std::uint64_t>::min(),
      std::numeric_limits<std::uint64_t>::max());
  for (int i = 0; i < kKeyNumber; i++) {
    CheckCorrectnessForOneKey(RandomKey(), kMaxBuckets);
  }
}

TEST(JumpConsistentHashTest, TestIntMaxBuckets) {
  std::vector<int32_t> outputs;
  outputs.reserve(kKeyNumber);
  for (int i = 0; i < kKeyNumber; i++) {
    outputs.push_back(JumpConsistentHash(
        RandomKey(), std::numeric_limits<std::int32_t>::max()));
  }
  EXPECT_THAT(outputs, Each(Ge(0)));
}

}  // namespace
}  // namespace wfa_virtual_people
