// Copyright 2026 The Cross-Media Measurement Authors
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

#include "wfa/virtual_people/core/model/utils/feistel.h"

#include <cstdint>
#include <string>
#include <unordered_set>

#include "gtest/gtest.h"

namespace wfa_virtual_people {
namespace {

TEST(FeistelPermuteTest, DomainSizeOne) {
  EXPECT_EQ(FeistelPermute(0, 1, "seed"), 0);
}

TEST(FeistelPermuteTest, DomainSizeZero) {
  EXPECT_EQ(FeistelPermute(0, 0, "seed"), 0);
}

TEST(FeistelPermuteTest, DomainSizeTwo) {
  std::unordered_set<uint64_t> outputs;
  for (uint64_t i = 0; i < 2; ++i) {
    uint64_t result = FeistelPermute(i, 2, "test-seed");
    EXPECT_LT(result, 2);
    outputs.insert(result);
  }
  EXPECT_EQ(outputs.size(), 2);
}

TEST(FeistelPermuteTest, BijectivitySmallDomain) {
  const uint64_t domain_size = 100;
  std::unordered_set<uint64_t> outputs;
  for (uint64_t i = 0; i < domain_size; ++i) {
    uint64_t result = FeistelPermute(i, domain_size, "bijectivity-seed");
    EXPECT_LT(result, domain_size);
    outputs.insert(result);
  }
  EXPECT_EQ(outputs.size(), domain_size);
}

TEST(FeistelPermuteTest, BijectivityMediumDomain) {
  const uint64_t domain_size = 1000;
  std::unordered_set<uint64_t> outputs;
  for (uint64_t i = 0; i < domain_size; ++i) {
    uint64_t result = FeistelPermute(i, domain_size, "medium-seed");
    EXPECT_LT(result, domain_size);
    outputs.insert(result);
  }
  EXPECT_EQ(outputs.size(), domain_size);
}

TEST(FeistelPermuteTest, BijectivityPrimeDomain) {
  const uint64_t domain_size = 997;  // Prime number.
  std::unordered_set<uint64_t> outputs;
  for (uint64_t i = 0; i < domain_size; ++i) {
    uint64_t result = FeistelPermute(i, domain_size, "prime-seed");
    EXPECT_LT(result, domain_size);
    outputs.insert(result);
  }
  EXPECT_EQ(outputs.size(), domain_size);
}

TEST(FeistelPermuteTest, Determinism) {
  const uint64_t domain_size = 500;
  const std::string seed = "determinism-seed";
  for (uint64_t i = 0; i < domain_size; ++i) {
    EXPECT_EQ(FeistelPermute(i, domain_size, seed),
              FeistelPermute(i, domain_size, seed));
  }
}

TEST(FeistelPermuteTest, DifferentSeedsDifferentOutputs) {
  const uint64_t domain_size = 100;
  bool any_different = false;
  for (uint64_t i = 0; i < domain_size; ++i) {
    if (FeistelPermute(i, domain_size, "seed-a") !=
        FeistelPermute(i, domain_size, "seed-b")) {
      any_different = true;
      break;
    }
  }
  EXPECT_TRUE(any_different);
}

TEST(FeistelPermuteTest, OutputInRange) {
  const uint64_t domain_size = 1234;
  for (uint64_t i = 0; i < domain_size; ++i) {
    EXPECT_LT(FeistelPermute(i, domain_size, "range-seed"), domain_size);
  }
}

TEST(FeistelPermuteTest, GoldenVectorsCrossLanguageParity) {
  // These values must match the Kotlin Feistel.permute output to ensure both
  // labelers produce identical VIDs for the same inputs.
  EXPECT_EQ(FeistelPermute(0, 100, "bijectivity-seed"), 39);
  EXPECT_EQ(FeistelPermute(1, 100, "bijectivity-seed"), 33);
  EXPECT_EQ(FeistelPermute(99, 100, "bijectivity-seed"), 27);
  EXPECT_EQ(FeistelPermute(0, 1000, "medium-seed"), 252);
  EXPECT_EQ(FeistelPermute(1, 1000, "medium-seed"), 392);
  EXPECT_EQ(FeistelPermute(999, 1000, "medium-seed"), 344);
}

}  // namespace
}  // namespace wfa_virtual_people
