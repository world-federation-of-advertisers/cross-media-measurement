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

#include "wfa/virtual_people/core/model/utils/virtual_person_selector.h"

#include <memory>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "gmock/gmock.h"
#include "google/protobuf/text_format.h"
#include "gtest/gtest.h"
#include "wfa/virtual_people/common/model.pb.h"

namespace wfa_virtual_people {
namespace {

using ::testing::DoubleNear;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;
using ::wfa::StatusIs;

constexpr int kSeedNumber = 10000;

TEST(VirtualPersonSelectorTest, TestGetVirtualPersonId) {
  PopulationNode population_node;
  // 18446744073709551515 = std::numeric_limits<uint64_t>::max()-100
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        pools { population_offset: 10 total_population: 3 }
        pools { population_offset: 18446744073709551515 total_population: 3 }
        pools { population_offset: 20 total_population: 4 }
      )pb",
      &population_node));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<VirtualPersonSelector> selector,
                       VirtualPersonSelector::Build(population_node.pools()));

  absl::flat_hash_map<uint64_t, int32_t> id_counts;

  for (int seed = 0; seed < kSeedNumber; ++seed) {
    uint64_t id = selector->GetVirtualPersonId(static_cast<uint64_t>(seed));
    ++id_counts[id];
  }

  // The expected count for getting a given virtual person id is 1 / 10 *
  // kSeedNumber = 1000.  We compare to the exact values to make sure the kotlin
  // and c++ implementations behave the same.
  EXPECT_THAT(id_counts, UnorderedElementsAre(
                             Pair(10, 993), Pair(11, 997), Pair(12, 994),
                             Pair(20, 980), Pair(21, 1027), Pair(22, 979),
                             Pair(23, 1020), Pair(18446744073709551515u, 1000),
                             Pair(18446744073709551516u, 1015),
                             Pair(18446744073709551517u, 995)));
}

TEST(VirtualPersonSelectorTest, TestInvalidPools) {
  // This is invalid as the total pools size is 0.
  PopulationNode population_node;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        pools { population_offset: 10 total_population: 0 }
        pools { population_offset: 30 total_population: 0 }
        pools { population_offset: 20 total_population: 0 }
      )pb",
      &population_node));
  EXPECT_THAT(VirtualPersonSelector::Build(population_node.pools()).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

}  // namespace
}  // namespace wfa_virtual_people
