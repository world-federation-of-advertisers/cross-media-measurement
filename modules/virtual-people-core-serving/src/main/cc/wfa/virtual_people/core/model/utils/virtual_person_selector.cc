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

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "absl/memory/memory.h"
#include "absl/status/statusor.h"
#include "google/protobuf/repeated_field.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/utils/consistent_hash.h"

namespace wfa_virtual_people {

using ::google::protobuf::RepeatedPtrField;

// Sets up total_population_ and pools_.
// For example, if the input pools are
// [
//   {
//     population_offset: 100
//     total_population: 10
//   },
//   {
//     population_offset: 200
//     total_population: 20
//   },
//   {
//     population_offset: 300
//     total_population: 30
//   }
// ]
// The total_population_ will be 60 = 10 + 20 + 30.
// And the pools_ will be
// [
//   {
//     virtual_people_id_offset: 100
//     population_index_offset: 0
//   },
//   {
//     virtual_people_id_offset: 200
//     population_index_offset: 10
//   },
//   {
//     virtual_people_id_offset: 300
//     population_index_offset: 30 = 10 + 20
//   }
// ]
absl::StatusOr<std::unique_ptr<VirtualPersonSelector>>
VirtualPersonSelector::Build(
    const RepeatedPtrField<PopulationNode::VirtualPersonPool>& pools) {
  uint64_t total_population = 0;
  std::vector<VirtualPersonIdPool> compiled_pools;
  for (const PopulationNode::VirtualPersonPool& pool : pools) {
    if (pool.total_population() == 0) {
      // This pool is empty.
      continue;
    }
    compiled_pools.emplace_back();
    compiled_pools.back().virtual_people_id_offset = pool.population_offset();
    compiled_pools.back().population_index_offset = total_population;
    total_population += pool.total_population();
  }
  if (total_population == 0) {
    return absl::InvalidArgumentError(
        "The total population of the pools is 0. The model is invalid.");
  }
  return absl::make_unique<VirtualPersonSelector>(total_population,
                                                  std::move(compiled_pools));
}

VirtualPersonSelector::VirtualPersonSelector(
    const uint64_t total_population,
    std::vector<VirtualPersonIdPool>&& compiled_pools)
    : total_population_(total_population), pools_(std::move(compiled_pools)) {}

uint64_t VirtualPersonSelector::GetVirtualPersonId(
    const uint64_t random_seed) const {
  uint64_t population_index =
      JumpConsistentHash(random_seed, static_cast<int32_t>(total_population_));

  // Gets the first pool with population_index_offset larger than
  // population_index.
  auto it = std::upper_bound(
      pools_.begin(), pools_.end(), population_index,
      [](uint64_t population_index, const VirtualPersonIdPool& pool) {
        return population_index < pool.population_index_offset;
      });
  // The previous pool is the last pool with population_index_offset less than
  // or equal to population_index.
  it = std::prev(it);

  uint64_t virtual_people_id = population_index - it->population_index_offset +
                               it->virtual_people_id_offset;
  return virtual_people_id;
}

}  // namespace wfa_virtual_people
