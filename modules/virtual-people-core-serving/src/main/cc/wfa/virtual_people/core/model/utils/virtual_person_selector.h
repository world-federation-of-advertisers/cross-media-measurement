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

#ifndef SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_UTILS_VIRTUAL_PERSON_SELECTOR_H_
#define SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_UTILS_VIRTUAL_PERSON_SELECTOR_H_

#include <cstdint>
#include <memory>
#include <vector>

#include "absl/status/statusor.h"
#include "google/protobuf/repeated_field.h"
#include "wfa/virtual_people/common/model.pb.h"

namespace wfa_virtual_people {

using ::google::protobuf::RepeatedPtrField;

// Includes the information of a virtual person pool.
//
// The first virtual person id of this pool is virtual_people_id_offset.
// The first population index of this pool is population_index_offset.
// population_index_offset equals to the accumulated population of all previous
// pools.
struct VirtualPersonIdPool {
  uint64_t virtual_people_id_offset;
  uint64_t population_index_offset;
};

// Selects an id from a set of virtual person pools.
//
// The selection is based on consistent hashing.
//
// The possible id space is the combination of all pools. E.g.
// If the input pools are
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
// The possible id space is [100, 109], [200, 219], [300, 329].
class VirtualPersonSelector {
 public:
  // Always use Build to get a VirtualPersonSelector object. User should not
  // call the constructor below directly.
  static absl::StatusOr<std::unique_ptr<VirtualPersonSelector>> Build(
      const RepeatedPtrField<PopulationNode::VirtualPersonPool>& pools);

  // Never call the constructor directly.
  explicit VirtualPersonSelector(
      uint64_t total_population,
      std::vector<VirtualPersonIdPool>&& compiled_pools);

  VirtualPersonSelector(const VirtualPersonSelector&) = delete;
  VirtualPersonSelector& operator=(const VirtualPersonSelector&) = delete;

  // Selects and returns an id from the virtual person pools, using consistent
  // hashing based on the random_seed.
  uint64_t GetVirtualPersonId(uint64_t random_seed) const;

 private:
  // The sum of total population of all pools. Required for hashing.
  uint64_t total_population_;

  // Stores the required information to compute the virtual person id after
  // hashing.
  std::vector<VirtualPersonIdPool> pools_;
};

}  // namespace wfa_virtual_people

#endif  // SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_UTILS_VIRTUAL_PERSON_SELECTOR_H_
