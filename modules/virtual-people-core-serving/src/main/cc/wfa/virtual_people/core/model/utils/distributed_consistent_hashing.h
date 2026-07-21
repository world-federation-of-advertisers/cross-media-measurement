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

#ifndef SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_UTILS_DISTRIBUTED_CONSISTENT_HASHING_H_
#define SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_UTILS_DISTRIBUTED_CONSISTENT_HASHING_H_

#include <memory>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace wfa_virtual_people {

struct DistributionChoice {
  int32_t choice_id;
  double probability;
};

// The C++ implementation of consistent hashing for distributions.
//
// A distribution is represented by a set of choices, with a given probability
// for each choice to be the output of the hashing.
//
// The details of the algorithm can be found in
// https://github.com/world-federation-of-advertisers/virtual_people_examples/blob/main/notebooks/Consistent_Hashing.ipynb
class DistributedConsistentHashing {
 public:
  // Always use Build to get a DistributedConsistentHashing object. Users should
  // not call the constructor below directly.
  //
  // @distribution is represented by a list of (choice_id, probability) structs.
  // The probabilities will be normalized.
  //
  // Returns error status if any of the following happens:
  //   @distribution is empty.
  //   Any probability in @distribution is negative.
  //   Sum of probabilities in @distribution is not positive.
  static absl::StatusOr<std::unique_ptr<DistributedConsistentHashing>> Build(
      std::vector<DistributionChoice>&& distribution);

  // Never call the constructor directly.
  explicit DistributedConsistentHashing(
      std::vector<DistributionChoice>&& distribution)
      : distribution_(std::move(distribution)) {}

  DistributedConsistentHashing(const DistributedConsistentHashing&) = delete;
  DistributedConsistentHashing& operator=(const DistributedConsistentHashing&) =
      delete;

  // Returns the selected choice id.
  int32_t Hash(absl::string_view random_seed) const;

 private:
  std::vector<DistributionChoice> distribution_;
};

}  // namespace wfa_virtual_people

#endif  // SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_UTILS_DISTRIBUTED_CONSISTENT_HASHING_H_
