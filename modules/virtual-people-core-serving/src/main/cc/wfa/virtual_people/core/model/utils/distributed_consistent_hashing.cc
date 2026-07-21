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

#include <cmath>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "wfa/virtual_people/core/model/utils/hash.h"

namespace wfa_virtual_people {

constexpr double kNormalizeError = 0.0000001;

absl::StatusOr<std::unique_ptr<DistributedConsistentHashing>>
DistributedConsistentHashing::Build(
    std::vector<DistributionChoice>&& distribution) {
  if (distribution.empty()) {
    return absl::InvalidArgumentError("The given distribution is empty.");
  }
  // Returns error status for any negative probability, and gets sum of
  // probabilities.
  double probabilities_sum = 0.0;
  for (const DistributionChoice& choice : distribution) {
    if (choice.probability < 0) {
      return absl::InvalidArgumentError("Negative probability is provided.");
    }
    probabilities_sum += choice.probability;
  }

  if (probabilities_sum < 1 - kNormalizeError ||
      probabilities_sum > 1 + kNormalizeError) {
    return absl::InvalidArgumentError("Probabilities do not sum to 1.");
  }
  // Normalizes the probabilities.
  for (DistributionChoice& choice : distribution) {
    choice.probability /= probabilities_sum;
  }
  return absl::make_unique<DistributedConsistentHashing>(
      std::move(distribution));
}

std::string GetFullSeed(absl::string_view random_seed, const int32_t choice) {
  return absl::StrFormat("consistent-hashing-%s-%d", random_seed, choice);
}

double ComputeXi(absl::string_view random_seed, const int32_t choice,
                 const double probability) {
  return ExpHash(GetFullSeed(random_seed, choice)) / probability;
}

// A C++ version of the Python function ConsistentHashing.hash in
// https://github.com/world-federation-of-advertisers/virtual_people_examples/blob/main/notebooks/Consistent_Hashing.ipynb
int32_t DistributedConsistentHashing::Hash(
    absl::string_view random_seed) const {
  int32_t choice_id = 0;
  double choice_xi = std::numeric_limits<double>::max();
  for (const auto& choice : distribution_) {
    double xi = ComputeXi(random_seed, choice.choice_id, choice.probability);
    if (choice_xi > xi) {
      choice_id = choice.choice_id;
      choice_xi = xi;
    }
  }
  return choice_id;
}

}  // namespace wfa_virtual_people
