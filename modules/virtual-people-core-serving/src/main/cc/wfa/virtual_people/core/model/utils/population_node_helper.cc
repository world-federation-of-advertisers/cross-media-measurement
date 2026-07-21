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

#include "wfa/virtual_people/core/model/utils/population_node_helper.h"

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "common_cpp/macros/macros.h"
#include "wfa/virtual_people/common/label.pb.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/utils/distributed_consistent_hashing.h"

namespace wfa_virtual_people {

absl::Status CollapseQuantumLabel(const QuantumLabel& quantum_label,
                                  absl::string_view seed_suffix,
                                  PersonLabelAttributes& output_label) {
  if (quantum_label.labels_size() == 0) {
    return absl::InvalidArgumentError("Empty quantum label.");
  }
  if (quantum_label.labels_size() != quantum_label.probabilities_size()) {
    return absl::InvalidArgumentError(absl::StrCat(
        "The sizes of labels and probabilities are different in quantum label ",
        quantum_label.DebugString()));
  }
  std::vector<DistributionChoice> distribution;
  distribution.reserve(quantum_label.probabilities_size());
  for (int i = 0; i < quantum_label.probabilities_size(); ++i) {
    distribution.emplace_back(
        DistributionChoice({i, quantum_label.probabilities(i)}));
  }
  ASSIGN_OR_RETURN(
      std::unique_ptr<DistributedConsistentHashing> hashing,
      DistributedConsistentHashing::Build(std::move(distribution)));
  int32_t index = hashing->Hash(absl::StrCat(
      "quantum-label-collapse-", quantum_label.seed(), seed_suffix));
  output_label.MergeFrom(quantum_label.labels(index));
  return absl::OkStatus();
}

}  // namespace wfa_virtual_people
