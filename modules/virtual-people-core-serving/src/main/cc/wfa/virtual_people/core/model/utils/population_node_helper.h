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

#ifndef SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_UTILS_POPULATION_NODE_HELPER_H_
#define SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_UTILS_POPULATION_NODE_HELPER_H_

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "wfa/virtual_people/common/label.pb.h"
#include "wfa/virtual_people/common/model.pb.h"

namespace wfa_virtual_people {

// Collapses @quantum_label to a single label based on its probabilities and
// merges the result into @output_label. Shared by PopulationNodeImpl and
// RankedPopulationNodeImpl so a fix in one applies to both.
absl::Status CollapseQuantumLabel(const QuantumLabel& quantum_label,
                                  absl::string_view seed_suffix,
                                  PersonLabelAttributes& output_label);

}  // namespace wfa_virtual_people

#endif  // SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_UTILS_POPULATION_NODE_HELPER_H_
