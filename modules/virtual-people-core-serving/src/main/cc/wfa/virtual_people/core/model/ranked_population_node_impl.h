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

#ifndef SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_RANKED_POPULATION_NODE_IMPL_H_
#define SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_RANKED_POPULATION_NODE_IMPL_H_

#include <cstdint>
#include <memory>
#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/model_node.h"

namespace wfa_virtual_people {

// Implementation of CompiledNode with ranked_population_node set.
// Splits VID assignment into ranked (Feistel, collision-free) and unranked
// (hash-based) sub-ranges. Supports DISJOINT and FULL_POOL modes.
class RankedPopulationNodeImpl : public ModelNode {
 public:
  static absl::StatusOr<std::unique_ptr<RankedPopulationNodeImpl>> Build(
      const CompiledNode& node_config);

  // Never call the constructor directly.
  RankedPopulationNodeImpl(const CompiledNode& node_config,
                           std::string random_seed, uint64_t ranked_size,
                           RankedPopulationNode::UnrankedMode unranked_mode,
                           uint64_t pool_offset, uint64_t pool_size);

  RankedPopulationNodeImpl(const RankedPopulationNodeImpl&) = delete;
  RankedPopulationNodeImpl& operator=(const RankedPopulationNodeImpl&) = delete;
  ~RankedPopulationNodeImpl() override {}

  absl::Status Apply(LabelerEvent& event) const override;

 private:
  const std::string random_seed_;
  const uint64_t ranked_size_;
  const RankedPopulationNode::UnrankedMode unranked_mode_;
  const uint64_t pool_offset_;
  const uint64_t pool_size_;
};

}  // namespace wfa_virtual_people

#endif  // SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_RANKED_POPULATION_NODE_IMPL_H_
