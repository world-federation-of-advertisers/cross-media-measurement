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

#include "wfa/virtual_people/core/model/model_node.h"

#include <memory>

#include "absl/status/statusor.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/branch_node_impl.h"
#include "wfa/virtual_people/core/model/population_node_impl.h"
#include "wfa/virtual_people/core/model/ranked_population_node_impl.h"
#include "wfa/virtual_people/core/model/stop_node_impl.h"

namespace wfa_virtual_people {

absl::StatusOr<std::unique_ptr<ModelNode>> ModelNode::Build(
    const CompiledNode& config,
    absl::flat_hash_map<uint32_t, std::unique_ptr<ModelNode>>& node_refs) {
  if (config.type_case() == CompiledNode::TypeCase::kBranchNode) {
    return BranchNodeImpl::Build(config, node_refs);
  }
  return ModelNode::Build(config);
}

absl::StatusOr<std::unique_ptr<ModelNode>> ModelNode::Build(
    const CompiledNode& config) {
  switch (config.type_case()) {
    case CompiledNode::TypeCase::kBranchNode: {
      absl::flat_hash_map<uint32_t, std::unique_ptr<ModelNode>> node_refs;
      return BranchNodeImpl::Build(config, node_refs);
    }
    case CompiledNode::TypeCase::kStopNode:
      return StopNodeImpl::Build(config);
    case CompiledNode::TypeCase::kPopulationNode:
      return PopulationNodeImpl::Build(config);
    case CompiledNode::TypeCase::kRankedPopulationNode:
      return RankedPopulationNodeImpl::Build(config);
    default:
      return absl::InvalidArgumentError("Node type is not set.");
  }
}

ModelNode::ModelNode(const CompiledNode& node_config)
    : name_(node_config.name()),
      from_model_builder_config_(
          node_config.debug_info().directly_from_model_builder_config()) {}

}  // namespace wfa_virtual_people
