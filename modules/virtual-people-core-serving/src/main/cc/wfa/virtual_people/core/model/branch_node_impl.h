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

#ifndef SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_BRANCH_NODE_IMPL_H_
#define SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_BRANCH_NODE_IMPL_H_

#include <memory>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/variant.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/attributes_updater.h"
#include "wfa/virtual_people/core/model/model_node.h"
#include "wfa/virtual_people/core/model/multiplicity_impl.h"
#include "wfa/virtual_people/core/model/utils/distributed_consistent_hashing.h"
#include "wfa/virtual_people/core/model/utils/field_filters_matcher.h"

namespace wfa_virtual_people {

// The implementation of the CompiledNode with branch_node set.
//
// The field branch_node in @node_config must be set.
class BranchNodeImpl : public ModelNode {
 public:
  // Always use ModelNode::Build to get a ModelNode object.
  // Users should never call the factory function or constructor of the derived
  // class directly.
  //
  // Returns error status if any of the following happens:
  // * @node_config.branch_node is not set.
  // * @node_config.branch_node.branches is empty.
  // * There is at least one of @node_config.branch_node.branches, which has
  //   neither node_index nor node set.
  // * There is at least one of @node_config.branch_node.branches, which has
  //   neither chance nor condition set.
  // * At least one of @node_config.branch_node.branches has chance set, and at
  //   least one of @node_config.branch_node.branches has condition set.
  // * When node_index is set in @node_config.branch_node.branches, @node_refs
  //   has no entry for this node_index.
  //
  // For any @node_config.branch_node.branches with node_index set, the
  // corresponding child node is retrieved from @node_refs using node_index as
  // the key. The entry is removed from @node_refs after the ownership of the
  // std::unique_ptr<ModelNode> is moved to this class.
  static absl::StatusOr<std::unique_ptr<BranchNodeImpl>> Build(
      const CompiledNode& node_config,
      absl::flat_hash_map<uint32_t, std::unique_ptr<ModelNode>>& node_refs);

  explicit BranchNodeImpl(
      const CompiledNode& node_config,
      std::vector<std::unique_ptr<ModelNode>>&& child_nodes,
      std::unique_ptr<DistributedConsistentHashing> hashing,
      absl::string_view random_seed,
      std::unique_ptr<FieldFiltersMatcher> matcher,
      std::vector<std::unique_ptr<AttributesUpdaterInterface>>&& updaters,
      std::unique_ptr<MultiplicityImpl> multiplicity);
  ~BranchNodeImpl() override {}

  BranchNodeImpl(const BranchNodeImpl&) = delete;
  BranchNodeImpl& operator=(const BranchNodeImpl&) = delete;

  // Based on action(oneof updates/multiplicty) at this node:
  // - If no action, just apply child node.
  // - If updates is set, update attributes, then apply child node.
  // - If multiplicity is set, clone event, apply child node to each clone,
  //   then merge labeling outputs.
  absl::Status Apply(LabelerEvent& event) const override;

 private:
  // Uses @hashing_ or @matcher_ to select one of @child_nodes_, and apply
  // the selected node to @event.
  absl::Status ApplyChild(LabelerEvent& event) const;

  // Steps:
  // 1. Compute multiplicity for @event, clone the event accordingly.
  // 2. Apply child node to each clone.
  // 3. Merge the labeling outputs.
  absl::Status ApplyMultiplicity(LabelerEvent& event) const;

  // Include the child nodes in all the branches, in the same order as the
  // branches in @node_config.
  std::vector<std::unique_ptr<ModelNode>> child_nodes_;

  // If chance is set in each branches in @node_config, hashing_ is set, and
  // used to select a child node with random_seed_ when Apply is called.
  std::unique_ptr<DistributedConsistentHashing> hashing_;
  const std::string random_seed_;

  // If condition is set in each branches in @node_config, matcher_ is set, and
  // used to select the first child node whose condition matches when Apply is
  // called.
  std::unique_ptr<FieldFiltersMatcher> matcher_;

  // If updates is set in @node_config, updaters_ is set.
  // When calling Apply, entries of @updaters_ is applied to the event in order.
  // Each entry of @updaters_ updates the value of some fields in the event.
  std::vector<std::unique_ptr<AttributesUpdaterInterface>> updaters_;

  // If multiplicity is set in @node_config, multiplicity_ is set.
  // When calling Apply, call ApplyMultiplicity.
  std::unique_ptr<MultiplicityImpl> multiplicity_;
};

}  // namespace wfa_virtual_people

#endif  // SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_BRANCH_NODE_IMPL_H_
