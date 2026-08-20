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

#include "wfa/virtual_people/core/model/attributes_updater.h"

#include <memory>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/conditional_assignment_impl.h"
#include "wfa/virtual_people/core/model/conditional_merge_impl.h"
#include "wfa/virtual_people/core/model/geometric_shredder_impl.h"
#include "wfa/virtual_people/core/model/model_node.h"
#include "wfa/virtual_people/core/model/sparse_update_matrix_impl.h"
#include "wfa/virtual_people/core/model/update_matrix_impl.h"
#include "wfa/virtual_people/core/model/update_tree_impl.h"

namespace wfa_virtual_people {

absl::StatusOr<std::unique_ptr<AttributesUpdaterInterface>>
AttributesUpdaterInterface::Build(
    const BranchNode::AttributesUpdater& config,
    absl::flat_hash_map<uint32_t, std::unique_ptr<ModelNode>>& node_refs) {
  if (config.update_case() ==
      BranchNode::AttributesUpdater::UpdateCase::kUpdateTree) {
    return UpdateTreeImpl::Build(config.update_tree(), node_refs);
  }
  return AttributesUpdaterInterface::Build(config);
}

absl::StatusOr<std::unique_ptr<AttributesUpdaterInterface>>
AttributesUpdaterInterface::Build(const BranchNode::AttributesUpdater& config) {
  switch (config.update_case()) {
    case BranchNode::AttributesUpdater::UpdateCase::kUpdateMatrix:
      return UpdateMatrixImpl::Build(config.update_matrix());
    case BranchNode::AttributesUpdater::UpdateCase::kSparseUpdateMatrix:
      return SparseUpdateMatrixImpl::Build(config.sparse_update_matrix());
    case BranchNode::AttributesUpdater::UpdateCase::kConditionalMerge:
      return ConditionalMergeImpl::Build(config.conditional_merge());
    case BranchNode::AttributesUpdater::UpdateCase::kUpdateTree: {
      absl::flat_hash_map<uint32_t, std::unique_ptr<ModelNode>> node_refs;
      return UpdateTreeImpl::Build(config.update_tree(), node_refs);
    }
    case BranchNode::AttributesUpdater::UpdateCase::kConditionalAssignment:
      return ConditionalAssignmentImpl::Build(config.conditional_assignment());
    case BranchNode::AttributesUpdater::UpdateCase::kGeometricShredder:
      return GeometricShredderImpl::Build(config.geometric_shredder());
    default:
      return absl::InvalidArgumentError("config.update is not set.");
  }
}

}  // namespace wfa_virtual_people
