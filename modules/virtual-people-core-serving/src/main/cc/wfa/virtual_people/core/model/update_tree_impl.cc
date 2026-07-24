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

#include "wfa/virtual_people/core/model/update_tree_impl.h"

#include <memory>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common_cpp/macros/macros.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/attributes_updater.h"
#include "wfa/virtual_people/core/model/model_node.h"

namespace wfa_virtual_people {

absl::StatusOr<std::unique_ptr<UpdateTreeImpl>> UpdateTreeImpl::Build(
    const UpdateTree& config,
    absl::flat_hash_map<uint32_t, std::unique_ptr<ModelNode>>& node_refs) {
  ASSIGN_OR_RETURN(std::unique_ptr<ModelNode> root,
                   ModelNode::Build(config.root(), node_refs));

  if (!root) {
    return absl::InternalError("ModelNode::Build should never return NULL.");
  }

  return absl::make_unique<UpdateTreeImpl>(std::move(root));
}

absl::Status UpdateTreeImpl::Update(LabelerEvent& event) const {
  return root_->Apply(event);
}

}  // namespace wfa_virtual_people
