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

#ifndef SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_UPDATE_TREE_IMPL_H_
#define SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_UPDATE_TREE_IMPL_H_

#include <memory>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/attributes_updater.h"
#include "wfa/virtual_people/core/model/model_node.h"

namespace wfa_virtual_people {

class UpdateTreeImpl : public AttributesUpdaterInterface {
 public:
  // Always use AttributesUpdaterInterface::Build to get an
  // AttributesUpdaterInterface object. Users should
  // not call the factory method or the constructor of the derived classes
  // directly.
  //
  // Returns error status when it fails to build ModelNode from @config.root.
  static absl::StatusOr<std::unique_ptr<UpdateTreeImpl>> Build(
      const UpdateTree& config,
      absl::flat_hash_map<uint32_t, std::unique_ptr<ModelNode>>& node_refs);

  explicit UpdateTreeImpl(std::unique_ptr<ModelNode> root)
      : root_(std::move(root)) {}

  UpdateTreeImpl(const UpdateTreeImpl&) = delete;
  UpdateTreeImpl& operator=(const UpdateTreeImpl&) = delete;

  // Applies the attached model tree, which is represented by the root node, to
  // the @event.
  absl::Status Update(LabelerEvent& event) const override;

 private:
  std::unique_ptr<ModelNode> root_;
};

}  // namespace wfa_virtual_people

#endif  // SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_UPDATE_TREE_IMPL_H_
