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

#ifndef SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_ATTRIBUTES_UPDATER_H_
#define SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_ATTRIBUTES_UPDATER_H_

#include <memory>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/model_node.h"

namespace wfa_virtual_people {

// The C++ implementation of BranchNode.AttributesUpdater protobuf.
class AttributesUpdaterInterface {
 public:
  // Always use Build to get an AttributesUpdaterInterface object. Users should
  // not call the factory method or the constructor of the derived classes
  // directly.
  //
  // @node_refs is the mapping from indexes to the ModelNode objects, which
  // should contain the child nodes referenced by indexes in the attached model
  // trees.
  static absl::StatusOr<std::unique_ptr<AttributesUpdaterInterface>> Build(
      const BranchNode::AttributesUpdater& config,
      absl::flat_hash_map<uint32_t, std::unique_ptr<ModelNode>>& node_refs);

  // Builds AttributesUpdater with no attatched model tree, or the model tree is
  // defined without any child node referenced by index.
  static absl::StatusOr<std::unique_ptr<AttributesUpdaterInterface>> Build(
      const BranchNode::AttributesUpdater& config);

  virtual ~AttributesUpdaterInterface() = default;

  AttributesUpdaterInterface(const AttributesUpdaterInterface&) = delete;
  AttributesUpdaterInterface& operator=(const AttributesUpdaterInterface&) =
      delete;

  // Applies the attributes updater to the @event.
  //
  // In general, there are 2 steps:
  // 1. Find the attributes to be merged into @event, by conditions matching and
  //    probabilities.
  // 2. Merge the attributes into @event.
  virtual absl::Status Update(LabelerEvent& event) const = 0;

 protected:
  AttributesUpdaterInterface() = default;
};

}  // namespace wfa_virtual_people

#endif  // SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_ATTRIBUTES_UPDATER_H_
