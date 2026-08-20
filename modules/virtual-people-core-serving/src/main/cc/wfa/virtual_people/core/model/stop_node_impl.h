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

#ifndef SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_STOP_NODE_IMPL_H_
#define SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_STOP_NODE_IMPL_H_

#include <memory>

#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/model_node.h"

namespace wfa_virtual_people {

// The implementation of the CompiledNode with stop_node set.
class StopNodeImpl : public ModelNode {
 public:
  // Always use ModelNode::Build to get a ModelNode object.
  // Users should never call the factory function or constructor of the derived
  // class directly.
  static absl::StatusOr<std::unique_ptr<StopNodeImpl>> Build(
      const CompiledNode& node_config) {
    return absl::make_unique<StopNodeImpl>(node_config);
  }

  // Never call the constructor directly.
  explicit StopNodeImpl(const CompiledNode& node_config)
      : ModelNode(node_config) {}
  ~StopNodeImpl() override {}

  StopNodeImpl(const StopNodeImpl&) = delete;
  StopNodeImpl& operator=(const StopNodeImpl&) = delete;

  // No operation is needed when applying a StopNode.
  absl::Status Apply(LabelerEvent& event) const override {
    return absl::OkStatus();
  }
};

}  // namespace wfa_virtual_people

#endif  // SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_STOP_NODE_IMPL_H_
