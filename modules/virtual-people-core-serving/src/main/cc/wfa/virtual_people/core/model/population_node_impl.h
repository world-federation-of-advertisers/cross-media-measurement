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

#ifndef SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_POPULATION_NODE_IMPL_H_
#define SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_POPULATION_NODE_IMPL_H_

#include <memory>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/model_node.h"
#include "wfa/virtual_people/core/model/utils/virtual_person_selector.h"

namespace wfa_virtual_people {

// The implementation of the CompiledNode with population_node set.
class PopulationNodeImpl : public ModelNode {
 public:
  // Always use ModelNode::Build to get a ModelNode object.
  // Users should never call the factory function or constructor of the derived
  // class directly.
  //
  // Returns error status if any of the following happens:
  // * @node_config.population_node is not set.
  // * The total population of the pools is 0 and the pools do not represent
  //   an empty population pool. An empty population pool contains only 1
  //   VirtualPersonPool, and its population_offset and total_population are 0.
  static absl::StatusOr<std::unique_ptr<PopulationNodeImpl>> Build(
      const CompiledNode& node_config);

  // Never call the constructor directly.
  explicit PopulationNodeImpl(
      const CompiledNode& node_config,
      std::unique_ptr<VirtualPersonSelector> virtual_person_selector,
      absl::string_view random_seed);
  ~PopulationNodeImpl() override {}

  PopulationNodeImpl(const PopulationNodeImpl&) = delete;
  PopulationNodeImpl& operator=(const PopulationNodeImpl&) = delete;

  // When Apply is called, exactly one id will be selected from the pools in
  // population_node, and assigned to virtual_person_activities[0] in @event.
  absl::Status Apply(LabelerEvent& event) const override;

 private:
  // Used to get a virtual person id. Is nullptr when the pools represent an
  // empty population pool.
  std::unique_ptr<VirtualPersonSelector> virtual_person_selector_;
  const std::string random_seed_;
};

}  // namespace wfa_virtual_people

#endif  // SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_POPULATION_NODE_IMPL_H_
