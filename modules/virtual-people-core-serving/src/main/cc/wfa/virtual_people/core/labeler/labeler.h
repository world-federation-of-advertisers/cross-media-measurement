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

#ifndef SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_LABELER_LABELER_H_
#define SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_LABELER_LABELER_H_

#include <memory>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "wfa/virtual_people/common/event.pb.h"
#include "wfa/virtual_people/common/label.pb.h"
#include "wfa/virtual_people/common/model.pb.h"
#include "wfa/virtual_people/core/model/model_node.h"

namespace wfa_virtual_people {

enum class LabelingMode {
  kFull,          // Default: assign VID (today's behavior).
  kPoolIdentity,  // Pass 1: emit pool assignment, no VID.
};

class Labeler {
 public:
  // Always use Labeler::Build to get a Labeler object.
  // Users should never call the constructor directly.
  //
  // There are 3 ways to represent a full model:
  // * Option 1:
  //   A single root node, with all the other nodes in the model tree attached
  //   directly to their parent nodes. Example (node1 is the root node):
  //       _node1_
  //      |       |
  //   node2    _node3_
  //      |    |       |
  //   node4  node5 node6
  // * Option 2:
  //   A list of nodes. All nodes except the root node must have index set.
  //   For any node with child nodes, the child nodes are referenced by indexes.
  //   Example (node1 is the root node):
  //   node1: index = null, child_nodes = [2, 3]
  //   node2: index = 2, child_nodes = [4]
  //   node3: index = 3, child_nodes = [5, 6]
  //   node4: index = 4, child_nodes = []
  //   node5: index = 5, child_nodes = []
  //   node6: index = 6, child_nodes = []
  // * Option 3:
  //   Mix of the above 2. Some nodes are referenced directly, while others are
  //   referenced by indexes. For any node referenced by index, an entry must be
  //   included in @nodes, with the index field set.
  //   Example (node1 is the root node):
  //   node1:
  //       _node1_
  //      |       |
  //      2     _node3_
  //           |       |
  //           5       6
  //   node2: index = 2
  //        node2
  //          |
  //        node4
  //   node5: index = 5
  //   node6: index = 6

  // Build the model with the @root node. Handles option 1 above.
  //
  // All the other nodes are referenced directly in branch_node.branches.node of
  // the parent nodes.
  // Any index or node_index field is ignored.
  static absl::StatusOr<std::unique_ptr<Labeler>> Build(
      const CompiledNode& root);

  // Build the model with all the @nodes. Handles option 2 and 3 above.
  //
  // Nodes are allowed to be referenced by branch_node.branches.node_index.
  //
  // For CompiledNodes in @nodes, only the root node is allowed to not have
  // index set.
  //
  // @nodes must be sorted in the order that any child node is prior to its
  // parent node.
  static absl::StatusOr<std::unique_ptr<Labeler>> Build(
      const std::vector<CompiledNode>& nodes);

  explicit Labeler(std::unique_ptr<ModelNode> root) : root_(std::move(root)) {}

  Labeler(const Labeler&) = delete;
  Labeler& operator=(const Labeler&) = delete;

  // Apply the model to generate the labels.
  // Invalid inputs will result in an error status.
  absl::Status Label(const LabelerInput& input, LabelerOutput& output) const;

  // Apply the model with a specific labeling mode.
  // In kPoolIdentity mode, RankedPopulationNode leaves emit PoolAssignment
  // entries instead of assigning VIDs.
  absl::Status Label(const LabelerInput& input, LabelerOutput& output,
                     LabelingMode mode) const;

 private:
  std::unique_ptr<ModelNode> root_;
};

}  // namespace wfa_virtual_people

#endif  // SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_LABELER_LABELER_H_
