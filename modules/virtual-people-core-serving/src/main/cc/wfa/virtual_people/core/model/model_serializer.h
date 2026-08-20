// Copyright 2022 The Cross-Media Measurement Authors
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

#ifndef SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_MODEL_SERIALIZER_H_
#define SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_MODEL_SERIALIZER_H_

#include <vector>

#include "absl/status/statusor.h"
#include "wfa/virtual_people/common/model.pb.h"

namespace wfa_virtual_people {

// A full model can be represented as a single node or a list of nodes.
// * Option 1:
//   A single root node, with all the other nodes in the model tree attached
//   directly to their parent nodes. Example (node1 is the root node):
//       _node1_
//      |       |
//   node2    _node3_
//      |    |       |
//   node4  node5 node6
// * Option 2:
//   A list of nodes. All nodes have index set.
//   For any node with child nodes, the child nodes are referenced by indexes.
//   Example (node1 is the root node):
//   node1: index = 1, child_nodes = [2, 3]
//   node2: index = 2, child_nodes = [4]
//   node3: index = 3, child_nodes = [5, 6]
//   node4: index = 4, no child_nodes
//   node5: index = 5, no child_nodes
//   node6: index = 6, no child_nodes
//
// Converts the single node representation to the node list representation.
// Note that during the conversion, the input @root is modified to use index
// reference for child nodes. This is to avoid making a copy of @root.
//
// Returns error status if any of the following happens:
// * The original single node representation reference a node by index.
// * In a BranchNode, child_node is not set in some branches.
absl::StatusOr<std::vector<CompiledNode>> ToNodeListRepresentation(
    CompiledNode& root);

}  // namespace wfa_virtual_people

#endif  // SRC_MAIN_CC_WFA_VIRTUAL_PEOPLE_CORE_MODEL_MODEL_SERIALIZER_H_
