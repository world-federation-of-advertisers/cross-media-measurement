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

#include "wfa/virtual_people/core/model/model_serializer.h"

#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "common_cpp/macros/macros.h"
#include "wfa/virtual_people/common/model.pb.h"

namespace wfa_virtual_people {

namespace {

// Forward declaration.
absl::StatusOr<int64_t> AddChildren(int64_t next_index, CompiledNode& node,
                                    std::vector<CompiledNode>& node_list);

// Add all nodes in the subtree under @node to @node_list.
// @next_index is the node index to use for the next node added.
// Returns the updated next index.
absl::StatusOr<int64_t> AddToNodeList(int64_t next_index, CompiledNode& node,
                                      std::vector<CompiledNode>& node_list) {
  if (node.has_branch_node()) {
    // Add children nodes and replace with index.
    ASSIGN_OR_RETURN(next_index, AddChildren(next_index, node, node_list));
  }

  // Add current node.
  node.set_index(next_index);
  node_list.emplace_back(node);

  return next_index + 1;
}

// Add child nodes of a BranchNode to @node_list. Update the child node to
// use node index reference.
// @next_index is the node index to use for the next node added.
// Returns the updated next index.
absl::StatusOr<int64_t> AddChildren(int64_t next_index, CompiledNode& node,
                                    std::vector<CompiledNode>& node_list) {
  if (!node.has_branch_node()) {
    return absl::InvalidArgumentError(
        absl::StrCat("AddChildren is called but this is not a branch node. ",
                     node.DebugString()));
  }

  BranchNode* branch_node = node.mutable_branch_node();
  for (BranchNode::Branch& branch : *branch_node->mutable_branches()) {
    if (branch.has_node_index()) {
      return absl::InvalidArgumentError(
          absl::StrCat("Single node representation shouldn't use node_index. ",
                       branch.DebugString()));
    }

    if (!branch.has_node()) {
      return absl::InvalidArgumentError(
          absl::StrCat("branch child_node is not set. ", branch.DebugString()));
    }

    // Add child node to list, then change child to index reference.
    ASSIGN_OR_RETURN(
        next_index,
        AddToNodeList(next_index, *branch.mutable_node(), node_list));
    // The returned value of AddToNodeList = child node index + 1.
    branch.set_node_index(next_index - 1);
  }

  return next_index;
}

}  // namespace

absl::StatusOr<std::vector<CompiledNode>> ToNodeListRepresentation(
    CompiledNode& root) {
  std::vector<CompiledNode> node_list;
  int64_t next_index = 0;
  absl::StatusOr<int64_t> res = AddToNodeList(next_index, root, node_list);
  if (res.ok()) {
    return node_list;
  }
  return res.status();
}

}  // namespace wfa_virtual_people
