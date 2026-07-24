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

package org.wfanet.virtualpeople.core.labeler

import com.google.protobuf.TextFormat
import org.wfanet.virtualpeople.common.CompiledNode
import org.wfanet.virtualpeople.common.LabelerEvent
import org.wfanet.virtualpeople.common.LabelerInput
import org.wfanet.virtualpeople.common.LabelerOutput
import org.wfanet.virtualpeople.common.UserInfo
import org.wfanet.virtualpeople.common.labelerEvent
import org.wfanet.virtualpeople.common.labelerOutput
import org.wfanet.virtualpeople.core.common.Hashing
import org.wfanet.virtualpeople.core.model.ModelNode

enum class LabelingMode {
  FULL,
  POOL_IDENTITY,
}

class Labeler private constructor(private val rootNode: ModelNode) {

  /**
   * Apply the model to generate the labels. Invalid inputs will result in an error.
   *
   * Thread safety: a single [Labeler] instance may be called from multiple threads concurrently —
   * the model is immutable. Each [label] call uses its own mutable event builder internally.
   */
  fun label(input: LabelerInput): LabelerOutput = label(input, LabelingMode.FULL)

  /**
   * Apply the model with a specific labeling mode.
   *
   * In [LabelingMode.POOL_IDENTITY] mode, RankedPopulationNode leaves emit PoolAssignment entries
   * instead of assigning VIDs.
   */
  fun label(input: LabelerInput, mode: LabelingMode): LabelerOutput {
    val eventBuilder = labelerEvent { labelerInput = input }.toBuilder()
    setFingerprints(eventBuilder)

    if (mode == LabelingMode.POOL_IDENTITY) {
      eventBuilder.poolIdentityMode = true
    }

    rootNode.apply(eventBuilder)

    return labelerOutput {
      people += eventBuilder.virtualPersonActivitiesList
      poolAssignments += eventBuilder.poolAssignmentsList
      if (input.enableDebugTrace) {
        serializedDebugTrace = TextFormat.printer().printToString(eventBuilder.build())
      }
    }
  }

  companion object {
    /**
     * Always use Labeler::Build to get a Labeler object. Users should never call the constructor
     * directly.
     *
     * ```
     * There are 3 ways to represent a full model:
     * - Option 1:
     *   A single root node, with all the other nodes in the model tree attached
     *   directly to their parent nodes. Example (node1 is the root node):
     *         _node1_
     *       |        |
     *    node2     _node3_
     *      |     |        |
     *   node4   node5  node6
     * - Option 2:
     *   A list of nodes. All nodes except the root node must have index set.
     *   For any node with child nodes, the child nodes are referenced by indexes.
     *   Example (node1 is the root node):
     *   node1: index = null, child_nodes = [2, 3]
     *   node2: index = 2, child_nodes = [4]
     *   node3: index = 3, child_nodes = [5, 6]
     *   node4: index = 4, child_nodes = []
     *   node5: index = 5, child_nodes = []
     *   node6: index = 6, child_nodes = []
     * - Option 3:
     *   Mix of the above 2. Some nodes are referenced directly, while others are
     *   referenced by indexes. For any node referenced by index, an entry must be
     *   included in @nodes, with the index field set.
     *   Example (node1 is the root node):
     *   node1:
     *         _node1_
     *       |        |
     *       2     _node3_
     *           |        |
     *           5        6
     *   node2: index = 2
     *         node2
     *          |
     *        node4
     *   node5: index = 5
     *   node6: index = 6
     * ```
     *
     * Build the model with the @root node. Handles option 1 above.
     *
     * All the other nodes are referenced directly in branch_node.branches.node of the parent nodes.
     * Any index or node_index field is ignored.
     */
    @JvmStatic
    fun build(root: CompiledNode): Labeler {
      return Labeler(ModelNode.build(root))
    }

    /**
     * Build the model with all the [nodes]. Handles option 2 and 3 above.
     *
     * Nodes are allowed to be referenced by branch_node.branches.node_index.
     *
     * For CompiledNodes in [nodes], only the root node is allowed to not have index set.
     *
     * [nodes] must be sorted in the order that any child node is prior to its parent node.
     */
    @JvmStatic
    fun build(nodes: List<CompiledNode>): Labeler {
      var root: ModelNode? = null
      val nodeRefs = mutableMapOf<Int, ModelNode>()
      nodes.forEach { nodeConfig ->
        if (root != null) {
          error("No node is allowed after the root node.")
        }
        if (nodeConfig.hasIndex()) {
          if (nodeRefs.containsKey(nodeConfig.index)) {
            error("Duplicated indexes: ${nodeConfig.index}")
          }
          nodeRefs[nodeConfig.index] = ModelNode.build(nodeConfig, nodeRefs)
        } else {
          root = ModelNode.build(nodeConfig, nodeRefs)
        }
      }

      if (root == null) {
        if (nodeRefs.isEmpty()) {
          // This should never happen.
          error("Cannot find root node.")
        }
        if (nodeRefs.size > 1) {
          // We expect only 1 node in the node_refs map, which is the root node.
          error("Only 1 root node is expected in the node_refs map")
        }
        val entry = nodeRefs.entries.first()
        root = entry.value
        nodeRefs.remove(entry.key)
      }
      if (nodeRefs.isNotEmpty()) {
        error("Some nodes are not in the model tree.")
      }

      // root is guaranteed to be not null at this point.
      return Labeler(root!!)
    }

    private fun setUserInfoFingerprint(userInfo: UserInfo.Builder) {
      if (userInfo.hasUserId()) {
        userInfo.userIdFingerprint = Hashing.hashFingerprint64Long(userInfo.userId)
      }
    }

    private fun setFingerprints(eventBuilder: LabelerEvent.Builder) {
      val labelerInputBuilder = eventBuilder.labelerInputBuilder
      if (labelerInputBuilder.hasEventId()) {
        val eventIdFingerprint = Hashing.hashFingerprint64Long(labelerInputBuilder.eventId.id)
        labelerInputBuilder.eventIdBuilder.idFingerprint = eventIdFingerprint
        eventBuilder.actingFingerprint = eventIdFingerprint
      }

      if (!labelerInputBuilder.hasProfileInfo()) return

      val profileInfoBuilder = labelerInputBuilder.profileInfoBuilder
      if (profileInfoBuilder.hasEmailUserInfo()) {
        setUserInfoFingerprint(profileInfoBuilder.emailUserInfoBuilder)
      }
      if (profileInfoBuilder.hasPhoneUserInfo()) {
        setUserInfoFingerprint(profileInfoBuilder.phoneUserInfoBuilder)
      }
      if (profileInfoBuilder.hasLoggedInIdUserInfo()) {
        setUserInfoFingerprint(profileInfoBuilder.loggedInIdUserInfoBuilder)
      }
      if (profileInfoBuilder.hasLoggedOutIdUserInfo()) {
        setUserInfoFingerprint(profileInfoBuilder.loggedOutIdUserInfoBuilder)
      }
      if (profileInfoBuilder.hasProprietaryIdSpace1UserInfo()) {
        setUserInfoFingerprint(profileInfoBuilder.proprietaryIdSpace1UserInfoBuilder)
      }
      if (profileInfoBuilder.hasProprietaryIdSpace2UserInfo()) {
        setUserInfoFingerprint(profileInfoBuilder.proprietaryIdSpace2UserInfoBuilder)
      }
      if (profileInfoBuilder.hasProprietaryIdSpace3UserInfo()) {
        setUserInfoFingerprint(profileInfoBuilder.proprietaryIdSpace3UserInfoBuilder)
      }
      if (profileInfoBuilder.hasProprietaryIdSpace4UserInfo()) {
        setUserInfoFingerprint(profileInfoBuilder.proprietaryIdSpace4UserInfoBuilder)
      }
      if (profileInfoBuilder.hasProprietaryIdSpace5UserInfo()) {
        setUserInfoFingerprint(profileInfoBuilder.proprietaryIdSpace5UserInfoBuilder)
      }
      if (profileInfoBuilder.hasProprietaryIdSpace6UserInfo()) {
        setUserInfoFingerprint(profileInfoBuilder.proprietaryIdSpace6UserInfoBuilder)
      }
      if (profileInfoBuilder.hasProprietaryIdSpace7UserInfo()) {
        setUserInfoFingerprint(profileInfoBuilder.proprietaryIdSpace7UserInfoBuilder)
      }
      if (profileInfoBuilder.hasProprietaryIdSpace8UserInfo()) {
        setUserInfoFingerprint(profileInfoBuilder.proprietaryIdSpace8UserInfoBuilder)
      }
      if (profileInfoBuilder.hasProprietaryIdSpace9UserInfo()) {
        setUserInfoFingerprint(profileInfoBuilder.proprietaryIdSpace9UserInfoBuilder)
      }
      if (profileInfoBuilder.hasProprietaryIdSpace10UserInfo()) {
        setUserInfoFingerprint(profileInfoBuilder.proprietaryIdSpace10UserInfoBuilder)
      }
    }
  }
}
