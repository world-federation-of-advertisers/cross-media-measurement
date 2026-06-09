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

package org.wfanet.virtualpeople.core.model

import org.wfanet.virtualpeople.common.*
import org.wfanet.virtualpeople.common.BranchNode.Branch.SelectByCase
import org.wfanet.virtualpeople.common.fieldfilter.utils.setValueToProtoBuilder
import org.wfanet.virtualpeople.core.model.utils.DistributedConsistentHashing
import org.wfanet.virtualpeople.core.model.utils.DistributionChoice
import org.wfanet.virtualpeople.core.model.utils.FieldFiltersMatcher

/**
 * The implementation of the CompiledNode with branch_node set.
 *
 * @param nodeConfig the compiledNode used to build the branch node. The field branch_node must be
 *   set.
 * @param childNodes include the child nodes in all the branches, in the same order as the branches
 *   in nodeConfig.
 * @param hashing if chance is set in each branches in nodeConfig, hashing is set and used to select
 *   a child node with randomSeed when Apply is called.
 * @param randomSeed a seed used by the hashing.
 * @param matcher if condition is set in each branches in nodeConfig, matcher is set and used to
 *   select the first child node whose condition matches when Apply is called.
 * @param updaters if updates is set in nodeConfig, updaters is set. When calling Apply, entries of
 *   [updaters] is applied to the event in order. Each entry of updaters updates the value of some
 *   fields in the event.
 * @param multiplicity if multiplicity is set in nodeConfig, multiplicity is set. When calling
 *   Apply, call ApplyMultiplicity.
 */
internal class BranchNodeImpl
private constructor(
  nodeConfig: CompiledNode,
  private val childNodes: List<ModelNode>,
  private val hashing: DistributedConsistentHashing?,
  private val randomSeed: String,
  private val matcher: FieldFiltersMatcher?,
  private val updaters: List<AttributesUpdaterInterface>,
  private val multiplicity: MultiplicityImpl?
) : ModelNode(nodeConfig) {

  private fun applyChild(event: LabelerEvent.Builder) {
    /**
     * The seed uses the string representation of actingFingerprint as an unsigned 64-bit integer
     */
    val selectedIndex =
      hashing?.hash("$randomSeed${event.actingFingerprint.toULong()}")
        ?: (matcher?.getFirstMatch(event) ?: error("No select options is set for the BranchNode."))
    if (selectedIndex == -1) {
      error("No condition matches the input event.")
    }

    if (selectedIndex < 0 || selectedIndex >= childNodes.size) {
      /** This should never happen. */
      error("The returned index is out of range.")
    }
    childNodes[selectedIndex].apply(event)
    return
  }

  private fun applyMultiplicity(event: LabelerEvent.Builder) {
    if (multiplicity == null) {
      error("ApplyMultiplicity is called with null multiplicity.")
    }
    val cloneCount = multiplicity.computeEventMultiplicity(event)
    if (cloneCount == 1) {
      /** Don't need to copy. Still need to set index. */
      val personIndex = 0
      setValueToProtoBuilder(event, multiplicity.personIndexFieldDescriptor(), personIndex)
      applyChild(event)
      return
    }

    /** Clone events. */
    val personIndexField = multiplicity.personIndexFieldDescriptor()
    val originalFingerprint = event.actingFingerprint.toULong()
    val clones: List<LabelerEvent.Builder> =
      (0 until cloneCount).map { personIndex ->
        val cloneFingerprint = multiplicity.getFingerprintForIndex(originalFingerprint, personIndex)
        val clonedEventBuilder =
          event.build().copy { actingFingerprint = cloneFingerprint.toLong() }.toBuilder()
        setValueToProtoBuilder(clonedEventBuilder, personIndexField, personIndex)
        clonedEventBuilder
      }

    clones.forEach { clone ->
      applyChild(clone)
      clone.virtualPersonActivitiesList.forEach { person ->
        event.addVirtualPersonActivities(person)
      }
      /**
       * Fold back pool assignments too. In pool-identity (pass-1) mode, leaf nodes emit pool
       * assignments instead of virtual person activities; without this, assignments from cloned
       * multiplicity events would be dropped.
       */
      clone.poolAssignmentsList.forEach { poolAssignment ->
        event.addPoolAssignments(poolAssignment)
      }
    }
    return
  }

  /**
   * Applies the node to the [event].
   *
   * Uses hashing or matcher to select one of childNodes, and apply the selected node to [event].
   */
  override fun apply(event: LabelerEvent.Builder) {
    if (multiplicity != null && updaters.isNotEmpty()) {
      error("BranchNode cannot have both updaters and multiplicity.")
    }
    if (multiplicity != null) {
      applyMultiplicity(event)
      return
    }
    updaters.forEach { it.update(event) }
    applyChild(event)
  }

  companion object {
    /**
     * Always use ModelNode::Build to get a ModelNode object. Users should never call the factory
     * function or constructor of the derived class directly.
     *
     * Throws an error if any of the following happens:
     * 1. [nodeConfig].branch_node is not set.
     * 2. [nodeConfig].branch_node.branches is empty.
     * 3. There is at least one of [nodeConfig].branch_node.branches, which has neither node_index
     *    nor node set.
     * 4. There is at least one of [nodeConfig].branch_node.branches, which has neither chance nor
     *    condition set.
     * 5. At least one of [nodeConfig].branch_node.branches has chance set, and at least one of
     *    [nodeConfig].branch_node.branches has condition set.
     * 6. When node_index is set in [nodeConfig].branch_node.branches. [nodeRefs] has no entry for
     *    this node_index.
     *
     * For any [nodeConfig].branch_node.branches with node_index set, the corresponding child node
     * is retrieved from [nodeRefs] using node_index as the key.
     */
    fun build(nodeConfig: CompiledNode, nodeRefs: MutableMap<Int, ModelNode>): BranchNodeImpl {
      if (!nodeConfig.hasBranchNode()) {
        error("This is not a branch node.")
      }
      val branchNode = nodeConfig.branchNode
      if (branchNode.branchesCount == 0) {
        error("BranchNode must have at least 1 branch.")
      }

      val childNodes =
        branchNode.branchesList.map { branch ->
          if (branch.hasNodeIndex()) {
            /**
             * The child node is referenced by node index.
             *
             * The node is removed from the [nodeRefs] once it is added as a childNode of other
             * node.
             */
            nodeRefs.remove(branch.nodeIndex)
              ?: error(
                "The ModelNode object of the child node index ${branch.nodeIndex} is not provided."
              )
          } else if (branch.hasNode()) {
            /** Create the ModelNode object and store. */
            ModelNode.build(branch.node, nodeRefs)
          } else {
            error("BranchNode must have one of node_index and node.")
          }
        }

      val selectByCase: SelectByCase = branchNode.getBranches(0).selectByCase

      /**
       * If all branchNode.branchesList have chance, use chance. If all branchNode.branchesList have
       * condition, use condition. Else return error.
       */
      branchNode.branchesList.forEach {
        if (it.selectByCase != selectByCase) {
          error("All branches should use the same select_by type.")
        }
      }

      var hashing: DistributedConsistentHashing? = null
      var matcher: FieldFiltersMatcher? = null
      when (selectByCase) {
        SelectByCase.CHANCE -> {
          val distribution =
            branchNode.branchesList.mapIndexed { index, branch ->
              DistributionChoice(index, branch.chance)
            }
          hashing = DistributedConsistentHashing(distribution)
        }
        SelectByCase.CONDITION -> {
          val filterConfigs = branchNode.branchesList.map { it.condition }
          matcher = FieldFiltersMatcher.build(filterConfigs)
        }
        SelectByCase.SELECTBY_NOT_SET -> error("BranchNode must have one of chance and condition.")
      }

      val updaters: List<AttributesUpdaterInterface> =
        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
        when (branchNode.actionCase) {
          BranchNode.ActionCase.UPDATES ->
            branchNode.updates.updatesList.map { AttributesUpdaterInterface.build(it) }
          BranchNode.ActionCase.MULTIPLICITY,
          BranchNode.ActionCase.ACTION_NOT_SET -> listOf()
        }

      val multiplicity: MultiplicityImpl? =
        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
        when (branchNode.actionCase) {
          BranchNode.ActionCase.MULTIPLICITY -> MultiplicityImpl.build(branchNode.multiplicity)
          BranchNode.ActionCase.UPDATES,
          BranchNode.ActionCase.ACTION_NOT_SET -> null
        }

      return BranchNodeImpl(
        nodeConfig,
        childNodes,
        hashing,
        branchNode.randomSeed,
        matcher,
        updaters,
        multiplicity
      )
    }
  }
}
