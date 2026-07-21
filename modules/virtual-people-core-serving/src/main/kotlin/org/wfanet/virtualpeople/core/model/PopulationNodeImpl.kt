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

import org.wfanet.virtualpeople.common.CompiledNode
import org.wfanet.virtualpeople.common.LabelerEvent
import org.wfanet.virtualpeople.common.PopulationNode.VirtualPersonPool
import org.wfanet.virtualpeople.common.VirtualPersonActivity
import org.wfanet.virtualpeople.core.model.utils.PopulationNodeHelper
import org.wfanet.virtualpeople.core.model.utils.VirtualPeopleSelector

/** The implementation of the CompiledNode with population_node set. */
internal class PopulationNodeImpl
private constructor(
  nodeConfig: CompiledNode,
  private val virtualPeopleSelector: VirtualPeopleSelector?,
  private val randomSeed: String
) : ModelNode(nodeConfig) {

  /**
   * Applies the node to the [event].
   *
   * When Apply is called, exactly one id will be selected from the pools in population_node, and
   * assigned to virtual_person_activities[0] in [event].
   */
  override fun apply(event: LabelerEvent.Builder) {
    // Pass-1 (pool-identity) mode: a plain PopulationNode has no ranked pool to
    // announce, so it produces no output. This lets a model mix ranked and
    // unranked leaves; events routing here are labeled normally in pass-2.
    if (event.poolIdentityMode) {
      return
    }

    // Creates a new virtual_person_activities in the event and writes the
    // virtual person id and label. No virtual_person_activity should be added
    // by previous nodes.
    if (event.virtualPersonActivitiesCount > 0) {
      error("virtual_person_activities should only be created in leaf nodes.")
    }

    val virtualPeopleActivity = VirtualPersonActivity.newBuilder()
    /** Only populate virtual_person_id when the pools is not an empty population pool. */
    if (virtualPeopleSelector != null) {
      val seed = PopulationNodeHelper.computeVidSeed(randomSeed, event.actingFingerprint)
      /**
       * virtual_person_id is uint64 in the proto, we need to use the signed value to set it in
       * kotlin.
       */
      virtualPeopleActivity.virtualPersonId =
        virtualPeopleSelector.getVirtualPersonId(seed).toLong()
    }

    /** Write to virtual_person_activity.label from quantum labels. */
    if (event.hasQuantumLabels()) {
      val seedSuffix =
        if (virtualPeopleActivity.hasVirtualPersonId()) {
          virtualPeopleActivity.virtualPersonId.toULong().toString()
        } else {
          event.actingFingerprint.toULong().toString()
        }
      event.quantumLabels.quantumLabelsList.forEach {
        PopulationNodeHelper.collapseQuantumLabel(
          it,
          seedSuffix,
          virtualPeopleActivity.labelBuilder
        )
      }
    }
    /** Write to virtual_person_activity.label from classic label. */
    if (event.hasLabel()) {
      virtualPeopleActivity.labelBuilder.mergeFrom(event.label)
    }

    event.addVirtualPersonActivities(virtualPeopleActivity)
  }

  companion object {
    /** Check if the [pools] represent an empty population pool. */
    private fun isEmptyPopulationPool(pools: List<VirtualPersonPool>): Boolean {
      if (pools.size != 1) {
        return false
      }
      return pools[0].populationOffset == 0L && pools[0].totalPopulation == 0L
    }

    /**
     * Always use ModelNode::Build to get a ModelNode object. Users should never call the factory
     * function or constructor of the derived class directly.
     *
     * Throws an error if any of the following happens:
     * 1. [nodeConfig].population_node is not set.
     * 2. The total population of the pools is 0 and the pools do not represent an empty population
     *    pool. An empty population pool contains only 1 [VirtualPersonPool], and its
     *    population_offset and total_population are 0.
     */
    fun build(nodeConfig: CompiledNode): PopulationNodeImpl {
      if (!nodeConfig.hasPopulationNode()) {
        error("This is not a population node.")
      }
      val virtualPeopleSelector: VirtualPeopleSelector? =
        if (isEmptyPopulationPool(nodeConfig.populationNode.poolsList)) null
        else VirtualPeopleSelector.build(nodeConfig.populationNode.poolsList)
      return PopulationNodeImpl(
        nodeConfig,
        virtualPeopleSelector,
        nodeConfig.populationNode.randomSeed
      )
    }
  }
}
