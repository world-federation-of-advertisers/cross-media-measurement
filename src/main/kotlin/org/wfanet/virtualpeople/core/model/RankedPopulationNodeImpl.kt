// Copyright 2026 The Cross-Media Measurement Authors
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
import org.wfanet.virtualpeople.common.RankedPopulationNode.UnrankedMode
import org.wfanet.virtualpeople.common.VirtualPersonActivity
import org.wfanet.virtualpeople.common.poolAssignment
import org.wfanet.virtualpeople.core.model.utils.Feistel
import org.wfanet.virtualpeople.core.model.utils.PopulationNodeHelper
import org.wfanet.virtualpeople.core.model.utils.jumpConsistentHash

/**
 * Implementation of CompiledNode with ranked_population_node set.
 *
 * Splits VID assignment into ranked (Feistel, collision-free) and unranked (hash-based) sub-ranges.
 * If a pre-computed rank is available and within [0, rankedSize), the Feistel path is used. If the
 * rank is >= rankedSize or no rank is provided, the event falls back to hash-based assignment per
 * the configured UnrankedMode.
 */
internal class RankedPopulationNodeImpl
private constructor(
  nodeConfig: CompiledNode,
  private val randomSeed: String,
  private val rankedSize: ULong,
  private val unrankedMode: UnrankedMode,
  private val poolOffset: ULong,
  private val poolSize: ULong,
) : ModelNode(nodeConfig) {

  override fun apply(event: LabelerEvent.Builder) {
    // Pass-1 mode: emit pool identity and return without assigning a VID.
    if (event.poolIdentityMode) {
      event.addPoolAssignments(
        poolAssignment {
          poolOffset = this@RankedPopulationNodeImpl.poolOffset.toLong()
          poolSize = this@RankedPopulationNodeImpl.poolSize.toLong()
          rankedSize = this@RankedPopulationNodeImpl.rankedSize.toLong()
        }
      )
      return
    }

    if (event.virtualPersonActivitiesCount > 0) {
      error("virtual_person_activities should only be created in leaf nodes.")
    }

    val activity = VirtualPersonActivity.newBuilder()

    val rankAssignments =
      if (event.hasLabelerInput()) event.labelerInput.rankAssignmentsList else emptyList()
    val rankAssignment = rankAssignments.firstOrNull { it.poolOffset.toULong() == poolOffset }

    if (rankAssignments.isNotEmpty() && rankAssignment == null) {
      error(
        "RankAssignment provided but none match pool_offset=$poolOffset. " +
          "Available: ${rankAssignments.map { it.poolOffset }}."
      )
    }

    val virtualPersonId =
      if (rankAssignment != null && rankAssignment.localRank.toULong() < rankedSize) {
        poolOffset + Feistel.permute(rankAssignment.localRank.toULong(), rankedSize, randomSeed)
      } else {
        val seed = PopulationNodeHelper.computeVidSeed(randomSeed, event.actingFingerprint)
        when (unrankedMode) {
          UnrankedMode.DISJOINT -> {
            val unrankedSize = poolSize - rankedSize
            check(unrankedSize > 0uL) {
              "DISJOINT mode with ranked_size == pool_size leaves no unranked space."
            }
            poolOffset + rankedSize + jumpConsistentHash(seed, unrankedSize.toInt()).toULong()
          }
          UnrankedMode.FULL_POOL ->
            poolOffset + jumpConsistentHash(seed, poolSize.toInt()).toULong()
          UnrankedMode.UNRANKED_MODE_UNSPECIFIED,
          UnrankedMode.UNRECOGNIZED -> error("UnrankedMode must be DISJOINT or FULL_POOL.")
        }
      }

    activity.virtualPersonId = virtualPersonId.toLong()

    if (event.hasQuantumLabels()) {
      val seedSuffix = virtualPersonId.toString()
      event.quantumLabels.quantumLabelsList.forEach {
        PopulationNodeHelper.collapseQuantumLabel(it, seedSuffix, activity.labelBuilder)
      }
    }
    if (event.hasLabel()) {
      activity.labelBuilder.mergeFrom(event.label)
    }

    event.addVirtualPersonActivities(activity)
  }

  companion object {
    fun build(nodeConfig: CompiledNode): RankedPopulationNodeImpl {
      if (!nodeConfig.hasRankedPopulationNode()) {
        error("This is not a ranked population node.")
      }
      val config = nodeConfig.rankedPopulationNode

      require(config.poolsCount == 1) {
        "RankedPopulationNode requires exactly one pool, got ${config.poolsCount}."
      }
      val pool = config.poolsList.single()
      val poolOffset = pool.populationOffset.toULong()
      val poolSize = pool.totalPopulation.toULong()

      require(poolSize > 0uL) { "RankedPopulationNode total pool size must be > 0." }

      val rankedSize = config.rankedSize.toULong()
      require(rankedSize <= poolSize) {
        "ranked_size ($rankedSize) must not exceed pool_size ($poolSize)."
      }

      return RankedPopulationNodeImpl(
        nodeConfig,
        config.randomSeed,
        rankedSize,
        config.unrankedMode,
        poolOffset,
        poolSize,
      )
    }
  }
}
