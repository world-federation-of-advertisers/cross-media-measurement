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
import org.wfanet.virtualpeople.common.CompiledNode.TypeCase
import org.wfanet.virtualpeople.common.LabelerEvent

/**
 * The Kotlin implementation of [CompiledNode] proto. Each node in the model tree will be converted
 * to a [ModelNode]. Except for debugging purposes, this should be used by VID Labeler only.
 *
 * This is a base class for all model node classes. Never add any behavior here. Only fields
 * required for all model node classes should be added here.
 */
sealed class ModelNode(nodeConfig: CompiledNode) {
  private val name: String
  private val fromModelBuilderConfig: Boolean

  init {
    name = nodeConfig.name
    fromModelBuilderConfig = nodeConfig.debugInfo.directlyFromModelBuilderConfig
  }

  /** Applies the node to the [event]. */
  abstract fun apply(event: LabelerEvent.Builder)

  companion object {
    /**
     * Always use [ModelNode]::Build to get a [ModelNode] object. Users should never call the
     * factory function or constructor of the derived class directly.
     *
     * @param [nodeRefs] the mapping from indexes to the [ModelNode] objects, which should contain
     *   the child nodes referenced by indexes. Throws an error if any child node referenced by
     *   index is not found in [nodeRefs].
     */
    fun build(config: CompiledNode, nodeRefs: MutableMap<Int, ModelNode>): ModelNode {
      return if (config.typeCase == TypeCase.BRANCH_NODE) {
        BranchNodeImpl.build(config, nodeRefs)
      } else {
        build(config)
      }
    }

    /** Build nodes with no index references in the sub-tree. */
    fun build(config: CompiledNode): ModelNode {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      return when (config.typeCase) {
        TypeCase.BRANCH_NODE -> BranchNodeImpl.build(config, mutableMapOf())
        TypeCase.STOP_NODE -> StopNodeImpl.build(config)
        TypeCase.POPULATION_NODE -> PopulationNodeImpl.build(config)
        TypeCase.RANKED_POPULATION_NODE -> RankedPopulationNodeImpl.build(config)
        TypeCase.TYPE_NOT_SET -> error("Node type is not set.")
      }
    }
  }
}
