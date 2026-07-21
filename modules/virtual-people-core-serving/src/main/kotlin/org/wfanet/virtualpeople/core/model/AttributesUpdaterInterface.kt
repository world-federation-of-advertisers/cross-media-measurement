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

import org.wfanet.virtualpeople.common.BranchNode.AttributesUpdater
import org.wfanet.virtualpeople.common.BranchNode.AttributesUpdater.UpdateCase
import org.wfanet.virtualpeople.common.LabelerEvent

/** The Kotlin implementation of [AttributesUpdater] protobuf. */
sealed interface AttributesUpdaterInterface {

  /**
   * Applies the [AttributesUpdater] to the [event].
   *
   * In general, there are 2 steps:
   * 1. Find the attributes to be merged into [event], by conditions matching and probabilities.
   * 2. Merge the attributes into [event].
   */
  fun update(event: LabelerEvent.Builder)

  companion object {
    /**
     * Always use [build] to get an [AttributesUpdaterInterface] object. Users should not call the
     * factory method or the constructor of the derived classes directly.
     *
     * [nodeRefs] is the mapping from indexes to the [ModelNode] objects, which should contain the
     * child nodes referenced by indexes in the attached model trees.
     */
    fun build(
      config: AttributesUpdater,
      nodeRefs: MutableMap<Int, ModelNode>,
    ): AttributesUpdaterInterface {
      return if (config.updateCase == UpdateCase.UPDATE_TREE) {
        UpdateTreeImpl.build(config.updateTree, nodeRefs)
      } else {
        build(config)
      }
    }

    /**
     * Builds [AttributesUpdater] with no attached model tree, or the model tree is defined without
     * any child node referenced by index.
     */
    fun build(config: AttributesUpdater): AttributesUpdaterInterface {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      return when (config.updateCase) {
        UpdateCase.UPDATE_MATRIX -> UpdateMatrixImpl.build(config.updateMatrix)
        UpdateCase.SPARSE_UPDATE_MATRIX -> SparseUpdateMatrixImpl.build(config.sparseUpdateMatrix)
        UpdateCase.CONDITIONAL_MERGE -> ConditionalMergeImpl.build(config.conditionalMerge)
        UpdateCase.CONDITIONAL_ASSIGNMENT ->
          ConditionalAssignmentImpl.build(config.conditionalAssignment)
        UpdateCase.UPDATE_TREE -> UpdateTreeImpl.build(config.updateTree, mutableMapOf())
        UpdateCase.GEOMETRIC_SHREDDER -> GeometricShredderImpl.build(config.geometricShredder)
        UpdateCase.UPDATE_NOT_SET -> error("config.update is not set.")
      }
    }
  }
}

enum class PassThroughNonMatches {
  NO,
  YES,
}
