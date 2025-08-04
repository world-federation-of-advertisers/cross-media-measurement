// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.db.computation

import org.wfanet.measurement.common.numberAsLong
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.config.RoleInComputation
import org.wfanet.measurement.internal.duchy.protocol.TrusTee
import org.wfanet.measurement.internal.duchy.protocol.TrusTee.Stage
import org.wfanet.measurement.internal.duchy.protocol.TrusTee.Stage.COMPLETE
import org.wfanet.measurement.internal.duchy.protocol.TrusTee.Stage.COMPUTING
import org.wfanet.measurement.internal.duchy.protocol.TrusTee.Stage.INITIALIZED
import org.wfanet.measurement.internal.duchy.protocol.TrusTee.Stage.STAGE_UNSPECIFIED
import org.wfanet.measurement.internal.duchy.protocol.TrusTee.Stage.UNRECOGNIZED
import org.wfanet.measurement.internal.duchy.protocol.TrusTee.Stage.WAIT_TO_START

object TrusTeeProtocol {
  object EnumStages : ProtocolStageEnumHelper<Stage> {
    override val validInitialStages = setOf(INITIALIZED)
    override val validTerminalStages = setOf(COMPLETE)

    override val validSuccessors =
      mapOf(
          INITIALIZED to setOf(WAIT_TO_START),
          WAIT_TO_START to setOf(COMPUTING),
          COMPUTING to setOf(COMPLETE),
        )
        .withDefault { setOf() }

    override fun longToEnum(value: Long): Stage {
      // forNumber() returns null for unrecognized enum values for the proto.
      return Stage.forNumber(value.toInt()) ?: UNRECOGNIZED
    }

    override fun enumToLong(value: TrusTee.Stage): Long {
      return value.numberAsLong
    }

    /** Translates [TrusTee.Stage] s into [ComputationStageDetails]. */
    object Details :
      ProtocolStageDetails<Stage, ComputationStageDetails, TrusTee.ComputationDetails> {

      override fun validateRoleForStage(
        stage: Stage,
        details: TrusTee.ComputationDetails,
      ): Boolean {
        return details.role == RoleInComputation.AGGREGATOR
      }

      override fun afterTransitionForStage(stage: Stage): AfterTransition {
        return when (stage) {
          COMPUTING -> AfterTransition.ADD_UNCLAIMED_TO_QUEUE
          WAIT_TO_START -> AfterTransition.DO_NOT_ADD_TO_QUEUE
          COMPLETE -> error("Computation is in a terminal stage")
          UNRECOGNIZED,
          STAGE_UNSPECIFIED,
          INITIALIZED -> error("Cannot make transition function to stage $stage")
        }
      }

      override fun outputBlobNumbersForStage(
        stage: Stage,
        computationDetails: TrusTee.ComputationDetails,
      ): Int {
        return when (stage) {
          WAIT_TO_START,
          COMPUTING -> 0
          COMPLETE -> error("Computation is in a terminal stage")
          UNRECOGNIZED,
          STAGE_UNSPECIFIED,
          INITIALIZED -> error("Cannot make transition function to stage $stage")
        }
      }

      override fun detailsFor(
        stage: Stage,
        computationDetails: TrusTee.ComputationDetails,
      ): ComputationStageDetails = ComputationStageDetails.getDefaultInstance()

      override fun parseDetails(bytes: ByteArray): ComputationStageDetails =
        ComputationStageDetails.getDefaultInstance()
    }
  }

  /**
   * Implementation of [ProtocolStageEnumHelper] for [TrusTee.Stage] wrapped in a
   * [ComputationStage].
   */
  object ComputationStages : ProtocolStageEnumHelper<ComputationStage> {
    override val validInitialStages = EnumStages.validInitialStages.toSetOfComputationStages()
    override val validTerminalStages = EnumStages.validTerminalStages.toSetOfComputationStages()

    override val validSuccessors =
      EnumStages.validSuccessors
        .map { it.key.toProtocolStage() to it.value.toSetOfComputationStages() }
        .toMap()

    override fun enumToLong(value: ComputationStage): Long = EnumStages.enumToLong(value.trusTee)

    override fun longToEnum(value: Long): ComputationStage =
      EnumStages.longToEnum(value).toProtocolStage()

    /**
     * Translates [TrusTee.Stage] s wrapped in a [ComputationStage] into [ComputationStageDetails].
     */
    object Details :
      ProtocolStageDetails<ComputationStage, ComputationStageDetails, ComputationDetails> {
      override fun validateRoleForStage(
        stage: ComputationStage,
        details: ComputationDetails,
      ): Boolean {
        return EnumStages.Details.validateRoleForStage(stage.trusTee, details.trusTee)
      }

      override fun afterTransitionForStage(stage: ComputationStage): AfterTransition {
        return EnumStages.Details.afterTransitionForStage(stage.trusTee)
      }

      override fun outputBlobNumbersForStage(
        stage: ComputationStage,
        computationDetails: ComputationDetails,
      ): Int {
        return EnumStages.Details.outputBlobNumbersForStage(
          stage.trusTee,
          computationDetails.trusTee,
        )
      }

      override fun detailsFor(
        stage: ComputationStage,
        computationDetails: ComputationDetails,
      ): ComputationStageDetails =
        EnumStages.Details.detailsFor(stage.trusTee, computationDetails.trusTee)

      override fun parseDetails(bytes: ByteArray): ComputationStageDetails =
        EnumStages.Details.parseDetails(bytes)
    }
  }
}

private fun Set<Stage>.toSetOfComputationStages() = this.map { it.toProtocolStage() }.toSet()
