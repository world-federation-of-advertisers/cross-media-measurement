// Copyright 2020 The Cross-Media Measurement Authors
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
import org.wfanet.measurement.internal.duchy.config.LiquidLegionsV2SetupConfig.RoleInComputation
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.COMPLETE
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.CONFIRMATION_PHASE
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_THREE
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_TWO
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.INITIALIZATION_PHASE
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.SETUP_PHASE
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.UNRECOGNIZED
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.WAIT_REQUISITIONS_AND_KEY_SET
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.WAIT_SETUP_PHASE_INPUTS
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.WAIT_TO_START

/**
 * Helper classes for working with stages of the Liquid Legions Sketch Aggregation V2 MPC defined in
 * [LiquidLegionsSketchAggregationV2.Stage].
 *
 * The [LiquidLegionsSketchAggregationV2.Stage] is one of the computation protocols defined in the
 * [ComputationStage] proto used in the storage layer API. There are helper objects for both the raw
 * enum values and those enum value wrapped in the proto message. Typically the right helper to use
 * is [ComputationStages] as it is the API level abstraction. [EnumStages] is visible in case it is
 * needed.
 *
 * [EnumStages.Details] is a helper to create [ComputationStageDetails] from
 * [LiquidLegionsSketchAggregationV2.Stage] enum values. [ComputationStages.Details] is a helper to
 * create [ComputationStageDetails] from [ComputationStage] protos wrapping a
 * [LiquidLegionsSketchAggregationV2.Stage] enum values.
 */
object LiquidLegionsSketchAggregationV2Protocol {
  /** Implementation of [ProtocolStageEnumHelper] for [LiquidLegionsSketchAggregationV2.Stage]. */
  object EnumStages : ProtocolStageEnumHelper<LiquidLegionsSketchAggregationV2.Stage> {
    override val validInitialStages = setOf(INITIALIZATION_PHASE)
    override val validTerminalStages = setOf(COMPLETE)

    override val validSuccessors =
      mapOf(
          INITIALIZATION_PHASE to setOf(WAIT_REQUISITIONS_AND_KEY_SET),
          WAIT_REQUISITIONS_AND_KEY_SET to setOf(CONFIRMATION_PHASE),
          CONFIRMATION_PHASE to setOf(WAIT_TO_START, WAIT_SETUP_PHASE_INPUTS),
          WAIT_TO_START to setOf(SETUP_PHASE),
          WAIT_SETUP_PHASE_INPUTS to setOf(SETUP_PHASE),
          SETUP_PHASE to setOf(WAIT_EXECUTION_PHASE_ONE_INPUTS),
          WAIT_EXECUTION_PHASE_ONE_INPUTS to setOf(EXECUTION_PHASE_ONE),
          EXECUTION_PHASE_ONE to setOf(WAIT_EXECUTION_PHASE_TWO_INPUTS),
          WAIT_EXECUTION_PHASE_TWO_INPUTS to setOf(EXECUTION_PHASE_TWO),
          EXECUTION_PHASE_TWO to setOf(WAIT_EXECUTION_PHASE_THREE_INPUTS),
          WAIT_EXECUTION_PHASE_THREE_INPUTS to setOf(EXECUTION_PHASE_THREE),
          EXECUTION_PHASE_THREE to setOf()
        )
        .withDefault { setOf() }

    override fun enumToLong(value: LiquidLegionsSketchAggregationV2.Stage): Long {
      return value.numberAsLong
    }

    override fun longToEnum(value: Long): LiquidLegionsSketchAggregationV2.Stage {
      // forNumber() returns null for unrecognized enum values for the proto.
      return LiquidLegionsSketchAggregationV2.Stage.forNumber(value.toInt()) ?: UNRECOGNIZED
    }

    /** Translates [LiquidLegionsSketchAggregationV2.Stage] s into [ComputationStageDetails]. */
    object Details :
      ProtocolStageDetails<
        LiquidLegionsSketchAggregationV2.Stage,
        ComputationStageDetails,
        LiquidLegionsSketchAggregationV2.ComputationDetails
      > {

      override fun validateRoleForStage(
        stage: LiquidLegionsSketchAggregationV2.Stage,
        details: LiquidLegionsSketchAggregationV2.ComputationDetails
      ): Boolean {
        return when (stage) {
          WAIT_TO_START -> details.role == RoleInComputation.NON_AGGREGATOR
          WAIT_SETUP_PHASE_INPUTS -> details.role == RoleInComputation.AGGREGATOR
          else -> true /* Stage can be executed at either primary or non-primary */
        }
      }

      override fun afterTransitionForStage(
        stage: LiquidLegionsSketchAggregationV2.Stage
      ): AfterTransition {
        return when (stage) {
          // Stages of computation mapping some number of inputs to single output.
          CONFIRMATION_PHASE,
          SETUP_PHASE,
          EXECUTION_PHASE_ONE,
          EXECUTION_PHASE_TWO,
          EXECUTION_PHASE_THREE -> AfterTransition.ADD_UNCLAIMED_TO_QUEUE
          WAIT_REQUISITIONS_AND_KEY_SET,
          WAIT_TO_START,
          WAIT_SETUP_PHASE_INPUTS,
          WAIT_EXECUTION_PHASE_ONE_INPUTS,
          WAIT_EXECUTION_PHASE_TWO_INPUTS,
          WAIT_EXECUTION_PHASE_THREE_INPUTS -> AfterTransition.DO_NOT_ADD_TO_QUEUE
          COMPLETE -> error("Computation should be ended with call to endComputation(...)")
          // Stages that we can't transition to ever.
          UNRECOGNIZED,
          LiquidLegionsSketchAggregationV2.Stage.STAGE_UNKNOWN,
          INITIALIZATION_PHASE -> error("Cannot make transition function to stage $stage")
        }
      }

      override fun outputBlobNumbersForStage(
        stage: LiquidLegionsSketchAggregationV2.Stage,
        computationDetails: LiquidLegionsSketchAggregationV2.ComputationDetails
      ): Int {
        return when (stage) {
          WAIT_REQUISITIONS_AND_KEY_SET,
          CONFIRMATION_PHASE,
          WAIT_TO_START -> 0
          WAIT_EXECUTION_PHASE_ONE_INPUTS,
          WAIT_EXECUTION_PHASE_TWO_INPUTS,
          WAIT_EXECUTION_PHASE_THREE_INPUTS,
          SETUP_PHASE,
          EXECUTION_PHASE_ONE,
          EXECUTION_PHASE_TWO,
          EXECUTION_PHASE_THREE ->
            // The output is the intermediate computation result either received from another duchy
            // or computed locally.
            1
          WAIT_SETUP_PHASE_INPUTS ->
            // The output contains otherDuchiesInComputation sketches from the other duchies.
            computationDetails.participantCount - 1
          // Mill have nothing to do for this stage.
          COMPLETE -> error("Computation should be ended with call to endComputation(...)")
          // Stages that we can't transition to ever.
          UNRECOGNIZED,
          LiquidLegionsSketchAggregationV2.Stage.STAGE_UNKNOWN,
          INITIALIZATION_PHASE -> error("Cannot make transition function to stage $stage")
        }
      }

      override fun detailsFor(
        stage: LiquidLegionsSketchAggregationV2.Stage,
        computationDetails: LiquidLegionsSketchAggregationV2.ComputationDetails
      ): ComputationStageDetails {
        return when (stage) {
          WAIT_SETUP_PHASE_INPUTS ->
            ComputationStageDetails.newBuilder()
              .apply {
                liquidLegionsV2Builder.waitSetupPhaseInputsDetailsBuilder.apply {
                  val participants = computationDetails.participantList
                  val nonAggregators = participants.subList(0, participants.size - 1)
                  putAllExternalDuchyLocalBlobId(
                    nonAggregators
                      .mapIndexed { idx, duchy -> duchy.duchyId to idx.toLong() }
                      .toMap()
                  )
                }
              }
              .build()
          else -> ComputationStageDetails.getDefaultInstance()
        }
      }

      override fun parseDetails(bytes: ByteArray): ComputationStageDetails =
        ComputationStageDetails.parseFrom(bytes)
    }
  }

  /**
   * Implementation of [ProtocolStageEnumHelper] for [LiquidLegionsSketchAggregationV2.Stage]
   * wrapped in a [ComputationStage].
   */
  object ComputationStages : ProtocolStageEnumHelper<ComputationStage> {
    override val validInitialStages = EnumStages.validInitialStages.toSetOfComputationStages()
    override val validTerminalStages = EnumStages.validTerminalStages.toSetOfComputationStages()

    override val validSuccessors =
      EnumStages.validSuccessors
        .map { it.key.toProtocolStage() to it.value.toSetOfComputationStages() }
        .toMap()

    override fun enumToLong(value: ComputationStage): Long =
      EnumStages.enumToLong(value.liquidLegionsSketchAggregationV2)

    override fun longToEnum(value: Long): ComputationStage =
      EnumStages.longToEnum(value).toProtocolStage()

    /**
     * Translates [LiquidLegionsSketchAggregationV2.Stage] s wrapped in a [ComputationStage] into
     * [ComputationStageDetails].
     */
    object Details :
      ProtocolStageDetails<ComputationStage, ComputationStageDetails, ComputationDetails> {
      override fun validateRoleForStage(
        stage: ComputationStage,
        details: ComputationDetails
      ): Boolean {
        return EnumStages.Details.validateRoleForStage(
          stage.liquidLegionsSketchAggregationV2,
          details.liquidLegionsV2
        )
      }

      override fun afterTransitionForStage(stage: ComputationStage): AfterTransition {
        return EnumStages.Details.afterTransitionForStage(stage.liquidLegionsSketchAggregationV2)
      }

      override fun outputBlobNumbersForStage(
        stage: ComputationStage,
        computationDetails: ComputationDetails
      ): Int {
        return EnumStages.Details.outputBlobNumbersForStage(
          stage.liquidLegionsSketchAggregationV2,
          computationDetails.liquidLegionsV2
        )
      }

      override fun detailsFor(
        stage: ComputationStage,
        computationDetails: ComputationDetails
      ): ComputationStageDetails =
        EnumStages.Details.detailsFor(
          stage.liquidLegionsSketchAggregationV2,
          computationDetails.liquidLegionsV2
        )

      override fun parseDetails(bytes: ByteArray): ComputationStageDetails =
        EnumStages.Details.parseDetails(bytes)
    }
  }
}

private fun Set<LiquidLegionsSketchAggregationV2.Stage>.toSetOfComputationStages() =
  this.map { it.toProtocolStage() }.toSet()
