// Copyright 2020 The Measurement System Authors
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
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.Stage.COMPLETE
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.Stage.CONFIRM_REQUISITIONS_PHASE
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.Stage.FILTERING_PHASE
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.Stage.FREQUENCY_ESTIMATION_PHASE
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.Stage.REACH_ESTIMATION_PHASE
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.Stage.SETUP_PHASE
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.Stage.UNRECOGNIZED
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.Stage.WAIT_FILTERING_PHASE_INPUTS
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.Stage.WAIT_FREQUENCY_ESTIMATION_PHASE_INPUTS
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.Stage.WAIT_REACH_ESTIMATION_PHASE_INPUTS
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.Stage.WAIT_SETUP_PHASE_INPUTS
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.Stage.WAIT_TO_START

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
 * [LiquidLegionsSketchAggregationV2.Stage] enum values.
 * [ComputationStages.Details] is a helper to create [ComputationStageDetails] from
 * [ComputationStage] protos wrapping a [LiquidLegionsSketchAggregationV2.Stage] enum values.
 */
object LiquidLegionsSketchAggregationV2Protocol {
  /**
   * Implementation of [ProtocolStageEnumHelper] for [LiquidLegionsSketchAggregationV2.Stage].
   */
  object EnumStages : ProtocolStageEnumHelper<LiquidLegionsSketchAggregationV2.Stage> {
    override val validInitialStages = setOf(CONFIRM_REQUISITIONS_PHASE)
    override val validTerminalStages = setOf(COMPLETE)

    override val validSuccessors =
      mapOf(
        CONFIRM_REQUISITIONS_PHASE to setOf(
          WAIT_TO_START,
          WAIT_SETUP_PHASE_INPUTS
        ),
        WAIT_TO_START to setOf(SETUP_PHASE),
        WAIT_SETUP_PHASE_INPUTS to setOf(SETUP_PHASE),
        SETUP_PHASE to setOf(WAIT_REACH_ESTIMATION_PHASE_INPUTS),
        WAIT_REACH_ESTIMATION_PHASE_INPUTS to setOf(REACH_ESTIMATION_PHASE),
        REACH_ESTIMATION_PHASE to setOf(
          WAIT_FILTERING_PHASE_INPUTS
        ),
        WAIT_FILTERING_PHASE_INPUTS to setOf(FILTERING_PHASE),
        FILTERING_PHASE to setOf(WAIT_FREQUENCY_ESTIMATION_PHASE_INPUTS),
        WAIT_FREQUENCY_ESTIMATION_PHASE_INPUTS to setOf(
          FREQUENCY_ESTIMATION_PHASE
        ),
        FREQUENCY_ESTIMATION_PHASE to setOf()
      ).withDefault { setOf() }

    override fun enumToLong(value: LiquidLegionsSketchAggregationV2.Stage): Long {
      return value.numberAsLong
    }

    override fun longToEnum(value: Long): LiquidLegionsSketchAggregationV2.Stage {
      // forNumber() returns null for unrecognized enum values for the proto.
      return LiquidLegionsSketchAggregationV2.Stage.forNumber(value.toInt()) ?: UNRECOGNIZED
    }

    /** Translates [Stage]s into [ComputationStageDetails]. */
    class Details(val otherDuchies: List<String>) :
      ProtocolStageDetails<
        LiquidLegionsSketchAggregationV2.Stage,
        ComputationStageDetails,
        LiquidLegionsSketchAggregationV2.ComputationDetails> {
      override fun validateRoleForStage(
        stage: LiquidLegionsSketchAggregationV2.Stage,
        details: LiquidLegionsSketchAggregationV2.ComputationDetails
      ): Boolean {
        TODO("Not yet implemented")
      }

      override fun afterTransitionForStage(stage: LiquidLegionsSketchAggregationV2.Stage):
        AfterTransition {
          TODO("Not yet implemented")
        }

      override fun outputBlobNumbersForStage(stage: LiquidLegionsSketchAggregationV2.Stage): Int {
        TODO("Not yet implemented")
      }

      override fun detailsFor(stage: LiquidLegionsSketchAggregationV2.Stage):
        ComputationStageDetails {
          return when (stage) {
            WAIT_SETUP_PHASE_INPUTS ->
              ComputationStageDetails.newBuilder().apply {
                liquidLegionsV2Builder.waitSetupPhaseInputsDetailsBuilder.apply {
                  // The WAIT_SKETCHES stage has exactly one input which is the noised sketches from
                  // the primary duchy running the wait operation. It is not an output of the stage
                  // because it is a result of a locally running stage.
                  putAllExternalDuchyLocalBlobId(
                    otherDuchies.mapIndexed { idx, duchy -> duchy to (idx + 1).toLong() }.toMap()
                  )
                }
              }.build()
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
    override val validInitialStages =
      EnumStages.validInitialStages.toSetOfComputationStages()
    override val validTerminalStages =
      EnumStages.validTerminalStages.toSetOfComputationStages()

    override val validSuccessors =
      EnumStages.validSuccessors
        .map { it.key.toProtocolStage() to it.value.toSetOfComputationStages() }.toMap()

    override fun enumToLong(value: ComputationStage): Long =
      EnumStages.enumToLong(value.liquidLegionsSketchAggregationV2)

    override fun longToEnum(value: Long): ComputationStage =
      EnumStages.longToEnum(value).toProtocolStage()

    /**
     *  Translates [LiquidLegionsSketchAggregationV2.Stage]s wrapped in a [ComputationStage] into
     * [ComputationStageDetails].
     */
    class Details(otherDuchies: List<String>) :
      ProtocolStageDetails<ComputationStage, ComputationStageDetails, ComputationDetails> {
      override fun validateRoleForStage(stage: ComputationStage, details: ComputationDetails):
        Boolean {
          TODO("Not yet implemented")
        }

      override fun afterTransitionForStage(stage: ComputationStage): AfterTransition {
        TODO("Not yet implemented")
      }

      override fun outputBlobNumbersForStage(stage: ComputationStage): Int {
        TODO("Not yet implemented")
      }

      private val enumBasedDetails = EnumStages.Details(otherDuchies)

      override fun detailsFor(stage: ComputationStage): ComputationStageDetails =
        enumBasedDetails.detailsFor(stage.liquidLegionsSketchAggregationV2)

      override fun parseDetails(bytes: ByteArray): ComputationStageDetails =
        enumBasedDetails.parseDetails(bytes)
    }
  }
}

private fun Set<LiquidLegionsSketchAggregationV2.Stage>.toSetOfComputationStages() =
  this.map { it.toProtocolStage() }.toSet()
