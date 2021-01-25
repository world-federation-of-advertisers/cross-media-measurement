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
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1.Stage.COMPLETED
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1.Stage.TO_ADD_NOISE
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1.Stage.TO_APPEND_SKETCHES_AND_ADD_NOISE
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1.Stage.TO_BLIND_POSITIONS
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1.Stage.TO_BLIND_POSITIONS_AND_JOIN_REGISTERS
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1.Stage.TO_CONFIRM_REQUISITIONS
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1.Stage.TO_DECRYPT_FLAG_COUNTS
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1.Stage.TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1.Stage.UNRECOGNIZED
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1.Stage.WAIT_CONCATENATED
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1.Stage.WAIT_FLAG_COUNTS
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1.Stage.WAIT_SKETCHES
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1.Stage.WAIT_TO_START

/**
 * Helper classes for working with stages of the Liquid Legions Sketch Aggregation MPC defined in
 * [LiquidLegionsSketchAggregationV1.Stage].
 *
 * The [LiquidLegionsSketchAggregationV1.Stage] is one of the computation protocols defined in the
 * [ComputationStage] proto used in the storage layer API. There are helper objects for both the raw
 * enum values and those enum value wrapped in the proto message. Typically the right helper to use
 * is [ComputationStages] as it is the API level abstraction. [EnumStages] is visible in case it is
 * needed.
 *
 * [EnumStages.Details] is a helper to create [ComputationStageDetails] from
 * [LiquidLegionsSketchAggregationV1.Stage] enum values.
 * [ComputationStages.Details] is a helper to create [ComputationStageDetails] from
 * [ComputationStage] protos wrapping a [LiquidLegionsSketchAggregationV1.Stage] enum values.
 */
object LiquidLegionsSketchAggregationV1Protocol {
  /**
   * Implementation of [ProtocolStageEnumHelper] for [LiquidLegionsSketchAggregationV1.Stage].
   */
  object EnumStages : ProtocolStageEnumHelper<LiquidLegionsSketchAggregationV1.Stage> {
    override val validInitialStages = setOf(TO_CONFIRM_REQUISITIONS)
    override val validTerminalStages = setOf(COMPLETED)

    override val validSuccessors =
      mapOf(
        TO_CONFIRM_REQUISITIONS to setOf(WAIT_SKETCHES, WAIT_TO_START),
        WAIT_TO_START to setOf(TO_ADD_NOISE),
        TO_ADD_NOISE to setOf(WAIT_CONCATENATED),
        WAIT_SKETCHES to setOf(TO_APPEND_SKETCHES_AND_ADD_NOISE),
        TO_APPEND_SKETCHES_AND_ADD_NOISE to setOf(WAIT_CONCATENATED),
        WAIT_CONCATENATED to setOf(
          TO_BLIND_POSITIONS,
          TO_BLIND_POSITIONS_AND_JOIN_REGISTERS
        ),
        TO_BLIND_POSITIONS to setOf(WAIT_FLAG_COUNTS),
        TO_BLIND_POSITIONS_AND_JOIN_REGISTERS to setOf(WAIT_FLAG_COUNTS),
        WAIT_FLAG_COUNTS to setOf(
          TO_DECRYPT_FLAG_COUNTS,
          TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS
        ),
        TO_DECRYPT_FLAG_COUNTS to setOf(),
        TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS to setOf()
      ).withDefault { setOf() }

    override fun enumToLong(value: LiquidLegionsSketchAggregationV1.Stage): Long {
      return value.numberAsLong
    }

    override fun longToEnum(value: Long): LiquidLegionsSketchAggregationV1.Stage {
      // forNumber() returns null for unrecognized enum values for the proto.
      return LiquidLegionsSketchAggregationV1.Stage.forNumber(value.toInt()) ?: UNRECOGNIZED
    }

    /** Translates [LiquidLegionsSketchAggregationV1.Stage]s into [ComputationStageDetails]. */
    class Details(val otherDuchies: List<String>) :
      ProtocolStageDetails<
        LiquidLegionsSketchAggregationV1.Stage,
        ComputationStageDetails,
        LiquidLegionsSketchAggregationV1.ComputationDetails> {
      override fun validateRoleForStage(
        stage: LiquidLegionsSketchAggregationV1.Stage,
        details: LiquidLegionsSketchAggregationV1.ComputationDetails
      ): Boolean {
        return when (stage) {
          WAIT_SKETCHES,
          TO_APPEND_SKETCHES_AND_ADD_NOISE,
          TO_BLIND_POSITIONS_AND_JOIN_REGISTERS,
          TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS ->
            details.role ==
              LiquidLegionsSketchAggregationV1.ComputationDetails.RoleInComputation.PRIMARY
          WAIT_TO_START,
          TO_ADD_NOISE,
          TO_BLIND_POSITIONS,
          TO_DECRYPT_FLAG_COUNTS ->
            details.role ==
              LiquidLegionsSketchAggregationV1.ComputationDetails.RoleInComputation.SECONDARY
          else ->
            true /* Stage can be executed at either primary or non-primary */
        }
      }

      override fun afterTransitionForStage(stage: LiquidLegionsSketchAggregationV1.Stage):
        AfterTransition {
          return when (stage) {
            // Stages of computation mapping some number of inputs to single output.
            TO_ADD_NOISE,
            TO_APPEND_SKETCHES_AND_ADD_NOISE,
            TO_BLIND_POSITIONS,
            TO_BLIND_POSITIONS_AND_JOIN_REGISTERS,
            TO_DECRYPT_FLAG_COUNTS,
            TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS ->
              AfterTransition.ADD_UNCLAIMED_TO_QUEUE
            WAIT_TO_START,
            WAIT_SKETCHES,
            WAIT_CONCATENATED,
            WAIT_FLAG_COUNTS ->
              AfterTransition.DO_NOT_ADD_TO_QUEUE
            COMPLETED -> error("Computation should be ended with call to endComputation(...)")
            // Stages that we can't transition to ever.
            UNRECOGNIZED,
            LiquidLegionsSketchAggregationV1.Stage.STAGE_UNKNOWN,
            TO_CONFIRM_REQUISITIONS ->
              error("Cannot make transition function to stage $stage")
          }
        }

      override fun outputBlobNumbersForStage(stage: LiquidLegionsSketchAggregationV1.Stage): Int {
        return when (stage) {
          WAIT_TO_START ->
            // There is no output in this stage, the input is forwarded to the next stage as input.
            0
          WAIT_CONCATENATED,
          WAIT_FLAG_COUNTS,
          TO_ADD_NOISE,
          TO_APPEND_SKETCHES_AND_ADD_NOISE,
          TO_BLIND_POSITIONS,
          TO_BLIND_POSITIONS_AND_JOIN_REGISTERS,
          TO_DECRYPT_FLAG_COUNTS,
          TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS ->
            // The output is the intermediate computation result either received from another duchy
            // or computed locally.
            1
          WAIT_SKETCHES ->
            // The output contains otherDuchiesInComputation sketches from the other duchies.
            otherDuchies.size
          // Mill have nothing to do for this stage.
          COMPLETED -> error("Computation should be ended with call to endComputation(...)")
          // Stages that we can't transition to ever.
          UNRECOGNIZED,
          LiquidLegionsSketchAggregationV1.Stage.STAGE_UNKNOWN,
          TO_CONFIRM_REQUISITIONS ->
            error("Cannot make transition function to stage $stage")
        }
      }

      override fun detailsFor(stage: LiquidLegionsSketchAggregationV1.Stage):
        ComputationStageDetails {
          return when (stage) {
            WAIT_SKETCHES ->
              ComputationStageDetails.newBuilder().apply {
                liquidLegionsV1Builder.waitSketchStageDetailsBuilder.apply {
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
   * Implementation of [ProtocolStageEnumHelper] for [LiquidLegionsSketchAggregationV1.Stage]
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
      EnumStages.enumToLong(value.liquidLegionsSketchAggregationV1)

    override fun longToEnum(value: Long): ComputationStage =
      EnumStages.longToEnum(value).toProtocolStage()

    /**
     *  Translates [LiquidLegionsSketchAggregationV1.Stage]s wrapped in a [ComputationStage] into
     * [ComputationStageDetails].
     */
    class Details(otherDuchies: List<String>) :
      ProtocolStageDetails<ComputationStage, ComputationStageDetails, ComputationDetails> {

      private val enumBasedDetails = EnumStages.Details(otherDuchies)
      override fun validateRoleForStage(stage: ComputationStage, details: ComputationDetails):
        Boolean {
          return enumBasedDetails.validateRoleForStage(
            stage.liquidLegionsSketchAggregationV1,
            details.liquidLegionsV1
          )
        }

      override fun afterTransitionForStage(stage: ComputationStage): AfterTransition {
        return enumBasedDetails.afterTransitionForStage(stage.liquidLegionsSketchAggregationV1)
      }

      override fun outputBlobNumbersForStage(stage: ComputationStage): Int {
        return enumBasedDetails.outputBlobNumbersForStage(stage.liquidLegionsSketchAggregationV1)
      }

      override fun detailsFor(stage: ComputationStage): ComputationStageDetails =
        enumBasedDetails.detailsFor(stage.liquidLegionsSketchAggregationV1)

      override fun parseDetails(bytes: ByteArray): ComputationStageDetails =
        enumBasedDetails.parseDetails(bytes)
    }
  }
}

private fun Set<LiquidLegionsSketchAggregationV1.Stage>.toSetOfComputationStages() =
  this.map { it.toProtocolStage() }.toSet()
