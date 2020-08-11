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

package org.wfanet.measurement.db.duchy.computation

import org.wfanet.measurement.common.numberAsLong
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.COMPLETED
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_ADD_NOISE
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_APPEND_SKETCHES_AND_ADD_NOISE
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_BLIND_POSITIONS
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_BLIND_POSITIONS_AND_JOIN_REGISTERS
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_CONFIRM_REQUISITIONS
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_DECRYPT_FLAG_COUNTS
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.UNRECOGNIZED
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.WAIT_CONCATENATED
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.WAIT_FLAG_COUNTS
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.WAIT_SKETCHES
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.WAIT_TO_START
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.WaitSketchesStageDetails
import org.wfanet.measurement.service.internal.duchy.computation.storage.toProtocolStage

/**
 * Helper classes for working with stages of the Liquid Legions Sketch Aggregation MPC defined in
 * [LiquidLegionsSketchAggregationStage].
 *
 * The [LiquidLegionsSketchAggregationStage] is one of the computation protocols defined in the
 * [ComputationStage] proto used in the storage layer API. There are helper objects for both the raw
 * enum values and those enum value wrapped in the proto message. Typically the right helper to use
 * is [ComputationStages] as it is the API level abstraction. [EnumStages] is visible in case it is
 * needed.
 *
 * [EnumStages.Details] is a helper to create [ComputationStageDetails] from
 * [LiquidLegionsSketchAggregationStage] enum values.
 * [ComputationStages.Details] is a helper to create [ComputationStageDetails] from
 * [ComputationStage] protos wrapping a [LiquidLegionsSketchAggregationStage] enum values.
 */
object LiquidLegionsSketchAggregationProtocol {
  /**
   * Implementation of [ProtocolStageEnumHelper] for [LiquidLegionsSketchAggregationStage].
   */
  object EnumStages : ProtocolStageEnumHelper<LiquidLegionsSketchAggregationStage> {
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
        TO_BLIND_POSITIONS to setOf(WAIT_FLAG_COUNTS, COMPLETED),
        TO_BLIND_POSITIONS_AND_JOIN_REGISTERS to setOf(WAIT_FLAG_COUNTS),
        WAIT_FLAG_COUNTS to setOf(
          TO_DECRYPT_FLAG_COUNTS,
          TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS
        ),
        TO_DECRYPT_FLAG_COUNTS to setOf(),
        TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS to setOf()
      ).withDefault { setOf() }

    override fun enumToLong(value: LiquidLegionsSketchAggregationStage): Long {
      return value.numberAsLong
    }

    override fun longToEnum(value: Long): LiquidLegionsSketchAggregationStage {
      // forNumber() returns null for unrecognized enum values for the proto.
      return LiquidLegionsSketchAggregationStage.forNumber(value.toInt()) ?: UNRECOGNIZED
    }

    /** Translates [LiquidLegionsSketchAggregationStage]s into [ComputationStageDetails]. */
    class Details(private val otherDuchies: List<String>) :
      ProtocolStageDetails<LiquidLegionsSketchAggregationStage, ComputationStageDetails> {
      override fun detailsFor(stage: LiquidLegionsSketchAggregationStage): ComputationStageDetails {
        return when (stage) {
          WAIT_SKETCHES ->
            ComputationStageDetails.newBuilder()
              .setWaitSketchStageDetails(
                WaitSketchesStageDetails.newBuilder()
                  // The WAIT_SKETCHES stage has exactly one input which is the noised sketches from
                  // the primary duchy running the wait operation. It is not an output of the stage
                  // because it is a result of a locally running stage.
                  .putAllExternalDuchyLocalBlobId(
                    otherDuchies.mapIndexed { idx, duchy -> duchy to (idx + 1).toLong() }.toMap()
                  )
              )
              .build()
          else -> ComputationStageDetails.getDefaultInstance()
        }
      }

      override fun parseDetails(bytes: ByteArray): ComputationStageDetails =
        ComputationStageDetails.parseFrom(bytes)
    }
  }

  /**
   * Implementation of [ProtocolStageEnumHelper] for [LiquidLegionsSketchAggregationStage] wrapped
   * in a [ComputationStage].
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
      EnumStages.enumToLong(value.liquidLegionsSketchAggregation)

    override fun longToEnum(value: Long): ComputationStage =
      EnumStages.longToEnum(value).toProtocolStage()

    /**
     *  Translates [LiquidLegionsSketchAggregationStage]s wrapped in a [ComputationStage] into
     * [ComputationStageDetails].
     */
    class Details(otherDuchies: List<String>) :
      ProtocolStageDetails<ComputationStage, ComputationStageDetails> {

      private val enumBasedDetails = EnumStages.Details(otherDuchies)

      override fun detailsFor(stage: ComputationStage): ComputationStageDetails =
        enumBasedDetails.detailsFor(stage.liquidLegionsSketchAggregation)

      override fun parseDetails(bytes: ByteArray): ComputationStageDetails =
        enumBasedDetails.parseDetails(bytes)
    }
  }
}

private fun Set<LiquidLegionsSketchAggregationStage>.toSetOfComputationStages() =
  this.map { it.toProtocolStage() }.toSet()
