// Copyright 2023 The Cross-Media Measurement Authors
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
import org.wfanet.measurement.internal.duchy.computationStageDetails
import org.wfanet.measurement.internal.duchy.config.RoleInComputation
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle.Stage.AGGREGATION_PHASE
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle.Stage.COMPLETE
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle.Stage.INITIALIZED
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle.Stage.SETUP_PHASE
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle.Stage.SHUFFLE_PHASE
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle.Stage.STAGE_UNSPECIFIED
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle.Stage.UNRECOGNIZED
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle.Stage.WAIT_ON_AGGREGATION_INPUT
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle.Stage.WAIT_ON_SHUFFLE_INPUT_PHASE_ONE
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle.Stage.WAIT_ON_SHUFFLE_INPUT_PHASE_TWO
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle.Stage.WAIT_TO_START
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffleKt.stageDetails
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffleKt.waitOnAggregationInputDetails

/**
 * Helper classes for working with stages of the Hones Majority Share Shuffle protocol defined in
 * [HonestMajorityShareShuffle.Stage].
 *
 * The [HonestMajorityShareShuffle.Stage] is one of the computation protocols defined in the
 * [ComputationStage] proto used in the storage layer API. There are helper objects for both the raw
 * enum values and those enum value wrapped in the proto message. Typically, the right helper to use
 * is [ComputationStages] as it is the API level abstraction. [EnumStages] is visible in case it is
 * needed.
 *
 * [EnumStages.Details] is a helper to create [ComputationStageDetails] from
 * [HonestMajorityShareShuffle.Stage] enum values. [ComputationStages.Details] is a helper to create
 * [ComputationStageDetails] from [ComputationStage] protos wrapping a
 * [HonestMajorityShareShuffle.Stage] enum values.
 */
object HonestMajorityShareShuffleProtocol {
  /** Implementation of [ProtocolStageEnumHelper] for [HonestMajorityShareShuffle.Stage]. */
  object EnumStages : ProtocolStageEnumHelper<HonestMajorityShareShuffle.Stage> {
    override val validInitialStages = setOf(INITIALIZED)
    override val validTerminalStages = setOf(COMPLETE)

    override val validSuccessors =
      mapOf(
          INITIALIZED to
            setOf(WAIT_TO_START, WAIT_ON_SHUFFLE_INPUT_PHASE_ONE, WAIT_ON_AGGREGATION_INPUT),
          WAIT_TO_START to setOf(SETUP_PHASE),
          WAIT_ON_SHUFFLE_INPUT_PHASE_ONE to setOf(SETUP_PHASE),
          SETUP_PHASE to setOf(WAIT_ON_SHUFFLE_INPUT_PHASE_TWO, SHUFFLE_PHASE),
          WAIT_ON_SHUFFLE_INPUT_PHASE_TWO to setOf(SHUFFLE_PHASE),
          WAIT_ON_AGGREGATION_INPUT to setOf(AGGREGATION_PHASE),
          SHUFFLE_PHASE to setOf(COMPLETE),
          AGGREGATION_PHASE to setOf(COMPLETE),
        )
        .withDefault { setOf() }

    override fun enumToLong(value: HonestMajorityShareShuffle.Stage): Long {
      return value.numberAsLong
    }

    override fun longToEnum(value: Long): HonestMajorityShareShuffle.Stage {
      // forNumber() returns null for unrecognized enum values for the proto.
      return HonestMajorityShareShuffle.Stage.forNumber(value.toInt()) ?: UNRECOGNIZED
    }

    /** Translates [HonestMajorityShareShuffle.Stage] s into [ComputationStageDetails]. */
    object Details :
      ProtocolStageDetails<
        HonestMajorityShareShuffle.Stage,
        ComputationStageDetails,
        HonestMajorityShareShuffle.ComputationDetails,
      > {

      override fun validateRoleForStage(
        stage: HonestMajorityShareShuffle.Stage,
        details: HonestMajorityShareShuffle.ComputationDetails,
      ): Boolean {
        return when (stage) {
          WAIT_TO_START,
          WAIT_ON_SHUFFLE_INPUT_PHASE_TWO -> details.role == RoleInComputation.FIRST_NON_AGGREGATOR
          WAIT_ON_SHUFFLE_INPUT_PHASE_ONE -> details.role == RoleInComputation.SECOND_NON_AGGREGATOR
          SETUP_PHASE,
          SHUFFLE_PHASE ->
            details.role == RoleInComputation.FIRST_NON_AGGREGATOR ||
              details.role == RoleInComputation.SECOND_NON_AGGREGATOR
          WAIT_ON_AGGREGATION_INPUT,
          AGGREGATION_PHASE -> details.role == RoleInComputation.AGGREGATOR
          INITIALIZED,
          COMPLETE -> true /* Stage can be executed at either AGGREGATOR or NON_AGGREGATOR */
          STAGE_UNSPECIFIED,
          UNRECOGNIZED -> error("Invalid Stage. $stage")
        }
      }

      override fun afterTransitionForStage(
        stage: HonestMajorityShareShuffle.Stage
      ): AfterTransition {
        return when (stage) {
          // Stages of computation mapping some number of inputs to single output.
          SETUP_PHASE,
          SHUFFLE_PHASE,
          AGGREGATION_PHASE -> AfterTransition.ADD_UNCLAIMED_TO_QUEUE
          WAIT_TO_START,
          WAIT_ON_SHUFFLE_INPUT_PHASE_ONE,
          WAIT_ON_SHUFFLE_INPUT_PHASE_TWO,
          WAIT_ON_AGGREGATION_INPUT -> AfterTransition.DO_NOT_ADD_TO_QUEUE
          COMPLETE -> error("Computation should be ended with call to endComputation(...)")
          // Stages that we can't transition to ever.
          UNRECOGNIZED,
          STAGE_UNSPECIFIED,
          INITIALIZED -> error("Cannot make transition function to stage $stage")
        }
      }

      override fun outputBlobNumbersForStage(
        stage: HonestMajorityShareShuffle.Stage,
        computationDetails: HonestMajorityShareShuffle.ComputationDetails,
      ): Int {
        return when (stage) {
          SETUP_PHASE,
          WAIT_TO_START,
          // TODO(@renjiez): consider writing intermediate result as output for shuffle phase and
          // aggregation phase.
          SHUFFLE_PHASE,
          AGGREGATION_PHASE -> 0
          // The output of these stages are the data received from the peer non-aggregator duchy:
          WAIT_ON_SHUFFLE_INPUT_PHASE_ONE,
          WAIT_ON_SHUFFLE_INPUT_PHASE_TWO -> 1
          // The output of these stages are the computed intermediate data:
          WAIT_ON_AGGREGATION_INPUT -> 2
          COMPLETE -> error("Computation should be ended with call to endComputation(...)")
          INITIALIZED,
          UNRECOGNIZED,
          STAGE_UNSPECIFIED -> error("Cannot make transition function to stage $stage")
        }
      }

      override fun detailsFor(
        stage: HonestMajorityShareShuffle.Stage,
        computationDetails: HonestMajorityShareShuffle.ComputationDetails,
      ): ComputationStageDetails {
        return when (stage) {
          WAIT_ON_AGGREGATION_INPUT ->
            computationStageDetails {
              honestMajorityShareShuffle = stageDetails {
                waitOnAggregationInputDetails = waitOnAggregationInputDetails {
                  computationDetails.nonAggregatorsList.mapIndexed { idx, duchyId ->
                    externalDuchyLocalBlobId[duchyId] = idx.toLong()
                  }
                }
              }
            }
          else -> ComputationStageDetails.getDefaultInstance()
        }
      }

      override fun parseDetails(bytes: ByteArray): ComputationStageDetails =
        ComputationStageDetails.parseFrom(bytes)
    }
  }

  /**
   * Implementation of [ProtocolStageEnumHelper] for [HonestMajorityShareShuffle.Stage] wrapped in a
   * [ComputationStage].
   */
  object ComputationStages : ProtocolStageEnumHelper<ComputationStage> {
    override val validInitialStages = EnumStages.validInitialStages.toSetOfComputationStages()
    override val validTerminalStages = EnumStages.validTerminalStages.toSetOfComputationStages()

    override val validSuccessors =
      EnumStages.validSuccessors
        .map { it.key.toProtocolStage() to it.value.toSetOfComputationStages() }
        .toMap()

    override fun enumToLong(value: ComputationStage): Long =
      EnumStages.enumToLong(value.honestMajorityShareShuffle)

    override fun longToEnum(value: Long): ComputationStage =
      EnumStages.longToEnum(value).toProtocolStage()

    /**
     * Translates [HonestMajorityShareShuffle.Stage] s wrapped in a [ComputationStage] into
     * [ComputationStageDetails].
     */
    object Details :
      ProtocolStageDetails<ComputationStage, ComputationStageDetails, ComputationDetails> {
      override fun validateRoleForStage(
        stage: ComputationStage,
        details: ComputationDetails,
      ): Boolean {
        return EnumStages.Details.validateRoleForStage(
          stage.honestMajorityShareShuffle,
          details.honestMajorityShareShuffle,
        )
      }

      override fun afterTransitionForStage(stage: ComputationStage): AfterTransition {
        return EnumStages.Details.afterTransitionForStage(stage.honestMajorityShareShuffle)
      }

      override fun outputBlobNumbersForStage(
        stage: ComputationStage,
        computationDetails: ComputationDetails,
      ): Int {
        return EnumStages.Details.outputBlobNumbersForStage(
          stage.honestMajorityShareShuffle,
          computationDetails.honestMajorityShareShuffle,
        )
      }

      override fun detailsFor(
        stage: ComputationStage,
        computationDetails: ComputationDetails,
      ): ComputationStageDetails =
        EnumStages.Details.detailsFor(
          stage.honestMajorityShareShuffle,
          computationDetails.honestMajorityShareShuffle,
        )

      override fun parseDetails(bytes: ByteArray): ComputationStageDetails =
        EnumStages.Details.parseDetails(bytes)
    }
  }
}

private fun Set<HonestMajorityShareShuffle.Stage>.toSetOfComputationStages() =
  this.map { it.toProtocolStage() }.toSet()
