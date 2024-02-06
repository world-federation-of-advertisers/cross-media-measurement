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

package org.wfanet.measurement.duchy.service.internal.computationcontrol

import org.wfanet.measurement.duchy.db.computation.singleOutputBlobMetadata
import org.wfanet.measurement.duchy.service.internal.computations.outputPathList
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.ComputationStageInput
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.copy
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2
import org.wfanet.measurement.internal.duchy.protocol.copy

class IllegalStageException(val computationStage: ComputationStage, buildMessage: () -> String) :
  IllegalArgumentException(buildMessage())

/** Utility class for protocol stages. */
sealed class ProtocolStages(val stageType: ComputationStage.StageCase) {
  /**
   * Returns the [ComputationStageBlobMetadata] output from a token for the specified origin Duchy.
   *
   * @throws IllegalStageException if the [token] stage is illegal
   */
  abstract fun outputBlob(token: ComputationToken, dataOrigin: String): ComputationStageBlobMetadata

  /**
   * Returns the next stage for a computation.
   *
   * @throws IllegalStageException if [stage] is illegal
   */
  abstract fun nextStage(stage: ComputationStage): ComputationStage

  /** Returns whether the current stage is valid to process the advance request */
  abstract fun isValidStage(currentStage: ComputationStage, requestStage: ComputationStage): Boolean

  /** Returns whether the stage expects the advance request with a blob. */
  abstract fun expectBlob(stage: ComputationStage): Boolean

  /**
   * Returns whether the [ComputationStage] of the [ComputationToken] expects the advance request
   * with protocol specific input.
   *
   * If the [token] has the fields set already, return false to skip.
   */
  abstract fun expectStageInput(token: ComputationToken): Boolean

  /** Returns the updated [ComputationDetails] with values in [ComputationStageInput]. */
  abstract fun updateComputationDetails(
    details: ComputationDetails,
    input: ComputationStageInput,
  ): ComputationDetails

  /** Returns whether the [ComputationToken] is in the state to advance to the next stage. */
  abstract fun readyForNextStage(token: ComputationToken): Boolean

  companion object {
    fun forStageType(stageType: ComputationStage.StageCase): ProtocolStages? {
      return when (stageType) {
        ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 -> LiquidLegionsV2Stages()
        ComputationStage.StageCase.REACH_ONLY_LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 ->
          ReachOnlyLiquidLegionsV2Stages()
        ComputationStage.StageCase.HONEST_MAJORITY_SHARE_SHUFFLE ->
          HonestMajorityShareShuffleStages()
        ComputationStage.StageCase.STAGE_NOT_SET -> null
      }
    }
  }
}

/** [ProtocolStages] for the Liquid Legions v2 protocol. */
class LiquidLegionsV2Stages() :
  ProtocolStages(ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2) {
  override fun outputBlob(
    token: ComputationToken,
    dataOrigin: String,
  ): ComputationStageBlobMetadata =
    when (val protocolStage = token.computationStage.liquidLegionsSketchAggregationV2) {
      LiquidLegionsSketchAggregationV2.Stage.WAIT_SETUP_PHASE_INPUTS -> {
        // Get the blob id by looking up the sender in the stage specific details.
        val stageDetails = token.stageSpecificDetails.liquidLegionsV2.waitSetupPhaseInputsDetails
        val blobId = checkNotNull(stageDetails.externalDuchyLocalBlobIdMap[dataOrigin])
        token.blobsList.single {
          it.dependencyType == ComputationBlobDependency.OUTPUT && it.blobId == blobId
        }
      }
      LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS,
      LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS,
      LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS ->
        token.singleOutputBlobMetadata()
      LiquidLegionsSketchAggregationV2.Stage.INITIALIZATION_PHASE,
      LiquidLegionsSketchAggregationV2.Stage.WAIT_REQUISITIONS_AND_KEY_SET,
      LiquidLegionsSketchAggregationV2.Stage.CONFIRMATION_PHASE,
      LiquidLegionsSketchAggregationV2.Stage.WAIT_TO_START,
      LiquidLegionsSketchAggregationV2.Stage.SETUP_PHASE,
      LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE,
      LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_TWO,
      LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_THREE,
      LiquidLegionsSketchAggregationV2.Stage.COMPLETE,
      LiquidLegionsSketchAggregationV2.Stage.STAGE_UNSPECIFIED,
      LiquidLegionsSketchAggregationV2.Stage.UNRECOGNIZED ->
        throw IllegalStageException(token.computationStage) {
          "Unexpected $stageType stage: $protocolStage"
        }
    }

  override fun nextStage(stage: ComputationStage): ComputationStage {
    require(stage.stageCase == ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2)

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enums fields cannot be null.
    return when (val protocolStage = stage.liquidLegionsSketchAggregationV2) {
      LiquidLegionsSketchAggregationV2.Stage.WAIT_SETUP_PHASE_INPUTS ->
        LiquidLegionsSketchAggregationV2.Stage.SETUP_PHASE
      LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS ->
        LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE
      LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS ->
        LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_TWO
      LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS ->
        LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_THREE
      LiquidLegionsSketchAggregationV2.Stage.INITIALIZATION_PHASE,
      LiquidLegionsSketchAggregationV2.Stage.WAIT_REQUISITIONS_AND_KEY_SET,
      LiquidLegionsSketchAggregationV2.Stage.CONFIRMATION_PHASE,
      LiquidLegionsSketchAggregationV2.Stage.WAIT_TO_START,
      LiquidLegionsSketchAggregationV2.Stage.SETUP_PHASE,
      LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE,
      LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_TWO,
      LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_THREE,
      LiquidLegionsSketchAggregationV2.Stage.COMPLETE,
      LiquidLegionsSketchAggregationV2.Stage.STAGE_UNSPECIFIED,
      LiquidLegionsSketchAggregationV2.Stage.UNRECOGNIZED ->
        throw IllegalStageException(stage) { "Next $stageType stage unknown for $protocolStage" }
    }.toProtocolStage()
  }

  override fun isValidStage(
    currentStage: ComputationStage,
    requestStage: ComputationStage,
  ): Boolean = currentStage == requestStage

  override fun expectBlob(stage: ComputationStage): Boolean = true

  override fun expectStageInput(token: ComputationToken): Boolean = false

  override fun updateComputationDetails(
    details: ComputationDetails,
    input: ComputationStageInput,
  ): ComputationDetails =
    throw IllegalStageException(
      LiquidLegionsSketchAggregationV2.Stage.STAGE_UNSPECIFIED.toProtocolStage()
    ) {
      "Invalid $stageType to update ComputationDetails."
    }

  override fun readyForNextStage(token: ComputationToken): Boolean =
    !token.outputPathList().any(String::isEmpty)
}

/** [ProtocolStages] for the Reach-Only Liquid Legions v2 protocol. */
class ReachOnlyLiquidLegionsV2Stages() :
  ProtocolStages(ComputationStage.StageCase.REACH_ONLY_LIQUID_LEGIONS_SKETCH_AGGREGATION_V2) {
  override fun outputBlob(
    token: ComputationToken,
    dataOrigin: String,
  ): ComputationStageBlobMetadata =
    when (val protocolStage = token.computationStage.reachOnlyLiquidLegionsSketchAggregationV2) {
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_SETUP_PHASE_INPUTS -> {
        // Get the blob id by looking up the sender in the stage specific details.
        val stageDetails =
          token.stageSpecificDetails.reachOnlyLiquidLegionsV2.waitSetupPhaseInputsDetails
        val blobId = checkNotNull(stageDetails.externalDuchyLocalBlobIdMap[dataOrigin])
        token.blobsList.single {
          it.dependencyType == ComputationBlobDependency.OUTPUT && it.blobId == blobId
        }
      }
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_INPUTS ->
        token.singleOutputBlobMetadata()
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.INITIALIZATION_PHASE,
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_REQUISITIONS_AND_KEY_SET,
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.CONFIRMATION_PHASE,
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_TO_START,
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.SETUP_PHASE,
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE,
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.COMPLETE,
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.STAGE_UNSPECIFIED,
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.UNRECOGNIZED ->
        throw IllegalStageException(token.computationStage) {
          "Unexpected $stageType stage: $protocolStage"
        }
    }

  override fun nextStage(stage: ComputationStage): ComputationStage {
    require(
      stage.stageCase == ComputationStage.StageCase.REACH_ONLY_LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
    )

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enums fields cannot be null.
    return when (val protocolStage = stage.reachOnlyLiquidLegionsSketchAggregationV2) {
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_SETUP_PHASE_INPUTS ->
        ReachOnlyLiquidLegionsSketchAggregationV2.Stage.SETUP_PHASE
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_INPUTS ->
        ReachOnlyLiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.INITIALIZATION_PHASE,
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_REQUISITIONS_AND_KEY_SET,
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.CONFIRMATION_PHASE,
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_TO_START,
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.SETUP_PHASE,
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE,
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.COMPLETE,
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.STAGE_UNSPECIFIED,
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.UNRECOGNIZED ->
        throw IllegalStageException(stage) { "Next $stageType stage unknown for $protocolStage" }
    }.toProtocolStage()
  }

  override fun isValidStage(
    currentStage: ComputationStage,
    requestStage: ComputationStage,
  ): Boolean = currentStage == requestStage

  override fun expectBlob(stage: ComputationStage): Boolean = true

  override fun expectStageInput(token: ComputationToken): Boolean = false

  override fun updateComputationDetails(
    details: ComputationDetails,
    input: ComputationStageInput,
  ): ComputationDetails =
    throw IllegalStageException(
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.STAGE_UNSPECIFIED.toProtocolStage()
    ) {
      "Invalid $stageType to update ComputationDetails"
    }

  override fun readyForNextStage(token: ComputationToken): Boolean =
    !token.outputPathList().any(String::isEmpty)
}

/** [ProtocolStages] for the Honest Majority Share Shuffle protocol. */
class HonestMajorityShareShuffleStages() :
  ProtocolStages(ComputationStage.StageCase.HONEST_MAJORITY_SHARE_SHUFFLE) {
  override fun outputBlob(
    token: ComputationToken,
    dataOrigin: String,
  ): ComputationStageBlobMetadata {
    val protocolStage = token.computationStage.honestMajorityShareShuffle

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enums fields cannot be null.
    return when (protocolStage) {
      HonestMajorityShareShuffle.Stage.WAIT_ON_AGGREGATION_INPUT -> {
        // Get the blob id by looking up the sender in the stage specific details.
        val stageDetails =
          token.stageSpecificDetails.honestMajorityShareShuffle.waitOnAggregationInputDetails
        val blobId = checkNotNull(stageDetails.externalDuchyLocalBlobIdMap[dataOrigin])
        token.blobsList.single {
          it.dependencyType == ComputationBlobDependency.OUTPUT && it.blobId == blobId
        }
      }
      HonestMajorityShareShuffle.Stage.INITIALIZED,
      HonestMajorityShareShuffle.Stage.SETUP_PHASE,
      HonestMajorityShareShuffle.Stage.WAIT_ON_SHUFFLE_INPUT,
      HonestMajorityShareShuffle.Stage.SHUFFLE_PHASE,
      HonestMajorityShareShuffle.Stage.AGGREGATION_PHASE,
      HonestMajorityShareShuffle.Stage.COMPLETE,
      HonestMajorityShareShuffle.Stage.STAGE_UNSPECIFIED,
      HonestMajorityShareShuffle.Stage.UNRECOGNIZED ->
        throw IllegalStageException(token.computationStage) {
          "Unexpected $stageType for stage: $protocolStage"
        }
    }
  }

  override fun nextStage(stage: ComputationStage): ComputationStage {
    require(stage.hasHonestMajorityShareShuffle())
    val protocolStage = stage.honestMajorityShareShuffle

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enums fields cannot be null.
    return when (protocolStage) {
      HonestMajorityShareShuffle.Stage.WAIT_ON_SHUFFLE_INPUT ->
        HonestMajorityShareShuffle.Stage.SHUFFLE_PHASE.toProtocolStage()
      HonestMajorityShareShuffle.Stage.WAIT_ON_AGGREGATION_INPUT ->
        HonestMajorityShareShuffle.Stage.AGGREGATION_PHASE.toProtocolStage()
      HonestMajorityShareShuffle.Stage.INITIALIZED,
      HonestMajorityShareShuffle.Stage.SETUP_PHASE,
      HonestMajorityShareShuffle.Stage.SHUFFLE_PHASE,
      HonestMajorityShareShuffle.Stage.AGGREGATION_PHASE,
      HonestMajorityShareShuffle.Stage.COMPLETE,
      HonestMajorityShareShuffle.Stage.STAGE_UNSPECIFIED,
      HonestMajorityShareShuffle.Stage.UNRECOGNIZED ->
        throw IllegalStageException(stage) { "Next $stageType stage invalid for $protocolStage" }
    }
  }

  override fun isValidStage(
    currentStage: ComputationStage,
    requestStage: ComputationStage,
  ): Boolean {
    require(currentStage.hasHonestMajorityShareShuffle())
    require(requestStage.hasHonestMajorityShareShuffle())
    val currentProtocolStage = currentStage.honestMajorityShareShuffle
    val requestProtocolStage = requestStage.honestMajorityShareShuffle

    return when (requestProtocolStage) {
      HonestMajorityShareShuffle.Stage.WAIT_ON_SHUFFLE_INPUT ->
        // Non-aggregators execute SETUP phase simultaneously. It is supposed to tolerate current
        // stage in INITIALIZED or SETUP_PHASE although the WAIT_ON_SHUFFLE_INPUT is the desired
        // one.
        setOf(
            HonestMajorityShareShuffle.Stage.INITIALIZED,
            HonestMajorityShareShuffle.Stage.SETUP_PHASE,
            HonestMajorityShareShuffle.Stage.WAIT_ON_SHUFFLE_INPUT,
          )
          .contains(currentProtocolStage)
      else -> currentProtocolStage == requestProtocolStage
    }
  }

  override fun expectBlob(stage: ComputationStage): Boolean {
    require(stage.hasHonestMajorityShareShuffle())

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enums fields cannot be null.
    return when (stage.honestMajorityShareShuffle) {
      HonestMajorityShareShuffle.Stage.WAIT_ON_AGGREGATION_INPUT -> true
      HonestMajorityShareShuffle.Stage.INITIALIZED,
      HonestMajorityShareShuffle.Stage.SETUP_PHASE,
      HonestMajorityShareShuffle.Stage.WAIT_ON_SHUFFLE_INPUT,
      HonestMajorityShareShuffle.Stage.SHUFFLE_PHASE,
      HonestMajorityShareShuffle.Stage.AGGREGATION_PHASE,
      HonestMajorityShareShuffle.Stage.COMPLETE,
      HonestMajorityShareShuffle.Stage.STAGE_UNSPECIFIED,
      HonestMajorityShareShuffle.Stage.UNRECOGNIZED -> false
    }
  }

  override fun expectStageInput(token: ComputationToken): Boolean {
    require(token.computationStage.hasHonestMajorityShareShuffle())
    val stage = token.computationStage

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enums fields cannot be null.
    return when (stage.honestMajorityShareShuffle) {
      HonestMajorityShareShuffle.Stage.INITIALIZED,
      HonestMajorityShareShuffle.Stage.SETUP_PHASE,
      HonestMajorityShareShuffle.Stage.WAIT_ON_SHUFFLE_INPUT -> {
        token.computationDetails.honestMajorityShareShuffle.seeds.commonRandomSeedFromPeer.isEmpty
      }
      HonestMajorityShareShuffle.Stage.WAIT_ON_AGGREGATION_INPUT,
      HonestMajorityShareShuffle.Stage.SHUFFLE_PHASE,
      HonestMajorityShareShuffle.Stage.AGGREGATION_PHASE,
      HonestMajorityShareShuffle.Stage.COMPLETE,
      HonestMajorityShareShuffle.Stage.STAGE_UNSPECIFIED,
      HonestMajorityShareShuffle.Stage.UNRECOGNIZED -> false
    }
  }

  override fun updateComputationDetails(
    details: ComputationDetails,
    input: ComputationStageInput,
  ): ComputationDetails {
    require(details.hasHonestMajorityShareShuffle())
    require(!input.honestMajorityShareShuffleShufflePhaseInput.peerRandomSeed.isEmpty)
    return details.copy {
      honestMajorityShareShuffle =
        honestMajorityShareShuffle.copy {
          seeds =
            seeds.copy {
              commonRandomSeedFromPeer =
                input.honestMajorityShareShuffleShufflePhaseInput.peerRandomSeed
            }
        }
    }
  }

  private fun ComputationToken.hasPeerSeed(): Boolean =
    !computationDetails.honestMajorityShareShuffle.seeds.commonRandomSeedFromPeer.isEmpty

  private fun ComputationToken.requisitionsFulfilled(): Boolean =
    requisitionsList.all { !it.secretSeed.isEmpty && it.path.isNotBlank() }

  override fun readyForNextStage(token: ComputationToken): Boolean {
    require(token.computationStage.hasHonestMajorityShareShuffle())

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enums fields cannot be null.
    return when (token.computationStage.honestMajorityShareShuffle) {
      HonestMajorityShareShuffle.Stage.WAIT_ON_SHUFFLE_INPUT ->
        token.hasPeerSeed() && token.requisitionsFulfilled()
      HonestMajorityShareShuffle.Stage.WAIT_ON_AGGREGATION_INPUT ->
        token.outputPathList().all(String::isNotEmpty)
      HonestMajorityShareShuffle.Stage.INITIALIZED,
      HonestMajorityShareShuffle.Stage.SETUP_PHASE,
      HonestMajorityShareShuffle.Stage.SHUFFLE_PHASE,
      HonestMajorityShareShuffle.Stage.AGGREGATION_PHASE,
      HonestMajorityShareShuffle.Stage.COMPLETE,
      HonestMajorityShareShuffle.Stage.STAGE_UNSPECIFIED,
      HonestMajorityShareShuffle.Stage.UNRECOGNIZED -> false
    }
  }
}
