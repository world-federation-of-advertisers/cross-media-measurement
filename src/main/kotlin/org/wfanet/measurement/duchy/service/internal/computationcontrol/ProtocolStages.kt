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
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage

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

  companion object {
    fun forStageType(stageType: ComputationStage.StageCase): ProtocolStages? {
      return when (stageType) {
        ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 -> LiquidLegionsV2Stages()
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
    dataOrigin: String
  ): ComputationStageBlobMetadata =
    when (val protocolStage = token.computationStage.liquidLegionsSketchAggregationV2) {
      Stage.WAIT_SETUP_PHASE_INPUTS -> {
        // Get the blob id by looking up the sender in the stage specific details.
        val stageDetails = token.stageSpecificDetails.liquidLegionsV2.waitSetupPhaseInputsDetails
        val blobId = checkNotNull(stageDetails.externalDuchyLocalBlobIdMap[dataOrigin])
        token.blobsList.single {
          it.dependencyType == ComputationBlobDependency.OUTPUT && it.blobId == blobId
        }
      }
      Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS,
      Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS,
      Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS -> token.singleOutputBlobMetadata()
      Stage.INITIALIZATION_PHASE,
      Stage.WAIT_REQUISITIONS_AND_KEY_SET,
      Stage.CONFIRMATION_PHASE,
      Stage.WAIT_TO_START,
      Stage.SETUP_PHASE,
      Stage.EXECUTION_PHASE_ONE,
      Stage.EXECUTION_PHASE_TWO,
      Stage.EXECUTION_PHASE_THREE,
      Stage.COMPLETE,
      Stage.STAGE_UNKNOWN,
      Stage.UNRECOGNIZED ->
        throw IllegalStageException(token.computationStage) {
          "Unexpected $stageType stage: $protocolStage"
        }
    }

  override fun nextStage(stage: ComputationStage): ComputationStage {
    require(stage.stageCase == ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2)

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enums fields cannot be null.
    return when (val protocolStage = stage.liquidLegionsSketchAggregationV2) {
      Stage.WAIT_SETUP_PHASE_INPUTS -> Stage.SETUP_PHASE
      Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS -> Stage.EXECUTION_PHASE_ONE
      Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS -> Stage.EXECUTION_PHASE_TWO
      Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS -> Stage.EXECUTION_PHASE_THREE
      Stage.INITIALIZATION_PHASE,
      Stage.WAIT_REQUISITIONS_AND_KEY_SET,
      Stage.CONFIRMATION_PHASE,
      Stage.WAIT_TO_START,
      Stage.SETUP_PHASE,
      Stage.EXECUTION_PHASE_ONE,
      Stage.EXECUTION_PHASE_TWO,
      Stage.EXECUTION_PHASE_THREE,
      Stage.COMPLETE,
      Stage.STAGE_UNKNOWN,
      Stage.UNRECOGNIZED ->
        throw IllegalStageException(stage) { "Next $stageType stage unknown for $protocolStage" }
    }.toProtocolStage()
  }
}
