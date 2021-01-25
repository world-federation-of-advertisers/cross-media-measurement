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

import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.duchy.db.computation.singleOutputBlobMetadata
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.AdvanceComputationRequest
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.Stage

/**
 * Context for a single [AdvanceComputationRequest] for a LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
 *  computation.
 *
 * @param request the request received by the Async Computation Control Service for a
 *   LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 computation. It is the context in which these functions
 *   operate.
 */
class LiquidLegionsSketchAggregationV2Context(
  override val request: AdvanceComputationRequest
) : SingleRequestContext {

  override val computationType = LIQUID_LEGIONS_SKETCH_AGGREGATION_V2

  override fun outputBlob(token: ComputationToken): ComputationStageBlobMetadata =
    when (token.computationStage.liquidLegionsSketchAggregationV2) {
      Stage.WAIT_SETUP_PHASE_INPUTS -> {
        // Get the blob id by looking up the sender in the stage specific details.
        val stageDetails = token.stageSpecificDetails.liquidLegionsV2.waitSetupPhaseInputsDetails
        val blobId = checkNotNull(stageDetails.externalDuchyLocalBlobIdMap[request.dataOrigin])
        token.blobsList.single {
          it.dependencyType == ComputationBlobDependency.OUTPUT && it.blobId == blobId
        }
      }
      Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS,
      Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS,
      Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS -> token.singleOutputBlobMetadata()
      else -> failGrpc {
        "Not expecting to advance computation in stage ${token.computationStage} through the " +
          "AsyncComputationControl Service"
      }
    }

  override fun nextStage(
    computationDetails: ComputationDetails,
    stage: ComputationStage
  ): ComputationStage {
    return when (stage.liquidLegionsSketchAggregationV2) {
      Stage.WAIT_SETUP_PHASE_INPUTS -> Stage.SETUP_PHASE
      Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS -> Stage.EXECUTION_PHASE_ONE
      Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS -> Stage.EXECUTION_PHASE_TWO
      Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS -> Stage.EXECUTION_PHASE_THREE
      else -> failGrpc {
        "Next liquid legions v2 stage unknown for ($computationDetails, stage: $stage)"
      }
    }.toProtocolStage()
  }
}
