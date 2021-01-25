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
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1.ComputationDetails.RoleInComputation
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1.Stage

/**
 * Context for a single [AdvanceComputationRequest] for a LIQUID_LEGIONS_SKETCH_AGGREGATION_V1
 *  computation.
 *
 * @param request the request received by the Async Computation Control Service for a
 *   LIQUID_LEGIONS_SKETCH_AGGREGATION_V1 computation. It is the context in which these functions
 *   operate.
 *
 * @see [NEXT_STAGE_BY_ROLE]
 */
class LiquidLegionsSketchAggregationV1Context(
  override val request: AdvanceComputationRequest
) : SingleRequestContext {
  override val computationType: ComputationType = LIQUID_LEGIONS_SKETCH_AGGREGATION_V1

  override fun outputBlob(token: ComputationToken): ComputationStageBlobMetadata {
    return when (token.computationStage.liquidLegionsSketchAggregationV1) {
      Stage.WAIT_SKETCHES -> {
        val stageDetails = token.stageSpecificDetails.liquidLegionsV1.waitSketchStageDetails
        val blobId = checkNotNull(stageDetails.externalDuchyLocalBlobIdMap[request.dataOrigin])
        return token.blobsList.single {
          it.dependencyType == ComputationBlobDependency.OUTPUT &&
            it.blobId == blobId
        }
      }
      Stage.WAIT_CONCATENATED,
      Stage.WAIT_FLAG_COUNTS -> token.singleOutputBlobMetadata()
      else -> failGrpc {
        "Not expecting to advance computation in stage ${token.computationStage} through the " +
          "AsyncComputationControl Service"
      }
    }
  }

  override fun nextStage(
    computationDetails: ComputationDetails,
    stage: ComputationStage
  ): ComputationStage {
    val role = computationDetails.liquidLegionsV1.role
    return NEXT_STAGE_BY_ROLE[role]
      ?.get(stage.liquidLegionsSketchAggregationV1)
      ?.toProtocolStage()
      ?: failGrpc {
        "Next liquid legions v1 stage unknown for (role: $role, stage: $stage)"
      }
  }

  companion object {
    /**
     * Mapping of successor stages to a computation stage by a duchy's role in the computation.
     *
     * The table is a two layer map. The first layer has keys for all valid roles. The second has
     * entries for stage transitions in that role. This map is specific to transitions carried out
     * at the async computation control service, so it therefore only has keys for stages from
     * which the service is responsible for transitioning.
     */
    val NEXT_STAGE_BY_ROLE: Map<RoleInComputation, Map<Stage, Stage>> =
      mapOf(
        RoleInComputation.PRIMARY to
          mapOf(
            Stage.WAIT_SKETCHES to Stage.TO_APPEND_SKETCHES_AND_ADD_NOISE,
            Stage.WAIT_CONCATENATED to Stage.TO_BLIND_POSITIONS_AND_JOIN_REGISTERS,
            Stage.WAIT_FLAG_COUNTS to Stage.TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS
          ),
        RoleInComputation.SECONDARY to
          mapOf(
            Stage.WAIT_CONCATENATED to Stage.TO_BLIND_POSITIONS,
            Stage.WAIT_FLAG_COUNTS to Stage.TO_DECRYPT_FLAG_COUNTS
          )
      )
  }
}
