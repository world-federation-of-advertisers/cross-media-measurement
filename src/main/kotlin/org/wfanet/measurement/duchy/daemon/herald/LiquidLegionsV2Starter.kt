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

package org.wfanet.measurement.duchy.daemon.herald

import java.util.logging.Logger
import org.wfanet.measurement.common.DuchyPosition
import org.wfanet.measurement.common.DuchyRole
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetails
import org.wfanet.measurement.duchy.db.computation.advanceComputationStage
import org.wfanet.measurement.duchy.service.internal.computation.outputPathList
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.CreateComputationRequest
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.ComputationDetails.RoleInComputation.AGGREGATOR
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.ComputationDetails.RoleInComputation.NON_AGGREGATOR
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.Stage
import org.wfanet.measurement.system.v1alpha.GlobalComputation

object LiquidLegionsV2Starter : ProtocolStarter {

  override suspend fun createComputation(
    computationStorageClient: ComputationsCoroutineStub,
    globalComputation: GlobalComputation,
    duchyPosition: DuchyPosition,
    blobStorageBucket: String
  ) {
    val globalId: String = checkNotNull(globalComputation.key?.globalComputationId)
    val initialComputationDetails = ComputationDetails.newBuilder().apply {
      // TODO: use fixed role for all computations in this duchy.
      liquidLegionsV2Builder.apply {
        role = when (duchyPosition.role) {
          DuchyRole.PRIMARY -> AGGREGATOR
          else -> NON_AGGREGATOR
        }
        incomingNodeId = duchyPosition.prev
        outgoingNodeId = duchyPosition.next
        aggregatorNodeId = duchyPosition.primary
        totalRequisitionCount = globalComputation.totalRequisitionCount
      }
      blobsStoragePrefix = "$blobStorageBucket/$globalId"
    }.build()

    computationStorageClient.createComputation(
      CreateComputationRequest.newBuilder().apply {
        computationType = ComputationTypeEnum.ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
        globalComputationId = globalId
        stageDetailsBuilder
          .liquidLegionsV2Builder
          .toConfirmRequisitionsStageDetailsBuilder
          .addAllKeys(globalComputation.toRequisitionKeys())
        computationDetails = initialComputationDetails
      }.build()
    )
  }

  override suspend fun startComputation(
    token: ComputationToken,
    computationStorageClient: ComputationsCoroutineStub,
    computationProtocolStageDetails: ComputationProtocolStageDetails,
    logger: Logger
  ) {
    require(token.computationDetails.hasLiquidLegionsV2()) {
      "Liquid Legions V2 computation required"
    }

    val stage = token.computationStage.liquidLegionsSketchAggregationV2
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (stage) {
      // We expect stage WAIT_TO_START.
      Stage.WAIT_TO_START -> {
        computationStorageClient.advanceComputationStage(
          computationToken = token,
          inputsToNextStage = token.outputPathList(),
          stage = Stage.SETUP_PHASE.toProtocolStage(),
          computationProtocolStageDetails = computationProtocolStageDetails
        )
        logger.info("[id=${token.globalComputationId}] Computation is now started")
        return
      }

      // For past stages, we throw.
      Stage.CONFIRM_REQUISITIONS_PHASE -> {
        error(
          "[id=${token.globalComputationId}]: cannot start a computation still" +
            " in state CONFIRM_REQUISITIONS_PHASE"
        )
      }

      // For future stages, we log and exit.
      Stage.WAIT_SETUP_PHASE_INPUTS,
      Stage.SETUP_PHASE,
      Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS,
      Stage.EXECUTION_PHASE_ONE,
      Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS,
      Stage.EXECUTION_PHASE_TWO,
      Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS,
      Stage.EXECUTION_PHASE_THREE,
      Stage.COMPLETE -> {
        logger.info(
          "[id=${token.globalComputationId}]: not starting," +
            " stage '$stage' is after WAIT_TO_START"
        )
        return
      }

      // For weird stages, we throw.
      Stage.UNRECOGNIZED,
      Stage.STAGE_UNKNOWN -> {
        error("[id=${token.globalComputationId}]: Unrecognized stage '$stage'")
      }
    }
  }
}
