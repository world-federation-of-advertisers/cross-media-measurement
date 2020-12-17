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

package org.wfanet.measurement.duchy.daemon.herald

import java.util.logging.Logger
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetails
import org.wfanet.measurement.duchy.db.computation.advanceComputationStage
import org.wfanet.measurement.duchy.service.internal.computation.outputPathList
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.CreateComputationRequest
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1.Stage
import org.wfanet.measurement.protocol.RequisitionKey

object LiquidLegionsV1Starter : ProtocolStarter {

  override suspend fun createComputation(
    globalId: String,
    computationStorageClient: ComputationsCoroutineStub,
    requisitionKeys: List<RequisitionKey>
  ) {
    computationStorageClient.createComputation(
      CreateComputationRequest.newBuilder().apply {
        computationType = LIQUID_LEGIONS_SKETCH_AGGREGATION_V1
        globalComputationId = globalId
        stageDetailsBuilder
          .liquidLegionsV1Builder
          .toConfirmRequisitionsStageDetailsBuilder
          .addAllKeys(requisitionKeys)
      }.build()
    )
  }

  override suspend fun startComputation(
    token: ComputationToken,
    computationStorageClient: ComputationsCoroutineStub,
    computationProtocolStageDetails: ComputationProtocolStageDetails,
    logger: Logger
  ) {
    require(token.computationDetails.hasLiquidLegionsV1()) {
      "Liquid Legions V1 computation required"
    }

    val stage = token.computationStage.liquidLegionsSketchAggregationV1
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (stage) {
      // We expect stage WAIT_TO_START.
      Stage.WAIT_TO_START -> {
        computationStorageClient.advanceComputationStage(
          computationToken = token,
          inputsToNextStage = token.outputPathList(),
          stage = Stage.TO_ADD_NOISE.toProtocolStage(),
          computationProtocolStageDetails = computationProtocolStageDetails
        )
        logger.info("[id=${token.globalComputationId}] Computation is now started")
        return
      }

      // For past stages, we throw.
      Stage.TO_CONFIRM_REQUISITIONS -> {
        error(
          "[id=${token.globalComputationId}]: cannot start a computation still" +
            " in state TO_CONFIRM_REQUISITIONS"
        )
      }

      // For future stages, we log and exit.
      Stage.WAIT_SKETCHES,
      Stage.TO_ADD_NOISE,
      Stage.TO_APPEND_SKETCHES_AND_ADD_NOISE,
      Stage.WAIT_CONCATENATED,
      Stage.TO_BLIND_POSITIONS,
      Stage.TO_BLIND_POSITIONS_AND_JOIN_REGISTERS,
      Stage.WAIT_FLAG_COUNTS,
      Stage.TO_DECRYPT_FLAG_COUNTS,
      Stage.TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS,
      Stage.COMPLETED -> {
        logger.info(
          "[id=${token.globalComputationId}]: not starting, " +
            "stage '$stage' is after WAIT_TO_START"
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
