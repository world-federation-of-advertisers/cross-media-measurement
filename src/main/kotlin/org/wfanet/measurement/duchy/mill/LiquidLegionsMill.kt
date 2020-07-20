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

package org.wfanet.measurement.duchy.mill

import java.util.logging.Logger
import org.wfanet.measurement.common.Throttler
import org.wfanet.measurement.db.duchy.computation.LiquidLegionsSketchAggregationComputationStorageClients
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage
import org.wfanet.measurement.internal.duchy.ClaimWorkRequest
import org.wfanet.measurement.internal.duchy.ClaimWorkResponse
import org.wfanet.measurement.internal.duchy.ComputationControlServiceGrpcKt.ComputationControlServiceCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType

/**
 * Mill works on computations using the LiquidLegionSketchAggregationProtocol.
 *
 * @param[millId] The identifier of this mill, used to claim a work.
 * @param[storageClients] clients that have access to local computation storage, i.e., spanner
 *    table and blob store.
 * @param[workerStubs] A map from other duchies' Ids to their corresponding
 *    computationControlClients, used for passing computation to other duchies.
 * @param[throttler] A throttler used to rate limit the frequency of the mill polling from the
 *    computation table
 */
class LiquidLegionsMill(
  private val millId: String,
  private val storageClients: LiquidLegionsSketchAggregationComputationStorageClients,
  private val workerStubs: Map<String, ComputationControlServiceCoroutineStub>,
  private val throttler: Throttler
) {

  companion object {
    val logger: Logger = Logger.getLogger(this::class.java.name)
  }

  suspend fun processComputationQueue() {
    logger.info("Starting...")
    throttler.onReady {
      val claimWorkRequest = ClaimWorkRequest.newBuilder()
        .setComputationType(ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1)
        .setOwner(millId)
        .build()
      val claimWorkResponse: ClaimWorkResponse =
        // TODO: change the claimWork() to only return one computation
        storageClients.computationStorageClient.claimWork(claimWorkRequest)
      if (claimWorkResponse.tokenList.isNotEmpty()) {
        val token: ComputationToken = claimWorkResponse.getToken(0)
        when (token.computationStage.liquidLegionsSketchAggregation) {
          LiquidLegionsSketchAggregationStage.TO_CONFIRM_REQUISITIONS ->
            confirmRequisitions(token)
          LiquidLegionsSketchAggregationStage.TO_ADD_NOISE ->
            addNoise(token)
          LiquidLegionsSketchAggregationStage.TO_APPEND_SKETCHES_AND_ADD_NOISE ->
            appendSketchesAndAddNoise(token)
          LiquidLegionsSketchAggregationStage.TO_BLIND_POSITIONS_AND_JOIN_REGISTERS ->
            blindPositions(token)
          LiquidLegionsSketchAggregationStage.TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS ->
            decryptFlagCountsAndComputeMetrics(token)
          LiquidLegionsSketchAggregationStage.TO_DECRYPT_FLAG_COUNTS ->
            decryptFlagCounts(token)
          else -> error("Unexpected stage for mill to process: $token")
        }
      }
    }
  }

  /** Process computation in the TO_ADD_NOISE stage */
  private suspend fun confirmRequisitions(token: ComputationToken) {
    TODO("Not yet implemented")
  }

  /** Process computation in the TO_ADD_NOISE stage */
  private suspend fun addNoise(token: ComputationToken) {
    TODO("Not yet implemented")
  }

  /** Process computation in the TO_APPEND_SKETCHES_AND_ADD_NOISE stage */
  private suspend fun appendSketchesAndAddNoise(token: ComputationToken) {
    TODO("Not yet implemented")
  }

  /** Process computation in the TO_BLIND_POSITIONS stage */
  private suspend fun blindPositions(token: ComputationToken) {
    TODO("Not yet implemented")
  }

  /** Process computation in the TO_BLIND_POSITIONS_AND_JOIN_REGISTERS stage */
  private suspend fun blindPositionsAndJoinRegisters(token: ComputationToken) {
    TODO("Not yet implemented")
  }

  /** Process computation in the TO_DECRYPT_FLAG_COUNTS stage */
  private suspend fun decryptFlagCounts(token: ComputationToken) {
    TODO("Not yet implemented")
  }

  /** Process computation in the TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS stage */
  private suspend fun decryptFlagCountsAndComputeMetrics(token: ComputationToken) {
    TODO("Not yet implemented")
  }
}
