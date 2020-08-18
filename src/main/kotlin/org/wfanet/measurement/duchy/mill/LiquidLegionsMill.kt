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

import com.google.protobuf.ByteString
import java.util.logging.Logger
import kotlinx.coroutines.flow.flowOf
import org.wfanet.measurement.common.MinimumIntervalThrottler
import org.wfanet.measurement.common.logAndSuppressExceptionSuspend
import org.wfanet.measurement.db.duchy.computation.LiquidLegionsSketchAggregationComputationStorageClients
import org.wfanet.measurement.db.duchy.computation.singleOutputBlobMetadata
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage
import org.wfanet.measurement.internal.duchy.BlindOneLayerRegisterIndexRequest
import org.wfanet.measurement.internal.duchy.ClaimWorkRequest
import org.wfanet.measurement.internal.duchy.ClaimWorkResponse
import org.wfanet.measurement.internal.duchy.ComputationControlServiceGrpcKt.ComputationControlServiceCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.HandleConcatenatedSketchRequest
import org.wfanet.measurement.service.internal.duchy.computation.storage.outputPathList

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
  private val cryptoKeySet: CryptoKeySet,
  private val cryptoWorker: LiquidLegionsCryptoWorker,
  private val throttler: MinimumIntervalThrottler
) {
  companion object {
    val logger: Logger = Logger.getLogger(this::class.java.name)
  }

  suspend fun continuallyProcessComputationQueue() {
    logger.info("Starting...")
    logAndSuppressExceptionSuspend { throttler.onReady { pollAndProcessNextComputation() } }
  }

  suspend fun pollAndProcessNextComputation() {
    val claimWorkRequest = ClaimWorkRequest.newBuilder()
      .setComputationType(ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1)
      .setOwner(millId)
      .build()
    val claimWorkResponse: ClaimWorkResponse =
      storageClients.computationStorageClient.claimWork(claimWorkRequest)
    if (claimWorkResponse.hasToken()) {
      val token: ComputationToken = claimWorkResponse.token
      when (token.computationStage.liquidLegionsSketchAggregation) {
        LiquidLegionsSketchAggregationStage.TO_CONFIRM_REQUISITIONS ->
          confirmRequisitions()
        LiquidLegionsSketchAggregationStage.TO_ADD_NOISE ->
          addNoise()
        LiquidLegionsSketchAggregationStage.TO_APPEND_SKETCHES_AND_ADD_NOISE ->
          appendSketchesAndAddNoise()
        LiquidLegionsSketchAggregationStage.TO_BLIND_POSITIONS ->
          blindPositions(token)
        LiquidLegionsSketchAggregationStage.TO_BLIND_POSITIONS_AND_JOIN_REGISTERS ->
          blindPositionsAndJoinRegisters()
        LiquidLegionsSketchAggregationStage.TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS ->
          decryptFlagCountsAndComputeMetrics()
        LiquidLegionsSketchAggregationStage.TO_DECRYPT_FLAG_COUNTS ->
          decryptFlagCounts()
        else -> error("Unexpected stage for mill to process: $token")
      }
    }
  }

  /** Process computation in the TO_ADD_NOISE stage */
  private suspend fun confirmRequisitions() {
    TODO("Not yet implemented")
  }

  /** Process computation in the TO_ADD_NOISE stage */
  private suspend fun addNoise() {
    TODO("Not yet implemented")
  }

  /** Process computation in the TO_APPEND_SKETCHES_AND_ADD_NOISE stage */
  private suspend fun appendSketchesAndAddNoise() {
    TODO("Not yet implemented")
  }

  /** Process computation in the TO_BLIND_POSITIONS stage */
  private suspend fun blindPositions(token: ComputationToken): ComputationToken {
    // TODO: Catch permanent errors and terminate the Computation

    val cachedResult: CachedResult =
      if (token.singleOutputBlobMetadata().path.isNotEmpty()) {
        // Reuse cached result if it exists
        CachedResult(ByteString.copyFrom(storageClients.readSingleOutputBlob(token)), token)
      } else {
        // compute a new result if no cache exists
        val newResult = cryptoWorker.BlindOneLayerRegisterIndex(
          BlindOneLayerRegisterIndexRequest.newBuilder()
            .setCompositeElGamalKeys(cryptoKeySet.clientPublicKey)
            .setCurveId(cryptoKeySet.curveId)
            .setLocalElGamalKeys(cryptoKeySet.ownPublicAndPrivateKeys)
            .setSketch(readAndCombineAllInputBlobs(token, 1))
            .build()
        ).sketch
        // cache the newly computed results
        val newToken = storageClients.writeSingleOutputBlob(token, newResult.toByteArray())
        CachedResult(newResult, newToken)
      }

    // Pass the computation to the next duchy.
    // TODO: partition the result to chunks to send
    val requestToNextDuchy = HandleConcatenatedSketchRequest.newBuilder()
      .setComputationId(cachedResult.token.globalComputationId)
      .setPartialSketch(cachedResult.data)
      .build()

    (workerStubs[cachedResult.token.nextDuchy] ?: error("Cannot find the target of the next duchy"))
      .handleConcatenatedSketch(
        flowOf(
          requestToNextDuchy
        )
      )
    return storageClients.transitionComputationToStage(
      cachedResult.token,
      inputsToNextStage = cachedResult.token.outputPathList(),
      stage = LiquidLegionsSketchAggregationStage.WAIT_FLAG_COUNTS
    )
  }

  /** Process computation in the TO_BLIND_POSITIONS_AND_JOIN_REGISTERS stage */
  private suspend fun blindPositionsAndJoinRegisters() {
    TODO("Not yet implemented")
  }

  /** Process computation in the TO_DECRYPT_FLAG_COUNTS stage */
  private suspend fun decryptFlagCounts() {
    TODO("Not yet implemented")
  }

  /** Process computation in the TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS stage */
  private suspend fun decryptFlagCountsAndComputeMetrics() {
    TODO("Not yet implemented")
  }

  private suspend fun readAndCombineAllInputBlobs(token: ComputationToken, count: Int): ByteString {
    val blobMap = storageClients.readInputBlobs(token)
    require(blobMap.size == count) {
      "Unexpected number of input blobs. expected $count, actual ${blobMap.size}."
    }
    return ByteString.copyFrom(blobMap.values.fold(ByteArray(0)) { acc, e -> acc.plus(e) })
  }

  private data class CachedResult(val data: ByteString, val token: ComputationToken)
}
