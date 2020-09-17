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
import io.grpc.Status
import io.grpc.StatusException
import java.nio.file.Paths
import java.time.Clock
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.dropWhile
import kotlinx.coroutines.flow.flattenConcat
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEmpty
import kotlinx.coroutines.withContext
import org.wfanet.estimation.Estimators
import org.wfanet.measurement.api.v1alpha.ConfirmGlobalComputationRequest
import org.wfanet.measurement.api.v1alpha.CreateGlobalComputationStatusUpdateRequest
import org.wfanet.measurement.api.v1alpha.FinishGlobalComputationRequest
import org.wfanet.measurement.api.v1alpha.GlobalComputationStatusUpdate.ErrorDetails.ErrorType
import org.wfanet.measurement.api.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineStub
import org.wfanet.measurement.api.v1alpha.MetricRequisition
import org.wfanet.measurement.common.MinimumIntervalThrottler
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.loadLibrary
import org.wfanet.measurement.common.protoTimestamp
import org.wfanet.measurement.db.duchy.computation.BlobRef
import org.wfanet.measurement.db.duchy.computation.LiquidLegionsSketchAggregationComputationStorageClients
import org.wfanet.measurement.db.duchy.computation.singleOutputBlobMetadata
import org.wfanet.measurement.duchy.mpcAlgorithm
import org.wfanet.measurement.duchy.name
import org.wfanet.measurement.duchy.number
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage as LiquidLegionsStage
import org.wfanet.measurement.internal.duchy.AddNoiseToSketchRequest
import org.wfanet.measurement.internal.duchy.BlindLastLayerIndexThenJoinRegistersRequest
import org.wfanet.measurement.internal.duchy.BlindOneLayerRegisterIndexRequest
import org.wfanet.measurement.internal.duchy.ClaimWorkRequest
import org.wfanet.measurement.internal.duchy.ClaimWorkResponse
import org.wfanet.measurement.internal.duchy.ComputationControlServiceGrpcKt.ComputationControlServiceCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationDetails.CompletedReason
import org.wfanet.measurement.internal.duchy.ComputationDetails.RoleInComputation
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.DecryptLastLayerFlagAndCountRequest
import org.wfanet.measurement.internal.duchy.DecryptLastLayerFlagAndCountResponse
import org.wfanet.measurement.internal.duchy.DecryptOneLayerFlagAndCountRequest
import org.wfanet.measurement.internal.duchy.FinishComputationRequest
import org.wfanet.measurement.internal.duchy.GetMetricValueRequest
import org.wfanet.measurement.internal.duchy.HandleConcatenatedSketchRequest
import org.wfanet.measurement.internal.duchy.HandleEncryptedFlagsAndCountsRequest
import org.wfanet.measurement.internal.duchy.HandleNoisedSketchRequest
import org.wfanet.measurement.internal.duchy.MetricValue.ResourceKey
import org.wfanet.measurement.internal.duchy.MetricValuesGrpcKt.MetricValuesCoroutineStub
import org.wfanet.measurement.internal.duchy.StreamMetricValueRequest
import org.wfanet.measurement.internal.duchy.ToConfirmRequisitionsStageDetails.RequisitionKey
import org.wfanet.measurement.service.internal.duchy.computation.storage.outputPathList

/**
 * Mill works on computations using the LiquidLegionSketchAggregationProtocol.
 *
 * @param millId The identifier of this mill, used to claim a work.
 * @param storageClients clients that have access to local computation storage, i.e., spanner
 *    table and blob store.
 * @param metricValuesClient client of the own duchy's MetricValuesService.
 * @param globalComputationsClient client of the kingdom's GlobalComputationsService.
 * @param workerStubs A map from other duchies' Ids to their corresponding
 *    computationControlClients, used for passing computation to other duchies.
 * @param cryptoKeySet The set of crypto keys used in the computation.
 * @param cryptoWorker The cryptoWorker that performs the actual computation.
 * @param throttler A throttler used to rate limit the frequency of the mill polling from the
 *    computation table.
 * @param chunkSize The size of data chunk when sending result to other duchies.
 * @param liquidLegionsConfig The configuration of the LiquidLegions sketch.
 * @param clock A clock
 */
class LiquidLegionsMill(
  private val millId: String,
  private val storageClients: LiquidLegionsSketchAggregationComputationStorageClients,
  private val metricValuesClient: MetricValuesCoroutineStub,
  private val globalComputationsClient: GlobalComputationsCoroutineStub,
  private val workerStubs: Map<String, ComputationControlServiceCoroutineStub>,
  private val cryptoKeySet: CryptoKeySet,
  private val cryptoWorker: LiquidLegionsCryptoWorker,
  private val throttler: MinimumIntervalThrottler,
  private val chunkSize: Int = 2_000_000,
  private val liquidLegionsConfig: LiquidLegionsConfig = LiquidLegionsConfig(12.0, 10_000_000L, 10),
  private val clock: Clock = Clock.systemUTC()
) {
  suspend fun continuallyProcessComputationQueue() {
    logger.info("Starting...")
    withContext(CoroutineName("Mill $millId")) {
      throttler.loopOnReady {
        pollAndProcessNextComputation()
      }
    }
  }

  suspend fun pollAndProcessNextComputation() {
    logger.info("@Mill $millId: Polling available computations...")
    val claimWorkRequest = ClaimWorkRequest.newBuilder()
      .setComputationType(ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1)
      .setOwner(millId)
      .build()
    val claimWorkResponse: ClaimWorkResponse =
      storageClients.computationStorageClient.claimWork(claimWorkRequest)
    if (claimWorkResponse.hasToken()) {
      processNextComputation(claimWorkResponse.token)
    } else {
      logger.info("@Mill $millId: No computation available, waiting for the next poll...")
    }
  }

  private suspend fun processNextComputation(token: ComputationToken) {
    val stage = token.computationStage.liquidLegionsSketchAggregation
    logger.info("@Mill $millId: Processing computation ${token.globalComputationId}, stage $stage")
    try {
      when (token.computationStage.liquidLegionsSketchAggregation) {
        LiquidLegionsStage.TO_CONFIRM_REQUISITIONS ->
          confirmRequisitions(token)
        LiquidLegionsStage.TO_ADD_NOISE,
        LiquidLegionsStage.TO_APPEND_SKETCHES_AND_ADD_NOISE ->
          addNoise(token)
        LiquidLegionsStage.TO_BLIND_POSITIONS ->
          blindPositions(token)
        LiquidLegionsStage.TO_BLIND_POSITIONS_AND_JOIN_REGISTERS ->
          blindPositionsAndJoinRegisters(token)
        LiquidLegionsStage.TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS ->
          decryptFlagCountsAndComputeMetrics(token)
        LiquidLegionsStage.TO_DECRYPT_FLAG_COUNTS ->
          decryptFlagCounts(token)
        else -> error("Unexpected stage: $stage")
      }
    } catch (e: Exception) {
      when (e) {
        is IllegalStateException,
        is PermanentComputationError -> {
          logger.log(Level.SEVERE, "${token.globalComputationId}@$millId: PERMANENT error:", e)
          sendStatusUpdateToKingdom(newErrorUpdateRequest(token, e.toString(), ErrorType.PERMANENT))
          // Mark the computation FAILED for all permanent errors
          completeComputation(token, CompletedReason.FAILED)
        }
        else -> {
          // Treat all other errors as transient.
          logger.log(Level.SEVERE, "${token.globalComputationId}@$millId: TRANSIENT error", e)
          sendStatusUpdateToKingdom(newErrorUpdateRequest(token, e.toString(), ErrorType.TRANSIENT))
        }
      }
    }
  }

  /** Process computation in the TO_CONFIRM_REQUISITIONS stage */
  @OptIn(FlowPreview::class) // For `flattenConcat`.
  private suspend fun confirmRequisitions(token: ComputationToken): ComputationToken {
    val requisitionsToConfirm =
      token.stageSpecificDetails.toConfirmRequisitionsStageDetails.keysList
    val availableRequisitions = requisitionsToConfirm.filter { metricValueExists(it) }

    globalComputationsClient.confirmGlobalComputation(
      ConfirmGlobalComputationRequest.newBuilder().apply {
        addAllReadyRequisitions(availableRequisitions.map { it.toMetricRequisitionKey() })
        keyBuilder.globalComputationId = token.globalComputationId.toString()
      }.build()
    )

    if (availableRequisitions.size != requisitionsToConfirm.size) {
      val errorMessage =
        """
        @Mill $millId:
          Computation ${token.globalComputationId} failed due to missing requisitions.
        Expected:
          $requisitionsToConfirm
        Actual:
          $availableRequisitions
        """.trimIndent()
      throw PermanentComputationError(Exception(errorMessage))
    }

    // cache the combined local requisitions to blob store.
    val concatenatedContents = streamMetricValueContents(availableRequisitions).flattenConcat()
    val nextToken = storageClients.writeSingleOutputBlob(token, concatenatedContents)
    return storageClients.transitionComputationToStage(
      nextToken,
      inputsToNextStage = nextToken.outputPathList(),
      stage = when (checkNotNull(nextToken.role)) {
        RoleInComputation.PRIMARY -> LiquidLegionsStage.WAIT_SKETCHES
        RoleInComputation.SECONDARY -> LiquidLegionsStage.WAIT_TO_START
        RoleInComputation.UNKNOWN,
        RoleInComputation.UNRECOGNIZED -> error("Unknown role: ${nextToken.role}")
      }
    )
  }

  /**
   * Returns whether a value exists for the requisition by calling the
   * MetricValues service.
   */
  private suspend fun metricValueExists(key: RequisitionKey): Boolean {
    return try {
      metricValuesClient.getMetricValue(
        GetMetricValueRequest.newBuilder()
          .setResourceKey(key.toResourceKey())
          .build()
      )
      true
    } catch (e: StatusException) {
      when (e.status.code) {
        Status.Code.NOT_FOUND -> false
        else -> throw e
      }
    }
  }

  private fun streamMetricValueContents(
    availableRequisitions: Iterable<RequisitionKey>
  ): Flow<Flow<ByteString>> = flow {
    for (requisitionKey in availableRequisitions) {
      val responses = metricValuesClient.streamMetricValue(
        StreamMetricValueRequest.newBuilder()
          .setResourceKey(requisitionKey.toResourceKey())
          .build()
      )
      emit(responses.dropWhile { it.hasHeader() }.map { it.chunk.data })
    }
  }

  /** Process computation in the TO_ADD_NOISE stage */
  private suspend fun addNoise(token: ComputationToken): ComputationToken {
    val (bytes, nextToken) = existingOutputOr(token) {
      val inputCount = when (token.role) {
        RoleInComputation.PRIMARY -> workerStubs.size + 1
        RoleInComputation.SECONDARY -> 1
        else -> error("Unknown role: ${token.role}")
      }
      cryptoWorker.addNoiseToSketch(
        // TODO: set other parameters when AddNoise actually adds noise.
        //  Now it just shuffle the registers.
        AddNoiseToSketchRequest.newBuilder()
          .setSketch(readAndCombineAllInputBlobs(token, inputCount))
          .build()
      ).sketch
    }

    val bytesFlow = bytes.chunkedFlow()
    when (token.role) {
      RoleInComputation.PRIMARY -> sendConcatenatedSketch(nextToken, bytesFlow)
      RoleInComputation.SECONDARY -> sendNoisedSketch(nextToken, bytesFlow)
      else -> error("Unknown role: ${token.role}")
    }

    return storageClients.transitionComputationToStage(
      nextToken,
      inputsToNextStage = nextToken.outputPathList(),
      stage = LiquidLegionsStage.WAIT_CONCATENATED
    )
  }

  /** Send the merged sketch to the next duchy to start MPC round 1. */
  private suspend fun sendConcatenatedSketch(token: ComputationToken, bytes: Flow<ByteString>) {
    nextDuchyStub(token)
      .handleConcatenatedSketch(
        bytes.map {
          HandleConcatenatedSketchRequest.newBuilder()
            .setComputationId(token.globalComputationId)
            .setPartialSketch(it)
            .build()
        }
      )
  }

  /** Send the noised sketch to the primary duchy. */
  private suspend fun sendNoisedSketch(token: ComputationToken, bytes: Flow<ByteString>) {
    primaryDuchyStub(token)
      .handleNoisedSketch(
        bytes.map {
          HandleNoisedSketchRequest.newBuilder()
            .setComputationId(token.globalComputationId)
            .setPartialSketch(it)
            .build()
        }
      )
  }

  /** Process computation in the TO_BLIND_POSITIONS stage */
  private suspend fun blindPositions(token: ComputationToken): ComputationToken {
    val (bytes, nextToken) = existingOutputOr(token) {
      cryptoWorker.blindOneLayerRegisterIndex(
        BlindOneLayerRegisterIndexRequest.newBuilder()
          .setCompositeElGamalKeys(cryptoKeySet.clientPublicKey)
          .setCurveId(cryptoKeySet.curveId.toLong())
          .setLocalElGamalKeys(cryptoKeySet.ownPublicAndPrivateKeys)
          .setSketch(readAndCombineAllInputBlobs(token, 1))
          .build()
      ).sketch
    }

    // Pass the computation to the next duchy.
    nextDuchyStub(nextToken)
      .handleConcatenatedSketch(
        bytes.chunkedFlow()
          .map {
            HandleConcatenatedSketchRequest.newBuilder()
              .setComputationId(nextToken.globalComputationId)
              .setPartialSketch(it)
              .build()
          }
      )
    return storageClients.transitionComputationToStage(
      nextToken,
      inputsToNextStage = nextToken.outputPathList(),
      stage = LiquidLegionsStage.WAIT_FLAG_COUNTS
    )
  }

  /** Process computation in the TO_BLIND_POSITIONS_AND_JOIN_REGISTERS stage */
  private suspend fun blindPositionsAndJoinRegisters(token: ComputationToken): ComputationToken {
    val (bytes, nextToken) = existingOutputOr(token) {
      cryptoWorker.blindLastLayerIndexThenJoinRegisters(
        BlindLastLayerIndexThenJoinRegistersRequest.newBuilder()
          .setCompositeElGamalKeys(cryptoKeySet.clientPublicKey)
          .setCurveId(cryptoKeySet.curveId.toLong())
          .setLocalElGamalKeys(cryptoKeySet.ownPublicAndPrivateKeys)
          .setSketch(readAndCombineAllInputBlobs(token, 1))
          .build()
      ).flagCounts
    }

    // Pass the computation to the next duchy.
    nextDuchyStub(nextToken)
      .handleEncryptedFlagsAndCounts(
        bytes.chunkedFlow()
          .map {
            HandleEncryptedFlagsAndCountsRequest.newBuilder()
              .setComputationId(nextToken.globalComputationId)
              .setPartialData(it)
              .build()
          }
      )
    return storageClients.transitionComputationToStage(
      nextToken,
      inputsToNextStage = nextToken.outputPathList(),
      stage = LiquidLegionsStage.WAIT_FLAG_COUNTS
    )
  }

  /** Process computation in the TO_DECRYPT_FLAG_COUNTS stage */
  private suspend fun decryptFlagCounts(token: ComputationToken): ComputationToken {
    val (bytes, nextToken) = existingOutputOr(token) {
      cryptoWorker.decryptOneLayerFlagAndCount(
        DecryptOneLayerFlagAndCountRequest.newBuilder()
          .setCurveId(cryptoKeySet.curveId.toLong())
          .setLocalElGamalKeys(cryptoKeySet.ownPublicAndPrivateKeys)
          .setFlagCounts(readAndCombineAllInputBlobs(token, 1))
          .build()
      ).flagCounts
    }

    // Pass the computation to the next duchy.
    nextDuchyStub(nextToken)
      .handleEncryptedFlagsAndCounts(
        bytes.chunkedFlow()
          .map {
            HandleEncryptedFlagsAndCountsRequest.newBuilder()
              .setComputationId(nextToken.globalComputationId)
              .setPartialData(it)
              .build()
          }
      )
    // This duchy's responsibility for the computation is done. Mark it COMPLETED locally.
    return completeComputation(nextToken, CompletedReason.SUCCEEDED)
  }

  /** Process computation in the TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS stage */
  private suspend fun decryptFlagCountsAndComputeMetrics(
    token: ComputationToken
  ): ComputationToken {
    val (bytes, nextToken) = existingOutputOr(token) {
      cryptoWorker.decryptLastLayerFlagAndCount(
        DecryptLastLayerFlagAndCountRequest.newBuilder()
          .setCurveId(cryptoKeySet.curveId.toLong())
          .setLocalElGamalKeys(cryptoKeySet.ownPublicAndPrivateKeys)
          .setFlagCounts(readAndCombineAllInputBlobs(token, 1))
          .setMaximumFrequency(liquidLegionsConfig.maxFrequency)
          .build()
      ).toByteString()
    }

    val flagCounts = DecryptLastLayerFlagAndCountResponse.parseFrom(bytes).flagCountsList
    val frequencyHistogram: Map<Long, Long> = flagCounts
      .filter { it.isNotDestroyed }
      .groupBy { it.frequency }
      .entries
      .associate { it.key.toLong() to it.value.size.toLong() }
    val cardinality: Long = Estimators.EstimateCardinalityLiquidLegions(
      liquidLegionsConfig.decayRate,
      liquidLegionsConfig.size,
      flagCounts.size.toLong()
    )
    globalComputationsClient.finishGlobalComputation(
      FinishGlobalComputationRequest.newBuilder().apply {
        keyBuilder.globalComputationId = token.globalComputationId.toString()
        resultBuilder.apply {
          reach = cardinality
          putAllFrequency(frequencyHistogram)
        }
      }.build()
    )

    return completeComputation(nextToken, CompletedReason.SUCCEEDED)
  }

  private suspend fun existingOutputOr(
    token: ComputationToken,
    block: suspend () -> ByteString
  ): CachedResult {
    if (token.singleOutputBlobMetadata().path.isNotEmpty()) {
      // Reuse cached result if it exists
      return CachedResult(
        checkNotNull(storageClients.readSingleOutputBlob(token)).flatten(),
        token
      )
    }
    val newResult: ByteString =
      try {
        block()
      } catch (error: Throwable) {
        // All errors from block() are permanent and would cause the computation to FAIL
        throw PermanentComputationError(error)
      }
    return CachedResult(newResult, storageClients.writeSingleOutputBlob(token, newResult))
  }

  private suspend fun readAndCombineAllInputBlobs(token: ComputationToken, count: Int): ByteString {
    val blobMap: Map<BlobRef, ByteString> = storageClients.readInputBlobs(token)
    if (blobMap.size != count) {
      throw PermanentComputationError(
        Exception("Unexpected number of input blobs. expected $count, actual ${blobMap.size}.")
      )
    }
    return blobMap.values.flatten()
  }

  /** Partition a [ByteString] to a [Flow] of chunks of size at most [chunkSize]. */
  @OptIn(ExperimentalCoroutinesApi::class) // For `onEmpty`.
  private fun ByteString.chunkedFlow(): Flow<ByteString> {
    return asBufferedFlow(chunkSize).onEmpty { emit(ByteString.EMPTY) }
  }

  private suspend fun completeComputation(
    token: ComputationToken,
    reason: CompletedReason
  ): ComputationToken {
    val response = storageClients.computationStorageClient.finishComputation(
      FinishComputationRequest.newBuilder()
        .setToken(token)
        .setEndingComputationStage(
          ComputationStage.newBuilder()
            .setLiquidLegionsSketchAggregation(LiquidLegionsStage.COMPLETED)
        )
        .setReason(reason)
        .build()
    )
    return response.token
  }

  private suspend fun sendStatusUpdateToKingdom(
    request: CreateGlobalComputationStatusUpdateRequest
  ) {
    try {
      globalComputationsClient.createGlobalComputationStatusUpdate(request)
    } catch (ignored: Exception) {
      logger.warning("Failed to update status change to the kingdom. $ignored")
    }
  }

  private fun RequisitionKey.toResourceKey(): ResourceKey {
    return ResourceKey.newBuilder().apply {
      dataProviderResourceId = dataProviderId
      campaignResourceId = campaignId
      metricRequisitionResourceId = metricRequisitionId
    }.build()
  }

  private fun RequisitionKey.toMetricRequisitionKey(): MetricRequisition.Key {
    return MetricRequisition.Key.newBuilder()
      .setCampaignId(campaignId)
      .setDataProviderId(dataProviderId)
      .setMetricRequisitionId(metricRequisitionId)
      .build()
  }

  private fun newErrorUpdateRequest(
    token: ComputationToken,
    message: String,
    type: ErrorType
  ): CreateGlobalComputationStatusUpdateRequest {
    return CreateGlobalComputationStatusUpdateRequest.newBuilder().apply {
      parentBuilder.globalComputationId = token.globalComputationId
      statusUpdateBuilder.apply {
        selfReportedIdentifier = millId
        stageDetailsBuilder.apply {
          algorithm = token.computationStage.mpcAlgorithm
          stageNumber = token.computationStage.number.toLong()
          stageName = token.computationStage.name
          start = clock.protoTimestamp()
          attemptNumber = token.attempt.toLong()
        }
        updateMessage = "Computation ${token.globalComputationId} at stage " +
          "${token.computationStage.name}, attempt ${token.attempt} failed."
        errorDetailsBuilder.apply {
          errorTime = clock.protoTimestamp()
          errorType = type
          errorMessage = "${token.globalComputationId}@$millId: $message"
        }
      }
    }.build()
  }

  private fun nextDuchyStub(token: ComputationToken): ComputationControlServiceCoroutineStub {
    val nextDuchy = token.nextDuchy
    return workerStubs[nextDuchy]
      ?: throw PermanentComputationError(
        Exception("No ComputationControlService stub for next duchy '$nextDuchy'")
      )
  }

  private fun primaryDuchyStub(token: ComputationToken): ComputationControlServiceCoroutineStub {
    val primaryDuchy = token.primaryDuchy
    return workerStubs[primaryDuchy]
      ?: throw PermanentComputationError(
        Exception("No ComputationControlService stub for primary duchy '$primaryDuchy'")
      )
  }

  private data class CachedResult(val bytes: ByteString, val token: ComputationToken)

  data class LiquidLegionsConfig(val decayRate: Double, val size: Long, val maxFrequency: Int)

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    init {
      loadLibrary(
        name = "estimators",
        directoryPath = Paths.get("any_sketch/src/main/java/org/wfanet/estimation")
      )
    }
  }

  class PermanentComputationError(cause: Throwable) : Exception(cause)
}
