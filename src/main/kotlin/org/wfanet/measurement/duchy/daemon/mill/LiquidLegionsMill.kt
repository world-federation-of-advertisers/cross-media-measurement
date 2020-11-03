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

package org.wfanet.measurement.duchy.daemon.mill

import com.google.protobuf.ByteString
import io.grpc.Status
import io.grpc.StatusException
import java.lang.management.ManagementFactory
import java.lang.management.ThreadMXBean
import java.nio.file.Paths
import java.time.Clock
import java.time.Duration
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.system.measureTimeMillis
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.dropWhile
import kotlinx.coroutines.flow.flattenConcat
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.withContext
import org.wfanet.estimation.Estimators
import org.wfanet.measurement.common.crypto.AddNoiseToSketchRequest
import org.wfanet.measurement.common.crypto.AddNoiseToSketchResponse
import org.wfanet.measurement.common.crypto.BlindLastLayerIndexThenJoinRegistersRequest
import org.wfanet.measurement.common.crypto.BlindLastLayerIndexThenJoinRegistersResponse
import org.wfanet.measurement.common.crypto.BlindOneLayerRegisterIndexRequest
import org.wfanet.measurement.common.crypto.BlindOneLayerRegisterIndexResponse
import org.wfanet.measurement.common.crypto.DecryptLastLayerFlagAndCountRequest
import org.wfanet.measurement.common.crypto.DecryptLastLayerFlagAndCountResponse
import org.wfanet.measurement.common.crypto.DecryptOneLayerFlagAndCountRequest
import org.wfanet.measurement.common.crypto.DecryptOneLayerFlagAndCountResponse
import org.wfanet.measurement.common.crypto.ProtocolEncryption
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.loadLibrary
import org.wfanet.measurement.common.logAndSuppressExceptionSuspend
import org.wfanet.measurement.common.protoTimestamp
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.duchy.db.computation.BlobRef
import org.wfanet.measurement.duchy.db.computation.LiquidLegionsSketchAggregationComputationDataClients
import org.wfanet.measurement.duchy.db.computation.singleOutputBlobMetadata
import org.wfanet.measurement.duchy.mpcAlgorithm
import org.wfanet.measurement.duchy.name
import org.wfanet.measurement.duchy.number
import org.wfanet.measurement.duchy.service.internal.computation.outputPathList
import org.wfanet.measurement.duchy.service.system.v1alpha.ComputationControlRequests
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage as LiquidLegionsStage
import org.wfanet.measurement.internal.duchy.ClaimWorkRequest
import org.wfanet.measurement.internal.duchy.ClaimWorkResponse
import org.wfanet.measurement.internal.duchy.ComputationDetails.CompletedReason
import org.wfanet.measurement.internal.duchy.ComputationDetails.RoleInComputation
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.CreateComputationStatRequest
import org.wfanet.measurement.internal.duchy.EnqueueComputationRequest
import org.wfanet.measurement.internal.duchy.FinishComputationRequest
import org.wfanet.measurement.internal.duchy.GetComputationTokenRequest
import org.wfanet.measurement.internal.duchy.GetMetricValueRequest
import org.wfanet.measurement.internal.duchy.MetricValue.ResourceKey
import org.wfanet.measurement.internal.duchy.MetricValuesGrpcKt.MetricValuesCoroutineStub
import org.wfanet.measurement.internal.duchy.StreamMetricValueRequest
import org.wfanet.measurement.internal.duchy.ToConfirmRequisitionsStageDetails.RequisitionKey
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineStub
import org.wfanet.measurement.system.v1alpha.ConfirmGlobalComputationRequest
import org.wfanet.measurement.system.v1alpha.CreateGlobalComputationStatusUpdateRequest
import org.wfanet.measurement.system.v1alpha.FinishGlobalComputationRequest
import org.wfanet.measurement.system.v1alpha.GlobalComputationStatusUpdate.ErrorDetails.ErrorType
import org.wfanet.measurement.system.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineStub
import org.wfanet.measurement.system.v1alpha.MetricRequisitionKey

/**
 * Mill works on computations using the LiquidLegionSketchAggregationProtocol.
 *
 * @param millId The identifier of this mill, used to claim a work.
 * @param dataClients clients that have access to local computation storage, i.e., spanner
 *    table and blob store.
 * @param metricValuesClient client of the own duchy's MetricValuesService.
 * @param globalComputationsClient client of the kingdom's GlobalComputations.
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
  private val dataClients: LiquidLegionsSketchAggregationComputationDataClients,
  private val metricValuesClient: MetricValuesCoroutineStub,
  private val globalComputationsClient: GlobalComputationsCoroutineStub,
  private val computationStatsClient: ComputationStatsCoroutineStub,
  private val workerStubs: Map<String, ComputationControlCoroutineStub>,
  private val cryptoKeySet: CryptoKeySet,
  private val cryptoWorker: ProtocolEncryption,
  private val throttler: MinimumIntervalThrottler,
  chunkSize: Int = 1024 * 32, // 32 KiB
  private val liquidLegionsConfig: LiquidLegionsConfig = LiquidLegionsConfig(
    12.0,
    10_000_000L,
    10
  ),
  private val clock: Clock = Clock.systemUTC()
) {
  private val controlRequests = ComputationControlRequests(chunkSize)

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
      dataClients.computationsClient.claimWork(claimWorkRequest)
    if (claimWorkResponse.hasToken()) {
      val cpuTimeElapsedMillis = measureKotlinCpuTimeMillis {
        val wallTimeElapsedMills = measureTimeMillis {
          processNextComputation(claimWorkResponse.token)
        }
        logStageElapsedTime(claimWorkResponse.token, STAGE_WALL_CLOCK_TIME, wallTimeElapsedMills)
      }
      logStageElapsedTime(claimWorkResponse.token, STAGE_CPU_TIME, cpuTimeElapsedMillis)
    } else {
      logger.info("@Mill $millId: No computation available, waiting for the next poll...")
    }
  }

  // TODO: figure out if this is really the CPU time we want
  private fun getCpuTimeMillis(): Long {
    var cpuTime = Duration.ofNanos(threadBean.allThreadIds.map(threadBean::getThreadCpuTime).sum())
    return cpuTime.toMillis()
  }

  private suspend fun processNextComputation(token: ComputationToken) {
    val stage = token.computationStage.liquidLegionsSketchAggregation
    val globalId = token.globalComputationId
    logger.info("@Mill $millId: Processing computation $globalId, stage $stage")

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
      // The token version may have already changed. We need the latest token in order to complete
      // or enqueue the computation.
      val latestToken = getLatestComputationToken(globalId)
      when (e) {
        is IllegalStateException,
        is PermanentComputationError -> {
          logger.log(Level.SEVERE, "$globalId@$millId: PERMANENT error:", e)
          sendStatusUpdateToKingdom(
            newErrorUpdateRequest(latestToken, e.toString(), ErrorType.PERMANENT)
          )
          // Mark the computation FAILED for all permanent errors
          completeComputation(latestToken, CompletedReason.FAILED)
        }
        else -> {
          // Treat all other errors as transient.
          logger.log(Level.SEVERE, "$globalId@$millId: TRANSIENT error", e)
          sendStatusUpdateToKingdom(
            newErrorUpdateRequest(latestToken, e.toString(), ErrorType.TRANSIENT)
          )
          // Enqueue the computation again for future retry
          enqueueComputation(latestToken)
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
    val nextToken = dataClients.writeSingleOutputBlob(token, concatenatedContents)
    return dataClients.transitionComputationToStage(
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

  /** Processes computation in the TO_ADD_NOISE stage */
  private suspend fun addNoise(token: ComputationToken): ComputationToken {
    val (bytes, nextToken) = existingOutputOr(token) {
      val inputCount = when (token.role) {
        RoleInComputation.PRIMARY -> workerStubs.size + 1
        RoleInComputation.SECONDARY -> 1
        else -> error("Unknown role: ${token.role}")
      }
      val cryptoResult: AddNoiseToSketchResponse =
        cryptoWorker.addNoiseToSketch(
          // TODO: set other parameters when AddNoise actually adds noise.
          //  Now it just shuffle the registers.
          AddNoiseToSketchRequest.newBuilder()
            .setSketch(readAndCombineAllInputBlobs(token, inputCount))
            .build()
        )
      logStageElapsedTime(token, CRYPTO_LIB_CPU_TIME, cryptoResult.elapsedCpuTimeMillis)
      cryptoResult.sketch
    }

    when (token.role) {
      RoleInComputation.PRIMARY -> sendConcatenatedSketch(nextToken, bytes)
      RoleInComputation.SECONDARY -> sendNoisedSketch(nextToken, bytes)
      else -> error("Unknown role: ${token.role}")
    }

    return dataClients.transitionComputationToStage(
      nextToken,
      inputsToNextStage = nextToken.outputPathList(),
      stage = LiquidLegionsStage.WAIT_CONCATENATED
    )
  }

  /** Adds a logging hook to the flow to log the total number of bytes sent out in the rpc. */
  private fun addLoggingHook(
    token: ComputationToken,
    bytes: Flow<ByteString>
  ): Flow<ByteString> {
    var numOfBytes = 0
    return bytes.onEach { numOfBytes += it.size() }
      .onCompletion {
        logger.info(
          "@Mill $millId, ${token.globalComputationId}/${token.computationStage.name}, " +
            "send out rpc message with $numOfBytes bytes data."
        )
      }
  }

  /** Sends the merged sketch to the next duchy to start MPC round 1. */
  private suspend fun sendConcatenatedSketch(token: ComputationToken, bytes: Flow<ByteString>) {
    val requests =
      controlRequests.buildConcatenatedSketchRequests(
        token.globalComputationId, addLoggingHook(token, bytes)
      )
    nextDuchyStub(token).processConcatenatedSketch(requests)
  }

  /** Sends the noised sketch to the primary duchy. */
  private suspend fun sendNoisedSketch(token: ComputationToken, bytes: Flow<ByteString>) {
    val requests = controlRequests.buildNoisedSketchRequests(
      token.globalComputationId, addLoggingHook(token, bytes)
    )
    primaryDuchyStub(token).processNoisedSketch(requests)
  }

  /** Sends the encrypted flags and counts to the next Duchy. */
  private suspend fun sendEncryptedFlagsAndCounts(
    token: ComputationToken,
    bytes: Flow<ByteString>
  ) {
    val requests =
      controlRequests.buildEncryptedFlagsAndCountsRequests(
        token.globalComputationId, addLoggingHook(token, bytes)
      )
    nextDuchyStub(token).processEncryptedFlagsAndCounts(requests)
  }

  /** Processes computation in the TO_BLIND_POSITIONS stage */
  private suspend fun blindPositions(token: ComputationToken): ComputationToken {
    val (bytes, nextToken) = existingOutputOr(token) {
      val cryptoResult: BlindOneLayerRegisterIndexResponse =
        cryptoWorker.blindOneLayerRegisterIndex(
          BlindOneLayerRegisterIndexRequest.newBuilder()
            .setCompositeElGamalKeys(cryptoKeySet.clientPublicKey)
            .setCurveId(cryptoKeySet.curveId.toLong())
            .setLocalElGamalKeys(cryptoKeySet.ownPublicAndPrivateKeys)
            .setSketch(readAndCombineAllInputBlobs(token, 1))
            .build()
        )
      logStageElapsedTime(token, CRYPTO_LIB_CPU_TIME, cryptoResult.elapsedCpuTimeMillis)
      cryptoResult.sketch
    }

    // Passes the computation to the next duchy.
    sendConcatenatedSketch(nextToken, bytes)

    return dataClients.transitionComputationToStage(
      nextToken,
      inputsToNextStage = nextToken.outputPathList(),
      stage = LiquidLegionsStage.WAIT_FLAG_COUNTS
    )
  }

  /** Processes computation in the TO_BLIND_POSITIONS_AND_JOIN_REGISTERS stage */
  private suspend fun blindPositionsAndJoinRegisters(token: ComputationToken): ComputationToken {
    val (bytes, nextToken) = existingOutputOr(token) {
      val cryptoResult: BlindLastLayerIndexThenJoinRegistersResponse =
        cryptoWorker.blindLastLayerIndexThenJoinRegisters(
          BlindLastLayerIndexThenJoinRegistersRequest.newBuilder()
            .setCompositeElGamalKeys(cryptoKeySet.clientPublicKey)
            .setCurveId(cryptoKeySet.curveId.toLong())
            .setLocalElGamalKeys(cryptoKeySet.ownPublicAndPrivateKeys)
            .setSketch(readAndCombineAllInputBlobs(token, 1))
            .build()
        )
      logStageElapsedTime(token, CRYPTO_LIB_CPU_TIME, cryptoResult.elapsedCpuTimeMillis)
      cryptoResult.flagCounts
    }

    // Passes the computation to the next duchy.
    sendEncryptedFlagsAndCounts(nextToken, bytes)

    return dataClients.transitionComputationToStage(
      nextToken,
      inputsToNextStage = nextToken.outputPathList(),
      stage = LiquidLegionsStage.WAIT_FLAG_COUNTS
    )
  }

  /** Processes computation in the TO_DECRYPT_FLAG_COUNTS stage */
  private suspend fun decryptFlagCounts(token: ComputationToken): ComputationToken {
    val (bytes, nextToken) = existingOutputOr(token) {
      val cryptoResult: DecryptOneLayerFlagAndCountResponse =
        cryptoWorker.decryptOneLayerFlagAndCount(
          DecryptOneLayerFlagAndCountRequest.newBuilder()
            .setCurveId(cryptoKeySet.curveId.toLong())
            .setLocalElGamalKeys(cryptoKeySet.ownPublicAndPrivateKeys)
            .setFlagCounts(readAndCombineAllInputBlobs(token, 1))
            .build()
        )
      logStageElapsedTime(
        token,
        CRYPTO_LIB_CPU_TIME, cryptoResult.elapsedCpuTimeMillis
      )
      cryptoResult.flagCounts
    }

    // Passes the computation to the next duchy.
    sendEncryptedFlagsAndCounts(nextToken, bytes)

    // This duchy's responsibility for the computation is done. Mark it COMPLETED locally.
    return completeComputation(nextToken, CompletedReason.SUCCEEDED)
  }

  /** Processes computation in the TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS stage */
  private suspend fun decryptFlagCountsAndComputeMetrics(
    token: ComputationToken
  ): ComputationToken {
    val (bytes, nextToken) = existingOutputOr(token) {
      val cryptoResult: DecryptLastLayerFlagAndCountResponse =
        cryptoWorker.decryptLastLayerFlagAndCount(
          DecryptLastLayerFlagAndCountRequest.newBuilder()
            .setCurveId(cryptoKeySet.curveId.toLong())
            .setLocalElGamalKeys(cryptoKeySet.ownPublicAndPrivateKeys)
            .setFlagCounts(readAndCombineAllInputBlobs(token, 1))
            .setMaximumFrequency(liquidLegionsConfig.maxFrequency)
            .build()
        )
      logStageElapsedTime(token, CRYPTO_LIB_CPU_TIME, cryptoResult.elapsedCpuTimeMillis)
      cryptoResult.toByteString()
    }

    val flagCounts = DecryptLastLayerFlagAndCountResponse.parseFrom(bytes.flatten()).flagCountsList
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
        checkNotNull(dataClients.readSingleOutputBlob(token)),
        token
      )
    }
    val newResult: ByteString =
      try {
        val (timeElapsedMills, result) = measureTimeMillisAndReturnResult {
          block()
        }
        logStageElapsedTime(token, JNI_WALL_CLOCK_TIME, timeElapsedMills)
        result
      } catch (error: Throwable) {
        // All errors from block() are permanent and would cause the computation to FAIL
        throw PermanentComputationError(error)
      }
    return CachedResult(flowOf(newResult), dataClients.writeSingleOutputBlob(token, newResult))
  }

  private suspend fun readAndCombineAllInputBlobs(token: ComputationToken, count: Int): ByteString {
    val blobMap: Map<BlobRef, ByteString> = dataClients.readInputBlobs(token)
    if (blobMap.size != count) {
      throw PermanentComputationError(
        Exception("Unexpected number of input blobs. expected $count, actual ${blobMap.size}.")
      )
    }
    return blobMap.values.flatten()
  }

  private suspend fun completeComputation(
    token: ComputationToken,
    reason: CompletedReason
  ): ComputationToken {
    val response = dataClients.computationsClient.finishComputation(
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

  private suspend fun getLatestComputationToken(globalId: String): ComputationToken {
    return dataClients.computationsClient.getComputationToken(
      GetComputationTokenRequest.newBuilder().apply {
        computationType = ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1
        globalComputationId = globalId
      }.build()
    ).token
  }

  private suspend fun enqueueComputation(
    token: ComputationToken
  ) {
    dataClients.computationsClient.enqueueComputation(
      EnqueueComputationRequest.newBuilder()
        .setToken(token)
        .setDelaySecond(minOf(60, token.attempt * 5))
        .build()
    )
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

  private fun RequisitionKey.toMetricRequisitionKey(): MetricRequisitionKey {
    return MetricRequisitionKey.newBuilder()
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

  private fun nextDuchyStub(token: ComputationToken): ComputationControlCoroutineStub {
    val nextDuchy = token.nextDuchy
    return workerStubs[nextDuchy]
      ?: throw PermanentComputationError(
        Exception("No ComputationControlService stub for next duchy '$nextDuchy'")
      )
  }

  private fun primaryDuchyStub(token: ComputationToken): ComputationControlCoroutineStub {
    val primaryDuchy = token.primaryDuchy
    return workerStubs[primaryDuchy]
      ?: throw PermanentComputationError(
        Exception("No ComputationControlService stub for primary duchy '$primaryDuchy'")
      )
  }

  private suspend fun logStageElapsedTime(
    token: ComputationToken,
    description: String,
    elapsedMillis: Long
  ) {
    logger.info(
      "@Mill $millId, ${token.globalComputationId}/${token.computationStage.name}/$description:" +
        " ${elapsedMillis.toHumanFriendlyTime()}"
    )
    logAndSuppressExceptionSuspend {
      computationStatsClient.createComputationStat(
        CreateComputationStatRequest.newBuilder()
          .setLocalComputationId(token.localComputationId)
          .setGlobalComputationId(token.globalComputationId)
          .setAttempt(token.attempt)
          .setComputationStage(token.computationStage.number)
          .setRole(token.role)
          .setMetricName(description)
          .setMetricValue(elapsedMillis)
          .build()
      )
    }
  }

  private inline fun <T> measureKotlinCpuTimeMillis(block: () -> T): Long {
    val startCpuTimeMillis = getCpuTimeMillis()
    block()
    return getCpuTimeMillis() - startCpuTimeMillis
  }

  private inline fun <T> measureTimeMillisAndReturnResult(block: () -> T): Pair<Long, T> {
    val start = System.currentTimeMillis()
    val res = block()
    return Pair(System.currentTimeMillis() - start, res)
  }

  /**
   * Convert a milliseconds to a human friendly string
   */
  private fun Long.toHumanFriendlyTime(): String {
    val seconds = this / 1000
    val ms = this % 1000
    val hh = seconds / 3600
    val mm = (seconds % 3600) / 60
    val ss = seconds % 60
    val hoursString = if (hh == 0L) "" else "$hh hours "
    val minutesString = if (mm == 0L) "" else "$mm minutes "
    val secondsString = if (ss == 0L) "" else "$ss seconds "
    return "$hoursString$minutesString$secondsString$ms milliseconds"
  }

  private data class CachedResult(val bytes: Flow<ByteString>, val token: ComputationToken)

  data class LiquidLegionsConfig(val decayRate: Double, val size: Long, val maxFrequency: Int)

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val CRYPTO_LIB_CPU_TIME = "crypto_lib_cpu_time"
    private const val JNI_WALL_CLOCK_TIME = "jni_wall_clock_time"
    private const val STAGE_CPU_TIME = "stage_cpu_time"
    private const val STAGE_WALL_CLOCK_TIME = "stage_wall_clock_time"
    private val threadBean: ThreadMXBean = ManagementFactory.getThreadMXBean()

    init {
      loadLibrary(
        name = "estimators",
        directoryPath = Paths.get("any_sketch_java/src/main/java/org/wfanet/estimation")
      )
    }
  }

  class PermanentComputationError(cause: Throwable) : Exception(cause)
}
