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

package org.wfanet.measurement.duchy.daemon.mill.liquidlegionsv1

import java.nio.file.Paths
import java.time.Clock
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.flattenConcat
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
import org.wfanet.measurement.common.crypto.liquidlegionsv1.LiquidLegionsV1Encryption
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.loadLibrary
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.duchy.daemon.mill.CRYPTO_LIB_CPU_DURATION
import org.wfanet.measurement.duchy.daemon.mill.CryptoKeySet
import org.wfanet.measurement.duchy.daemon.mill.LiquidLegionsConfig
import org.wfanet.measurement.duchy.daemon.mill.MillBase
import org.wfanet.measurement.duchy.daemon.mill.PermanentComputationError
import org.wfanet.measurement.duchy.daemon.mill.toMetricRequisitionKey
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients
import org.wfanet.measurement.duchy.service.internal.computation.outputPathList
import org.wfanet.measurement.duchy.service.system.v1alpha.advanceComputationHeader
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ComputationDetails.CompletedReason
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.MetricValuesGrpcKt.MetricValuesCoroutineStub
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1.ComputationDetails.RoleInComputation
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1.Stage
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineStub
import org.wfanet.measurement.system.v1alpha.ConfirmGlobalComputationRequest
import org.wfanet.measurement.system.v1alpha.FinishGlobalComputationRequest
import org.wfanet.measurement.system.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineStub
import org.wfanet.measurement.system.v1alpha.LiquidLegionsV1

/**
 * Mill works on computations using the LiquidLegionSketchAggregationProtocol.
 *
 * @param millId The identifier of this mill, used to claim a work.
 * @param dataClients clients that have access to local computation storage, i.e., spanner
 *    table and blob store.
 * @param metricValuesClient client of the own duchy's MetricValuesService.
 * @param globalComputationsClient client of the kingdom's GlobalComputationsService.
 * @param computationStatsClient client of the duchy's ComputationStatsService.
 * @param throttler A throttler used to rate limit the frequency of the mill polling from the
 *    computation table.
 * @param requestChunkSizeBytes The size of data chunk when sending result to other duchies.
 * @param clock A clock
 * @param workerStubs A map from other duchies' Ids to their corresponding
 *    computationControlClients, used for passing computation to other duchies.
 * @param cryptoKeySet The set of crypto keys used in the computation.
 * @param cryptoWorker The cryptoWorker that performs the actual computation.
 * @param liquidLegionsConfig The configuration of the LiquidLegions sketch.
 */
class LiquidLegionsV1Mill(
  millId: String,
  dataClients: ComputationDataClients,
  metricValuesClient: MetricValuesCoroutineStub,
  globalComputationsClient: GlobalComputationsCoroutineStub,
  computationStatsClient: ComputationStatsCoroutineStub,
  throttler: MinimumIntervalThrottler,
  requestChunkSizeBytes: Int = 1024 * 32, // 32 KiB
  clock: Clock = Clock.systemUTC(),
  private val workerStubs: Map<String, ComputationControlCoroutineStub>,
  private val cryptoKeySet: CryptoKeySet,
  private val cryptoWorker: LiquidLegionsV1Encryption,
  private val liquidLegionsConfig: LiquidLegionsConfig = LiquidLegionsConfig(
    12.0,
    10_000_000L,
    10
  )
) : MillBase(
  millId,
  dataClients,
  globalComputationsClient,
  metricValuesClient,
  computationStatsClient,
  throttler,
  ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1,
  requestChunkSizeBytes,
  clock
) {

  override val endingStage: ComputationStage = ComputationStage.newBuilder().apply {
    liquidLegionsSketchAggregationV1 = Stage.COMPLETED
  }.build()

  override suspend fun processComputationImpl(token: ComputationToken) {
    require(token.computationDetails.hasLiquidLegionsV1()) {
      "Only Liquid Legions V1 computation is supported in this mill."
    }

    when (token.computationStage.liquidLegionsSketchAggregationV1) {
      Stage.TO_CONFIRM_REQUISITIONS ->
        confirmRequisitions(token)
      Stage.TO_ADD_NOISE,
      Stage.TO_APPEND_SKETCHES_AND_ADD_NOISE ->
        addNoise(token)
      Stage.TO_BLIND_POSITIONS ->
        blindPositions(token)
      Stage.TO_BLIND_POSITIONS_AND_JOIN_REGISTERS ->
        blindPositionsAndJoinRegisters(token)
      Stage.TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS ->
        decryptFlagCountsAndComputeMetrics(token)
      Stage.TO_DECRYPT_FLAG_COUNTS ->
        decryptFlagCounts(token)
      else -> error("Unexpected stage: ${token.computationStage.liquidLegionsSketchAggregationV1}")
    }
  }

  /** Process computation in the TO_CONFIRM_REQUISITIONS stage */
  @OptIn(FlowPreview::class) // For `flattenConcat`.
  private suspend fun confirmRequisitions(token: ComputationToken): ComputationToken {
    val requisitionsToConfirm =
      token.stageSpecificDetails.liquidLegionsV1.toConfirmRequisitionsStageDetails.keysList
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
      passThroughBlobs = nextToken.outputPathList(),
      stage = when (checkNotNull(nextToken.computationDetails.liquidLegionsV1.role)) {
        RoleInComputation.PRIMARY -> Stage.WAIT_SKETCHES.toProtocolStage()
        RoleInComputation.SECONDARY -> Stage.WAIT_TO_START.toProtocolStage()
        RoleInComputation.UNKNOWN,
        RoleInComputation.UNRECOGNIZED ->
          error("Unknown role: ${nextToken.computationDetails.liquidLegionsV1.role}")
      }
    )
  }

  /** Processes computation in the TO_ADD_NOISE stage */
  private suspend fun addNoise(token: ComputationToken): ComputationToken {
    val (bytes, nextToken) = existingOutputOr(token) {
      val inputCount = when (token.computationDetails.liquidLegionsV1.role) {
        RoleInComputation.PRIMARY -> workerStubs.size + 1
        RoleInComputation.SECONDARY -> 1
        else -> error("Unknown role: ${token.computationDetails.liquidLegionsV1.role}")
      }
      val cryptoResult: AddNoiseToSketchResponse =
        cryptoWorker.addNoiseToSketch(
          // TODO: set other parameters when AddNoise actually adds noise.
          //  Now it just shuffle the registers.
          AddNoiseToSketchRequest.newBuilder()
            .setSketch(readAndCombineAllInputBlobs(token, inputCount))
            .build()
        )
      logStageDurationMetric(token, CRYPTO_LIB_CPU_DURATION, cryptoResult.elapsedCpuTimeMillis)
      cryptoResult.sketch
    }

    when (token.computationDetails.liquidLegionsV1.role) {
      RoleInComputation.PRIMARY ->
        sendAdvanceComputationRequest(
          header = advanceComputationHeader(
            LiquidLegionsV1.Description.CONCATENATED_SKETCH,
            token.globalComputationId
          ),
          content = addLoggingHook(token, bytes),
          stub = nextDuchyStub(token)
        )
      RoleInComputation.SECONDARY ->
        sendAdvanceComputationRequest(
          header = advanceComputationHeader(
            LiquidLegionsV1.Description.NOISED_SKETCH,
            token.globalComputationId
          ),
          content = addLoggingHook(token, bytes),
          stub = primaryDuchyStub(token)
        )
      else -> error("Unknown role: ${token.computationDetails.liquidLegionsV1.role}")
    }

    return dataClients.transitionComputationToStage(
      nextToken,
      inputsToNextStage = nextToken.outputPathList(),
      stage = Stage.WAIT_CONCATENATED.toProtocolStage()
    )
  }

  /** Processes computation in the TO_BLIND_POSITIONS stage */
  private suspend fun blindPositions(token: ComputationToken): ComputationToken {
    val (bytes, nextToken) = existingOutputOr(token) {
      val cryptoResult: BlindOneLayerRegisterIndexResponse =
        cryptoWorker.blindOneLayerRegisterIndex(
          BlindOneLayerRegisterIndexRequest.newBuilder()
            .setCompositeElGamalPublicKey(cryptoKeySet.clientPublicKey)
            .setCurveId(cryptoKeySet.curveId.toLong())
            .setLocalElGamalKeyPair(cryptoKeySet.ownPublicAndPrivateKeys)
            .setSketch(readAndCombineAllInputBlobs(token, 1))
            .build()
        )
      logStageDurationMetric(token, CRYPTO_LIB_CPU_DURATION, cryptoResult.elapsedCpuTimeMillis)
      cryptoResult.sketch
    }

    // Passes the computation to the next duchy.
    sendAdvanceComputationRequest(
      header = advanceComputationHeader(
        LiquidLegionsV1.Description.CONCATENATED_SKETCH,
        token.globalComputationId
      ),
      content = addLoggingHook(token, bytes),
      stub = nextDuchyStub(token)
    )

    return dataClients.transitionComputationToStage(
      nextToken,
      inputsToNextStage = nextToken.outputPathList(),
      stage = Stage.WAIT_FLAG_COUNTS.toProtocolStage()
    )
  }

  /** Processes computation in the TO_BLIND_POSITIONS_AND_JOIN_REGISTERS stage */
  private suspend fun blindPositionsAndJoinRegisters(token: ComputationToken): ComputationToken {
    val (bytes, nextToken) = existingOutputOr(token) {
      val cryptoResult: BlindLastLayerIndexThenJoinRegistersResponse =
        cryptoWorker.blindLastLayerIndexThenJoinRegisters(
          BlindLastLayerIndexThenJoinRegistersRequest.newBuilder()
            .setCompositeElGamalPublicKey(cryptoKeySet.clientPublicKey)
            .setCurveId(cryptoKeySet.curveId.toLong())
            .setLocalElGamalKeyPair(cryptoKeySet.ownPublicAndPrivateKeys)
            .setSketch(readAndCombineAllInputBlobs(token, 1))
            .build()
        )
      logStageDurationMetric(token, CRYPTO_LIB_CPU_DURATION, cryptoResult.elapsedCpuTimeMillis)
      cryptoResult.flagCounts
    }

    // Passes the computation to the next duchy.
    sendAdvanceComputationRequest(
      header = advanceComputationHeader(
        LiquidLegionsV1.Description.ENCRYPTED_FLAGS_AND_COUNTS,
        token.globalComputationId
      ),
      content = addLoggingHook(token, bytes),
      stub = nextDuchyStub(token)
    )

    return dataClients.transitionComputationToStage(
      nextToken,
      inputsToNextStage = nextToken.outputPathList(),
      stage = Stage.WAIT_FLAG_COUNTS.toProtocolStage()
    )
  }

  /** Processes computation in the TO_DECRYPT_FLAG_COUNTS stage */
  private suspend fun decryptFlagCounts(token: ComputationToken): ComputationToken {
    val (bytes, nextToken) = existingOutputOr(token) {
      val cryptoResult: DecryptOneLayerFlagAndCountResponse =
        cryptoWorker.decryptOneLayerFlagAndCount(
          DecryptOneLayerFlagAndCountRequest.newBuilder()
            .setCurveId(cryptoKeySet.curveId.toLong())
            .setLocalElGamalKeyPair(cryptoKeySet.ownPublicAndPrivateKeys)
            .setFlagCounts(readAndCombineAllInputBlobs(token, 1))
            .build()
        )
      logStageDurationMetric(
        token,
        CRYPTO_LIB_CPU_DURATION, cryptoResult.elapsedCpuTimeMillis
      )
      cryptoResult.flagCounts
    }

    // Passes the computation to the next duchy.
    sendAdvanceComputationRequest(
      header = advanceComputationHeader(
        LiquidLegionsV1.Description.ENCRYPTED_FLAGS_AND_COUNTS,
        token.globalComputationId
      ),
      content = addLoggingHook(token, bytes),
      stub = nextDuchyStub(token)
    )

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
            .setLocalElGamalKeyPair(cryptoKeySet.ownPublicAndPrivateKeys)
            .setFlagCounts(readAndCombineAllInputBlobs(token, 1))
            .setMaximumFrequency(liquidLegionsConfig.maxFrequency)
            .build()
        )
      logStageDurationMetric(token, CRYPTO_LIB_CPU_DURATION, cryptoResult.elapsedCpuTimeMillis)
      cryptoResult.toByteString()
    }

    val flagCounts = DecryptLastLayerFlagAndCountResponse.parseFrom(bytes.flatten()).flagCountsList
    val frequencyDistribution: Map<Int, Int> = flagCounts
      .filter { it.isNotDestroyed }
      .groupingBy { it.frequency }
      .eachCount()
    val totalAppearance = frequencyDistribution.values.sum()
    val relativeFrequencyDistribution: Map<Long, Double> = frequencyDistribution
      .mapKeys { it.key.toLong() }
      .mapValues { it.value.toDouble() / totalAppearance }

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
          putAllFrequency(relativeFrequencyDistribution)
        }
      }.build()
    )

    return completeComputation(nextToken, CompletedReason.SUCCEEDED)
  }

  private fun nextDuchyStub(token: ComputationToken): ComputationControlCoroutineStub {
    val nextDuchy = token.computationDetails.liquidLegionsV1.outgoingNodeId
    return workerStubs[nextDuchy]
      ?: throw PermanentComputationError(
        Exception("No ComputationControlService stub for next duchy '$nextDuchy'")
      )
  }

  private fun primaryDuchyStub(token: ComputationToken): ComputationControlCoroutineStub {
    val primaryDuchy = token.computationDetails.liquidLegionsV1.primaryNodeId
    return workerStubs[primaryDuchy]
      ?: throw PermanentComputationError(
        Exception("No ComputationControlService stub for primary duchy '$primaryDuchy'")
      )
  }

  companion object {
    init {
      loadLibrary(
        name = "estimators",
        directoryPath = Paths.get("any_sketch_java/src/main/java/org/wfanet/estimation")
      )
    }
  }
}
