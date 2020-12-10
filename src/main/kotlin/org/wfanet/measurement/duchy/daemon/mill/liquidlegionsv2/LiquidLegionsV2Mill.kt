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

package org.wfanet.measurement.duchy.daemon.mill.liquidlegionsv2

import java.nio.file.Paths
import java.time.Clock
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.flattenConcat
import org.wfanet.measurement.common.crypto.CompleteSetupPhaseRequest
import org.wfanet.measurement.common.crypto.CompleteSetupPhaseResponse
import org.wfanet.measurement.common.crypto.liquidlegionsv2.LiquidLegionsV2Encryption
import org.wfanet.measurement.common.loadLibrary
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.duchy.daemon.mill.CRYPTO_LIB_CPU_TIME
import org.wfanet.measurement.duchy.daemon.mill.CryptoKeySet
import org.wfanet.measurement.duchy.daemon.mill.LiquidLegionsConfig
import org.wfanet.measurement.duchy.daemon.mill.MillBase
import org.wfanet.measurement.duchy.daemon.mill.PermanentComputationError
import org.wfanet.measurement.duchy.daemon.mill.toMetricRequisitionKey
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients
import org.wfanet.measurement.duchy.service.internal.computation.outputPathList
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.MetricValuesGrpcKt.MetricValuesCoroutineStub
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.ComputationDetails.RoleInComputation
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.Stage
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineStub
import org.wfanet.measurement.system.v1alpha.ConfirmGlobalComputationRequest
import org.wfanet.measurement.system.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineStub
import org.wfanet.measurement.system.v1alpha.LiquidLegionsV2

/**
 * Mill works on computations using the LiquidLegionSketchAggregationProtocol.
 *
 * @param millId The identifier of this mill, used to claim a work.
 * @param dataClients clients that have access to local computation storage, i.e., spanner
 *    table and blob store.
 * @param metricValuesClient client of the own duchy's MetricValuesService.
 * @param globalComputationsClient client of the kingdom's GlobalComputationsService.
 * @param computationStatsClient client of the duchy's ComputationStatsService.
 * @param workerStubs A map from other duchies' Ids to their corresponding
 *    computationControlClients, used for passing computation to other duchies.
 * @param cryptoKeySet The set of crypto keys used in the computation.
 * @param cryptoWorker The cryptoWorker that performs the actual computation.
 * @param throttler A throttler used to rate limit the frequency of the mill polling from the
 *    computation table.
 * @param requestChunkSizeBytes The size of data chunk when sending result to other duchies.
 * @param liquidLegionsConfig The configuration of the LiquidLegions sketch.
 * @param clock A clock
 */
class LiquidLegionsV2Mill(
  private val millId: String,
  private val dataClients: ComputationDataClients,
  private val metricValuesClient: MetricValuesCoroutineStub,
  private val globalComputationsClient: GlobalComputationsCoroutineStub,
  private val computationStatsClient: ComputationStatsCoroutineStub,
  private val workerStubs: Map<String, ComputationControlCoroutineStub>,
  private val cryptoKeySet: CryptoKeySet,
  private val cryptoWorker: LiquidLegionsV2Encryption,
  private val throttler: MinimumIntervalThrottler,
  private val requestChunkSizeBytes: Int = 1024 * 32, // 32 KiB
  private val liquidLegionsConfig: LiquidLegionsConfig = LiquidLegionsConfig(
    12.0,
    10_000_000L,
    10
  ),
  private val clock: Clock = Clock.systemUTC()
) : MillBase(
  millId,
  dataClients,
  metricValuesClient,
  globalComputationsClient,
  computationStatsClient,
  throttler,
  ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2,
  requestChunkSizeBytes,
  clock
) {

  override suspend fun processComputationImpl(token: ComputationToken) {
    require(token.computationDetails.hasLiquidLegionsV2()) {
      "Only Liquid Legions V2 computation is supported in this mill."
    }
    when (token.computationStage.liquidLegionsSketchAggregationV2) {
      Stage.CONFIRM_REQUISITIONS_PHASE ->
        confirmRequisitions(token)
      Stage.SETUP_PHASE ->
        completeSetupPhase(token)
      Stage.REACH_ESTIMATION_PHASE ->
        completeReachEstimationPhase()
      Stage.FILTERING_PHASE ->
        completeFilteringPhase()
      Stage.FREQUENCY_ESTIMATION_PHASE ->
        completeFrequencyEstimationPhase()
      else -> error("Unexpected stage: $token.computationStage.liquidLegionsSketchAggregationV2")
    }
  }

  /** Process computation in the confirm requisitions phase*/
  @OptIn(FlowPreview::class) // For `flattenConcat`.
  private suspend fun confirmRequisitions(token: ComputationToken): ComputationToken {
    val requisitionsToConfirm =
      token.stageSpecificDetails.liquidLegionsV2.toConfirmRequisitionsStageDetails.keysList
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
      stage = when (checkNotNull(nextToken.computationDetails.liquidLegionsV2.role)) {
        RoleInComputation.AGGREGATOR -> Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage()
        RoleInComputation.NON_AGGREGATOR -> Stage.WAIT_TO_START.toProtocolStage()
        RoleInComputation.UNKNOWN,
        RoleInComputation.UNRECOGNIZED ->
          error("Unknown role: ${nextToken.computationDetails.liquidLegionsV2.role}")
      }
    )
  }

  /** Processes computation in the setup phase. */
  private suspend fun completeSetupPhase(token: ComputationToken): ComputationToken {
    val (bytes, nextToken) = existingOutputOr(token) {
      val inputCount = when (token.computationDetails.liquidLegionsV2.role) {
        RoleInComputation.AGGREGATOR -> workerStubs.size + 1
        RoleInComputation.NON_AGGREGATOR -> 1
        else -> error("Unknown role: ${token.computationDetails.liquidLegionsV2.role}")
      }
      val cryptoResult: CompleteSetupPhaseResponse =
        cryptoWorker.completeSetupPhase(
          // TODO: set other parameters when we start to add noise.
          //  Now it just shuffles the registers.
          CompleteSetupPhaseRequest.newBuilder()
            .setCombinedRegisterVector(readAndCombineAllInputBlobs(token, inputCount))
            .build()
        )
      logStageMetric(token, CRYPTO_LIB_CPU_TIME, cryptoResult.elapsedCpuTimeMillis)
      cryptoResult.combinedRegisterVector
    }

    when (token.computationDetails.liquidLegionsV2.role) {
      RoleInComputation.AGGREGATOR ->
        sendAdvanceComputationRequest(
          token.globalComputationId,
          bytes,
          LiquidLegionsV2.Description.REACH_ESTIMATION_PHASE_INPUT,
          nextDuchyStub(token)
        )
      RoleInComputation.NON_AGGREGATOR ->
        sendAdvanceComputationRequest(
          token.globalComputationId,
          bytes,
          LiquidLegionsV2.Description.SETUP_PHASE_INPUT,
          aggregatorDuchyStub(token)
        )
      else -> error("Unknown role: ${token.computationDetails.liquidLegionsV2.role}")
    }

    return dataClients.transitionComputationToStage(
      nextToken,
      inputsToNextStage = nextToken.outputPathList(),
      stage = Stage.WAIT_REACH_ESTIMATION_PHASE_INPUTS.toProtocolStage()
    )
  }

  /** Processes computation in the reach estimation phase. */
  private suspend fun completeReachEstimationPhase(): ComputationToken {
    TODO("unimplemented")
  }

  /** Processes computation in the filtering phase. */
  private suspend fun completeFilteringPhase(): ComputationToken {
    TODO("unimplemented")
  }

  /** Processes computation in the frequency estimation phase. */
  private suspend fun completeFrequencyEstimationPhase(): ComputationToken {
    TODO("unimplemented")
  }

  private fun nextDuchyStub(token: ComputationToken): ComputationControlCoroutineStub {
    val nextDuchy = token.computationDetails.liquidLegionsV2.outgoingNodeId
    return workerStubs[nextDuchy]
      ?: throw PermanentComputationError(
        Exception("No ComputationControlService stub for next duchy '$nextDuchy'")
      )
  }

  private fun aggregatorDuchyStub(token: ComputationToken): ComputationControlCoroutineStub {
    val primaryDuchy = token.computationDetails.liquidLegionsV2.aggregatorNodeId
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
