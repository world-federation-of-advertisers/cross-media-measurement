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

package org.wfanet.measurement.duchy.daemon.mill.liquidlegionsv2

import com.google.protobuf.ByteString
import java.nio.file.Paths
import java.time.Clock
import org.wfanet.anysketch.crypto.CombineElGamalPublicKeysRequest
import org.wfanet.anysketch.crypto.CombineElGamalPublicKeysResponse
import org.wfanet.anysketch.crypto.SketchEncrypterAdapter
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.loadLibrary
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.duchy.daemon.mill.CRYPTO_LIB_CPU_DURATION
import org.wfanet.measurement.duchy.daemon.mill.CryptoKeySet
import org.wfanet.measurement.duchy.daemon.mill.LiquidLegionsConfig
import org.wfanet.measurement.duchy.daemon.mill.MillBase
import org.wfanet.measurement.duchy.daemon.mill.PermanentComputationError
import org.wfanet.measurement.duchy.daemon.mill.liquidlegionsv2.crypto.LiquidLegionsV2Encryption
import org.wfanet.measurement.duchy.daemon.mill.toAnySketchElGamalPublicKey
import org.wfanet.measurement.duchy.daemon.mill.toCmmsElGamalPublicKey
import org.wfanet.measurement.duchy.daemon.utils.Duchy
import org.wfanet.measurement.duchy.daemon.utils.getDuchyOrderByPublicKeysAndComputationId
import org.wfanet.measurement.duchy.daemon.utils.getFollowingDuchies
import org.wfanet.measurement.duchy.daemon.utils.getNextDuchy
import org.wfanet.measurement.duchy.daemon.utils.toPublicApiElGamalPublicKeyBytes
import org.wfanet.measurement.duchy.daemon.utils.toPublicApiVersion
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients
import org.wfanet.measurement.duchy.service.internal.computation.outputPathList
import org.wfanet.measurement.duchy.service.system.v1alpha.advanceComputationHeader
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ComputationDetails.CompletedReason
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.ElGamalPublicKey
import org.wfanet.measurement.internal.duchy.MetricValuesGrpcKt.MetricValuesCoroutineStub
import org.wfanet.measurement.internal.duchy.RequisitionMetadata
import org.wfanet.measurement.internal.duchy.UpdateComputationDetailsRequest
import org.wfanet.measurement.internal.duchy.config.LiquidLegionsV2SetupConfig.RoleInComputation.AGGREGATOR
import org.wfanet.measurement.internal.duchy.config.LiquidLegionsV2SetupConfig.RoleInComputation.NON_AGGREGATOR
import org.wfanet.measurement.internal.duchy.config.LiquidLegionsV2SetupConfig.RoleInComputation.UNKNOWN
import org.wfanet.measurement.internal.duchy.config.LiquidLegionsV2SetupConfig.RoleInComputation.UNRECOGNIZED
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseOneAtAggregatorRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseOneAtAggregatorResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseOneRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseOneResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseThreeAtAggregatorRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseThreeAtAggregatorResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseThreeRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseThreeResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseTwoAtAggregatorRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseTwoAtAggregatorResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseTwoRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseTwoResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteInitializationPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteSetupPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteSetupPhaseResponse
import org.wfanet.measurement.internal.duchy.protocol.FlagCountTupleNoiseGenerationParameters
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.ComputationDetails.ComputationParticipant
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsV2NoiseConfig
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub
import org.wfanet.measurement.system.v1alpha.ConfirmComputationParticipantRequest
import org.wfanet.measurement.system.v1alpha.FinishGlobalComputationRequest
import org.wfanet.measurement.system.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineStub
import org.wfanet.measurement.system.v1alpha.LiquidLegionsV2
import org.wfanet.measurement.system.v1alpha.SetParticipantRequisitionParamsRequest

/**
 * Mill works on computations using the LiquidLegionSketchAggregationProtocol.
 *
 * @param millId The identifier of this mill, used to claim a work.
 * @param dataClients clients that have access to local computation storage, i.e., spanner table and
 * blob store.
 * @param metricValuesClient client of the own duchy's MetricValuesService.
 * @param globalComputationsClient client of the kingdom's GlobalComputationsService.
 * @param systemComputationParticipantsClient client of the kingdom's
 * ComputationParticipantsService.
 * @param computationStatsClient client of the duchy's ComputationStatsService.
 * @param throttler A throttler used to rate limit the frequency of the mill polling from the
 * computation table.
 * @param requestChunkSizeBytes The size of data chunk when sending result to other duchies.
 * @param clock A clock
 * @param workerStubs A map from other duchies' Ids to their corresponding
 * computationControlClients, used for passing computation to other duchies.
 * @param cryptoKeySet The set of crypto keys used in the computation.
 * @param cryptoWorker The cryptoWorker that performs the actual computation.
 * @param maxFrequency The maximum frequency to reveal in the frequency histogram.
 * @param liquidLegionsConfig The configuration of the LiquidLegions sketch.
 * @param noiseConfig The configuration of the noises added in the protocol.
 */
class LiquidLegionsV2Mill(
  millId: String,
  duchyId: String,
  dataClients: ComputationDataClients,
  metricValuesClient: MetricValuesCoroutineStub,
  globalComputationsClient: GlobalComputationsCoroutineStub,
  systemComputationParticipantsClient: ComputationParticipantsCoroutineStub,
  computationStatsClient: ComputationStatsCoroutineStub,
  throttler: MinimumIntervalThrottler,
  requestChunkSizeBytes: Int = 1024 * 32, // 32 KiB
  clock: Clock = Clock.systemUTC(),
  private val workerStubs: Map<String, ComputationControlCoroutineStub>,
  private val cryptoKeySet: CryptoKeySet,
  private val cryptoWorker: LiquidLegionsV2Encryption,
  private val maxFrequency: Int = 10,
  private val liquidLegionsConfig: LiquidLegionsConfig = LiquidLegionsConfig(12.0, 10_000_000L),
  private val noiseConfig: LiquidLegionsV2NoiseConfig,
  private val aggregatorId: String
) :
  MillBase(
    millId,
    duchyId,
    dataClients,
    globalComputationsClient,
    systemComputationParticipantsClient,
    metricValuesClient,
    computationStatsClient,
    throttler,
    ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2,
    requestChunkSizeBytes,
    clock
  ) {
  override val endingStage: ComputationStage =
    ComputationStage.newBuilder()
      .apply { liquidLegionsSketchAggregationV2 = Stage.COMPLETE }
      .build()

  private val actions =
    mapOf(
      Pair(Stage.INITIALIZATION_PHASE, AGGREGATOR) to ::initializationPhase,
      Pair(Stage.INITIALIZATION_PHASE, NON_AGGREGATOR) to ::initializationPhase,
      Pair(Stage.CONFIRMATION_PHASE, AGGREGATOR) to ::confirmationPhase,
      Pair(Stage.CONFIRMATION_PHASE, NON_AGGREGATOR) to ::confirmationPhase,
      Pair(Stage.SETUP_PHASE, AGGREGATOR) to ::completeSetupPhaseAtAggregator,
      Pair(Stage.SETUP_PHASE, NON_AGGREGATOR) to ::completeSetupPhaseAtNonAggregator,
      Pair(Stage.EXECUTION_PHASE_ONE, AGGREGATOR) to ::completeExecutionPhaseOneAtAggregator,
      Pair(Stage.EXECUTION_PHASE_ONE, NON_AGGREGATOR) to ::completeExecutionPhaseOneAtNonAggregator,
      Pair(Stage.EXECUTION_PHASE_TWO, AGGREGATOR) to ::completeExecutionPhaseTwoAtAggregator,
      Pair(Stage.EXECUTION_PHASE_TWO, NON_AGGREGATOR) to ::completeExecutionPhaseTwoAtNonAggregator,
      Pair(Stage.EXECUTION_PHASE_THREE, AGGREGATOR) to ::completeExecutionPhaseThreeAtAggregator,
      Pair(Stage.EXECUTION_PHASE_THREE, NON_AGGREGATOR) to
        ::completeExecutionPhaseThreeAtNonAggregator
    )

  // The key is the concatenation of the sorted duchy ids.
  private var partiallyCombinedPublicKeyMap = HashMap<String, ElGamalPublicKey>()

  override suspend fun processComputationImpl(token: ComputationToken) {
    require(token.computationDetails.hasLiquidLegionsV2()) {
      "Only Liquid Legions V2 computation is supported in this mill."
    }
    val stage = token.computationStage.liquidLegionsSketchAggregationV2
    val role = token.computationDetails.liquidLegionsV2.role
    val action = actions[Pair(stage, role)] ?: error("Unexpected stage or role: ($stage, $role)")
    action(token)
  }

  /** Sends requisition params to the kingdom. */
  private suspend fun sendRequisitionParamsToKingdom(token: ComputationToken) {
    val llv2ComputationDetails = token.computationDetails.liquidLegionsV2
    require(llv2ComputationDetails.hasLocalElgamalKey()) { "Missing local elgamal key." }
    val publicApiVersion =
      token.computationDetails.kingdomComputation.publicApiVersion.toPublicApiVersion()
    val elGamalPublicKeyBytes =
      llv2ComputationDetails.localElgamalKey.publicKey.toPublicApiElGamalPublicKeyBytes(
        publicApiVersion
      )

    val request =
      SetParticipantRequisitionParamsRequest.newBuilder()
        .apply {
          keyBuilder.also {
            it.computationId = token.globalComputationId
            it.duchyId = duchyId
          }
          requisitionParamsBuilder.apply {
            // TODO(wangyaopw): set the correct certificate and elGamalPublicKeySignature.
            duchyCertificateId = "TODO"
            duchyCertificate = ByteString.copyFromUtf8("TODO")
            liquidLegionsV2Builder.apply {
              elGamalPublicKey = elGamalPublicKeyBytes
              elGamalPublicKeySignature = ByteString.copyFromUtf8("TODO")
            }
          }
        }
        .build()
    systemComputationParticipantsClient.setParticipantRequisitionParams(request)
  }

  /** Processes computation in the initialization phase */
  private suspend fun initializationPhase(token: ComputationToken): ComputationToken {
    val llv2ComputationDetails = token.computationDetails.liquidLegionsV2
    val ellipticCurveId = llv2ComputationDetails.parameters.ellipticCurveId
    require(ellipticCurveId > 0) { "invalid ellipticCurveId $ellipticCurveId" }

    val nextToken =
      if (llv2ComputationDetails.hasLocalElgamalKey()) {
        // Reuses the key if it is already generated for this computation.
        token
      } else {
        // Generates a new set of ElGamalKeyPair.
        val request =
          CompleteInitializationPhaseRequest.newBuilder()
            .apply { curveId = ellipticCurveId.toLong() }
            .build()
        val cryptoResult = cryptoWorker.completeInitializationPhase(request)
        logStageDurationMetric(token, CRYPTO_LIB_CPU_DURATION, cryptoResult.elapsedCpuTimeMillis)

        // Updates the newly generated localElgamalKey to the ComputationDetails.
        dataClients.computationsClient.updateComputationDetails(
            UpdateComputationDetailsRequest.newBuilder()
              .also {
                it.token = token
                it.details =
                  token
                    .computationDetails
                    .toBuilder()
                    .apply { liquidLegionsV2Builder.localElgamalKey = cryptoResult.elGamalKeyPair }
                    .build()
              }
              .build()
          )
          .token
      }

    sendRequisitionParamsToKingdom(nextToken)

    return dataClients.transitionComputationToStage(
      nextToken,
      stage = Stage.WAIT_REQUISITIONS_AND_KEY_SET.toProtocolStage()
    )
  }

  /**
   * Verifies the Edp signature and the local existence of requisitions. Returns a list of error
   * messages if anything is wrong. Otherwise return an empty list.
   */
  private fun verifyEdpSignatureAndRequisition(requisition: RequisitionMetadata): List<String> {
    // TODO(wangyaopw): also verify data_provider_participation_signature
    val errorList = mutableListOf<String>()
    if (requisition.details.externalFulfillingDuchyId == duchyId && requisition.path.isBlank()) {
      errorList.add("Missing expected data for requisition ${requisition.externalRequisitionId}.")
    }
    return errorList
  }

  /**
   * Verifies the duchy Elgamal Key signature. Returns a list of error messages if anything is
   * wrong. Otherwise return an empty list.
   */
  private fun verifyDuchySignature(duchy: ComputationParticipant): List<String> {
    val errorList = mutableListOf<String>()
    // TODO(wangyaopw): verify el_gamal_public_key_signature and update errorMessage if necessary.
    if (duchy.duchyId != duchyId && !workerStubs.containsKey(duchy.duchyId)) {
      errorList.add("Unrecognized duchy ${duchy.duchyId}.")
    }
    return errorList
  }

  /** Fails a computation both locally and at the kingdom when the confirmation fails. */
  private fun failComputationAtConfirmationPhase(
    token: ComputationToken,
    errorList: List<String>
  ): ComputationToken {
    // TODO: send failComputationParticipant request to kingdom when the proto is in.
    val errorMessage =
      """
        @Mill $millId:
          Computation ${token.globalComputationId} failed due to:.
        ${errorList.joinToString(separator = "\n")}
        """.trimIndent()
    throw PermanentComputationError(Exception(errorMessage))
  }

  private fun List<ElGamalPublicKey>.toCombinedPublicKey(curveId: Int): ElGamalPublicKey {
    val request =
      CombineElGamalPublicKeysRequest.newBuilder()
        .also {
          it.curveId = curveId.toLong()
          it.addAllElGamalKeys(this.map { key -> key.toAnySketchElGamalPublicKey() })
        }
        .build()
    return cryptoWorker.combineElGamalPublicKeys(request).elGamalKeys.toCmmsElGamalPublicKey()
  }

  /**
   * Computes the fully and partially combined Elgamal public keys and caches the result in the
   * computationDetails.
   */
  private suspend fun updatePublicElgamalKey(token: ComputationToken): ComputationToken {
    val llv2Details = token.computationDetails.liquidLegionsV2
    val fullParticipantList = llv2Details.participantList
    val combinedPublicKey =
      fullParticipantList
        .map { it.publicKey }
        .toCombinedPublicKey(llv2Details.parameters.ellipticCurveId)

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    val partiallyCombinedPublicKey =
      when (llv2Details.role) {
        // For aggregator, the partial list is the same as the full list.
        AGGREGATOR -> combinedPublicKey
        NON_AGGREGATOR ->
          fullParticipantList
            .subList(
              fullParticipantList.indexOfFirst { it.duchyId == duchyId } + 1,
              fullParticipantList.size
            )
            .map { it.publicKey }
            .toCombinedPublicKey(llv2Details.parameters.ellipticCurveId)
        UNKNOWN, UNRECOGNIZED -> error("Invalid role ${llv2Details.role}")
      }

    return dataClients.computationsClient.updateComputationDetails(
        UpdateComputationDetailsRequest.newBuilder()
          .also {
            it.token = token
            it.details =
              token
                .computationDetails
                .toBuilder()
                .apply {
                  liquidLegionsV2Builder.also {
                    it.combinedPublicKey = combinedPublicKey
                    it.partiallyCombinedPublicKey = partiallyCombinedPublicKey
                  }
                }
                .build()
          }
          .build()
      )
      .token
  }

  /** Sends confirmation to the kingdom and transits the local computation to the next stage. */
  private suspend fun passConfirmationPhase(token: ComputationToken): ComputationToken {
    systemComputationParticipantsClient.confirmComputationParticipant(
      ConfirmComputationParticipantRequest.newBuilder()
        .apply {
          keyBuilder.also {
            it.computationId = token.globalComputationId
            it.duchyId = duchyId
          }
        }
        .build()
    )
    val latestToken = updatePublicElgamalKey(token)
    return dataClients.transitionComputationToStage(
      latestToken,
      stage =
        when (checkNotNull(token.computationDetails.liquidLegionsV2.role)) {
          AGGREGATOR -> Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage()
          NON_AGGREGATOR -> Stage.WAIT_TO_START.toProtocolStage()
          else -> error("Unknown role: ${latestToken.computationDetails.liquidLegionsV2.role}")
        }
    )
  }

  /** Processes computation in the confirmation phase */
  private suspend fun confirmationPhase(token: ComputationToken): ComputationToken {
    val errorList = mutableListOf<String>()
    token.requisitionsList.forEach { errorList.addAll(verifyEdpSignatureAndRequisition(it)) }
    token.computationDetails.liquidLegionsV2.participantList.forEach {
      errorList.addAll(verifyDuchySignature(it))
    }
    return if (errorList.isEmpty()) {
      passConfirmationPhase(token)
    } else {
      failComputationAtConfirmationPhase(token, errorList)
    }
  }

  private suspend fun completeSetupPhaseAtAggregator(token: ComputationToken): ComputationToken {
    require(AGGREGATOR == token.computationDetails.liquidLegionsV2.role) {
      "invalid role for this function."
    }
    val (bytes, nextToken) =
      existingOutputOr(token) {
        val request =
          readAndCombineAllInputBlobs(token, workerStubs.size + 1)
            .toCompleteSetupPhaseRequest(
              token.computationDetails.liquidLegionsV2.totalRequisitionCount
            )
        val cryptoResult: CompleteSetupPhaseResponse = cryptoWorker.completeSetupPhase(request)
        logStageDurationMetric(token, CRYPTO_LIB_CPU_DURATION, cryptoResult.elapsedCpuTimeMillis)
        cryptoResult.combinedRegisterVector
      }

    sendAdvanceComputationRequest(
      header =
        advanceComputationHeader(
          LiquidLegionsV2.Description.EXECUTION_PHASE_ONE_INPUT,
          token.globalComputationId
        ),
      content = addLoggingHook(token, bytes),
      stub = nextDuchyStub(token)
    )

    return dataClients.transitionComputationToStage(
      nextToken,
      inputsToNextStage = nextToken.outputPathList(),
      stage = Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage()
    )
  }

  private suspend fun completeSetupPhaseAtNonAggregator(token: ComputationToken): ComputationToken {
    require(NON_AGGREGATOR == token.computationDetails.liquidLegionsV2.role) {
      "invalid role for this function."
    }
    val (bytes, nextToken) =
      existingOutputOr(token) {
        val request =
          readAndCombineAllInputBlobs(token, 1)
            .toCompleteSetupPhaseRequest(
              token.computationDetails.liquidLegionsV2.totalRequisitionCount
            )
        val cryptoResult: CompleteSetupPhaseResponse = cryptoWorker.completeSetupPhase(request)
        logStageDurationMetric(token, CRYPTO_LIB_CPU_DURATION, cryptoResult.elapsedCpuTimeMillis)
        cryptoResult.combinedRegisterVector
      }

    sendAdvanceComputationRequest(
      header =
        advanceComputationHeader(
          LiquidLegionsV2.Description.SETUP_PHASE_INPUT,
          token.globalComputationId
        ),
      content = addLoggingHook(token, bytes),
      stub = aggregatorDuchyStub()
    )

    return dataClients.transitionComputationToStage(
      nextToken,
      inputsToNextStage = nextToken.outputPathList(),
      stage = Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage()
    )
  }

  private suspend fun completeExecutionPhaseOneAtAggregator(
    token: ComputationToken
  ): ComputationToken {
    require(AGGREGATOR == token.computationDetails.liquidLegionsV2.role) {
      "invalid role for this function."
    }
    val (bytes, nextToken) =
      existingOutputOr(token) {
        val requestBuilder =
          CompleteExecutionPhaseOneAtAggregatorRequest.newBuilder().apply {
            localElGamalKeyPair = cryptoKeySet.ownPublicAndPrivateKeys
            compositeElGamalPublicKey = cryptoKeySet.clientPublicKey
            curveId = cryptoKeySet.curveId.toLong()
            combinedRegisterVector = readAndCombineAllInputBlobs(token, 1)
            totalSketchesCount = token.computationDetails.liquidLegionsV2.totalRequisitionCount
          }
        if (noiseConfig.hasFrequencyNoiseConfig()) {
          requestBuilder.noiseParameters = getFrequencyNoiseParams()
        }
        val cryptoResult: CompleteExecutionPhaseOneAtAggregatorResponse =
          cryptoWorker.completeExecutionPhaseOneAtAggregator(requestBuilder.build())
        logStageDurationMetric(token, CRYPTO_LIB_CPU_DURATION, cryptoResult.elapsedCpuTimeMillis)
        cryptoResult.flagCountTuples
      }

    // Passes the computation to the next duchy.
    sendAdvanceComputationRequest(
      header =
        advanceComputationHeader(
          LiquidLegionsV2.Description.EXECUTION_PHASE_TWO_INPUT,
          token.globalComputationId
        ),
      content = addLoggingHook(token, bytes),
      stub = nextDuchyStub(token)
    )

    return dataClients.transitionComputationToStage(
      nextToken,
      inputsToNextStage = nextToken.outputPathList(),
      stage = Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS.toProtocolStage()
    )
  }

  private suspend fun completeExecutionPhaseOneAtNonAggregator(
    token: ComputationToken
  ): ComputationToken {
    require(NON_AGGREGATOR == token.computationDetails.liquidLegionsV2.role) {
      "invalid role for this function."
    }
    val (bytes, nextToken) =
      existingOutputOr(token) {
        val cryptoResult: CompleteExecutionPhaseOneResponse =
          cryptoWorker.completeExecutionPhaseOne(
            CompleteExecutionPhaseOneRequest.newBuilder()
              .apply {
                localElGamalKeyPair = cryptoKeySet.ownPublicAndPrivateKeys
                compositeElGamalPublicKey = cryptoKeySet.clientPublicKey
                curveId = cryptoKeySet.curveId.toLong()
                combinedRegisterVector = readAndCombineAllInputBlobs(token, 1)
              }
              .build()
          )
        logStageDurationMetric(token, CRYPTO_LIB_CPU_DURATION, cryptoResult.elapsedCpuTimeMillis)
        cryptoResult.combinedRegisterVector
      }

    // Passes the computation to the next duchy.
    sendAdvanceComputationRequest(
      header =
        advanceComputationHeader(
          LiquidLegionsV2.Description.EXECUTION_PHASE_ONE_INPUT,
          token.globalComputationId
        ),
      content = addLoggingHook(token, bytes),
      stub = nextDuchyStub(token)
    )

    return dataClients.transitionComputationToStage(
      nextToken,
      inputsToNextStage = nextToken.outputPathList(),
      stage = Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS.toProtocolStage()
    )
  }

  private suspend fun completeExecutionPhaseTwoAtAggregator(
    token: ComputationToken
  ): ComputationToken {
    require(AGGREGATOR == token.computationDetails.liquidLegionsV2.role) {
      "invalid role for this function."
    }
    var reach = 0L
    val (bytes, tempToken) =
      existingOutputOr(token) {
        val requestBuilder =
          CompleteExecutionPhaseTwoAtAggregatorRequest.newBuilder().apply {
            localElGamalKeyPair = cryptoKeySet.ownPublicAndPrivateKeys
            compositeElGamalPublicKey = cryptoKeySet.clientPublicKey
            curveId = cryptoKeySet.curveId.toLong()
            flagCountTuples = readAndCombineAllInputBlobs(token, 1)
            maximumFrequency = maxFrequency
            liquidLegionsParametersBuilder.apply {
              decayRate = liquidLegionsConfig.decayRate
              size = liquidLegionsConfig.size
            }
          }
        if (noiseConfig.hasReachNoiseConfig()) {
          requestBuilder.reachDpNoiseBaselineBuilder.apply {
            contributorsCount = workerStubs.size + 1
            globalReachDpNoise = noiseConfig.reachNoiseConfig.globalReachDpNoise
          }
        }
        if (noiseConfig.hasFrequencyNoiseConfig()) {
          requestBuilder.frequencyNoiseParametersBuilder.apply {
            contributorsCount = workerStubs.size + 1
            maximumFrequency = maxFrequency
            dpParams = noiseConfig.frequencyNoiseConfig
          }
        }

        val cryptoResult: CompleteExecutionPhaseTwoAtAggregatorResponse =
          cryptoWorker.completeExecutionPhaseTwoAtAggregator(requestBuilder.build())
        logStageDurationMetric(token, CRYPTO_LIB_CPU_DURATION, cryptoResult.elapsedCpuTimeMillis)
        reach = cryptoResult.reach
        cryptoResult.sameKeyAggregatorMatrix
      }

    val nextToken =
      if (token.computationDetails.liquidLegionsV2.hasReachEstimate()) {
        // Do nothing if the token has already contained the ReachEstimate
        tempToken
      } else {
        // Update the newly calculated reach to the ComputationDetails.
        dataClients.computationsClient.updateComputationDetails(
            UpdateComputationDetailsRequest.newBuilder()
              .also {
                it.token = tempToken
                it.details =
                  token
                    .computationDetails
                    .toBuilder()
                    .apply { liquidLegionsV2Builder.reachEstimateBuilder.reach = reach }
                    .build()
              }
              .build()
          )
          .token
      }

    // Passes the computation to the next duchy.
    sendAdvanceComputationRequest(
      header =
        advanceComputationHeader(
          LiquidLegionsV2.Description.EXECUTION_PHASE_THREE_INPUT,
          token.globalComputationId
        ),
      content = addLoggingHook(token, bytes),
      stub = nextDuchyStub(token)
    )

    return dataClients.transitionComputationToStage(
      nextToken,
      inputsToNextStage = nextToken.outputPathList(),
      stage = Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS.toProtocolStage()
    )
  }

  private suspend fun completeExecutionPhaseTwoAtNonAggregator(
    token: ComputationToken
  ): ComputationToken {
    require(NON_AGGREGATOR == token.computationDetails.liquidLegionsV2.role) {
      "invalid role for this function."
    }
    val (bytes, nextToken) =
      existingOutputOr(token) {
        val requestBuilder =
          CompleteExecutionPhaseTwoRequest.newBuilder().apply {
            localElGamalKeyPair = cryptoKeySet.ownPublicAndPrivateKeys
            compositeElGamalPublicKey = cryptoKeySet.clientPublicKey
            curveId = cryptoKeySet.curveId.toLong()
            flagCountTuples = readAndCombineAllInputBlobs(token, 1)
          }
        if (noiseConfig.hasFrequencyNoiseConfig()) {
          requestBuilder.apply {
            partialCompositeElGamalPublicKey = getPartiallyCombinedPublicKey(token)
            noiseParameters = getFrequencyNoiseParams()
          }
        }

        val cryptoResult: CompleteExecutionPhaseTwoResponse =
          cryptoWorker.completeExecutionPhaseTwo(requestBuilder.build())
        logStageDurationMetric(token, CRYPTO_LIB_CPU_DURATION, cryptoResult.elapsedCpuTimeMillis)
        cryptoResult.flagCountTuples
      }

    // Passes the computation to the next duchy.
    sendAdvanceComputationRequest(
      header =
        advanceComputationHeader(
          LiquidLegionsV2.Description.EXECUTION_PHASE_TWO_INPUT,
          token.globalComputationId
        ),
      content = addLoggingHook(token, bytes),
      stub = nextDuchyStub(token)
    )

    return dataClients.transitionComputationToStage(
      nextToken,
      inputsToNextStage = nextToken.outputPathList(),
      stage = Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS.toProtocolStage()
    )
  }

  private suspend fun completeExecutionPhaseThreeAtAggregator(
    token: ComputationToken
  ): ComputationToken {
    require(AGGREGATOR == token.computationDetails.liquidLegionsV2.role) {
      "invalid role for this function."
    }
    require(token.computationDetails.liquidLegionsV2.hasReachEstimate()) {
      "Reach estimate is missing."
    }
    val (bytes, nextToken) =
      existingOutputOr(token) {
        val requestBuilder =
          CompleteExecutionPhaseThreeAtAggregatorRequest.newBuilder().apply {
            localElGamalKeyPair = cryptoKeySet.ownPublicAndPrivateKeys
            curveId = cryptoKeySet.curveId.toLong()
            sameKeyAggregatorMatrix = readAndCombineAllInputBlobs(token, 1)
            maximumFrequency = maxFrequency
          }
        if (noiseConfig.hasFrequencyNoiseConfig()) {
          requestBuilder.globalFrequencyDpNoisePerBucketBuilder.apply {
            contributorsCount = workerStubs.size + 1
            dpParams = noiseConfig.frequencyNoiseConfig
          }
        }
        val cryptoResult: CompleteExecutionPhaseThreeAtAggregatorResponse =
          cryptoWorker.completeExecutionPhaseThreeAtAggregator(requestBuilder.build())
        logStageDurationMetric(token, CRYPTO_LIB_CPU_DURATION, cryptoResult.elapsedCpuTimeMillis)
        cryptoResult.toByteString()
      }

    val frequencyDistributionMap =
      CompleteExecutionPhaseThreeAtAggregatorResponse.parseFrom(bytes.flatten())
        .frequencyDistributionMap
    globalComputationsClient.finishGlobalComputation(
      FinishGlobalComputationRequest.newBuilder()
        .apply {
          keyBuilder.globalComputationId = token.globalComputationId.toString()
          resultBuilder.apply {
            reach = nextToken.computationDetails.liquidLegionsV2.reachEstimate.reach
            putAllFrequency(frequencyDistributionMap)
          }
        }
        .build()
    )

    return completeComputation(nextToken, CompletedReason.SUCCEEDED)
  }

  private suspend fun completeExecutionPhaseThreeAtNonAggregator(
    token: ComputationToken
  ): ComputationToken {
    require(NON_AGGREGATOR == token.computationDetails.liquidLegionsV2.role) {
      "invalid role for this function."
    }
    val (bytes, nextToken) =
      existingOutputOr(token) {
        val cryptoResult: CompleteExecutionPhaseThreeResponse =
          cryptoWorker.completeExecutionPhaseThree(
            CompleteExecutionPhaseThreeRequest.newBuilder()
              .apply {
                localElGamalKeyPair = cryptoKeySet.ownPublicAndPrivateKeys
                curveId = cryptoKeySet.curveId.toLong()
                sameKeyAggregatorMatrix = readAndCombineAllInputBlobs(token, 1)
              }
              .build()
          )
        logStageDurationMetric(token, CRYPTO_LIB_CPU_DURATION, cryptoResult.elapsedCpuTimeMillis)
        cryptoResult.sameKeyAggregatorMatrix
      }

    // Passes the computation to the next duchy.
    sendAdvanceComputationRequest(
      header =
        advanceComputationHeader(
          LiquidLegionsV2.Description.EXECUTION_PHASE_THREE_INPUT,
          token.globalComputationId
        ),
      content = addLoggingHook(token, bytes),
      stub = nextDuchyStub(token)
    )

    // This duchy's responsibility for the computation is done. Mark it COMPLETED locally.
    return completeComputation(nextToken, CompletedReason.SUCCEEDED)
  }

  // Gets the order of all duchies in the MPC ring. Since it is a ring, we put the aggregator at the
  // end for simplicity.
  private fun getMpcDuchyRing(token: ComputationToken): List<String> {
    // TODO(wangyaopw): read the duchy order from the ComputationDetails when it is populated by the
    //  herald.
    val nonAggregatorsSet =
      cryptoKeySet
        .allDuchyPublicKeys
        .filter { it.key != aggregatorId }
        .map { Duchy(it.key, it.value.element.toStringUtf8()) }
        .toSet()
    return getDuchyOrderByPublicKeysAndComputationId(nonAggregatorsSet, token.globalComputationId) +
      aggregatorId
  }

  private fun nextDuchyStub(token: ComputationToken): ComputationControlCoroutineStub {
    val mpcDuchyRing = getMpcDuchyRing(token)
    val nextDuchy = getNextDuchy(mpcDuchyRing, duchyId)
    return workerStubs[nextDuchy]
      ?: throw PermanentComputationError(
        IllegalArgumentException("No ComputationControlService stub for next duchy '$nextDuchy'")
      )
  }

  private fun aggregatorDuchyStub(): ComputationControlCoroutineStub {
    return workerStubs[aggregatorId]
      ?: throw PermanentComputationError(
        IllegalArgumentException(
          "No ComputationControlService stub for the Aggregator duchy '$aggregatorId'"
        )
      )
  }

  // Combines the public keys from the duchies after this duchy in the ring and the primary duchy.
  fun getPartiallyCombinedPublicKey(token: ComputationToken): ElGamalPublicKey {
    require(token.computationDetails.liquidLegionsV2.role == NON_AGGREGATOR) {
      "only NON_AGGREGATOR should call getPartiallyCombinedPublicKey()."
    }
    val partialList = getFollowingDuchies(getMpcDuchyRing(token), duchyId)
    val lookupKey = partialList.sorted().joinToString()

    return partiallyCombinedPublicKeyMap.computeIfAbsent(lookupKey) {
      val request =
        CombineElGamalPublicKeysRequest.newBuilder()
          .apply {
            curveId = cryptoKeySet.curveId.toLong()
            addAllElGamalKeys(
              partialList.map {
                cryptoKeySet.allDuchyPublicKeys[it]?.toAnySketchElGamalPublicKey()
                  ?: error("$it is not in the key set.")
              }
            )
          }
          .build()
      val response =
        CombineElGamalPublicKeysResponse.parseFrom(
          SketchEncrypterAdapter.CombineElGamalPublicKeys(request.toByteArray())
        )
      response.elGamalKeys.toCmmsElGamalPublicKey()
    }
  }

  private fun ByteString.toCompleteSetupPhaseRequest(
    totalRequisitionCount: Int
  ): CompleteSetupPhaseRequest {
    val requestBuilder = CompleteSetupPhaseRequest.newBuilder().setCombinedRegisterVector(this)
    if (noiseConfig.hasReachNoiseConfig()) {
      requestBuilder.noiseParametersBuilder.apply {
        compositeElGamalPublicKey = cryptoKeySet.clientPublicKey
        curveId = cryptoKeySet.curveId.toLong()
        contributorsCount = workerStubs.size + 1
        totalSketchesCount = totalRequisitionCount
        dpParamsBuilder.apply {
          blindHistogram = noiseConfig.reachNoiseConfig.blindHistogramNoise
          noiseForPublisherNoise = noiseConfig.reachNoiseConfig.noiseForPublisherNoise
          globalReachDpNoise = noiseConfig.reachNoiseConfig.globalReachDpNoise
        }
      }
    }
    return requestBuilder.build()
  }

  private fun getFrequencyNoiseParams(): FlagCountTupleNoiseGenerationParameters {
    return FlagCountTupleNoiseGenerationParameters.newBuilder()
      .apply {
        maximumFrequency = maxFrequency
        contributorsCount = workerStubs.size + 1
        dpParams = noiseConfig.frequencyNoiseConfig
      }
      .build()
  }

  companion object {
    init {
      loadLibrary(
        name = "estimators",
        directoryPath = Paths.get("any_sketch_java/src/main/java/org/wfanet/estimation")
      )
      loadLibrary(
        name = "sketch_encrypter_adapter",
        directoryPath = Paths.get("any_sketch_java/src/main/java/org/wfanet/anysketch/crypto")
      )
    }
  }
}
