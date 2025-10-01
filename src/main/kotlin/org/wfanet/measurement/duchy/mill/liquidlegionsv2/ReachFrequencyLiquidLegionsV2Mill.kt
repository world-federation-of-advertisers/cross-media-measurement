// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.mill.liquidlegionsv2

import com.google.protobuf.ByteString
import java.security.cert.X509Certificate
import java.time.Clock
import java.time.Duration
import java.util.logging.Level
import java.util.logging.Logger
import org.wfanet.anysketch.crypto.CombineElGamalPublicKeysRequest
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.consent.client.duchy.signElgamalPublicKey
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients
import org.wfanet.measurement.duchy.mill.CRYPTO_CPU_DURATION
import org.wfanet.measurement.duchy.mill.Certificate
import org.wfanet.measurement.duchy.mill.liquidlegionsv2.crypto.LiquidLegionsV2Encryption
import org.wfanet.measurement.duchy.service.internal.computations.outputPathList
import org.wfanet.measurement.duchy.service.system.v1alpha.advanceComputationHeader
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.duchy.utils.ReachAndFrequencyResult
import org.wfanet.measurement.duchy.utils.ReachResult
import org.wfanet.measurement.duchy.utils.toAnySketchElGamalPublicKey
import org.wfanet.measurement.duchy.utils.toCmmsElGamalPublicKey
import org.wfanet.measurement.duchy.utils.toV2AlphaElGamalPublicKey
import org.wfanet.measurement.internal.duchy.ComputationDetails.CompletedReason
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.ElGamalPublicKey
import org.wfanet.measurement.internal.duchy.UpdateComputationDetailsRequest
import org.wfanet.measurement.internal.duchy.config.RoleInComputation
import org.wfanet.measurement.internal.duchy.config.RoleInComputation.AGGREGATOR
import org.wfanet.measurement.internal.duchy.config.RoleInComputation.NON_AGGREGATOR
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseOneAtAggregatorResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseOneRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseOneResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseThreeAtAggregatorResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseThreeRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseThreeResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseTwoAtAggregatorResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseTwoResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteInitializationPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteSetupPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteSetupPhaseResponse
import org.wfanet.measurement.internal.duchy.protocol.FlagCountTupleNoiseGenerationParameters
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.ComputationDetails.Parameters
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage
import org.wfanet.measurement.internal.duchy.protocol.completeExecutionPhaseOneAtAggregatorRequest
import org.wfanet.measurement.internal.duchy.protocol.completeExecutionPhaseThreeAtAggregatorRequest
import org.wfanet.measurement.internal.duchy.protocol.completeExecutionPhaseTwoAtAggregatorRequest
import org.wfanet.measurement.internal.duchy.protocol.completeExecutionPhaseTwoRequest
import org.wfanet.measurement.internal.duchy.protocol.completeSetupPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.flagCountTupleNoiseGenerationParameters
import org.wfanet.measurement.internal.duchy.protocol.globalReachDpNoiseBaseline
import org.wfanet.measurement.internal.duchy.protocol.liquidLegionsSketchParameters
import org.wfanet.measurement.internal.duchy.protocol.perBucketFrequencyDpNoiseBaseline
import org.wfanet.measurement.internal.duchy.protocol.reachNoiseDifferentialPrivacyParams
import org.wfanet.measurement.internal.duchy.protocol.registerNoiseGenerationParameters
import org.wfanet.measurement.measurementconsumer.stats.LiquidLegionsV2Methodology
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKt
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt
import org.wfanet.measurement.system.v1alpha.LiquidLegionsV2

/**
 * Mill works on computations using the LiquidLegionSketchAggregationProtocol.
 *
 * @param millId The identifier of this mill, used to claim a work.
 * @param duchyId The identifier of this duchy who owns this mill.
 * @param signingKey handle to a signing private key for consent signaling.
 * @param consentSignalCert The [Certificate] used for consent signaling.
 * @param trustedCertificates [Map] of SKID to trusted certificate
 * @param dataClients clients that have access to local computation storage, i.e., spanner table and
 *   blob store.
 * @param systemComputationParticipantsClient client of the kingdom's system
 *   ComputationParticipantsService.
 * @param systemComputationsClient client of the kingdom's system computationsService.
 * @param systemComputationLogEntriesClient client of the kingdom's system
 *   computationLogEntriesService.
 * @param computationStatsClient client of the duchy's internal ComputationStatsService.
 * @param requestChunkSizeBytes The size of data chunk when sending result to other duchies.
 * @param clock A clock
 * @param maximumAttempts The maximum number of attempts on a computation at the same stage.
 * @param workerStubs A map from other duchies' Ids to their corresponding
 *   computationControlClients, used for passing computation to other duchies.
 * @param cryptoWorker The cryptoWorker that performs the actual computation.
 * @param parallelism The maximum number of threads used for crypto actions.
 */
class ReachFrequencyLiquidLegionsV2Mill(
  millId: String,
  duchyId: String,
  signingKey: SigningKeyHandle,
  consentSignalCert: Certificate,
  private val trustedCertificates: Map<ByteString, X509Certificate>,
  dataClients: ComputationDataClients,
  systemComputationParticipantsClient: ComputationParticipantsCoroutineStub,
  systemComputationsClient: ComputationsGrpcKt.ComputationsCoroutineStub,
  systemComputationLogEntriesClient: ComputationLogEntriesCoroutineStub,
  computationStatsClient: ComputationStatsCoroutineStub,
  private val workerStubs: Map<String, ComputationControlCoroutineStub>,
  private val cryptoWorker: LiquidLegionsV2Encryption,
  workLockDuration: Duration,
  requestChunkSizeBytes: Int = 1024 * 32,
  maximumAttempts: Int = 10,
  clock: Clock = Clock.systemUTC(),
  private val parallelism: Int = 1,
) :
  LiquidLegionsV2Mill(
    millId,
    duchyId,
    signingKey,
    consentSignalCert,
    trustedCertificates,
    dataClients,
    systemComputationParticipantsClient,
    systemComputationsClient,
    systemComputationLogEntriesClient,
    computationStatsClient,
    ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2,
    workerStubs,
    workLockDuration,
    requestChunkSizeBytes,
    maximumAttempts,
    clock,
  ) {
  override val endingStage = Stage.COMPLETE.toProtocolStage()

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
        ::completeExecutionPhaseThreeAtNonAggregator,
    )

  private val stageSequence =
    listOf(
      Stage.INITIALIZATION_PHASE,
      Stage.WAIT_REQUISITIONS_AND_KEY_SET,
      Stage.CONFIRMATION_PHASE,
      Stage.WAIT_TO_START,
      Stage.WAIT_SETUP_PHASE_INPUTS,
      Stage.SETUP_PHASE,
      Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS,
      Stage.EXECUTION_PHASE_ONE,
      Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS,
      Stage.EXECUTION_PHASE_TWO,
      Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS,
      Stage.EXECUTION_PHASE_THREE,
      Stage.COMPLETE,
    )

  override suspend fun processComputationImpl(token: ComputationToken) {
    require(token.computationDetails.hasLiquidLegionsV2()) {
      "Only Liquid Legions V2 computation is supported in this mill."
    }
    val stage = token.computationStage.liquidLegionsSketchAggregationV2
    val role = token.computationDetails.liquidLegionsV2.role
    val action = actions[Pair(stage, role)] ?: error("Unexpected stage or role: ($stage, $role)")
    val updatedToken = action(token)

    val globalId = token.globalComputationId
    val updatedStage = updatedToken.computationStage.liquidLegionsSketchAggregationV2
    logger.info("$globalId@$millId: Stage transitioned from $stage to $updatedStage")
  }

  /** Sends requisition params to the kingdom. */
  private suspend fun sendRequisitionParamsToKingdom(token: ComputationToken) {
    val llv2ComputationDetails = token.computationDetails.liquidLegionsV2
    require(llv2ComputationDetails.hasLocalElgamalKey()) { "Missing local elgamal key." }
    val signedElgamalPublicKey =
      when (Version.fromString(token.computationDetails.kingdomComputation.publicApiVersion)) {
        Version.V2_ALPHA ->
          signElgamalPublicKey(
            llv2ComputationDetails.localElgamalKey.publicKey.toV2AlphaElGamalPublicKey(),
            signingKey,
          )
      }
    val requisitionParams =
      ComputationParticipantKt.requisitionParams {
        duchyCertificate = consentSignalCert.name
        liquidLegionsV2 =
          ComputationParticipantKt.RequisitionParamsKt.liquidLegionsV2 {
            elGamalPublicKey = signedElgamalPublicKey.message.value
            elGamalPublicKeySignature = signedElgamalPublicKey.signature
            elGamalPublicKeySignatureAlgorithmOid = signedElgamalPublicKey.signatureAlgorithmOid
          }
      }

    sendRequisitionParamsToKingdom(token, requisitionParams)
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
        logStageDurationMetric(
          token,
          CRYPTO_CPU_DURATION,
          Duration.ofMillis(cryptoResult.elapsedCpuTimeMillis),
          cryptoCpuDurationHistogram,
        )

        // Updates the newly generated localElgamalKey to the ComputationDetails.
        updateComputationDetails(
          UpdateComputationDetailsRequest.newBuilder()
            .also {
              it.token = token
              it.details =
                token.computationDetails
                  .toBuilder()
                  .apply { liquidLegionsV2Builder.localElgamalKey = cryptoResult.elGamalKeyPair }
                  .build()
            }
            .build()
        )
      }

    sendRequisitionParamsToKingdom(nextToken)

    return dataClients.transitionComputationToStage(
      nextToken,
      stage = Stage.WAIT_REQUISITIONS_AND_KEY_SET.toProtocolStage(),
    )
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
              fullParticipantList.size,
            )
            .map { it.publicKey }
            .toCombinedPublicKey(llv2Details.parameters.ellipticCurveId)
        RoleInComputation.FIRST_NON_AGGREGATOR,
        RoleInComputation.SECOND_NON_AGGREGATOR,
        RoleInComputation.ROLE_IN_COMPUTATION_UNSPECIFIED,
        RoleInComputation.UNRECOGNIZED -> error("Invalid role ${llv2Details.role}")
      }

    return updateComputationDetails(
      UpdateComputationDetailsRequest.newBuilder()
        .apply {
          this.token = token
          details =
            token.computationDetails
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
  }

  /** Sends confirmation to the kingdom and transits the local computation to the next stage. */
  private suspend fun passConfirmationPhase(token: ComputationToken): ComputationToken {
    confirmComputationParticipant(token)

    val latestToken = updatePublicElgamalKey(token)
    return dataClients.transitionComputationToStage(
      latestToken,
      stage =
        when (checkNotNull(token.computationDetails.liquidLegionsV2.role)) {
          AGGREGATOR -> Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage()
          NON_AGGREGATOR -> Stage.WAIT_TO_START.toProtocolStage()
          else -> error("Unknown role: ${latestToken.computationDetails.liquidLegionsV2.role}")
        },
    )
  }

  /** Processes computation in the confirmation phase */
  private suspend fun confirmationPhase(token: ComputationToken): ComputationToken {
    val errorList = mutableListOf<String>()
    val kingdomComputation = token.computationDetails.kingdomComputation
    errorList.addAll(verifyEdpParticipation(kingdomComputation, token.requisitionsList))
    token.computationDetails.liquidLegionsV2.participantList.forEach {
      verifyDuchySignature(it, Version.fromString(kingdomComputation.publicApiVersion))?.also {
        error ->
        errorList.add(error)
      }
    }
    return if (errorList.isEmpty()) {
      passConfirmationPhase(token)
    } else {
      failComputationAtConfirmationPhase(token, errorList)
    }
  }

  private suspend fun completeSetupPhaseAtAggregator(token: ComputationToken): ComputationToken {
    val llv2Details = token.computationDetails.liquidLegionsV2
    require(AGGREGATOR == llv2Details.role) { "invalid role for this function." }
    val inputBlobCount = token.participantCount - 1
    val requisition = dataClients.readAllRequisitionBlobs(token, duchyId)
    val combinedRegisterVector = readAndCombineAllInputBlobs(token, inputBlobCount)
    val (bytes, nextToken) =
      existingOutputOr(token) {
        val request =
          toCompleteSetupPhaseRequest(
            requisition,
            combinedRegisterVector,
            llv2Details,
            token.requisitionsCount,
            token.participantCount,
          )
        val cryptoResult: CompleteSetupPhaseResponse = cryptoWorker.completeSetupPhase(request)
        logStageDurationMetric(
          token,
          CRYPTO_CPU_DURATION,
          Duration.ofMillis(cryptoResult.elapsedCpuTimeMillis),
          cryptoCpuDurationHistogram,
        )
        cryptoResult.combinedRegisterVector
      }

    val nextDuchyId = nextDuchyId(llv2Details.participantList)
    val nextDuchyStub = workerStubs[nextDuchyId] ?: error("$nextDuchyId stub not found")
    val nextDuchyStage =
      getComputationStageInOtherDuchy(token.globalComputationId, nextDuchyId, nextDuchyStub)
        .liquidLegionsSketchAggregationV2

    if (nextDuchyStage.isSequencedAfter(Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS)) {
      logger.log(Level.WARNING) {
        "Skipping advanceComputation for next duchy $nextDuchyId. " +
          "expected_stage=${Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS}, actual_stage=${nextDuchyStage}"
      }
    } else {
      sendAdvanceComputationRequest(
        header =
          advanceComputationHeader(
            LiquidLegionsV2.Description.EXECUTION_PHASE_ONE_INPUT,
            nextToken.globalComputationId,
          ),
        content = addLoggingHook(nextToken, bytes),
        stub = nextDuchyStub,
      )
    }

    return dataClients.transitionComputationToStage(
      nextToken,
      inputsToNextStage = nextToken.outputPathList(),
      stage = Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage(),
    )
  }

  private suspend fun completeSetupPhaseAtNonAggregator(token: ComputationToken): ComputationToken {
    val llv2Details = token.computationDetails.liquidLegionsV2
    require(NON_AGGREGATOR == llv2Details.role) { "invalid role for this function." }
    val requisition = dataClients.readAllRequisitionBlobs(token, duchyId)
    val (bytes, nextToken) =
      existingOutputOr(token) {
        val request =
          toCompleteSetupPhaseRequest(
            requisition,
            ByteString.EMPTY,
            llv2Details,
            token.requisitionsCount,
            token.participantCount,
          )
        val cryptoResult: CompleteSetupPhaseResponse = cryptoWorker.completeSetupPhase(request)
        logStageDurationMetric(
          token,
          CRYPTO_CPU_DURATION,
          Duration.ofMillis(cryptoResult.elapsedCpuTimeMillis),
          cryptoCpuDurationHistogram,
        )
        cryptoResult.combinedRegisterVector
      }

    val aggregatorId = llv2Details.participantList.last().duchyId
    val aggregatorStub = workerStubs[aggregatorId] ?: error("$aggregatorId stub not found")
    val aggregatorStage =
      getComputationStageInOtherDuchy(token.globalComputationId, aggregatorId, aggregatorStub)
        .liquidLegionsSketchAggregationV2

    if (aggregatorStage.isSequencedAfter(Stage.WAIT_SETUP_PHASE_INPUTS)) {
      logger.log(Level.WARNING) {
        "Skipping advanceComputation for next duchy $aggregatorId. " +
          "expected_stage=${Stage.WAIT_SETUP_PHASE_INPUTS}, actual_stage=${aggregatorStage}"
      }
    } else {
      sendAdvanceComputationRequest(
        header =
          advanceComputationHeader(
            LiquidLegionsV2.Description.SETUP_PHASE_INPUT,
            nextToken.globalComputationId,
          ),
        content = addLoggingHook(nextToken, bytes),
        stub = aggregatorStub,
      )
    }

    return dataClients.transitionComputationToStage(
      nextToken,
      inputsToNextStage = nextToken.outputPathList(),
      stage = Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage(),
    )
  }

  private suspend fun completeExecutionPhaseOneAtAggregator(
    token: ComputationToken
  ): ComputationToken {
    val llv2Details = token.computationDetails.liquidLegionsV2
    val llv2Parameters = llv2Details.parameters
    require(AGGREGATOR == llv2Details.role) { "invalid role for this function." }
    val maximumRequestedFrequency = llv2Parameters.maximumFrequency.coerceAtLeast(1)
    val (bytes, nextToken) =
      existingOutputOr(token) {
        val request = completeExecutionPhaseOneAtAggregatorRequest {
          localElGamalKeyPair = llv2Details.localElgamalKey
          compositeElGamalPublicKey = llv2Details.combinedPublicKey
          curveId = llv2Details.parameters.ellipticCurveId.toLong()
          parallelism = this@ReachFrequencyLiquidLegionsV2Mill.parallelism
          combinedRegisterVector = readAndCombineAllInputBlobs(token, 1)
          totalSketchesCount = token.requisitionsCount
          noiseMechanism = llv2Details.parameters.noise.noiseMechanism
          if (llv2Parameters.noise.hasFrequencyNoiseConfig() && maximumRequestedFrequency > 1) {
            noiseParameters = getFrequencyNoiseParams(llv2Parameters, token.participantCount)
          }
        }
        val cryptoResult: CompleteExecutionPhaseOneAtAggregatorResponse =
          cryptoWorker.completeExecutionPhaseOneAtAggregator(request)
        logStageDurationMetric(
          token,
          CRYPTO_CPU_DURATION,
          Duration.ofMillis(cryptoResult.elapsedCpuTimeMillis),
          cryptoCpuDurationHistogram,
        )
        cryptoResult.flagCountTuples
      }

    // Passes the computation to the next duchy.
    val nextDuchyId = nextDuchyId(llv2Details.participantList)
    val nextDuchyStub = workerStubs[nextDuchyId] ?: error("$nextDuchyId stub not found")
    val nextDuchyStage =
      getComputationStageInOtherDuchy(token.globalComputationId, nextDuchyId, nextDuchyStub)
        .liquidLegionsSketchAggregationV2

    if (nextDuchyStage.isSequencedAfter(Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS)) {
      logger.log(Level.WARNING) {
        "Skipping advanceComputation for next duchy $nextDuchyId. " +
          "expected_stage=${Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS}, actual_stage=${nextDuchyStage}"
      }
    } else {
      sendAdvanceComputationRequest(
        header =
          advanceComputationHeader(
            LiquidLegionsV2.Description.EXECUTION_PHASE_TWO_INPUT,
            nextToken.globalComputationId,
          ),
        content = addLoggingHook(nextToken, bytes),
        stub = nextDuchyStub,
      )
    }

    return dataClients.transitionComputationToStage(
      nextToken,
      inputsToNextStage = nextToken.outputPathList(),
      stage = Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS.toProtocolStage(),
    )
  }

  private suspend fun completeExecutionPhaseOneAtNonAggregator(
    token: ComputationToken
  ): ComputationToken {
    val llv2Details = token.computationDetails.liquidLegionsV2
    require(NON_AGGREGATOR == llv2Details.role) { "invalid role for this function." }
    val (bytes, nextToken) =
      existingOutputOr(token) {
        val cryptoResult: CompleteExecutionPhaseOneResponse =
          cryptoWorker.completeExecutionPhaseOne(
            CompleteExecutionPhaseOneRequest.newBuilder()
              .apply {
                localElGamalKeyPair = llv2Details.localElgamalKey
                compositeElGamalPublicKey = llv2Details.combinedPublicKey
                curveId = llv2Details.parameters.ellipticCurveId.toLong()
                parallelism = this@ReachFrequencyLiquidLegionsV2Mill.parallelism
                combinedRegisterVector = readAndCombineAllInputBlobs(token, 1)
              }
              .build()
          )
        logStageDurationMetric(
          token,
          CRYPTO_CPU_DURATION,
          Duration.ofMillis(cryptoResult.elapsedCpuTimeMillis),
          cryptoCpuDurationHistogram,
        )
        cryptoResult.combinedRegisterVector
      }

    // Passes the computation to the next duchy.
    val nextDuchyId = nextDuchyId(llv2Details.participantList)
    val nextDuchyStub = workerStubs[nextDuchyId] ?: error("$nextDuchyId stub not found")
    val nextDuchyStage =
      getComputationStageInOtherDuchy(token.globalComputationId, nextDuchyId, nextDuchyStub)
        .liquidLegionsSketchAggregationV2

    if (nextDuchyStage.isSequencedAfter(Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS)) {
      logger.log(Level.WARNING) {
        "Skipping advanceComputation for next duchy $nextDuchyId. " +
          "expected_stage=${Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS}, actual_stage=${nextDuchyStage}"
      }
    } else {
      sendAdvanceComputationRequest(
        header =
          advanceComputationHeader(
            LiquidLegionsV2.Description.EXECUTION_PHASE_ONE_INPUT,
            nextToken.globalComputationId,
          ),
        content = addLoggingHook(nextToken, bytes),
        stub = nextDuchyStub,
      )
    }

    return dataClients.transitionComputationToStage(
      nextToken,
      inputsToNextStage = nextToken.outputPathList(),
      stage = Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS.toProtocolStage(),
    )
  }

  private suspend fun completeExecutionPhaseTwoAtAggregator(
    token: ComputationToken
  ): ComputationToken {
    val llv2Details = token.computationDetails.liquidLegionsV2
    val llv2Parameters = llv2Details.parameters
    require(AGGREGATOR == token.computationDetails.liquidLegionsV2.role) {
      "invalid role for this function."
    }
    var reach = 0L
    val maximumRequestedFrequency = llv2Parameters.maximumFrequency.coerceAtLeast(1)
    val publicApiVersion =
      Version.fromString(token.computationDetails.kingdomComputation.publicApiVersion)

    val (bytes, tempToken) =
      existingOutputAnd(token) {
        val request = completeExecutionPhaseTwoAtAggregatorRequest {
          localElGamalKeyPair = llv2Details.localElgamalKey
          compositeElGamalPublicKey = llv2Details.combinedPublicKey
          curveId = llv2Parameters.ellipticCurveId.toLong()
          flagCountTuples = readAndCombineAllInputBlobs(token, 1)
          maximumFrequency = maximumRequestedFrequency
          sketchParameters = liquidLegionsSketchParameters {
            decayRate = llv2Parameters.sketchParameters.decayRate
            size = llv2Parameters.sketchParameters.size
          }
          when (publicApiVersion) {
            Version.V2_ALPHA -> {
              val measurementSpec =
                MeasurementSpec.parseFrom(
                  token.computationDetails.kingdomComputation.measurementSpec
                )
              vidSamplingIntervalWidth = measurementSpec.vidSamplingInterval.width
            }
          }
          if (llv2Parameters.noise.hasReachNoiseConfig()) {
            reachDpNoiseBaseline = globalReachDpNoiseBaseline {
              contributorsCount = token.participantCount
              globalReachDpNoise = llv2Parameters.noise.reachNoiseConfig.globalReachDpNoise
            }
          }
          if (llv2Parameters.noise.hasFrequencyNoiseConfig() && (maximumRequestedFrequency > 1)) {
            frequencyNoiseParameters = flagCountTupleNoiseGenerationParameters {
              contributorsCount = token.participantCount
              maximumFrequency = maximumRequestedFrequency
              dpParams = llv2Parameters.noise.frequencyNoiseConfig
            }
          }
          noiseMechanism = llv2Details.parameters.noise.noiseMechanism
        }

        val cryptoResult: CompleteExecutionPhaseTwoAtAggregatorResponse =
          cryptoWorker.completeExecutionPhaseTwoAtAggregator(request)
        logStageDurationMetric(
          token,
          CRYPTO_CPU_DURATION,
          Duration.ofMillis(cryptoResult.elapsedCpuTimeMillis),
          cryptoCpuDurationHistogram,
        )
        reach = cryptoResult.reach
        cryptoResult.sameKeyAggregatorMatrix
      }

    val nextToken =
      if (llv2Details.hasReachEstimate()) {
        // Do nothing if the token has already contained the ReachEstimate
        tempToken
      } else {
        // Update the newly calculated reach to the ComputationDetails.
        updateComputationDetails(
          UpdateComputationDetailsRequest.newBuilder()
            .also {
              it.token = tempToken
              it.details =
                token.computationDetails
                  .toBuilder()
                  .apply { liquidLegionsV2Builder.reachEstimateBuilder.reach = reach }
                  .build()
            }
            .build()
        )
      }

    // If this is a reach-only computation, then our job is done.
    if (maximumRequestedFrequency == 1) {
      sendResultToKingdom(
        token,
        ReachResult(
          reach,
          LiquidLegionsV2Methodology(
            llv2Parameters.sketchParameters.decayRate,
            llv2Parameters.sketchParameters.size,
            0,
          ),
        ),
      )
      return completeComputation(nextToken, CompletedReason.SUCCEEDED)
    }

    // Passes the computation to the next duchy.
    val nextDuchyId = nextDuchyId(llv2Details.participantList)
    val nextDuchyStub = workerStubs[nextDuchyId] ?: error("$nextDuchyId stub not found")
    val nextDuchyStage =
      getComputationStageInOtherDuchy(token.globalComputationId, nextDuchyId, nextDuchyStub)
        .liquidLegionsSketchAggregationV2

    if (nextDuchyStage.isSequencedAfter(Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS)) {
      logger.log(Level.WARNING) {
        "Skipping advanceComputation for next duchy $nextDuchyId. " +
          "expected_stage=${Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS}, actual_stage=${nextDuchyStage}"
      }
    } else {
      sendAdvanceComputationRequest(
        header =
          advanceComputationHeader(
            LiquidLegionsV2.Description.EXECUTION_PHASE_THREE_INPUT,
            nextToken.globalComputationId,
          ),
        content = addLoggingHook(nextToken, bytes),
        stub = nextDuchyStub,
      )
    }

    return dataClients.transitionComputationToStage(
      nextToken,
      inputsToNextStage = nextToken.outputPathList(),
      stage = Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS.toProtocolStage(),
    )
  }

  private suspend fun completeExecutionPhaseTwoAtNonAggregator(
    token: ComputationToken
  ): ComputationToken {
    val llv2Details = token.computationDetails.liquidLegionsV2
    val llv2Parameters = llv2Details.parameters
    require(NON_AGGREGATOR == llv2Details.role) { "invalid role for this function." }
    val maximumRequestedFrequency = llv2Parameters.maximumFrequency.coerceAtLeast(1)
    val (bytes, nextToken) =
      existingOutputOr(token) {
        val request = completeExecutionPhaseTwoRequest {
          localElGamalKeyPair = llv2Details.localElgamalKey
          compositeElGamalPublicKey = llv2Details.combinedPublicKey
          curveId = llv2Parameters.ellipticCurveId.toLong()
          parallelism = this@ReachFrequencyLiquidLegionsV2Mill.parallelism
          flagCountTuples = readAndCombineAllInputBlobs(token, 1)
          if (llv2Parameters.noise.hasFrequencyNoiseConfig()) {
            partialCompositeElGamalPublicKey = llv2Details.partiallyCombinedPublicKey
            if (maximumRequestedFrequency > 1) {
              noiseParameters = getFrequencyNoiseParams(llv2Parameters, token.participantCount)
            }
          }
          noiseMechanism = llv2Details.parameters.noise.noiseMechanism
        }

        val cryptoResult: CompleteExecutionPhaseTwoResponse =
          cryptoWorker.completeExecutionPhaseTwo(request)
        logStageDurationMetric(
          token,
          CRYPTO_CPU_DURATION,
          Duration.ofMillis(cryptoResult.elapsedCpuTimeMillis),
          cryptoCpuDurationHistogram,
        )
        cryptoResult.flagCountTuples
      }

    // Passes the computation to the next duchy.
    val nextDuchyId = nextDuchyId(llv2Details.participantList)
    val nextDuchyStub = workerStubs[nextDuchyId] ?: error("$nextDuchyId stub not found")
    val nextDuchyStage =
      getComputationStageInOtherDuchy(token.globalComputationId, nextDuchyId, nextDuchyStub)
        .liquidLegionsSketchAggregationV2

    if (nextDuchyStage.isSequencedAfter(Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS)) {
      logger.log(Level.WARNING) {
        "Skipping advanceComputation for next duchy $nextDuchyId. " +
          "expected_stage=${Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS}, actual_stage=${nextDuchyStage}"
      }
    } else {
      sendAdvanceComputationRequest(
        header =
          advanceComputationHeader(
            LiquidLegionsV2.Description.EXECUTION_PHASE_TWO_INPUT,
            nextToken.globalComputationId,
          ),
        content = addLoggingHook(nextToken, bytes),
        stub = nextDuchyStub,
      )
    }

    // If this is a reach-only computation, then our job is done.
    if (maximumRequestedFrequency == 1) {
      return completeComputation(nextToken, CompletedReason.SUCCEEDED)
    }

    return dataClients.transitionComputationToStage(
      nextToken,
      inputsToNextStage = nextToken.outputPathList(),
      stage = Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS.toProtocolStage(),
    )
  }

  private suspend fun completeExecutionPhaseThreeAtAggregator(
    token: ComputationToken
  ): ComputationToken {
    val llv2Details = token.computationDetails.liquidLegionsV2
    val llv2Parameters = llv2Details.parameters
    require(AGGREGATOR == llv2Details.role) { "invalid role for this function." }
    require(llv2Details.hasReachEstimate()) { "Reach estimate is missing." }
    val (bytes, nextToken) =
      existingOutputOr(token) {
        val request = completeExecutionPhaseThreeAtAggregatorRequest {
          localElGamalKeyPair = llv2Details.localElgamalKey
          curveId = llv2Parameters.ellipticCurveId.toLong()
          sameKeyAggregatorMatrix = readAndCombineAllInputBlobs(token, 1)
          maximumFrequency = llv2Parameters.maximumFrequency.coerceAtLeast(1)
          if (llv2Parameters.noise.hasFrequencyNoiseConfig()) {
            globalFrequencyDpNoisePerBucket = perBucketFrequencyDpNoiseBaseline {
              contributorsCount = token.participantCount
              dpParams = llv2Parameters.noise.frequencyNoiseConfig
            }
          }
          noiseMechanism = llv2Details.parameters.noise.noiseMechanism
        }

        val cryptoResult: CompleteExecutionPhaseThreeAtAggregatorResponse =
          cryptoWorker.completeExecutionPhaseThreeAtAggregator(request)
        logStageDurationMetric(
          token,
          CRYPTO_CPU_DURATION,
          Duration.ofMillis(cryptoResult.elapsedCpuTimeMillis),
          cryptoCpuDurationHistogram,
        )
        cryptoResult.toByteString()
      }

    val frequencyDistributionMap =
      CompleteExecutionPhaseThreeAtAggregatorResponse.parseFrom(bytes.flatten())
        .frequencyDistributionMap

    sendResultToKingdom(
      nextToken,
      ReachAndFrequencyResult(
        llv2Details.reachEstimate.reach,
        frequencyDistributionMap,
        LiquidLegionsV2Methodology(
          llv2Parameters.sketchParameters.decayRate,
          llv2Parameters.sketchParameters.size,
          0,
        ),
      ),
    )
    return completeComputation(nextToken, CompletedReason.SUCCEEDED)
  }

  private suspend fun completeExecutionPhaseThreeAtNonAggregator(
    token: ComputationToken
  ): ComputationToken {
    val llv2Details = token.computationDetails.liquidLegionsV2
    val llv2Parameters = llv2Details.parameters
    require(NON_AGGREGATOR == llv2Details.role) { "invalid role for this function." }
    val (bytes, nextToken) =
      existingOutputOr(token) {
        val cryptoResult: CompleteExecutionPhaseThreeResponse =
          cryptoWorker.completeExecutionPhaseThree(
            CompleteExecutionPhaseThreeRequest.newBuilder()
              .apply {
                localElGamalKeyPair = llv2Details.localElgamalKey
                curveId = llv2Parameters.ellipticCurveId.toLong()
                parallelism = this@ReachFrequencyLiquidLegionsV2Mill.parallelism
                sameKeyAggregatorMatrix = readAndCombineAllInputBlobs(token, 1)
              }
              .build()
          )
        logStageDurationMetric(
          token,
          CRYPTO_CPU_DURATION,
          Duration.ofMillis(cryptoResult.elapsedCpuTimeMillis),
          cryptoCpuDurationHistogram,
        )
        cryptoResult.sameKeyAggregatorMatrix
      }

    // Passes the computation to the next duchy.
    val nextDuchyId = nextDuchyId(llv2Details.participantList)
    val nextDuchyStub = workerStubs[nextDuchyId] ?: error("$nextDuchyId stub not found")
    val nextDuchyStage =
      getComputationStageInOtherDuchy(token.globalComputationId, nextDuchyId, nextDuchyStub)
        .liquidLegionsSketchAggregationV2

    if (nextDuchyStage.isSequencedAfter(Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS)) {
      logger.log(Level.WARNING) {
        "Skipping advanceComputation for next duchy $nextDuchyId. " +
          "expected_stage=${Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS}, actual_stage=${nextDuchyStage}"
      }
    } else {
      sendAdvanceComputationRequest(
        header =
          advanceComputationHeader(
            LiquidLegionsV2.Description.EXECUTION_PHASE_THREE_INPUT,
            nextToken.globalComputationId,
          ),
        content = addLoggingHook(nextToken, bytes),
        stub = nextDuchyStub,
      )
    }

    // This duchy's responsibility for the computation is done. Mark it COMPLETED locally.
    return completeComputation(nextToken, CompletedReason.SUCCEEDED)
  }

  private fun toCompleteSetupPhaseRequest(
    requisition: ByteString,
    combinedRegisterVector: ByteString,
    llv2Details: LiquidLegionsSketchAggregationV2.ComputationDetails,
    totalRequisitionsCount: Int,
    participantCount: Int,
  ): CompleteSetupPhaseRequest {
    val noiseConfig = llv2Details.parameters.noise
    return completeSetupPhaseRequest {
      this.requisitionRegisterVector = requisition
      this.combinedRegisterVector = combinedRegisterVector
      maximumFrequency = llv2Details.parameters.maximumFrequency.coerceAtLeast(1)
      if (noiseConfig.hasReachNoiseConfig()) {
        noiseParameters = registerNoiseGenerationParameters {
          compositeElGamalPublicKey = llv2Details.combinedPublicKey
          curveId = llv2Details.parameters.ellipticCurveId.toLong()
          contributorsCount = participantCount
          totalSketchesCount = totalRequisitionsCount
          dpParams = reachNoiseDifferentialPrivacyParams {
            blindHistogram = noiseConfig.reachNoiseConfig.blindHistogramNoise
            noiseForPublisherNoise = noiseConfig.reachNoiseConfig.noiseForPublisherNoise
            globalReachDpNoise = noiseConfig.reachNoiseConfig.globalReachDpNoise
          }
        }
        noiseMechanism = llv2Details.parameters.noise.noiseMechanism
        parallelism = this@ReachFrequencyLiquidLegionsV2Mill.parallelism
      }
    }
  }

  private fun getFrequencyNoiseParams(
    llv2Parameters: Parameters,
    participantCount: Int,
  ): FlagCountTupleNoiseGenerationParameters {
    return flagCountTupleNoiseGenerationParameters {
      maximumFrequency = llv2Parameters.maximumFrequency
      contributorsCount = participantCount
      dpParams = llv2Parameters.noise.frequencyNoiseConfig
    }
  }

  /** Returns whether this [Stage] is after [other] in [stageSequence]. */
  private fun Stage.isSequencedAfter(other: Stage): Boolean =
    stageSequence.indexOf(this) > stageSequence.indexOf(other)

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
