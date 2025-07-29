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
import org.wfanet.anysketch.crypto.combineElGamalPublicKeysRequest
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.consent.client.duchy.signElgamalPublicKey
import org.wfanet.measurement.duchy.db.computation.BlobRef
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients
import org.wfanet.measurement.duchy.mill.CRYPTO_CPU_DURATION
import org.wfanet.measurement.duchy.mill.Certificate
import org.wfanet.measurement.duchy.mill.liquidlegionsv2.crypto.ReachOnlyLiquidLegionsV2Encryption
import org.wfanet.measurement.duchy.service.internal.computations.outputPathList
import org.wfanet.measurement.duchy.service.system.v1alpha.advanceComputationHeader
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.duchy.utils.ReachResult
import org.wfanet.measurement.duchy.utils.toAnySketchElGamalPublicKey
import org.wfanet.measurement.duchy.utils.toCmmsElGamalPublicKey
import org.wfanet.measurement.duchy.utils.toV2AlphaElGamalPublicKey
import org.wfanet.measurement.internal.duchy.ComputationDetails.CompletedReason
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.ElGamalPublicKey
import org.wfanet.measurement.internal.duchy.computationDetails
import org.wfanet.measurement.internal.duchy.config.RoleInComputation
import org.wfanet.measurement.internal.duchy.config.RoleInComputation.AGGREGATOR
import org.wfanet.measurement.internal.duchy.config.RoleInComputation.NON_AGGREGATOR
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlyExecutionPhaseAtAggregatorResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlyExecutionPhaseResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlySetupPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlySetupPhaseResponse
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2.Stage
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2Kt
import org.wfanet.measurement.internal.duchy.protocol.completeReachOnlyExecutionPhaseAtAggregatorRequest
import org.wfanet.measurement.internal.duchy.protocol.completeReachOnlyExecutionPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.completeReachOnlyInitializationPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.completeReachOnlySetupPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.globalReachDpNoiseBaseline
import org.wfanet.measurement.internal.duchy.protocol.liquidLegionsSketchParameters
import org.wfanet.measurement.internal.duchy.protocol.reachNoiseDifferentialPrivacyParams
import org.wfanet.measurement.internal.duchy.protocol.registerNoiseGenerationParameters
import org.wfanet.measurement.internal.duchy.updateComputationDetailsRequest
import org.wfanet.measurement.measurementconsumer.stats.LiquidLegionsV2Methodology
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKt
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt
import org.wfanet.measurement.system.v1alpha.ReachOnlyLiquidLegionsV2

/**
 * Mill works on computations using the ReachOnlyLiquidLegionSketchAggregationProtocol.
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
class ReachOnlyLiquidLegionsV2Mill(
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
  private val cryptoWorker: ReachOnlyLiquidLegionsV2Encryption,
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
    ComputationType.REACH_ONLY_LIQUID_LEGIONS_SKETCH_AGGREGATION_V2,
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
      Pair(Stage.EXECUTION_PHASE, AGGREGATOR) to ::completeExecutionPhaseAtAggregator,
      Pair(Stage.EXECUTION_PHASE, NON_AGGREGATOR) to ::completeExecutionPhaseAtNonAggregator,
    )

  private val stageSequence =
    listOf(
      Stage.INITIALIZATION_PHASE,
      Stage.WAIT_REQUISITIONS_AND_KEY_SET,
      Stage.CONFIRMATION_PHASE,
      Stage.WAIT_TO_START,
      Stage.WAIT_SETUP_PHASE_INPUTS,
      Stage.SETUP_PHASE,
      Stage.WAIT_EXECUTION_PHASE_INPUTS,
      Stage.EXECUTION_PHASE,
      Stage.COMPLETE,
    )

  override suspend fun processComputationImpl(token: ComputationToken) {
    require(token.computationDetails.hasReachOnlyLiquidLegionsV2()) {
      "Only Reach Only Liquid Legions V2 computation is supported in this mill."
    }
    val stage = token.computationStage.reachOnlyLiquidLegionsSketchAggregationV2
    val role = token.computationDetails.reachOnlyLiquidLegionsV2.role
    val action = actions[Pair(stage, role)] ?: error("Unexpected stage or role: ($stage, $role)")
    val updatedToken = action(token)

    val globalId = token.globalComputationId
    val updatedStage = updatedToken.computationStage.reachOnlyLiquidLegionsSketchAggregationV2
    logger.info("$globalId@$millId: Stage transitioned from $stage to $updatedStage")
  }

  /** Sends requisition params to the kingdom. */
  private suspend fun sendRequisitionParamsToKingdom(token: ComputationToken) {
    val rollv2ComputationDetails = token.computationDetails.reachOnlyLiquidLegionsV2
    require(rollv2ComputationDetails.hasLocalElgamalKey()) { "Missing local elgamal key." }
    val signedElgamalPublicKey =
      when (Version.fromString(token.computationDetails.kingdomComputation.publicApiVersion)) {
        Version.V2_ALPHA ->
          signElgamalPublicKey(
            rollv2ComputationDetails.localElgamalKey.publicKey.toV2AlphaElGamalPublicKey(),
            signingKey,
          )
      }
    val requisitionParams =
      ComputationParticipantKt.requisitionParams {
        duchyCertificate = consentSignalCert.name
        reachOnlyLiquidLegionsV2 =
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
    val rollv2ComputationDetails = token.computationDetails.reachOnlyLiquidLegionsV2
    val ellipticCurveId = rollv2ComputationDetails.parameters.ellipticCurveId
    require(ellipticCurveId > 0) { "invalid ellipticCurveId $ellipticCurveId" }

    val nextToken =
      if (rollv2ComputationDetails.hasLocalElgamalKey()) {
        // Reuses the key if it is already generated for this computation.
        token
      } else {
        // Generates a new set of ElGamalKeyPair.
        val request = completeReachOnlyInitializationPhaseRequest {
          curveId = ellipticCurveId.toLong()
        }
        val cryptoResult = cryptoWorker.completeReachOnlyInitializationPhase(request)
        logStageDurationMetric(
          token,
          CRYPTO_CPU_DURATION,
          Duration.ofMillis(cryptoResult.elapsedCpuTimeMillis),
          cryptoCpuDurationHistogram,
        )

        // Updates the newly generated localElgamalKey to the ComputationDetails.
        updateComputationDetails(
          updateComputationDetailsRequest {
            this.token = token
            this.details = computationDetails {
              this.blobsStoragePrefix = token.computationDetails.blobsStoragePrefix
              this.endingState = token.computationDetails.endingState
              this.kingdomComputation = token.computationDetails.kingdomComputation
              this.reachOnlyLiquidLegionsV2 =
                ReachOnlyLiquidLegionsSketchAggregationV2Kt.computationDetails {
                  this.role = token.computationDetails.reachOnlyLiquidLegionsV2.role
                  this.parameters = token.computationDetails.reachOnlyLiquidLegionsV2.parameters
                  participant.addAll(
                    token.computationDetails.reachOnlyLiquidLegionsV2.participantList
                  )
                  this.localElgamalKey = cryptoResult.elGamalKeyPair
                }
            }
          }
        )
      }

    sendRequisitionParamsToKingdom(nextToken)

    return dataClients.transitionComputationToStage(
      nextToken,
      stage = Stage.WAIT_REQUISITIONS_AND_KEY_SET.toProtocolStage(),
    )
  }

  private fun List<ElGamalPublicKey>.toCombinedPublicKey(curveId: Int): ElGamalPublicKey {
    val request = combineElGamalPublicKeysRequest {
      this.curveId = curveId.toLong()
      this.elGamalKeys += map { it.toAnySketchElGamalPublicKey() }
    }
    return cryptoWorker.combineElGamalPublicKeys(request).elGamalKeys.toCmmsElGamalPublicKey()
  }

  /**
   * Computes the fully and partially combined Elgamal public keys and caches the result in the
   * computationDetails.
   */
  private suspend fun updatePublicElgamalKey(token: ComputationToken): ComputationToken {
    val rollv2Details = token.computationDetails.reachOnlyLiquidLegionsV2
    val fullParticipantList = rollv2Details.participantList
    val combinedPublicKey =
      fullParticipantList
        .map { it.publicKey }
        .toCombinedPublicKey(rollv2Details.parameters.ellipticCurveId)

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    val partiallyCombinedPublicKey =
      when (rollv2Details.role) {
        // For aggregator, the partial list is the same as the full list.
        AGGREGATOR -> combinedPublicKey
        NON_AGGREGATOR ->
          fullParticipantList
            .subList(
              fullParticipantList.indexOfFirst { it.duchyId == duchyId } + 1,
              fullParticipantList.size,
            )
            .map { it.publicKey }
            .toCombinedPublicKey(rollv2Details.parameters.ellipticCurveId)
        RoleInComputation.FIRST_NON_AGGREGATOR,
        RoleInComputation.SECOND_NON_AGGREGATOR,
        RoleInComputation.ROLE_IN_COMPUTATION_UNSPECIFIED,
        RoleInComputation.UNRECOGNIZED -> error("Invalid role ${rollv2Details.role}")
      }

    return updateComputationDetails(
      updateComputationDetailsRequest {
        this.token = token
        details = computationDetails {
          this.blobsStoragePrefix = token.computationDetails.blobsStoragePrefix
          this.endingState = token.computationDetails.endingState
          this.kingdomComputation = token.computationDetails.kingdomComputation
          this.reachOnlyLiquidLegionsV2 =
            ReachOnlyLiquidLegionsSketchAggregationV2Kt.computationDetails {
              this.role = token.computationDetails.reachOnlyLiquidLegionsV2.role
              this.parameters = token.computationDetails.reachOnlyLiquidLegionsV2.parameters
              token.computationDetails.reachOnlyLiquidLegionsV2.participantList.forEach {
                this.participant += it
              }
              this.localElgamalKey =
                token.computationDetails.reachOnlyLiquidLegionsV2.localElgamalKey
              this.combinedPublicKey = combinedPublicKey
              this.partiallyCombinedPublicKey = partiallyCombinedPublicKey
            }
        }
      }
    )
  }

  /** Sends confirmation to the kingdom and transits the local computation to the next stage. */
  private suspend fun passConfirmationPhase(token: ComputationToken): ComputationToken {
    confirmComputationParticipant(token)

    val latestToken = updatePublicElgamalKey(token)
    return dataClients.transitionComputationToStage(
      latestToken,
      stage =
        when (checkNotNull(token.computationDetails.reachOnlyLiquidLegionsV2.role)) {
          AGGREGATOR -> Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage()
          NON_AGGREGATOR -> Stage.WAIT_TO_START.toProtocolStage()
          else ->
            error("Unknown role: ${latestToken.computationDetails.reachOnlyLiquidLegionsV2.role}")
        },
    )
  }

  /** Processes computation in the confirmation phase */
  private suspend fun confirmationPhase(token: ComputationToken): ComputationToken {
    val errorList = mutableListOf<String>()
    val kingdomComputation = token.computationDetails.kingdomComputation
    errorList.addAll(verifyEdpParticipation(kingdomComputation, token.requisitionsList))
    token.computationDetails.reachOnlyLiquidLegionsV2.participantList.forEach {
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
    val rollv2Details = token.computationDetails.reachOnlyLiquidLegionsV2
    require(AGGREGATOR == rollv2Details.role) { "invalid role for this function." }
    val inputBlobCount = token.participantCount - 1
    val (bytes, nextToken) =
      existingOutputOr(token) {
        val request =
          dataClients
            .readAllRequisitionBlobs(token, duchyId)
            .concat(readAndCombineAllInputBlobsSetupPhaseAtAggregator(token, inputBlobCount))
            .toCompleteSetupPhaseAtAggregatorRequest(
              rollv2Details,
              token.requisitionsCount,
              token.participantCount,
            )
        val cryptoResult: CompleteReachOnlySetupPhaseResponse =
          cryptoWorker.completeReachOnlySetupPhaseAtAggregator(request)
        logStageDurationMetric(
          token,
          CRYPTO_CPU_DURATION,
          Duration.ofMillis(cryptoResult.elapsedCpuTimeMillis),
          cryptoCpuDurationHistogram,
        )
        // The nextToken consists of the CRV and the noise ciphertext.
        cryptoResult.combinedRegisterVector.concat(cryptoResult.serializedExcessiveNoiseCiphertext)
      }

    val nextDuchyId = nextDuchyId(rollv2Details.participantList)
    val nextDuchyStub = workerStubs[nextDuchyId] ?: error("$nextDuchyId stub not found")
    val nextDuchyStage =
      getComputationStageInOtherDuchy(token.globalComputationId, nextDuchyId, nextDuchyStub)
        .reachOnlyLiquidLegionsSketchAggregationV2

    if (nextDuchyStage.isSequencedAfter(Stage.WAIT_EXECUTION_PHASE_INPUTS)) {
      logger.log(Level.WARNING) {
        "Skipping advanceComputation for next duchy $nextDuchyId. " +
          "expected_stage=${Stage.WAIT_EXECUTION_PHASE_INPUTS}, actual_stage=${nextDuchyStage}"
      }
    } else {
      sendAdvanceComputationRequest(
        header =
          advanceComputationHeader(
            ReachOnlyLiquidLegionsV2.Description.EXECUTION_PHASE_INPUT,
            nextToken.globalComputationId,
          ),
        content = addLoggingHook(nextToken, bytes),
        stub = nextDuchyStub,
      )
    }

    return dataClients.transitionComputationToStage(
      nextToken,
      inputsToNextStage = nextToken.outputPathList(),
      stage = Stage.WAIT_EXECUTION_PHASE_INPUTS.toProtocolStage(),
    )
  }

  private suspend fun completeSetupPhaseAtNonAggregator(token: ComputationToken): ComputationToken {
    val rollv2Details = token.computationDetails.reachOnlyLiquidLegionsV2
    require(NON_AGGREGATOR == rollv2Details.role) { "invalid role for this function." }
    val (bytes, nextToken) =
      existingOutputOr(token) {
        val request =
          dataClients
            .readAllRequisitionBlobs(token, duchyId)
            .toCompleteReachOnlySetupPhaseRequest(
              rollv2Details,
              token.requisitionsCount,
              token.participantCount,
            )
        val cryptoResult: CompleteReachOnlySetupPhaseResponse =
          cryptoWorker.completeReachOnlySetupPhase(request)
        logStageDurationMetric(
          token,
          CRYPTO_CPU_DURATION,
          Duration.ofMillis(cryptoResult.elapsedCpuTimeMillis),
          cryptoCpuDurationHistogram,
        )
        // The nextToken consists of the CRV and the noise ciphertext.
        cryptoResult.combinedRegisterVector.concat(cryptoResult.serializedExcessiveNoiseCiphertext)
      }

    val aggregatorId = rollv2Details.participantList.last().duchyId
    val aggregatorStub = workerStubs[aggregatorId] ?: error("$aggregatorId stub not found")
    val aggregatorStage =
      getComputationStageInOtherDuchy(token.globalComputationId, aggregatorId, aggregatorStub)
        .reachOnlyLiquidLegionsSketchAggregationV2

    if (aggregatorStage.isSequencedAfter(Stage.WAIT_SETUP_PHASE_INPUTS)) {
      logger.log(Level.WARNING) {
        "Skipping advanceComputation for next duchy $aggregatorId. " +
          "expected_stage=${Stage.WAIT_SETUP_PHASE_INPUTS}, actual_stage=${aggregatorStage}"
      }
    } else {
      sendAdvanceComputationRequest(
        header =
          advanceComputationHeader(
            ReachOnlyLiquidLegionsV2.Description.SETUP_PHASE_INPUT,
            nextToken.globalComputationId,
          ),
        content = addLoggingHook(nextToken, bytes),
        stub = aggregatorStub,
      )
    }

    return dataClients.transitionComputationToStage(
      nextToken,
      inputsToNextStage = nextToken.outputPathList(),
      stage = Stage.WAIT_EXECUTION_PHASE_INPUTS.toProtocolStage(),
    )
  }

  private suspend fun completeExecutionPhaseAtAggregator(
    token: ComputationToken
  ): ComputationToken {
    val rollv2Details = token.computationDetails.reachOnlyLiquidLegionsV2
    require(AGGREGATOR == rollv2Details.role) { "invalid role for this function." }
    val rollv2Parameters = rollv2Details.parameters
    val noiseConfig = rollv2Details.parameters.noise
    val measurementSpec =
      MeasurementSpec.parseFrom(token.computationDetails.kingdomComputation.measurementSpec)
    val inputBlob = readAndCombineAllInputBlobs(token, 1)
    require(inputBlob.size() >= BYTES_PER_CIPHERTEXT) {
      "Invalid input blob size. Input blob ${inputBlob.toStringUtf8()} has size " +
        "${inputBlob.size()} which is less than ($BYTES_PER_CIPHERTEXT)."
    }
    val (_, nextToken) =
      existingOutputOr(token) {
        val request = completeReachOnlyExecutionPhaseAtAggregatorRequest {
          combinedRegisterVector = inputBlob.substring(0, inputBlob.size() - BYTES_PER_CIPHERTEXT)
          localElGamalKeyPair = rollv2Details.localElgamalKey
          curveId = rollv2Details.parameters.ellipticCurveId.toLong()
          serializedExcessiveNoiseCiphertext =
            inputBlob.substring(inputBlob.size() - BYTES_PER_CIPHERTEXT, inputBlob.size())
          if (rollv2Parameters.noise.hasReachNoiseConfig()) {
            reachDpNoiseBaseline = globalReachDpNoiseBaseline {
              contributorsCount = token.participantCount
              globalReachDpNoise = rollv2Parameters.noise.reachNoiseConfig.globalReachDpNoise
            }
          }
          sketchParameters = liquidLegionsSketchParameters {
            decayRate = rollv2Parameters.sketchParameters.decayRate
            size = rollv2Parameters.sketchParameters.size
          }
          vidSamplingIntervalWidth = measurementSpec.vidSamplingInterval.width
          if (noiseConfig.hasReachNoiseConfig()) {
            noiseParameters = registerNoiseGenerationParameters {
              compositeElGamalPublicKey = rollv2Details.combinedPublicKey
              curveId = rollv2Details.parameters.ellipticCurveId.toLong()
              contributorsCount = token.participantCount
              totalSketchesCount = token.requisitionsCount
              dpParams = reachNoiseDifferentialPrivacyParams {
                blindHistogram = noiseConfig.reachNoiseConfig.blindHistogramNoise
                noiseForPublisherNoise = noiseConfig.reachNoiseConfig.noiseForPublisherNoise
                globalReachDpNoise = noiseConfig.reachNoiseConfig.globalReachDpNoise
              }
            }
            noiseMechanism = rollv2Details.parameters.noise.noiseMechanism
          }
          parallelism = this@ReachOnlyLiquidLegionsV2Mill.parallelism
        }
        val cryptoResult: CompleteReachOnlyExecutionPhaseAtAggregatorResponse =
          cryptoWorker.completeReachOnlyExecutionPhaseAtAggregator(request)
        logStageDurationMetric(
          token,
          CRYPTO_CPU_DURATION,
          Duration.ofMillis(cryptoResult.elapsedCpuTimeMillis),
          cryptoCpuDurationHistogram,
        )
        sendResultToKingdom(
          token,
          ReachResult(
            cryptoResult.reach,
            LiquidLegionsV2Methodology(
              rollv2Parameters.sketchParameters.decayRate,
              rollv2Parameters.sketchParameters.size,
              0,
            ),
          ),
        )
        ByteString.EMPTY
      }

    return completeComputation(nextToken, CompletedReason.SUCCEEDED)
  }

  private suspend fun completeExecutionPhaseAtNonAggregator(
    token: ComputationToken
  ): ComputationToken {
    val rollv2Details = token.computationDetails.reachOnlyLiquidLegionsV2
    require(NON_AGGREGATOR == rollv2Details.role) { "invalid role for this function." }
    val inputBlob = readAndCombineAllInputBlobs(token, 1)
    require(inputBlob.size() >= BYTES_PER_CIPHERTEXT) {
      "Invalid input blob size. Input blob ${inputBlob.toStringUtf8()} has size " +
        "${inputBlob.size()} which is less than ($BYTES_PER_CIPHERTEXT)."
    }
    val (bytes, nextToken) =
      existingOutputOr(token) {
        val cryptoResult: CompleteReachOnlyExecutionPhaseResponse =
          cryptoWorker.completeReachOnlyExecutionPhase(
            completeReachOnlyExecutionPhaseRequest {
              combinedRegisterVector =
                inputBlob.substring(0, inputBlob.size() - BYTES_PER_CIPHERTEXT)
              localElGamalKeyPair = rollv2Details.localElgamalKey
              curveId = rollv2Details.parameters.ellipticCurveId.toLong()
              serializedExcessiveNoiseCiphertext =
                inputBlob.substring(inputBlob.size() - BYTES_PER_CIPHERTEXT, inputBlob.size())
              parallelism = this@ReachOnlyLiquidLegionsV2Mill.parallelism
            }
          )
        logStageDurationMetric(
          token,
          CRYPTO_CPU_DURATION,
          Duration.ofMillis(cryptoResult.elapsedCpuTimeMillis),
          cryptoCpuDurationHistogram,
        )
        cryptoResult.combinedRegisterVector.concat(cryptoResult.serializedExcessiveNoiseCiphertext)
      }

    // Passes the computation to the next duchy.
    val nextDuchyId = nextDuchyId(rollv2Details.participantList)
    val nextDuchyStub = workerStubs[nextDuchyId] ?: error("$nextDuchyId stub not found")
    val nextDuchyStage =
      getComputationStageInOtherDuchy(token.globalComputationId, nextDuchyId, nextDuchyStub)
        .reachOnlyLiquidLegionsSketchAggregationV2

    if (nextDuchyStage.isSequencedAfter(Stage.WAIT_EXECUTION_PHASE_INPUTS)) {
      logger.log(Level.WARNING) {
        "Skipping advanceComputation for next duchy $nextDuchyId. " +
          "expected_stage=${Stage.WAIT_EXECUTION_PHASE_INPUTS}, actual_stage=${nextDuchyStage}"
      }
    } else {
      sendAdvanceComputationRequest(
        header =
          advanceComputationHeader(
            ReachOnlyLiquidLegionsV2.Description.EXECUTION_PHASE_INPUT,
            nextToken.globalComputationId,
          ),
        content = addLoggingHook(nextToken, bytes),
        stub = nextDuchyStub,
      )
    }

    return completeComputation(nextToken, CompletedReason.SUCCEEDED)
  }

  private fun ByteString.toCompleteReachOnlySetupPhaseRequest(
    rollv2Details: ReachOnlyLiquidLegionsSketchAggregationV2.ComputationDetails,
    totalRequisitionsCount: Int,
    participantCount: Int,
  ): CompleteReachOnlySetupPhaseRequest {
    val noiseConfig = rollv2Details.parameters.noise
    return completeReachOnlySetupPhaseRequest {
      combinedRegisterVector = this@toCompleteReachOnlySetupPhaseRequest
      curveId = rollv2Details.parameters.ellipticCurveId.toLong()
      if (noiseConfig.hasReachNoiseConfig()) {
        noiseParameters = registerNoiseGenerationParameters {
          compositeElGamalPublicKey = rollv2Details.combinedPublicKey
          curveId = rollv2Details.parameters.ellipticCurveId.toLong()
          contributorsCount = participantCount
          totalSketchesCount = totalRequisitionsCount
          dpParams = reachNoiseDifferentialPrivacyParams {
            blindHistogram = noiseConfig.reachNoiseConfig.blindHistogramNoise
            noiseForPublisherNoise = noiseConfig.reachNoiseConfig.noiseForPublisherNoise
            globalReachDpNoise = noiseConfig.reachNoiseConfig.globalReachDpNoise
          }
        }
        noiseMechanism = rollv2Details.parameters.noise.noiseMechanism
      }
      compositeElGamalPublicKey = rollv2Details.combinedPublicKey
      serializedExcessiveNoiseCiphertext = ByteString.EMPTY
      parallelism = this@ReachOnlyLiquidLegionsV2Mill.parallelism
    }
  }

  private fun ByteString.toCompleteSetupPhaseAtAggregatorRequest(
    rollv2Details: ReachOnlyLiquidLegionsSketchAggregationV2.ComputationDetails,
    totalRequisitionsCount: Int,
    participantCount: Int,
  ): CompleteReachOnlySetupPhaseRequest {
    val noiseConfig = rollv2Details.parameters.noise
    val combinedInputBlobs = this@toCompleteSetupPhaseAtAggregatorRequest
    val combinedRegisterVectorSizeBytes =
      combinedInputBlobs.size() - (participantCount - 1) * BYTES_PER_CIPHERTEXT
    return completeReachOnlySetupPhaseRequest {
      combinedRegisterVector = combinedInputBlobs.substring(0, combinedRegisterVectorSizeBytes)
      curveId = rollv2Details.parameters.ellipticCurveId.toLong()
      if (noiseConfig.hasReachNoiseConfig()) {
        noiseParameters = registerNoiseGenerationParameters {
          compositeElGamalPublicKey = rollv2Details.combinedPublicKey
          curveId = rollv2Details.parameters.ellipticCurveId.toLong()
          contributorsCount = participantCount
          totalSketchesCount = totalRequisitionsCount
          dpParams = reachNoiseDifferentialPrivacyParams {
            blindHistogram = noiseConfig.reachNoiseConfig.blindHistogramNoise
            noiseForPublisherNoise = noiseConfig.reachNoiseConfig.noiseForPublisherNoise
            globalReachDpNoise = noiseConfig.reachNoiseConfig.globalReachDpNoise
          }
        }
        noiseMechanism = rollv2Details.parameters.noise.noiseMechanism
      }
      compositeElGamalPublicKey = rollv2Details.combinedPublicKey
      serializedExcessiveNoiseCiphertext =
        combinedInputBlobs.substring(combinedRegisterVectorSizeBytes, combinedInputBlobs.size())
      parallelism = this@ReachOnlyLiquidLegionsV2Mill.parallelism
    }
  }

  /** Reads all input blobs and combines all the bytes together. */
  private suspend fun readAndCombineAllInputBlobsSetupPhaseAtAggregator(
    token: ComputationToken,
    count: Int,
  ): ByteString {
    val blobMap: Map<BlobRef, ByteString> = dataClients.readInputBlobs(token)
    if (blobMap.size != count) {
      throw ComputationDataClients.PermanentErrorException(
        "Unexpected number of input blobs. expected $count, actual ${blobMap.size}."
      )
    }
    var combinedRegisterVector = ByteString.EMPTY
    var combinedNoiseCiphertext = ByteString.EMPTY
    for (str in blobMap.values) {
      require(str.size() >= BYTES_PER_CIPHERTEXT) {
        "Invalid input blob size. Input blob ${str.toStringUtf8()} has size " +
          "${str.size()} which is less than ($BYTES_PER_CIPHERTEXT)."
      }
      combinedRegisterVector =
        combinedRegisterVector.concat(str.substring(0, str.size() - BYTES_PER_CIPHERTEXT))
      combinedNoiseCiphertext =
        combinedNoiseCiphertext.concat(str.substring(str.size() - BYTES_PER_CIPHERTEXT, str.size()))
    }
    return combinedRegisterVector.concat(combinedNoiseCiphertext)
  }

  /** Returns whether this [Stage] is after [other] in [stageSequence]. */
  private fun Stage.isSequencedAfter(other: Stage): Boolean =
    stageSequence.indexOf(this) > stageSequence.indexOf(other)

  companion object {
    private const val BYTES_PER_CIPHERTEXT = 66

    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
