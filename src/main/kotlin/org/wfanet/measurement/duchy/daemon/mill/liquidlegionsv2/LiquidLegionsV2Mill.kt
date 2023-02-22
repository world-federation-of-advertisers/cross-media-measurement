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
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.metrics.LongHistogram
import io.opentelemetry.api.metrics.Meter
import java.nio.file.Paths
import java.security.SignatureException
import java.security.cert.CertPathValidatorException
import java.security.cert.X509Certificate
import java.time.Clock
import java.time.Duration
import java.util.logging.Logger
import kotlin.math.min
import org.wfanet.anysketch.crypto.CombineElGamalPublicKeysRequest
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.identity.DuchyInfo
import org.wfanet.measurement.common.loadLibrary
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.consent.client.duchy.encryptResult
import org.wfanet.measurement.consent.client.duchy.signElgamalPublicKey
import org.wfanet.measurement.consent.client.duchy.signResult
import org.wfanet.measurement.consent.client.duchy.verifyDataProviderParticipation
import org.wfanet.measurement.consent.client.duchy.verifyElGamalPublicKey
import org.wfanet.measurement.duchy.daemon.mill.CRYPTO_LIB_CPU_DURATION
import org.wfanet.measurement.duchy.daemon.mill.Certificate
import org.wfanet.measurement.duchy.daemon.mill.MillBase
import org.wfanet.measurement.duchy.daemon.mill.PermanentComputationError
import org.wfanet.measurement.duchy.daemon.mill.liquidlegionsv2.crypto.LiquidLegionsV2Encryption
import org.wfanet.measurement.duchy.daemon.utils.ReachAndFrequency
import org.wfanet.measurement.duchy.daemon.utils.toAnySketchElGamalPublicKey
import org.wfanet.measurement.duchy.daemon.utils.toCmmsElGamalPublicKey
import org.wfanet.measurement.duchy.daemon.utils.toV2AlphaElGamalPublicKey
import org.wfanet.measurement.duchy.daemon.utils.toV2AlphaEncryptionPublicKey
import org.wfanet.measurement.duchy.daemon.utils.toV2AlphaMeasurementResult
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients
import org.wfanet.measurement.duchy.service.internal.computations.outputPathList
import org.wfanet.measurement.duchy.service.system.v1alpha.advanceComputationHeader
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ComputationDetails.CompletedReason
import org.wfanet.measurement.internal.duchy.ComputationDetails.KingdomComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.ElGamalPublicKey
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
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.ComputationDetails.ComputationParticipant
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.ComputationDetails.Parameters
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKey
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt
import org.wfanet.measurement.system.v1alpha.ConfirmComputationParticipantRequest
import org.wfanet.measurement.system.v1alpha.LiquidLegionsV2
import org.wfanet.measurement.system.v1alpha.SetParticipantRequisitionParamsRequest

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
 * @param throttler A throttler used to rate limit the frequency of the mill polling from the
 *   computation table.
 * @param requestChunkSizeBytes The size of data chunk when sending result to other duchies.
 * @param clock A clock
 * @param maximumAttempts The maximum number of attempts on a computation at the same stage.
 * @param workerStubs A map from other duchies' Ids to their corresponding
 *   computationControlClients, used for passing computation to other duchies.
 * @param cryptoWorker The cryptoWorker that performs the actual computation.
 */
class LiquidLegionsV2Mill(
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
  throttler: MinimumIntervalThrottler,
  private val workerStubs: Map<String, ComputationControlCoroutineStub>,
  private val cryptoWorker: LiquidLegionsV2Encryption,
  workLockDuration: Duration,
  openTelemetry: OpenTelemetry,
  requestChunkSizeBytes: Int = 1024 * 32,
  maximumAttempts: Int = 10,
  clock: Clock = Clock.systemUTC(),
) :
  MillBase(
    millId,
    duchyId,
    signingKey,
    consentSignalCert,
    dataClients,
    systemComputationParticipantsClient,
    systemComputationsClient,
    systemComputationLogEntriesClient,
    computationStatsClient,
    throttler,
    ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2,
    workLockDuration,
    requestChunkSizeBytes,
    maximumAttempts,
    clock,
    openTelemetry
  ) {
  private val meter: Meter = openTelemetry.getMeter(LiquidLegionsV2Mill::class.java.name)

  private val initializationPhaseCryptoCpuTimeDurationHistogram: LongHistogram =
    meter.histogramBuilder("initialization_phase_crypto_cpu_time_duration_millis").ofLongs().build()

  private val setupPhaseCryptoCpuTimeDurationHistogram: LongHistogram =
    meter.histogramBuilder("setup_phase_crypto_cpu_time_duration_millis").ofLongs().build()

  private val executionPhaseOneCryptoCpuTimeDurationHistogram: LongHistogram =
    meter.histogramBuilder("execution_phase_one_crypto_cpu_time_duration_millis").ofLongs().build()

  private val executionPhaseTwoCryptoCpuTimeDurationHistogram: LongHistogram =
    meter.histogramBuilder("execution_phase_two_crypto_cpu_time_duration_millis").ofLongs().build()

  private val executionPhaseThreeCryptoCpuTimeDurationHistogram: LongHistogram =
    meter
      .histogramBuilder("execution_phase_three_crypto_cpu_time_duration_millis")
      .ofLongs()
      .build()

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
            signingKey
          )
        Version.VERSION_UNSPECIFIED -> error("Public api version is invalid or unspecified.")
      }

    val request =
      SetParticipantRequisitionParamsRequest.newBuilder()
        .apply {
          name = ComputationParticipantKey(token.globalComputationId, duchyId).toName()
          requisitionParamsBuilder.apply {
            duchyCertificate = consentSignalCert.name
            liquidLegionsV2Builder.apply {
              elGamalPublicKey = signedElgamalPublicKey.data
              elGamalPublicKeySignature = signedElgamalPublicKey.signature
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
        logStageDurationMetric(
          token,
          CRYPTO_LIB_CPU_DURATION,
          cryptoResult.elapsedCpuTimeMillis,
          initializationPhaseCryptoCpuTimeDurationHistogram
        )

        // Updates the newly generated localElgamalKey to the ComputationDetails.
        dataClients.computationsClient
          .updateComputationDetails(
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
          .token
      }

    sendRequisitionParamsToKingdom(nextToken)

    return dataClients.transitionComputationToStage(
      nextToken,
      stage = Stage.WAIT_REQUISITIONS_AND_KEY_SET.toProtocolStage()
    )
  }

  /**
   * Verifies that all EDPs have participated.
   *
   * @return a list of error messages if anything is wrong, otherwise an empty list.
   */
  private fun verifyEdpParticipation(
    details: KingdomComputationDetails,
    requisitions: Iterable<RequisitionMetadata>,
  ): List<String> {
    when (Version.fromString(details.publicApiVersion)) {
      Version.V2_ALPHA -> {}
      Version.VERSION_UNSPECIFIED -> error("Public api version is invalid or unspecified.")
    }

    val errorList = mutableListOf<String>()
    val measurementSpec = MeasurementSpec.parseFrom(details.measurementSpec)
    if (!verifyDataProviderParticipation(measurementSpec, requisitions.map { it.details.nonce })) {
      errorList.add("Cannot verify participation of all DataProviders.")
    }
    for (requisition in requisitions) {
      if (requisition.details.externalFulfillingDuchyId == duchyId && requisition.path.isBlank()) {
        errorList.add(
          "Missing expected data for requisition ${requisition.externalKey.externalRequisitionId}."
        )
      }
    }
    return errorList
  }

  /**
   * Verifies the ElGamal public key of [duchy].
   *
   * @return the error message if verification fails, or else `null`
   */
  private fun verifyDuchySignature(
    duchy: ComputationParticipant,
    publicApiVersion: Version
  ): String? {
    val duchyInfo: DuchyInfo.Entry =
      requireNotNull(DuchyInfo.getByDuchyId(duchy.duchyId)) {
        "DuchyInfo not found for ${duchy.duchyId}"
      }
    when (publicApiVersion) {
      Version.V2_ALPHA -> {
        try {
          verifyElGamalPublicKey(
            duchy.elGamalPublicKey,
            duchy.elGamalPublicKeySignature,
            readCertificate(duchy.duchyCertificateDer),
            trustedCertificates.getValue(duchyInfo.rootCertificateSkid)
          )
        } catch (e: CertPathValidatorException) {
          return "Certificate path invalid for Duchy ${duchy.duchyId}"
        } catch (e: SignatureException) {
          return "Invalid ElGamal public key signature for Duchy ${duchy.duchyId}"
        }
      }
      Version.VERSION_UNSPECIFIED -> error("Public api version is invalid or unspecified.")
    }
    return null
  }

  /** Fails a computation both locally and at the kingdom when the confirmation fails. */
  private fun failComputationAtConfirmationPhase(
    token: ComputationToken,
    errorList: List<String>
  ): ComputationToken {
    val errorMessage =
      "@Mill $millId, Computation ${token.globalComputationId} failed due to:\n" +
        errorList.joinToString(separator = "\n")
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
        UNKNOWN,
        UNRECOGNIZED -> error("Invalid role ${llv2Details.role}")
      }

    return dataClients.computationsClient
      .updateComputationDetails(
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
      .token
  }

  /** Sends confirmation to the kingdom and transits the local computation to the next stage. */
  private suspend fun passConfirmationPhase(token: ComputationToken): ComputationToken {
    systemComputationParticipantsClient.confirmComputationParticipant(
      ConfirmComputationParticipantRequest.newBuilder()
        .apply { name = ComputationParticipantKey(token.globalComputationId, duchyId).toName() }
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
    val (bytes, nextToken) =
      existingOutputOr(token) {
        val request =
          dataClients
            .readAllRequisitionBlobs(token, duchyId)
            .concat(readAndCombineAllInputBlobs(token, workerStubs.size))
            .toCompleteSetupPhaseRequest(token, llv2Details, token.requisitionsCount)
        val cryptoResult: CompleteSetupPhaseResponse = cryptoWorker.completeSetupPhase(request)
        logStageDurationMetric(
          token,
          CRYPTO_LIB_CPU_DURATION,
          cryptoResult.elapsedCpuTimeMillis,
          setupPhaseCryptoCpuTimeDurationHistogram
        )
        cryptoResult.combinedRegisterVector
      }

    sendAdvanceComputationRequest(
      header =
        advanceComputationHeader(
          LiquidLegionsV2.Description.EXECUTION_PHASE_ONE_INPUT,
          token.globalComputationId
        ),
      content = addLoggingHook(token, bytes),
      stub = nextDuchyStub(llv2Details.participantList)
    )

    return dataClients.transitionComputationToStage(
      nextToken,
      inputsToNextStage = nextToken.outputPathList(),
      stage = Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage()
    )
  }

  private suspend fun completeSetupPhaseAtNonAggregator(token: ComputationToken): ComputationToken {
    val llv2Details = token.computationDetails.liquidLegionsV2
    require(NON_AGGREGATOR == llv2Details.role) { "invalid role for this function." }
    val (bytes, nextToken) =
      existingOutputOr(token) {
        val request =
          dataClients
            .readAllRequisitionBlobs(token, duchyId)
            .toCompleteSetupPhaseRequest(token, llv2Details, token.requisitionsCount)
        val cryptoResult: CompleteSetupPhaseResponse = cryptoWorker.completeSetupPhase(request)
        logStageDurationMetric(
          token,
          CRYPTO_LIB_CPU_DURATION,
          cryptoResult.elapsedCpuTimeMillis,
          setupPhaseCryptoCpuTimeDurationHistogram
        )
        cryptoResult.combinedRegisterVector
      }

    sendAdvanceComputationRequest(
      header =
        advanceComputationHeader(
          LiquidLegionsV2.Description.SETUP_PHASE_INPUT,
          token.globalComputationId
        ),
      content = addLoggingHook(token, bytes),
      stub = aggregatorDuchyStub(llv2Details.participantList.last().duchyId)
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
    val llv2Details = token.computationDetails.liquidLegionsV2
    val llv2Parameters = llv2Details.parameters
    require(AGGREGATOR == llv2Details.role) { "invalid role for this function." }
    val (bytes, nextToken) =
      existingOutputOr(token) {
        val requestBuilder =
          CompleteExecutionPhaseOneAtAggregatorRequest.newBuilder().apply {
            localElGamalKeyPair = llv2Details.localElgamalKey
            compositeElGamalPublicKey = llv2Details.combinedPublicKey
            curveId = llv2Details.parameters.ellipticCurveId.toLong()
            combinedRegisterVector = readAndCombineAllInputBlobs(token, 1)
            totalSketchesCount = token.requisitionsCount
          }
        if (
          llv2Parameters.noise.hasFrequencyNoiseConfig() &&
            (getMaximumRequestedFrequency(token) > 1)
        ) {
          requestBuilder.noiseParameters = getFrequencyNoiseParams(token, llv2Parameters)
        }
        val cryptoResult: CompleteExecutionPhaseOneAtAggregatorResponse =
          cryptoWorker.completeExecutionPhaseOneAtAggregator(requestBuilder.build())
        logStageDurationMetric(
          token,
          CRYPTO_LIB_CPU_DURATION,
          cryptoResult.elapsedCpuTimeMillis,
          executionPhaseOneCryptoCpuTimeDurationHistogram
        )
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
      stub = nextDuchyStub(llv2Details.participantList)
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
                combinedRegisterVector = readAndCombineAllInputBlobs(token, 1)
              }
              .build()
          )
        logStageDurationMetric(
          token,
          CRYPTO_LIB_CPU_DURATION,
          cryptoResult.elapsedCpuTimeMillis,
          executionPhaseOneCryptoCpuTimeDurationHistogram
        )
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
      stub = nextDuchyStub(llv2Details.participantList)
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
    val llv2Details = token.computationDetails.liquidLegionsV2
    val llv2Parameters = llv2Details.parameters
    require(AGGREGATOR == token.computationDetails.liquidLegionsV2.role) {
      "invalid role for this function."
    }
    var reach = 0L
    val maximumRequestedFrequency = getMaximumRequestedFrequency(token)
    val (bytes, tempToken) =
      existingOutputOr(token) {
        when (Version.fromString(token.computationDetails.kingdomComputation.publicApiVersion)) {
          Version.V2_ALPHA -> {}
          Version.VERSION_UNSPECIFIED -> error("Public api version is invalid or unspecified.")
        }
        val measurementSpec =
          MeasurementSpec.parseFrom(token.computationDetails.kingdomComputation.measurementSpec)
        val requestBuilder =
          CompleteExecutionPhaseTwoAtAggregatorRequest.newBuilder().apply {
            localElGamalKeyPair = llv2Details.localElgamalKey
            compositeElGamalPublicKey = llv2Details.combinedPublicKey
            curveId = llv2Parameters.ellipticCurveId.toLong()
            flagCountTuples = readAndCombineAllInputBlobs(token, 1)
            maximumFrequency = maximumRequestedFrequency
            liquidLegionsParametersBuilder.apply {
              decayRate = llv2Parameters.liquidLegionsSketch.decayRate
              size = llv2Parameters.liquidLegionsSketch.size
            }
            vidSamplingIntervalWidth = measurementSpec.vidSamplingInterval.width
          }
        val noiseConfig = llv2Parameters.noise
        if (noiseConfig.hasReachNoiseConfig()) {
          requestBuilder.reachDpNoiseBaselineBuilder.apply {
            contributorsCount = workerStubs.size + 1
            globalReachDpNoise = noiseConfig.reachNoiseConfig.globalReachDpNoise
          }
        }
        if (noiseConfig.hasFrequencyNoiseConfig() && (maximumRequestedFrequency > 1)) {
          requestBuilder.frequencyNoiseParametersBuilder.apply {
            contributorsCount = workerStubs.size + 1
            maximumFrequency = maximumRequestedFrequency
            dpParams = noiseConfig.frequencyNoiseConfig
          }
        }

        val cryptoResult: CompleteExecutionPhaseTwoAtAggregatorResponse =
          cryptoWorker.completeExecutionPhaseTwoAtAggregator(requestBuilder.build())
        logStageDurationMetric(
          token,
          CRYPTO_LIB_CPU_DURATION,
          cryptoResult.elapsedCpuTimeMillis,
          executionPhaseTwoCryptoCpuTimeDurationHistogram
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
        dataClients.computationsClient
          .updateComputationDetails(
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
          .token
      }

    // If this is a reach-only computation, then our job is done.
    if (maximumRequestedFrequency == 1) {
      sendResultToKingdom(token, reach, mapOf(1L to 1.0))
      return completeComputation(nextToken, CompletedReason.SUCCEEDED)
    }

    // Passes the computation to the next duchy.
    sendAdvanceComputationRequest(
      header =
        advanceComputationHeader(
          LiquidLegionsV2.Description.EXECUTION_PHASE_THREE_INPUT,
          token.globalComputationId
        ),
      content = addLoggingHook(token, bytes),
      stub = nextDuchyStub(llv2Details.participantList)
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
    val llv2Details = token.computationDetails.liquidLegionsV2
    val llv2Parameters = llv2Details.parameters
    require(NON_AGGREGATOR == llv2Details.role) { "invalid role for this function." }
    val maximumRequestedFrequency = getMaximumRequestedFrequency(token)
    val (bytes, nextToken) =
      existingOutputOr(token) {
        val requestBuilder =
          CompleteExecutionPhaseTwoRequest.newBuilder().apply {
            localElGamalKeyPair = llv2Details.localElgamalKey
            compositeElGamalPublicKey = llv2Details.combinedPublicKey
            curveId = llv2Parameters.ellipticCurveId.toLong()
            flagCountTuples = readAndCombineAllInputBlobs(token, 1)
          }
        if (llv2Parameters.noise.hasFrequencyNoiseConfig()) {
          requestBuilder.apply {
            partialCompositeElGamalPublicKey = llv2Details.partiallyCombinedPublicKey
            if (maximumRequestedFrequency > 1) {
              noiseParameters = getFrequencyNoiseParams(token, llv2Parameters)
            }
          }
        }

        val cryptoResult: CompleteExecutionPhaseTwoResponse =
          cryptoWorker.completeExecutionPhaseTwo(requestBuilder.build())
        logStageDurationMetric(
          token,
          CRYPTO_LIB_CPU_DURATION,
          cryptoResult.elapsedCpuTimeMillis,
          executionPhaseTwoCryptoCpuTimeDurationHistogram
        )
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
      stub = nextDuchyStub(llv2Details.participantList)
    )

    return if (maximumRequestedFrequency == 1) {
      // If this is a reach-only computation, then our job is done.
      completeComputation(nextToken, CompletedReason.SUCCEEDED)
    } else {
      dataClients.transitionComputationToStage(
        nextToken,
        inputsToNextStage = nextToken.outputPathList(),
        stage = Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS.toProtocolStage()
      )
    }
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
        val requestBuilder =
          CompleteExecutionPhaseThreeAtAggregatorRequest.newBuilder().apply {
            localElGamalKeyPair = llv2Details.localElgamalKey
            curveId = llv2Parameters.ellipticCurveId.toLong()
            sameKeyAggregatorMatrix = readAndCombineAllInputBlobs(token, 1)
            maximumFrequency = getMaximumRequestedFrequency(token)
          }
        if (llv2Parameters.noise.hasFrequencyNoiseConfig()) {
          requestBuilder.globalFrequencyDpNoisePerBucketBuilder.apply {
            contributorsCount = workerStubs.size + 1
            dpParams = llv2Parameters.noise.frequencyNoiseConfig
          }
        }
        val cryptoResult: CompleteExecutionPhaseThreeAtAggregatorResponse =
          cryptoWorker.completeExecutionPhaseThreeAtAggregator(requestBuilder.build())
        logStageDurationMetric(
          token,
          CRYPTO_LIB_CPU_DURATION,
          cryptoResult.elapsedCpuTimeMillis,
          executionPhaseThreeCryptoCpuTimeDurationHistogram
        )
        cryptoResult.toByteString()
      }

    val frequencyDistributionMap =
      CompleteExecutionPhaseThreeAtAggregatorResponse.parseFrom(bytes.flatten())
        .frequencyDistributionMap

    sendResultToKingdom(token, llv2Details.reachEstimate.reach, frequencyDistributionMap)
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
                sameKeyAggregatorMatrix = readAndCombineAllInputBlobs(token, 1)
              }
              .build()
          )
        logStageDurationMetric(
          token,
          CRYPTO_LIB_CPU_DURATION,
          cryptoResult.elapsedCpuTimeMillis,
          executionPhaseThreeCryptoCpuTimeDurationHistogram
        )
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
      stub = nextDuchyStub(llv2Details.participantList)
    )

    // This duchy's responsibility for the computation is done. Mark it COMPLETED locally.
    return completeComputation(nextToken, CompletedReason.SUCCEEDED)
  }

  private suspend fun sendResultToKingdom(
    token: ComputationToken,
    reach: Long,
    frequency: Map<Long, Double>
  ) {
    val reachAndFrequency = ReachAndFrequency(reach, frequency)
    val kingdomComputation = token.computationDetails.kingdomComputation
    val serializedPublicApiEncryptionPublicKey: ByteString
    val encryptedResult =
      when (Version.fromString(kingdomComputation.publicApiVersion)) {
        Version.V2_ALPHA -> {
          val signedResult = signResult(reachAndFrequency.toV2AlphaMeasurementResult(), signingKey)
          val publicApiEncryptionPublicKey =
            kingdomComputation.measurementPublicKey.toV2AlphaEncryptionPublicKey()
          serializedPublicApiEncryptionPublicKey = publicApiEncryptionPublicKey.toByteString()
          encryptResult(signedResult, publicApiEncryptionPublicKey)
        }
        Version.VERSION_UNSPECIFIED -> error("Public api version is invalid or unspecified.")
      }
    sendResultToKingdom(
      globalId = token.globalComputationId,
      certificate = consentSignalCert,
      resultPublicKey = serializedPublicApiEncryptionPublicKey,
      encryptedResult = encryptedResult
    )
  }

  private fun nextDuchyStub(
    duchyList: List<ComputationParticipant>
  ): ComputationControlCoroutineStub {
    val index = duchyList.indexOfFirst { it.duchyId == duchyId }
    val nextDuchy = duchyList[(index + 1) % duchyList.size].duchyId
    return workerStubs[nextDuchy]
      ?: throw PermanentComputationError(
        IllegalArgumentException("No ComputationControlService stub for next duchy '$nextDuchy'")
      )
  }

  private fun aggregatorDuchyStub(aggregatorId: String): ComputationControlCoroutineStub {
    return workerStubs[aggregatorId]
      ?: throw PermanentComputationError(
        IllegalArgumentException(
          "No ComputationControlService stub for the Aggregator duchy '$aggregatorId'"
        )
      )
  }

  private fun ByteString.toCompleteSetupPhaseRequest(
    token: ComputationToken,
    llv2Details: LiquidLegionsSketchAggregationV2.ComputationDetails,
    totalRequisitionsCount: Int
  ): CompleteSetupPhaseRequest {
    val noiseConfig = llv2Details.parameters.noise
    val requestBuilder = CompleteSetupPhaseRequest.newBuilder().setCombinedRegisterVector(this)
    requestBuilder.maximumFrequency = getMaximumRequestedFrequency(token)
    if (noiseConfig.hasReachNoiseConfig()) {
      requestBuilder.noiseParametersBuilder.apply {
        compositeElGamalPublicKey = llv2Details.combinedPublicKey
        curveId = llv2Details.parameters.ellipticCurveId.toLong()
        contributorsCount = workerStubs.size + 1
        totalSketchesCount = totalRequisitionsCount
        dpParamsBuilder.apply {
          blindHistogram = noiseConfig.reachNoiseConfig.blindHistogramNoise
          noiseForPublisherNoise = noiseConfig.reachNoiseConfig.noiseForPublisherNoise
          globalReachDpNoise = noiseConfig.reachNoiseConfig.globalReachDpNoise
        }
      }
    }
    return requestBuilder.build()
  }

  private fun getFrequencyNoiseParams(
    token: ComputationToken,
    llv2Parameters: Parameters
  ): FlagCountTupleNoiseGenerationParameters {
    return FlagCountTupleNoiseGenerationParameters.newBuilder()
      .apply {
        maximumFrequency = getMaximumRequestedFrequency(token)
        contributorsCount = workerStubs.size + 1
        dpParams = llv2Parameters.noise.frequencyNoiseConfig
      }
      .build()
  }

  private fun getMaximumRequestedFrequency(token: ComputationToken): Int {
    var maximumRequestedFrequency =
      token.computationDetails.liquidLegionsV2.parameters.maximumFrequency
    val measurementSpec =
      MeasurementSpec.parseFrom(token.computationDetails.kingdomComputation.measurementSpec)
    val measurementSpecMaximumFrequency = measurementSpec.reachAndFrequency.maximumFrequencyPerUser
    if (measurementSpecMaximumFrequency > 0) {
      maximumRequestedFrequency = min(maximumRequestedFrequency, measurementSpecMaximumFrequency)
    }
    return maximumRequestedFrequency
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
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
