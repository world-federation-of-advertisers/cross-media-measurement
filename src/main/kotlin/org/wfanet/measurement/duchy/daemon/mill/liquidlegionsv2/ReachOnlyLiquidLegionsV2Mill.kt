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

package org.wfanet.measurement.duchy.daemon.mill.liquidlegionsv2

import com.google.protobuf.ByteString
import io.grpc.Status
import io.grpc.StatusException
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.metrics.LongHistogram
import io.opentelemetry.api.metrics.Meter
import java.security.SignatureException
import java.security.cert.CertPathValidatorException
import java.security.cert.X509Certificate
import java.time.Clock
import java.time.Duration
import java.util.logging.Logger
import org.wfanet.anysketch.crypto.combineElGamalPublicKeysRequest
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.common.crypto.SignatureAlgorithm
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.identity.DuchyInfo
import org.wfanet.measurement.consent.client.duchy.encryptResult
import org.wfanet.measurement.consent.client.duchy.signElgamalPublicKey
import org.wfanet.measurement.consent.client.duchy.signResult
import org.wfanet.measurement.consent.client.duchy.verifyDataProviderParticipation
import org.wfanet.measurement.consent.client.duchy.verifyElGamalPublicKey
import org.wfanet.measurement.duchy.daemon.mill.CRYPTO_LIB_CPU_DURATION
import org.wfanet.measurement.duchy.daemon.mill.Certificate
import org.wfanet.measurement.duchy.daemon.mill.MillBase
import org.wfanet.measurement.duchy.daemon.mill.liquidlegionsv2.crypto.ReachOnlyLiquidLegionsV2Encryption
import org.wfanet.measurement.duchy.daemon.utils.ComputationResult
import org.wfanet.measurement.duchy.daemon.utils.ReachResult
import org.wfanet.measurement.duchy.daemon.utils.toAnySketchElGamalPublicKey
import org.wfanet.measurement.duchy.daemon.utils.toCmmsElGamalPublicKey
import org.wfanet.measurement.duchy.daemon.utils.toV2AlphaElGamalPublicKey
import org.wfanet.measurement.duchy.daemon.utils.toV2AlphaEncryptionPublicKey
import org.wfanet.measurement.duchy.db.computation.BlobRef
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients
import org.wfanet.measurement.duchy.service.internal.computations.outputPathList
import org.wfanet.measurement.duchy.service.system.v1alpha.advanceComputationHeader
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ComputationDetails.CompletedReason
import org.wfanet.measurement.internal.duchy.ComputationDetails.KingdomComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.ElGamalPublicKey
import org.wfanet.measurement.internal.duchy.RequisitionMetadata
import org.wfanet.measurement.internal.duchy.computationDetails
import org.wfanet.measurement.internal.duchy.config.LiquidLegionsV2SetupConfig.RoleInComputation.AGGREGATOR
import org.wfanet.measurement.internal.duchy.config.LiquidLegionsV2SetupConfig.RoleInComputation.NON_AGGREGATOR
import org.wfanet.measurement.internal.duchy.config.LiquidLegionsV2SetupConfig.RoleInComputation.UNKNOWN
import org.wfanet.measurement.internal.duchy.config.LiquidLegionsV2SetupConfig.RoleInComputation.UNRECOGNIZED
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlyExecutionPhaseAtAggregatorResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlyExecutionPhaseResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlySetupPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlySetupPhaseResponse
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2.ComputationDetails.ComputationParticipant
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
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKey
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKt
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt
import org.wfanet.measurement.system.v1alpha.ReachOnlyLiquidLegionsV2
import org.wfanet.measurement.system.v1alpha.confirmComputationParticipantRequest
import org.wfanet.measurement.system.v1alpha.setParticipantRequisitionParamsRequest

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
  openTelemetry: OpenTelemetry,
  requestChunkSizeBytes: Int = 1024 * 32,
  maximumAttempts: Int = 10,
  clock: Clock = Clock.systemUTC(),
  private val parallelism: Int = 1,
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
    ComputationType.REACH_ONLY_LIQUID_LEGIONS_SKETCH_AGGREGATION_V2,
    workLockDuration,
    requestChunkSizeBytes,
    maximumAttempts,
    clock,
    openTelemetry
  ) {
  private val meter: Meter = openTelemetry.getMeter(ReachOnlyLiquidLegionsV2Mill::class.java.name)

  private val initializationPhaseCryptoCpuTimeDurationHistogram: LongHistogram =
    meter.histogramBuilder("initialization_phase_crypto_cpu_time_duration_millis").ofLongs().build()

  private val setupPhaseCryptoCpuTimeDurationHistogram: LongHistogram =
    meter.histogramBuilder("setup_phase_crypto_cpu_time_duration_millis").ofLongs().build()

  private val executionPhaseCryptoCpuTimeDurationHistogram: LongHistogram =
    meter.histogramBuilder("execution_phase_crypto_cpu_time_duration_millis").ofLongs().build()

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
            signingKey
          )
        Version.VERSION_UNSPECIFIED -> error("Public api version is invalid or unspecified.")
      }

    val request = setParticipantRequisitionParamsRequest {
      name = ComputationParticipantKey(token.globalComputationId, duchyId).toName()
      requisitionParams =
        ComputationParticipantKt.requisitionParams {
          duchyCertificate = consentSignalCert.name
          reachOnlyLiquidLegionsV2 =
            ComputationParticipantKt.RequisitionParamsKt.liquidLegionsV2 {
              elGamalPublicKey = signedElgamalPublicKey.data
              elGamalPublicKeySignature = signedElgamalPublicKey.signature
              elGamalPublicKeySignatureAlgorithmOid = signedElgamalPublicKey.signatureAlgorithmOid
            }
        }
    }
    try {
      systemComputationParticipantsClient.setParticipantRequisitionParams(request)
    } catch (e: StatusException) {
      val message = "Error setting participant requisition params"
      throw when (e.status.code) {
        Status.Code.UNAVAILABLE,
        Status.Code.ABORTED -> ComputationDataClients.TransientErrorException(message, e)
        else -> ComputationDataClients.PermanentErrorException(message, e)
      }
    }
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
          CRYPTO_LIB_CPU_DURATION,
          cryptoResult.elapsedCpuTimeMillis,
          initializationPhaseCryptoCpuTimeDurationHistogram
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
    val duchyCertificate: X509Certificate = readCertificate(duchy.duchyCertificateDer)
    val signatureAlgorithmOid =
      duchy.elGamalPublicKeySignatureAlgorithmOid.ifEmpty { duchyCertificate.sigAlgOID }
    val signatureAlgorithm =
      requireNotNull(SignatureAlgorithm.fromOid(signatureAlgorithmOid)) {
        "Unsupported signature algorithm OID $signatureAlgorithmOid"
      }
    when (publicApiVersion) {
      Version.V2_ALPHA -> {
        try {
          verifyElGamalPublicKey(
            duchy.elGamalPublicKey,
            duchy.elGamalPublicKeySignature,
            signatureAlgorithm,
            duchyCertificate,
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
    throw ComputationDataClients.PermanentErrorException(errorMessage)
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
              fullParticipantList.size
            )
            .map { it.publicKey }
            .toCombinedPublicKey(rollv2Details.parameters.ellipticCurveId)
        UNKNOWN,
        UNRECOGNIZED -> error("Invalid role ${rollv2Details.role}")
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
    try {
      systemComputationParticipantsClient.confirmComputationParticipant(
        confirmComputationParticipantRequest {
          name = ComputationParticipantKey(token.globalComputationId, duchyId).toName()
        }
      )
    } catch (e: StatusException) {
      val message = "Error confirming computation participant"
      throw when (e.status.code) {
        Status.Code.UNAVAILABLE,
        Status.Code.ABORTED -> ComputationDataClients.TransientErrorException(message, e)
        else -> ComputationDataClients.PermanentErrorException(message, e)
      }
    }
    val latestToken = updatePublicElgamalKey(token)
    return dataClients.transitionComputationToStage(
      latestToken,
      stage =
        when (checkNotNull(token.computationDetails.reachOnlyLiquidLegionsV2.role)) {
          AGGREGATOR -> Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage()
          NON_AGGREGATOR -> Stage.WAIT_TO_START.toProtocolStage()
          else ->
            error("Unknown role: ${latestToken.computationDetails.reachOnlyLiquidLegionsV2.role}")
        }
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
              token.participantCount
            )
        val cryptoResult: CompleteReachOnlySetupPhaseResponse =
          cryptoWorker.completeReachOnlySetupPhaseAtAggregator(request)
        logStageDurationMetric(
          token,
          CRYPTO_LIB_CPU_DURATION,
          cryptoResult.elapsedCpuTimeMillis,
          setupPhaseCryptoCpuTimeDurationHistogram
        )
        // The nextToken consists of the CRV and the noise ciphertext.
        cryptoResult.combinedRegisterVector.concat(cryptoResult.serializedExcessiveNoiseCiphertext)
      }

    sendAdvanceComputationRequest(
      header =
        advanceComputationHeader(
          ReachOnlyLiquidLegionsV2.Description.EXECUTION_PHASE_INPUT,
          token.globalComputationId
        ),
      content = addLoggingHook(token, bytes),
      stub = nextDuchyStub(rollv2Details.participantList)
    )

    return dataClients.transitionComputationToStage(
      nextToken,
      inputsToNextStage = nextToken.outputPathList(),
      stage = Stage.WAIT_EXECUTION_PHASE_INPUTS.toProtocolStage()
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
              token.participantCount
            )
        val cryptoResult: CompleteReachOnlySetupPhaseResponse =
          cryptoWorker.completeReachOnlySetupPhase(request)
        logStageDurationMetric(
          token,
          CRYPTO_LIB_CPU_DURATION,
          cryptoResult.elapsedCpuTimeMillis,
          setupPhaseCryptoCpuTimeDurationHistogram
        )
        // The nextToken consists of the CRV and the noise ciphertext.
        cryptoResult.combinedRegisterVector.concat(cryptoResult.serializedExcessiveNoiseCiphertext)
      }

    sendAdvanceComputationRequest(
      header =
        advanceComputationHeader(
          ReachOnlyLiquidLegionsV2.Description.SETUP_PHASE_INPUT,
          token.globalComputationId
        ),
      content = addLoggingHook(token, bytes),
      stub = aggregatorDuchyStub(rollv2Details.participantList.last().duchyId)
    )

    return dataClients.transitionComputationToStage(
      nextToken,
      inputsToNextStage = nextToken.outputPathList(),
      stage = Stage.WAIT_EXECUTION_PHASE_INPUTS.toProtocolStage()
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
    var reach = 0L
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
          CRYPTO_LIB_CPU_DURATION,
          cryptoResult.elapsedCpuTimeMillis,
          executionPhaseCryptoCpuTimeDurationHistogram
        )
        reach = cryptoResult.reach
        cryptoResult.toByteString()
      }

    sendResultToKingdom(token, ReachResult(reach))
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
          CRYPTO_LIB_CPU_DURATION,
          cryptoResult.elapsedCpuTimeMillis,
          executionPhaseCryptoCpuTimeDurationHistogram
        )
        cryptoResult.combinedRegisterVector.concat(cryptoResult.serializedExcessiveNoiseCiphertext)
      }

    // Passes the computation to the next duchy.
    sendAdvanceComputationRequest(
      header =
        advanceComputationHeader(
          ReachOnlyLiquidLegionsV2.Description.EXECUTION_PHASE_INPUT,
          token.globalComputationId
        ),
      content = addLoggingHook(token, bytes),
      stub = nextDuchyStub(rollv2Details.participantList)
    )

    return completeComputation(nextToken, CompletedReason.SUCCEEDED)
  }

  private suspend fun sendResultToKingdom(
    token: ComputationToken,
    computationResult: ComputationResult
  ) {
    val kingdomComputation = token.computationDetails.kingdomComputation
    val serializedPublicApiEncryptionPublicKey: ByteString
    val encryptedResult =
      when (Version.fromString(kingdomComputation.publicApiVersion)) {
        Version.V2_ALPHA -> {
          val signedResult = signResult(computationResult.toV2AlphaMeasurementResult(), signingKey)
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
      ?: throw ComputationDataClients.PermanentErrorException(
        "No ComputationControlService stub for next duchy '$nextDuchy'"
      )
  }

  private fun aggregatorDuchyStub(aggregatorId: String): ComputationControlCoroutineStub {
    return workerStubs[aggregatorId]
      ?: throw ComputationDataClients.PermanentErrorException(
        "No ComputationControlService stub for the Aggregator duchy '$aggregatorId'"
      )
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
    count: Int
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

  private val ComputationToken.participantCount: Int
    get() =
      if (computationDetails.kingdomComputation.participantCount != 0) {
        computationDetails.kingdomComputation.participantCount
      } else {
        // For legacy Computations. See world-federation-of-advertisers/cross-media-measurement#1194
        workerStubs.size + 1
      }

  companion object {
    private const val BYTES_PER_CIPHERTEXT = 66

    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
