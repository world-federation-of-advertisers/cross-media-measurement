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
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.ElGamalPublicKey as V2alphaElGamalPublicKey
import org.wfanet.measurement.api.v2alpha.HybridCipherSuite
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.loadLibrary
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.consent.client.duchy.Computation as ConsentSignalingComputation
import org.wfanet.measurement.consent.client.duchy.Requisition as ConsentSignalingRequisition
import org.wfanet.measurement.consent.client.duchy.encryptResult
import org.wfanet.measurement.consent.client.duchy.signElgamalPublicKey
import org.wfanet.measurement.consent.client.duchy.signResult
import org.wfanet.measurement.consent.client.duchy.verifyDataProviderParticipation
import org.wfanet.measurement.consent.client.duchy.verifyElgamalPublicKey
import org.wfanet.measurement.consent.crypto.hybridencryption.HybridCryptor
import org.wfanet.measurement.consent.crypto.hybridencryption.testing.ReversingHybridCryptor
import org.wfanet.measurement.consent.crypto.keystore.KeyStore
import org.wfanet.measurement.consent.crypto.keystore.PrivateKeyHandle
import org.wfanet.measurement.duchy.daemon.mill.CONSENT_SIGNALING_PRIVATE_KEY_ID
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
import org.wfanet.measurement.duchy.daemon.utils.toV2AlphaHybridCipherSuite
import org.wfanet.measurement.duchy.daemon.utils.toV2AlphaMeasurementResult
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients
import org.wfanet.measurement.duchy.service.internal.computation.outputPathList
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
 * @param keyStore The [keyStore] holding the private keys.
 * @param consentSignalCert The [Certificate] used for consent signaling.
 * @param dataClients clients that have access to local computation storage, i.e., spanner table and
 * blob store.
 * @param systemComputationParticipantsClient client of the kingdom's system
 * ComputationParticipantsService.
 * @param systemComputationsClient client of the kingdom's system computationsService.
 * @param systemComputationLogEntriesClient client of the kingdom's system
 * computationLogEntriesService.
 * @param computationStatsClient client of the duchy's internal ComputationStatsService.
 * @param throttler A throttler used to rate limit the frequency of the mill polling from the
 * computation table.
 * @param requestChunkSizeBytes The size of data chunk when sending result to other duchies.
 * @param clock A clock
 * @param workerStubs A map from other duchies' Ids to their corresponding
 * computationControlClients, used for passing computation to other duchies.
 * @param cryptoWorker The cryptoWorker that performs the actual computation.
 */
class LiquidLegionsV2Mill(
  millId: String,
  duchyId: String,
  keyStore: KeyStore,
  consentSignalCert: Certificate,
  dataClients: ComputationDataClients,
  systemComputationParticipantsClient: ComputationParticipantsCoroutineStub,
  systemComputationsClient: ComputationsGrpcKt.ComputationsCoroutineStub,
  systemComputationLogEntriesClient: ComputationLogEntriesCoroutineStub,
  computationStatsClient: ComputationStatsCoroutineStub,
  throttler: MinimumIntervalThrottler,
  requestChunkSizeBytes: Int = 1024 * 32, // 32 KiB
  clock: Clock = Clock.systemUTC(),
  private val workerStubs: Map<String, ComputationControlCoroutineStub>,
  private val cryptoWorker: LiquidLegionsV2Encryption
) :
  MillBase(
    millId,
    duchyId,
    keyStore,
    consentSignalCert,
    dataClients,
    systemComputationParticipantsClient,
    systemComputationsClient,
    systemComputationLogEntriesClient,
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
    val signedElgamalPublicKey =
      when (Version.fromString(token.computationDetails.kingdomComputation.publicApiVersion)) {
        Version.V2_ALPHA ->
          signElgamalPublicKey(
            llv2ComputationDetails.localElgamalKey.publicKey.toV2AlphaElGamalPublicKey(),
            PrivateKeyHandle(CONSENT_SIGNALING_PRIVATE_KEY_ID, keyStore),
            consentSignalCert.value
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
  private fun verifyEdpSignatureAndRequisition(
    requisition: RequisitionMetadata,
    details: KingdomComputationDetails
  ): List<String> {
    val errorList = mutableListOf<String>()
    if (!verifyDataProviderParticipation(
        requisition.details.dataProviderParticipationSignature,
        ConsentSignalingRequisition(
          readCertificate(requisition.details.dataProviderCertificateDer),
          requisition.details.requisitionSpecHash
        ),
        ConsentSignalingComputation(
          details.dataProviderList,
          details.dataProviderListSalt,
          details.measurementSpec
        )
      )
    ) {
      errorList.add(
        "Data provider participation signature of ${requisition.externalDataProviderId} is invalid."
      )
    }
    if (requisition.details.externalFulfillingDuchyId == duchyId && requisition.path.isBlank()) {
      errorList.add("Missing expected data for requisition ${requisition.externalRequisitionId}.")
    }
    return errorList
  }

  /**
   * Verifies the duchy Elgamal Key signature. Returns a list of error messages if anything is
   * wrong. Otherwise return an empty list.
   */
  private fun verifyDuchySignature(
    duchy: ComputationParticipant,
    publicApiVersion: Version
  ): List<String> {
    val errorList = mutableListOf<String>()
    if (duchy.duchyId != duchyId && !workerStubs.containsKey(duchy.duchyId)) {
      errorList.add("Unrecognized duchy ${duchy.duchyId}.")
    }
    when (publicApiVersion) {
      Version.V2_ALPHA -> {
        val publicApiElgamalKey = V2alphaElGamalPublicKey.parseFrom(duchy.elGamalPublicKey)
        if (!verifyElgamalPublicKey(
            duchy.elGamalPublicKeySignature,
            publicApiElgamalKey,
            readCertificate(duchy.duchyCertificateDer)
          )
        ) {
          errorList.add("ElGamalPublicKey signature of ${duchy.duchyId} is invalid.")
        }
      }
      Version.VERSION_UNSPECIFIED -> error("Public api version is invalid or unspecified.")
    }
    return errorList
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
    token.requisitionsList.forEach {
      errorList.addAll(verifyEdpSignatureAndRequisition(it, kingdomComputation))
    }
    token.computationDetails.liquidLegionsV2.participantList.forEach {
      errorList.addAll(
        verifyDuchySignature(it, Version.fromString(kingdomComputation.publicApiVersion))
      )
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
            .toCompleteSetupPhaseRequest(llv2Details, token.requisitionsCount)
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
            .toCompleteSetupPhaseRequest(llv2Details, token.requisitionsCount)
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
        if (llv2Parameters.noise.hasFrequencyNoiseConfig()) {
          requestBuilder.noiseParameters = getFrequencyNoiseParams(llv2Parameters)
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
    val (bytes, tempToken) =
      existingOutputOr(token) {
        val requestBuilder =
          CompleteExecutionPhaseTwoAtAggregatorRequest.newBuilder().apply {
            localElGamalKeyPair = llv2Details.localElgamalKey
            compositeElGamalPublicKey = llv2Details.combinedPublicKey
            curveId = llv2Parameters.ellipticCurveId.toLong()
            flagCountTuples = readAndCombineAllInputBlobs(token, 1)
            maximumFrequency = llv2Parameters.maximumFrequency
            liquidLegionsParametersBuilder.apply {
              decayRate = llv2Parameters.liquidLegionsSketch.decayRate
              size = llv2Parameters.liquidLegionsSketch.size
            }
          }
        val noiseConfig = llv2Parameters.noise
        if (noiseConfig.hasReachNoiseConfig()) {
          requestBuilder.reachDpNoiseBaselineBuilder.apply {
            contributorsCount = workerStubs.size + 1
            globalReachDpNoise = noiseConfig.reachNoiseConfig.globalReachDpNoise
          }
        }
        if (noiseConfig.hasFrequencyNoiseConfig()) {
          requestBuilder.frequencyNoiseParametersBuilder.apply {
            contributorsCount = workerStubs.size + 1
            maximumFrequency = llv2Parameters.maximumFrequency
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
      if (llv2Details.hasReachEstimate()) {
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
            noiseParameters = getFrequencyNoiseParams(llv2Parameters)
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
      stub = nextDuchyStub(llv2Details.participantList)
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
            maximumFrequency = llv2Parameters.maximumFrequency
          }
        if (llv2Parameters.noise.hasFrequencyNoiseConfig()) {
          requestBuilder.globalFrequencyDpNoisePerBucketBuilder.apply {
            contributorsCount = workerStubs.size + 1
            dpParams = llv2Parameters.noise.frequencyNoiseConfig
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

    val reachAndFrequency =
      ReachAndFrequency(llv2Details.reachEstimate.reach, frequencyDistributionMap)

    val kingdomComputation = token.computationDetails.kingdomComputation
    val serializedPublicApiEncryptionPublicKey: ByteString
    val encryptedResult =
      when (Version.fromString(kingdomComputation.publicApiVersion)) {
        Version.V2_ALPHA -> {
          val signedResult =
            signResult(
              reachAndFrequency.toV2AlphaMeasurementResult(),
              PrivateKeyHandle(CONSENT_SIGNALING_PRIVATE_KEY_ID, keyStore),
              consentSignalCert.value
            )
          val publicApiEncryptionPublicKey =
            kingdomComputation.measurementPublicKey.toV2AlphaEncryptionPublicKey()
          serializedPublicApiEncryptionPublicKey = publicApiEncryptionPublicKey.toByteString()
          encryptResult(
            signedResult,
            publicApiEncryptionPublicKey,
            kingdomComputation.cipherSuite.toV2AlphaHybridCipherSuite(),
            ::fakeGetHybridCryptorForCipherSuite // TODO: use the real HybridCryptor.
          )
        }
        Version.VERSION_UNSPECIFIED -> error("Public api version is invalid or unspecified.")
      }
    sendResultToKingdom(
      globalId = token.globalComputationId,
      certificate = consentSignalCert.value,
      resultPublicKey = serializedPublicApiEncryptionPublicKey,
      encryptedResult = encryptedResult
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
      stub = nextDuchyStub(llv2Details.participantList)
    )

    // This duchy's responsibility for the computation is done. Mark it COMPLETED locally.
    return completeComputation(nextToken, CompletedReason.SUCCEEDED)
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
    llv2Details: LiquidLegionsSketchAggregationV2.ComputationDetails,
    totalRequisitionsCount: Int
  ): CompleteSetupPhaseRequest {
    val noiseConfig = llv2Details.parameters.noise
    val requestBuilder = CompleteSetupPhaseRequest.newBuilder().setCombinedRegisterVector(this)
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
    llv2Parameters: Parameters
  ): FlagCountTupleNoiseGenerationParameters {
    return FlagCountTupleNoiseGenerationParameters.newBuilder()
      .apply {
        maximumFrequency = llv2Parameters.maximumFrequency
        contributorsCount = workerStubs.size + 1
        dpParams = llv2Parameters.noise.frequencyNoiseConfig
      }
      .build()
  }

  // TODO: delete this fake when the EciesCryptor is done.
  private fun fakeGetHybridCryptorForCipherSuite(cipherSuite: HybridCipherSuite): HybridCryptor {
    return ReversingHybridCryptor()
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
