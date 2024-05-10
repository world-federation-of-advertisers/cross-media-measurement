// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.daemon.mill.shareshuffle

import com.google.protobuf.ByteString
import io.grpc.Status
import io.grpc.StatusException
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.metrics.Meter
import java.security.SignatureException
import java.security.cert.CertPathValidatorException
import java.security.cert.X509Certificate
import java.time.Clock
import java.time.Duration
import java.util.logging.Logger
import kotlinx.coroutines.flow.flowOf
import org.wfanet.frequencycount.FrequencyVector
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.RandomSeed
import org.wfanet.measurement.api.v2alpha.SignedMessage
import org.wfanet.measurement.api.v2alpha.encryptedMessage
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.crypto.PrivateKeyStore
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.authorityKeyIdentifier
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.tink.TinkKeyId
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.xor
import org.wfanet.measurement.consent.client.duchy.decryptRandomSeed
import org.wfanet.measurement.consent.client.duchy.signEncryptionPublicKey
import org.wfanet.measurement.consent.client.duchy.verifyRandomSeed
import org.wfanet.measurement.duchy.daemon.mill.Certificate
import org.wfanet.measurement.duchy.daemon.mill.MillBase
import org.wfanet.measurement.duchy.daemon.mill.shareshuffle.crypto.HonestMajorityShareShuffleCryptor
import org.wfanet.measurement.duchy.daemon.utils.ReachAndFrequencyResult
import org.wfanet.measurement.duchy.daemon.utils.toV2AlphaEncryptionPublicKey
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients.PermanentErrorException
import org.wfanet.measurement.duchy.service.internal.computations.inputPathList
import org.wfanet.measurement.duchy.service.system.v1alpha.advanceComputationHeader
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.config.HonestMajorityShareShuffleSetupConfig
import org.wfanet.measurement.internal.duchy.config.RoleInComputation
import org.wfanet.measurement.internal.duchy.config.RoleInComputation.AGGREGATOR
import org.wfanet.measurement.internal.duchy.config.RoleInComputation.FIRST_NON_AGGREGATOR
import org.wfanet.measurement.internal.duchy.config.RoleInComputation.SECOND_NON_AGGREGATOR
import org.wfanet.measurement.internal.duchy.differentialPrivacyParams
import org.wfanet.measurement.internal.duchy.protocol.CompleteAggregationPhaseRequestKt
import org.wfanet.measurement.internal.duchy.protocol.CompleteShufflePhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteShufflePhaseRequestKt
import org.wfanet.measurement.internal.duchy.protocol.CompleteShufflePhaseRequestKt.sketchShare
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle.AggregationPhaseInput
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle.ShufflePhaseInput
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle.Stage
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffleKt.ShufflePhaseInputKt
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffleKt.aggregationPhaseInput
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffleKt.shufflePhaseInput
import org.wfanet.measurement.internal.duchy.protocol.completeAggregationPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.completeShufflePhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.copy
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKey
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKt
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt
import org.wfanet.measurement.system.v1alpha.HonestMajorityShareShuffle.Description
import org.wfanet.measurement.system.v1alpha.setParticipantRequisitionParamsRequest

class HonestMajorityShareShuffleMill(
  millId: String,
  duchyId: String,
  signingKey: SigningKeyHandle,
  consentSignalCert: Certificate,
  private val trustedCertificates: Map<ByteString, X509Certificate>,
  dataClients: ComputationDataClients,
  systemComputationParticipantsClient: ComputationParticipantsCoroutineStub,
  systemComputationsClient: ComputationsGrpcKt.ComputationsCoroutineStub,
  systemComputationLogEntriesClient: ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub,
  computationStatsClient: ComputationStatsGrpcKt.ComputationStatsCoroutineStub,
  private val certificateClient: CertificatesGrpcKt.CertificatesCoroutineStub,
  private val workerStubs: Map<String, ComputationControlCoroutineStub>,
  private val cryptoWorker: HonestMajorityShareShuffleCryptor,
  private val protocolSetupConfig: HonestMajorityShareShuffleSetupConfig,
  workLockDuration: Duration,
  openTelemetry: OpenTelemetry,
  private val privateKeyStore: PrivateKeyStore<TinkKeyId, TinkPrivateKeyHandle>? = null,
  requestChunkSizeBytes: Int = 1024 * 32,
  maximumAttempts: Int = 10,
  clock: Clock = Clock.systemUTC(),
) :
  MillBase(
    millId = millId,
    duchyId = duchyId,
    signingKey = signingKey,
    consentSignalCert = consentSignalCert,
    dataClients = dataClients,
    systemComputationParticipantsClient = systemComputationParticipantsClient,
    systemComputationsClient = systemComputationsClient,
    systemComputationLogEntriesClient = systemComputationLogEntriesClient,
    computationStatsClient = computationStatsClient,
    computationType = ComputationType.HONEST_MAJORITY_SHARE_SHUFFLE,
    workLockDuration = workLockDuration,
    requestChunkSizeBytes = requestChunkSizeBytes,
    maximumAttempts = maximumAttempts,
    clock = clock,
    openTelemetry = openTelemetry,
  ) {
  init {
    if (protocolSetupConfig.role == AGGREGATOR) {
      requireNotNull(privateKeyStore) { "private key store is not set up." }
    }
  }

  private val meter: Meter = openTelemetry.getMeter(HonestMajorityShareShuffleMill::class.java.name)

  // TODO(@renjiez): add metrics.

  override val endingStage = Stage.COMPLETE.toProtocolStage()

  private val actions =
    mapOf(
      Pair(Stage.INITIALIZED, FIRST_NON_AGGREGATOR) to ::initializationPhase,
      Pair(Stage.INITIALIZED, SECOND_NON_AGGREGATOR) to ::initializationPhase,
      Pair(Stage.INITIALIZED, AGGREGATOR) to ::initializationPhase,
      Pair(Stage.SETUP_PHASE, FIRST_NON_AGGREGATOR) to ::setupPhase,
      Pair(Stage.SETUP_PHASE, SECOND_NON_AGGREGATOR) to ::setupPhase,
      Pair(Stage.SHUFFLE_PHASE, FIRST_NON_AGGREGATOR) to ::shufflePhase,
      Pair(Stage.SHUFFLE_PHASE, SECOND_NON_AGGREGATOR) to ::shufflePhase,
      Pair(Stage.AGGREGATION_PHASE, AGGREGATOR) to ::aggregationPhase,
    )

  private fun nextStage(token: ComputationToken): Stage {
    val stage = token.computationStage.honestMajorityShareShuffle
    val role = token.computationDetails.honestMajorityShareShuffle.role

    return STAGE_TRANSITIONS[Pair(stage, role)]
      ?: error("Unexpected stage or role: ($stage, $role)")
  }

  override suspend fun processComputationImpl(token: ComputationToken) {
    require(token.computationDetails.hasHonestMajorityShareShuffle()) {
      "Only Honest Majority Share Shuffle computation is supported in this mill."
    }
    val stage = token.computationStage.honestMajorityShareShuffle
    val role = token.computationDetails.honestMajorityShareShuffle.role
    val action = actions[Pair(stage, role)] ?: error("Unexpected stage or role: ($stage, $role)")
    val updatedToken = action(token)

    val globalId = token.globalComputationId
    val updatedStage = updatedToken.computationStage.honestMajorityShareShuffle
    logger.info("$globalId@$millId: Stage transitioned from $stage to $updatedStage")
  }

  private suspend fun sendParticipantParamsToKingdom(token: ComputationToken) {
    val computationDetails = token.computationDetails.honestMajorityShareShuffle

    val request = setParticipantRequisitionParamsRequest {
      name = ComputationParticipantKey(token.globalComputationId, duchyId).toName()
      requisitionParams =
        ComputationParticipantKt.requisitionParams {
          duchyCertificate = consentSignalCert.name
          honestMajorityShareShuffle =
            ComputationParticipantKt.RequisitionParamsKt.honestMajorityShareShuffle {
              if (computationDetails.role != AGGREGATOR) {
                require(computationDetails.encryptionKeyPair.hasPublicKey()) {
                  "Public key not set."
                }

                val signedEncryptionPublicKey =
                  when (
                    Version.fromString(token.computationDetails.kingdomComputation.publicApiVersion)
                  ) {
                    Version.V2_ALPHA -> {
                      signEncryptionPublicKey(
                        computationDetails.encryptionKeyPair.publicKey
                          .toV2AlphaEncryptionPublicKey(),
                        signingKey,
                        signingKey.defaultAlgorithm,
                      )
                    }
                  }
                tinkPublicKey = signedEncryptionPublicKey.message.value
                tinkPublicKeySignature = signedEncryptionPublicKey.signature
                tinkPublicKeySignatureAlgorithmOid = signedEncryptionPublicKey.signatureAlgorithmOid
              }
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
        else -> PermanentErrorException(message, e)
      }
    }
  }

  private suspend fun initializationPhase(token: ComputationToken): ComputationToken {
    sendParticipantParamsToKingdom(token)

    return dataClients.transitionComputationToStage(
      token,
      stage = nextStage(token).toProtocolStage(),
    )
  }

  private fun peerDuchyStub(role: RoleInComputation): ComputationControlCoroutineStub {
    val peerDuchy =
      when (role) {
        FIRST_NON_AGGREGATOR -> {
          protocolSetupConfig.secondNonAggregatorDuchyId
        }
        SECOND_NON_AGGREGATOR -> {
          protocolSetupConfig.firstNonAggregatorDuchyId
        }
        AGGREGATOR,
        RoleInComputation.NON_AGGREGATOR,
        RoleInComputation.ROLE_IN_COMPUTATION_UNSPECIFIED,
        RoleInComputation.UNRECOGNIZED -> error("Unexpected role:$role for peerDuchyStub")
      }
    return workerStubs[peerDuchy]
      ?: throw PermanentErrorException(
        "No ComputationControlService stub for the peer duchy '$peerDuchy'"
      )
  }

  private fun aggregatorStub(): ComputationControlCoroutineStub {
    val aggregator = protocolSetupConfig.aggregatorDuchyId
    return workerStubs[aggregator]
      ?: throw PermanentErrorException(
        "No ComputationControlService stub for aggregator '$aggregator'"
      )
  }

  private suspend fun setupPhase(token: ComputationToken): ComputationToken {
    val hmssDetails = token.computationDetails.honestMajorityShareShuffle
    val role = hmssDetails.role

    val headerDescription =
      if (role == FIRST_NON_AGGREGATOR) Description.SHUFFLE_PHASE_INPUT_ONE
      else Description.SHUFFLE_PHASE_INPUT_TWO

    val shufflePhaseInput = shufflePhaseInput {
      peerRandomSeed = hmssDetails.randomSeed
      secretSeeds +=
        token.requisitionsList
          .filter { it.details.externalFulfillingDuchyId == duchyId }
          .map {
            val source = it
            require(source.path.isNotBlank()) {
              "Requisition blobPath is empty for ${source.externalKey.externalRequisitionId}"
            }
            val protocolDetails = source.details.protocol.honestMajorityShareShuffle
            require(!protocolDetails.secretSeedCiphertext.isEmpty) {
              "Requisition secretSeed is empty for ${source.externalKey.externalRequisitionId}"
            }
            ShufflePhaseInputKt.secretSeed {
              requisitionId = source.externalKey.externalRequisitionId
              secretSeedCiphertext = protocolDetails.secretSeedCiphertext
              registerCount = protocolDetails.registerCount
              dataProviderCertificate = protocolDetails.dataProviderCertificate
            }
          }
    }

    sendAdvanceComputationRequest(
      header = advanceComputationHeader(headerDescription, token.globalComputationId),
      content = addLoggingHook(token, flowOf(shufflePhaseInput.toByteString())),
      stub = peerDuchyStub(hmssDetails.role),
    )

    val nextStage = nextStage(token).toProtocolStage()
    return dataClients.transitionComputationToStage(
      token,
      stage = nextStage(token).toProtocolStage(),
      // For SECOND_NON_AGGREGATOR, the input of SETUP_PHASE is the ShufflePhaseInput from peer
      // worker. It should be forwarded to SHUFFLE_PHASE.
      inputsToNextStage =
        if (nextStage == Stage.SHUFFLE_PHASE.toProtocolStage()) {
          token.inputPathList()
        } else {
          emptyList()
        },
    )
  }

  private suspend fun getShufflePhaseInput(token: ComputationToken): ShufflePhaseInput {
    val shufflePhaseInputBlobs = dataClients.readInputBlobs(token)
    require(shufflePhaseInputBlobs.size == 1) { "Shuffle phase input does not exist." }
    val serializedInput = shufflePhaseInputBlobs.values.first()

    return ShufflePhaseInput.parseFrom(serializedInput)
  }

  private suspend fun getAggregationPhaseInputs(
    token: ComputationToken
  ): List<AggregationPhaseInput> {
    val aggregationPhaseInputBlobs = dataClients.readInputBlobs(token)
    require(aggregationPhaseInputBlobs.size == 2) {
      "aggregationPhaseInputBlobs count should be 2 instead of ${aggregationPhaseInputBlobs.size}"
    }
    return aggregationPhaseInputBlobs.map { AggregationPhaseInput.parseFrom(it.value) }
  }

  private suspend fun verifySecretSeed(
    secretSeed: ShufflePhaseInput.SecretSeed,
    duchyPrivateKeyId: String,
    apiVersion: Version,
  ): RandomSeed {
    requireNotNull(privateKeyStore) { "privateKeyStore is null for non-aggregator." }
    val privateKey =
      privateKeyStore.read(TinkKeyId(duchyPrivateKeyId.toLong()))
        ?: throw PermanentErrorException(
          "Fail to get private key for requisition ${secretSeed.requisitionId}"
        )

    val encryptedAndSignedSeed =
      when (apiVersion) {
        Version.V2_ALPHA -> {
          encryptedMessage {
            ciphertext = secretSeed.secretSeedCiphertext
            typeUrl = ProtoReflection.getTypeUrl(SignedMessage.getDescriptor())
          }
        }
      }
    val signedSeed = decryptRandomSeed(encryptedAndSignedSeed, privateKey)

    val dataProviderCertificateName = secretSeed.dataProviderCertificate
    val dataProviderCertificate =
      try {
        certificateClient.getCertificate(
          getCertificateRequest { name = dataProviderCertificateName }
        )
      } catch (e: StatusException) {
        throw PermanentErrorException("Fail to get certificate for $dataProviderCertificateName", e)
      }

    val x509Certificate: X509Certificate = readCertificate(dataProviderCertificate.x509Der)
    val trustedIssuer =
      trustedCertificates[x509Certificate.authorityKeyIdentifier]
        ?: throw PermanentErrorException(
          "trustedIssuer not found for $dataProviderCertificateName."
        )

    try {
      verifyRandomSeed(signedSeed, x509Certificate, trustedIssuer)
    } catch (e: CertPathValidatorException) {
      throw PermanentErrorException("Invalid certificate for $dataProviderCertificateName", e)
    } catch (e: SignatureException) {
      throw PermanentErrorException("Signature fails verification.", e)
    }

    return signedSeed.unpack()
  }

  private suspend fun shufflePhase(token: ComputationToken): ComputationToken {
    val requisitions = token.requisitionsList.sortedBy { it.externalKey.externalRequisitionId }

    val requisitionBlobs = dataClients.readRequisitionBlobs(token)
    val shufflePhaseInput = getShufflePhaseInput(token)
    val secretSeeds = shufflePhaseInput.secretSeedsList

    val request = completeShufflePhaseRequest {
      val hmss = token.computationDetails.honestMajorityShareShuffle
      sketchParams = hmss.parameters.sketchParams
      commonRandomSeed = hmss.randomSeed xor shufflePhaseInput.peerRandomSeed
      order =
        if (hmss.role == FIRST_NON_AGGREGATOR) {
          CompleteShufflePhaseRequest.NonAggregatorOrder.FIRST
        } else {
          CompleteShufflePhaseRequest.NonAggregatorOrder.SECOND
        }
      dpParams = differentialPrivacyParams {
        delta = hmss.parameters.dpParams.delta
        epsilon = hmss.parameters.dpParams.epsilon
      }
      noiseMechanism = hmss.parameters.noiseMechanism

      val registerCounts = mutableListOf<Long>()
      for (requisition in requisitions) {
        val requisitionId = requisition.externalKey.externalRequisitionId

        val blob = requisitionBlobs[requisitionId]
        if (blob != null) {
          // Requisition in format of blob.
          registerCounts += requisition.details.protocol.honestMajorityShareShuffle.registerCount
          sketchShares += sketchShare {
            data =
              CompleteShufflePhaseRequestKt.SketchShareKt.shareData {
                values += FrequencyVector.parseFrom(blob).dataList
              }
          }
        } else {
          // Requisition in format of random seed.
          val secretSeed =
            secretSeeds.find { it.requisitionId == requisitionId }
              ?: error("Neither blob and seed received for requisition $requisitionId")

          val publicApiVersion =
            Version.fromString(token.computationDetails.kingdomComputation.publicApiVersion)
          val seed =
            verifySecretSeed(secretSeed, hmss.encryptionKeyPair.privateKeyId, publicApiVersion)

          sketchShares += sketchShare { this.seed = seed.data }
        }
      }
      require(registerCounts.distinct().size == 1) {
        "All RegisterCount from requisitions must be the same. $registerCounts"
      }
      sketchParams = hmss.parameters.sketchParams.copy { registerCount = registerCounts.first() }
    }

    val result = cryptoWorker.completeShufflePhase(request)

    val aggregationPhaseInput = aggregationPhaseInput {
      combinedSketch += result.combinedSketchList
    }

    sendAdvanceComputationRequest(
      header =
        advanceComputationHeader(Description.AGGREGATION_PHASE_INPUT, token.globalComputationId),
      content = addLoggingHook(token, flowOf(aggregationPhaseInput.toByteString())),
      stub = aggregatorStub(),
    )

    return completeComputation(token, ComputationDetails.CompletedReason.SUCCEEDED)
  }

  private suspend fun aggregationPhase(token: ComputationToken): ComputationToken {
    val aggregationPhaseInputs = getAggregationPhaseInputs(token)

    val request = completeAggregationPhaseRequest {
      val hmss = token.computationDetails.honestMajorityShareShuffle
      sketchParams = hmss.parameters.sketchParams
      maximumFrequency = hmss.parameters.maximumFrequency
      val publicApiVersion =
        Version.fromString(token.computationDetails.kingdomComputation.publicApiVersion)
      when (publicApiVersion) {
        Version.V2_ALPHA -> {
          val measurementSpec =
            MeasurementSpec.parseFrom(token.computationDetails.kingdomComputation.measurementSpec)
          vidSamplingIntervalWidth = measurementSpec.vidSamplingInterval.width
        }
      }
      dpParams = differentialPrivacyParams {
        delta = hmss.parameters.dpParams.delta
        epsilon = hmss.parameters.dpParams.epsilon
      }
      noiseMechanism = hmss.parameters.noiseMechanism

      for (input in aggregationPhaseInputs) {
        sketchShares +=
          CompleteAggregationPhaseRequestKt.shareData { shareVector += input.combinedSketchList }
      }
    }

    val result = cryptoWorker.completeAggregationPhase(request)

    sendResultToKingdom(
      token,
      ReachAndFrequencyResult(result.reach, result.frequencyDistributionMap),
    )

    return completeComputation(token, ComputationDetails.CompletedReason.SUCCEEDED)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private val STAGE_TRANSITIONS =
      mapOf(
        Pair(Stage.INITIALIZED, FIRST_NON_AGGREGATOR) to Stage.WAIT_TO_START,
        Pair(Stage.INITIALIZED, SECOND_NON_AGGREGATOR) to Stage.WAIT_ON_SHUFFLE_INPUT_PHASE_ONE,
        Pair(Stage.INITIALIZED, AGGREGATOR) to Stage.WAIT_ON_AGGREGATION_INPUT,
        Pair(Stage.SETUP_PHASE, FIRST_NON_AGGREGATOR) to Stage.WAIT_ON_SHUFFLE_INPUT_PHASE_TWO,
        Pair(Stage.SETUP_PHASE, SECOND_NON_AGGREGATOR) to Stage.SHUFFLE_PHASE,
        Pair(Stage.SHUFFLE_PHASE, FIRST_NON_AGGREGATOR) to Stage.COMPLETE,
        Pair(Stage.SHUFFLE_PHASE, SECOND_NON_AGGREGATOR) to Stage.COMPLETE,
        Pair(Stage.AGGREGATION_PHASE, AGGREGATOR) to Stage.COMPLETE,
      )
  }
}
