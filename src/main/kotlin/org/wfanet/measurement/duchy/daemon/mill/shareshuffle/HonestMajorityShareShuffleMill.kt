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
import com.google.protobuf.kotlin.toByteString
import io.grpc.Status
import io.grpc.StatusException
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.metrics.Meter
import java.security.cert.X509Certificate
import java.time.Clock
import java.time.Duration
import java.util.logging.Logger
import kotlin.experimental.xor
import kotlinx.coroutines.flow.flowOf
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.EncryptedMessage
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.RandomSeed
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.crypto.PrivateKeyStore
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.authorityKeyIdentifier
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.tink.TinkKeyId
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.consent.client.duchy.decryptRandomSeed
import org.wfanet.measurement.consent.client.duchy.encryptResult
import org.wfanet.measurement.consent.client.duchy.verifyRandomSeed
import org.wfanet.measurement.consent.client.duchy.signEncryptionPublicKey
import org.wfanet.measurement.consent.client.duchy.signResult
import org.wfanet.measurement.duchy.daemon.mill.Certificate
import org.wfanet.measurement.duchy.daemon.mill.MillBase
import org.wfanet.measurement.duchy.daemon.mill.shareshuffle.crypto.HonestMajorityShareShuffleCryptor
import org.wfanet.measurement.duchy.daemon.utils.ReachAndFrequencyResult
import org.wfanet.measurement.duchy.daemon.utils.toV2AlphaEncryptionPublicKey
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients
import org.wfanet.measurement.duchy.service.system.v1alpha.advanceComputationHeader
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.RequisitionMetadata
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
import org.wfanet.measurement.internal.duchy.protocol.ShareShuffleSketch
import org.wfanet.measurement.internal.duchy.protocol.completeAggregationPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.completeShufflePhaseRequest
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
  private val privateKeyStore: PrivateKeyStore<TinkKeyId, TinkPrivateKeyHandle>,
  private val certificateClient: CertificatesGrpcKt.CertificatesCoroutineStub,
  private val workerStubs: Map<String, ComputationControlCoroutineStub>,
  private val cryptoWorker: HonestMajorityShareShuffleCryptor,
  workLockDuration: Duration,
  openTelemetry: OpenTelemetry,
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
  private val meter: Meter = openTelemetry.getMeter(HonestMajorityShareShuffleMill::class.java.name)

  // TODO(@renjiez): add metrics.

  override val endingStage = Stage.COMPLETE.toProtocolStage()

  private val actions =
    mapOf(
      Pair(Stage.INITIALIZED, FIRST_NON_AGGREGATOR) to ::initializationPhase,
      Pair(Stage.INITIALIZED, SECOND_NON_AGGREGATOR) to ::initializationPhase,
      Pair(Stage.SETUP_PHASE, FIRST_NON_AGGREGATOR) to ::setupPhase,
      Pair(Stage.SETUP_PHASE, SECOND_NON_AGGREGATOR) to ::setupPhase,
      Pair(Stage.SHUFFLE_PHASE, FIRST_NON_AGGREGATOR) to ::shufflePhase,
      Pair(Stage.SHUFFLE_PHASE, SECOND_NON_AGGREGATOR) to ::shufflePhase,
      Pair(Stage.AGGREGATION_PHASE, AGGREGATOR) to ::aggregationPhase,
    )

  private fun nextStage(token: ComputationToken): Stage {
    require(token.computationDetails.hasHonestMajorityShareShuffle()) {
      "Only Honest Majority Share Shuffle computation is supported in this mill."
    }

    val stage = token.computationStage.honestMajorityShareShuffle
    val role = token.computationDetails.honestMajorityShareShuffle.role

    val nextStages =
      mapOf(
        Pair(Stage.INITIALIZED, FIRST_NON_AGGREGATOR) to Stage.WAIT_TO_START,
        Pair(Stage.INITIALIZED, SECOND_NON_AGGREGATOR) to Stage.WAIT_ON_SHUFFLE_INPUT_PHASE_ONE,
        Pair(Stage.SETUP_PHASE, FIRST_NON_AGGREGATOR) to Stage.WAIT_ON_SHUFFLE_INPUT_PHASE_TWO,
        Pair(Stage.SETUP_PHASE, SECOND_NON_AGGREGATOR) to Stage.SETUP_PHASE,
        Pair(Stage.SHUFFLE_PHASE, FIRST_NON_AGGREGATOR) to Stage.COMPLETE,
        Pair(Stage.SHUFFLE_PHASE, SECOND_NON_AGGREGATOR) to Stage.COMPLETE,
        Pair(Stage.AGGREGATION_PHASE, AGGREGATOR) to Stage.COMPLETE,
      )
    return nextStages[Pair(stage, role)] ?: error("Unexpected stage or role: ($stage, $role)")
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
    require(computationDetails.encryptionKeyPair.hasPublicKey()) { "Public key not set." }

    val publicKey = computationDetails.encryptionKeyPair.publicKey

    val request = setParticipantRequisitionParamsRequest {
      name = ComputationParticipantKey(token.globalComputationId, duchyId).toName()
      requisitionParams =
        ComputationParticipantKt.requisitionParams {
          duchyCertificate = consentSignalCert.name
          honestMajorityShareShuffle =
            ComputationParticipantKt.RequisitionParamsKt.honestMajorityShareShuffle {
              val publicApiVersion = Version.fromString(token.computationDetails.kingdomComputation.publicApiVersion)
              when (publicApiVersion) {
                Version.V2_ALPHA -> {
                  val encryptionPublicKye = publicKey.toV2AlphaEncryptionPublicKey()
                  tinkPublicKey = signEncryptionPublicKey(encryptionPublicKye, signingKey, signingKey.defaultAlgorithm).toByteString()
                }
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
        else -> ComputationDataClients.PermanentErrorException(message, e)
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

  private fun peerDuchyStub(participants: List<String>): ComputationControlCoroutineStub {
    val peerDuchy = participants.dropLast(1).find { it != duchyId }
    return workerStubs[peerDuchy]
      ?: throw ComputationDataClients.PermanentErrorException(
        "No ComputationControlService stub for the peer duchy '$peerDuchy'"
      )
  }

  private fun aggregatorStub(participants: List<String>): ComputationControlCoroutineStub {
    val aggregator = participants.last()
    return workerStubs[aggregator]
      ?: throw ComputationDataClients.PermanentErrorException(
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
        token.requisitionsList.map {
          val source = it
          ShufflePhaseInputKt.secretSeed {
            requisitionId = source.externalKey.externalRequisitionId
            secretSeedCiphertext = source.secretSeedCiphertext
          }
        }
    }

    sendAdvanceComputationRequest(
      header = advanceComputationHeader(headerDescription, token.globalComputationId),
      content = addLoggingHook(token, flowOf(shufflePhaseInput.toByteString())),
      stub = peerDuchyStub(hmssDetails.participantsList),
    )

    return dataClients.transitionComputationToStage(
      token,
      stage = nextStage(token).toProtocolStage(),
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

  private fun combineRandomSeeds(seed1: ByteString, seed2: ByteString): ByteString {
    require(seed1.size() == seed2.size()) {
      "Seeds size does not match. size_1=${seed1.size()}, size_2=${seed2.size()}"
    }

    val combinedSeed = ByteArray(seed1.size())
    for (i in combinedSeed.indices) {
      combinedSeed[i] = seed1.byteAt(i) xor seed2.byteAt(i)
    }
    return combinedSeed.toByteString()
  }

  private suspend fun verifySecretSeed(
    secretSeedCiphertext: ByteString,
    requisitionMetadata: RequisitionMetadata,
    duchyPrivateKeyId: String,
  ): RandomSeed {
    val privateKey =
      privateKeyStore.read(TinkKeyId(duchyPrivateKeyId.toLong()))
        ?: error(
          "Fail to read private key for requisition ${requisitionMetadata.externalKey.externalRequisitionId}"
        )

    val encryptedAndSignedMessage = EncryptedMessage.parseFrom(secretSeedCiphertext)
    val signedMessage = decryptRandomSeed(encryptedAndSignedMessage, privateKey)

    val randomSeed: RandomSeed = signedMessage.unpack()

    val dataProviderCertificateName =
      requisitionMetadata.details.honestMajorityShareShuffle.dataProviderCertificate
    val dataProviderCertificate =
      certificateClient.getCertificate(getCertificateRequest { name = dataProviderCertificateName })
    val x509Certificate: X509Certificate = readCertificate(dataProviderCertificate.x509Der)
    val trustedIssuer =
      trustedCertificates[x509Certificate.authorityKeyIdentifier]
        ?: error("trustedIssuer not found for $dataProviderCertificateName.")

    verifyRandomSeed(signedMessage, x509Certificate, trustedIssuer)

    return randomSeed
  }

  private suspend fun shufflePhase(token: ComputationToken): ComputationToken {
    val requisitions = token.requisitionsList
    requisitions.sortedBy { it.externalKey.externalRequisitionId }

    val requisitionBlobs = dataClients.readRequisitionBlobs(token)
    val shufflePhaseInput = getShufflePhaseInput(token)
    val secretSeeds = shufflePhaseInput.secretSeedsList

    val request = completeShufflePhaseRequest {
      val hmss = token.computationDetails.honestMajorityShareShuffle
      sketchParams = hmss.parameters.sketchParams
      commonRandomSeed = combineRandomSeeds(hmss.randomSeed, shufflePhaseInput.peerRandomSeed)
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

      for (requisition in requisitions) {
        val requisitionId = requisition.externalKey.externalRequisitionId

        val blob = requisitionBlobs[requisitionId]
        if (blob != null) {
          // Requisition in format of blob.
          sketchShares += sketchShare {
            data = CompleteShufflePhaseRequestKt.SketchShareKt.shareData {
              values += ShareShuffleSketch.parseFrom(blob).dataList
            }
          }
        } else {
          // Requisition in format of random seed.
          val secretSeed = secretSeeds.find { it.requisitionId == requisitionId }
          require(secretSeed != null) {
            "Neither blob and seed received for requisition $requisitionId"
          }
          val seed =
            verifySecretSeed(
              secretSeed.secretSeedCiphertext,
              requisition,
              hmss.encryptionKeyPair.privateKeyId,
            )

          sketchShares += sketchShare { this.seed = seed.data }
        }
      }
    }

    val result = cryptoWorker.completeShufflePhase(request)

    val aggregationPhaseInput = aggregationPhaseInput {
      combinedSketch += result.combinedSketchList
    }

    sendAdvanceComputationRequest(
      header =
        advanceComputationHeader(Description.AGGREGATION_PHASE_INPUT, token.globalComputationId),
      content = addLoggingHook(token, flowOf(aggregationPhaseInput.toByteString())),
      stub = aggregatorStub(token.computationDetails.honestMajorityShareShuffle.participantsList),
    )

    return dataClients.transitionComputationToStage(token, stage = Stage.COMPLETE.toProtocolStage())
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
        sketchShares += CompleteAggregationPhaseRequestKt.shareData { shareVector += input.combinedSketchList }
      }
    }

    val result = cryptoWorker.completeAggregationPhase(request)

    sendResultToKingdom(token, ReachAndFrequencyResult(result.reach, result.frequencyDistributionMap))

    return completeComputation(token, ComputationDetails.CompletedReason.SUCCEEDED)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
