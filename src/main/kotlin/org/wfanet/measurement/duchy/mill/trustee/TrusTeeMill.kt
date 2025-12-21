// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.mill.trustee

import com.google.crypto.tink.BinaryKeysetReader
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.StreamingAead
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.ByteString
import java.nio.ByteBuffer
import java.nio.file.Path
import java.security.GeneralSecurityException
import java.time.Clock
import java.time.Duration
import java.util.logging.Logger
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.tink.GCloudWifCredentials
import org.wfanet.measurement.common.crypto.tink.KmsClientFactory
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients.PermanentErrorException
import org.wfanet.measurement.duchy.mill.Certificate
import org.wfanet.measurement.duchy.mill.MillBase
import org.wfanet.measurement.duchy.mill.trustee.processor.TrusTeeParams
import org.wfanet.measurement.duchy.mill.trustee.processor.TrusTeeProcessor
import org.wfanet.measurement.duchy.mill.trustee.processor.TrusTeeReachAndFrequencyParams
import org.wfanet.measurement.duchy.mill.trustee.processor.TrusTeeReachParams
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.RequisitionDetails
import org.wfanet.measurement.internal.duchy.RequisitionMetadata
import org.wfanet.measurement.internal.duchy.protocol.TrusTee
import org.wfanet.measurement.internal.duchy.protocol.TrusTee.ComputationDetails as TrusTeeDetails
import org.wfanet.measurement.internal.duchy.protocol.TrusTee.Stage
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt
import org.wfanet.measurement.system.v1alpha.ComputationParticipant
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKt
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt

class TrusTeeMill(
  millId: String,
  duchyId: String,
  signingKey: SigningKeyHandle,
  consentSignalCert: Certificate,
  dataClients: ComputationDataClients,
  systemComputationParticipantsClient: ComputationParticipantsCoroutineStub,
  systemComputationsClient: ComputationsGrpcKt.ComputationsCoroutineStub,
  systemComputationLogEntriesClient: ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub,
  computationStatsClient: ComputationStatsGrpcKt.ComputationStatsCoroutineStub,
  workLockDuration: Duration,
  private val trusTeeProcessorFactory: TrusTeeProcessor.Factory,
  private val kmsClientFactory: KmsClientFactory<GCloudWifCredentials>,
  private val attestationTokenPath: Path,
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
    computationType = ComputationType.TRUS_TEE,
    workLockDuration = workLockDuration,
    requestChunkSizeBytes = requestChunkSizeBytes,
    maximumAttempts = maximumAttempts,
    clock = clock,
  ) {

  override val endingStage = Stage.COMPLETE.toProtocolStage()

  override suspend fun processComputationImpl(token: ComputationToken) {
    require(token.computationDetails.hasTrusTee()) {
      "Only TrusTEE computation is supported in this mill."
    }
    val stage = token.computationStage.trusTee
    val action = actions[stage] ?: error("Unexpected stage $stage.")
    val updatedToken = action(token)

    val globalId = token.globalComputationId
    val updatedStage = updatedToken.computationStage.trusTee
    logger.info("$globalId@$millId: Stage transitioned from $stage to $updatedStage")
  }

  private val actions =
    mapOf(Stage.INITIALIZED to ::initializationPhase, Stage.COMPUTING to ::computingPhase)

  private suspend fun initializationPhase(token: ComputationToken): ComputationToken {
    val requisitionParams =
      ComputationParticipantKt.requisitionParams {
        duchyCertificate = consentSignalCert.name
        trusTee = ComputationParticipant.RequisitionParams.TrusTee.getDefaultInstance()
      }
    sendRequisitionParamsToKingdom(token, requisitionParams)

    return dataClients.transitionComputationToStage(
      token,
      stage = Stage.WAIT_TO_START.toProtocolStage(),
    )
  }

  private suspend fun computingPhase(token: ComputationToken): ComputationToken {
    val trusTeeDetails: TrusTeeDetails = token.computationDetails.trusTee
    val trusTeeParams = trusTeeDetails.toTrusTeeParams()

    val processor: TrusTeeProcessor = trusTeeProcessorFactory.create(trusTeeParams)

    for (requisition in token.requisitionsList) {
      val details = requisition.details

      val kmsClient = getKmsClient(kmsClientFactory, details.protocol.trusTee)
      val dek = getDekKeysetHandle(kmsClient, details.protocol.trusTee)
      // TODO(world-federation-of-advertisers/cross-media-measurement#2800): Use
      //  StreamingAeadStorage instead to read and decrypt requisition data.
      val rawRequisitionData = getRequisitionData(requisition)
      val decryptedRequisitionData = decryptRequisitionData(dek, rawRequisitionData)

      processor.addFrequencyVector(decryptedRequisitionData)
    }

    val computationResult = processor.computeResult()

    sendResultToKingdom(token, computationResult)

    return completeComputation(token, ComputationDetails.CompletedReason.SUCCEEDED)
  }

  /** Converts internal [TrusTee.ComputationDetails] to the internal [TrusTeeParams]. */
  fun TrusTeeDetails.toTrusTeeParams(): TrusTeeParams {
    return when (type) {
      TrusTeeDetails.Type.REACH -> {
        require(parameters.hasReachDpParams()) {
          "Reach DP params are required for a Reach-only TrusTee computation."
        }
        TrusTeeReachParams(parameters.vidSamplingIntervalWidth, parameters.reachDpParams)
      }
      TrusTeeDetails.Type.REACH_AND_FREQUENCY -> {
        require(parameters.hasReachDpParams()) {
          "Reach DP params are required for a Reach-and-Frequency TrusTee computation."
        }
        require(parameters.hasFrequencyDpParams()) {
          "Frequency DP params are required for a Reach-and-Frequency TrusTee computation."
        }
        TrusTeeReachAndFrequencyParams(
          parameters.maximumFrequency,
          parameters.vidSamplingIntervalWidth,
          parameters.reachDpParams,
          parameters.frequencyDpParams,
        )
      }
      TrusTeeDetails.Type.TYPE_UNSPECIFIED,
      TrusTeeDetails.Type.UNRECOGNIZED ->
        throw IllegalArgumentException(
          "Unsupported or unspecified TrusTEE computation type in ComputationDetails: $type"
        )
    }
  }

  private fun getKmsClient(
    kmsClientFactory: KmsClientFactory<GCloudWifCredentials>,
    protocol: RequisitionDetails.RequisitionProtocol.TrusTee,
  ): KmsClient {
    val credentials =
      GCloudWifCredentials(
        audience = protocol.workloadIdentityProvider,
        subjectTokenType = OAUTH_TOKEN_TYPE_ID_TOKEN,
        tokenUrl = GOOGLE_STS_TOKEN_URL,
        credentialSourceFilePath = attestationTokenPath.toString(),
        serviceAccountImpersonationUrl =
          IAM_IMPERSONATION_URL_FORMAT.format(protocol.impersonatedServiceAccount),
      )

    try {
      return kmsClientFactory.getKmsClient(credentials)
    } catch (e: GeneralSecurityException) {
      throw PermanentErrorException("Failed to create KMS client", e)
    }
  }

  private fun getDekKeysetHandle(
    kmsClient: KmsClient,
    protocol: RequisitionDetails.RequisitionProtocol.TrusTee,
  ): KeysetHandle {
    try {
      val kekAead = kmsClient.getAead(protocol.kmsKekUri)

      return KeysetHandle.read(
        BinaryKeysetReader.withBytes(protocol.encryptedDekCiphertext.toByteArray()),
        kekAead,
      )
    } catch (e: GeneralSecurityException) {
      throw PermanentErrorException("Failed to get DEK keyset due to a cryptographic error", e)
    }
  }

  private suspend fun getRequisitionData(requisition: RequisitionMetadata): ByteString {
    return dataClients.readSingleRequisitionBlob(requisition)
      ?: throw PermanentErrorException("Requisition data not found for ${requisition.externalKey}")
  }

  private fun decryptRequisitionData(dek: KeysetHandle, data: ByteString): ByteArray {
    try {
      // TODO(world-federation-of-advertisers/cross-media-measurement#3347): decrypt data by chunks
      //  while reading from the storage to improve the performance.
      val streamingAead = dek.getPrimitive(StreamingAead::class.java)
      val decryptingStream = streamingAead.newDecryptingStream(data.newInput(), byteArrayOf())
      return decryptingStream.readAllBytes()
    } catch (e: GeneralSecurityException) {
      throw PermanentErrorException(
        "Failed to decrypt requisition data due to a cryptographic error",
        e,
      )
    } catch (e: IllegalArgumentException) {
      throw PermanentErrorException("Decrypted requisition data has an invalid format", e)
    }
  }

  private fun toIntArray(bytes: ByteArray): IntArray {
    require(bytes.size % 4 == 0) { "Input ByteArray size (${bytes.size}) must be a multiple of 4." }

    val intArray = IntArray(bytes.size / 4)
    val buffer = ByteBuffer.wrap(bytes)
    buffer.asIntBuffer().get(intArray)

    return intArray
  }

  companion object {
    init {
      AeadConfig.register()
      StreamingAeadConfig.register()
    }

    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private const val OAUTH_TOKEN_TYPE_ID_TOKEN = "urn:ietf:params:oauth:token-type:id_token"
    private const val GOOGLE_STS_TOKEN_URL = "https://sts.googleapis.com/v1/token"
    private const val IAM_IMPERSONATION_URL_FORMAT =
      "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/%s:generateAccessToken"
  }
}
