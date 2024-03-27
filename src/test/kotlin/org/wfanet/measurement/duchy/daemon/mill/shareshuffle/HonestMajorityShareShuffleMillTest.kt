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

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import io.grpc.Status
import io.opentelemetry.api.GlobalOpenTelemetry
import java.security.SecureRandom
import java.security.cert.X509Certificate
import java.time.Clock
import java.time.Duration
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.UseConstructor
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.EncryptedMessage
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.randomSeed
import org.wfanet.measurement.api.v2alpha.setMessage
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.crypto.PrivateKeyStore
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.authorityKeyIdentifier
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.crypto.testing.TestData
import org.wfanet.measurement.common.crypto.tink.TinkKeyId
import org.wfanet.measurement.common.crypto.tink.TinkKeyStorageProvider
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.DuchyInfo
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.testing.verifyAndCapture
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoDuration
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.dataprovider.encryptRandomSeed
import org.wfanet.measurement.consent.client.dataprovider.signRandomSeed
import org.wfanet.measurement.consent.client.dataprovider.verifyEncryptionPublicKey
import org.wfanet.measurement.duchy.daemon.mill.Certificate
import org.wfanet.measurement.duchy.daemon.mill.shareshuffle.crypto.HonestMajorityShareShuffleCryptor
import org.wfanet.measurement.duchy.daemon.testing.TestRequisition
import org.wfanet.measurement.duchy.daemon.utils.toDuchyEncryptionPublicKey
import org.wfanet.measurement.duchy.daemon.utils.toV2AlphaEncryptionPublicKey
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients
import org.wfanet.measurement.duchy.db.computation.testing.FakeComputationsDatabase
import org.wfanet.measurement.duchy.service.internal.computations.ComputationsService
import org.wfanet.measurement.duchy.service.internal.computations.newOutputBlobMetadata
import org.wfanet.measurement.duchy.storage.ComputationBlobContext
import org.wfanet.measurement.duchy.storage.ComputationStore
import org.wfanet.measurement.duchy.storage.RequisitionBlobContext
import org.wfanet.measurement.duchy.storage.RequisitionStore
import org.wfanet.measurement.duchy.storage.TinkKeyStore
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationDetailsKt
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineImplBase
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.EncryptionPublicKey
import org.wfanet.measurement.internal.duchy.NoiseMechanism
import org.wfanet.measurement.internal.duchy.RequisitionDetailsKt.honestMajorityShareShuffleDetails
import org.wfanet.measurement.internal.duchy.computationDetails
import org.wfanet.measurement.internal.duchy.computationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.computationToken
import org.wfanet.measurement.internal.duchy.config.RoleInComputation
import org.wfanet.measurement.internal.duchy.copy
import org.wfanet.measurement.internal.duchy.differentialPrivacyParams
import org.wfanet.measurement.internal.duchy.encryptionKeyPair
import org.wfanet.measurement.internal.duchy.encryptionPublicKey
import org.wfanet.measurement.internal.duchy.protocol.CompleteAggregationPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteAggregationPhaseRequestKt
import org.wfanet.measurement.internal.duchy.protocol.CompleteShufflePhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteShufflePhaseRequestKt
import org.wfanet.measurement.internal.duchy.protocol.CompleteShufflePhaseRequestKt.sketchShare
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle.ShufflePhaseInput
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle.Stage
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffleKt
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffleKt.ShufflePhaseInputKt.secretSeed
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffleKt.aggregationPhaseInput
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffleKt.shufflePhaseInput
import org.wfanet.measurement.internal.duchy.protocol.completeAggregationPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.completeAggregationPhaseResponse
import org.wfanet.measurement.internal.duchy.protocol.completeShufflePhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.completeShufflePhaseResponse
import org.wfanet.measurement.internal.duchy.protocol.shareShuffleSketch
import org.wfanet.measurement.internal.duchy.protocol.shareShuffleSketchParams
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.measurement.system.v1alpha.AdvanceComputationRequest
import org.wfanet.measurement.system.v1alpha.AdvanceComputationRequestKt
import org.wfanet.measurement.system.v1alpha.AdvanceComputationResponse
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKey
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKt
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub as SystemComputationParticipantsCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt.ComputationsCoroutineImplBase as SystemComputationsCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.HonestMajorityShareShuffle as SystemHonestMajorityShareShuffle
import org.wfanet.measurement.system.v1alpha.Requisition
import org.wfanet.measurement.system.v1alpha.honestMajorityShareShuffle as systemHonestMajorityShareShuffle
import org.wfanet.measurement.system.v1alpha.setComputationResultRequest
import org.wfanet.measurement.system.v1alpha.setParticipantRequisitionParamsRequest

private const val PUBLIC_API_VERSION = "v2alpha"
private val RANDOM = SecureRandom()

private const val MILL_ID_SUFFIX = " mill"
private const val DUCHY_ONE_NAME = "worker_1"
private const val DUCHY_TWO_NAME = "worker_2"
private const val DUCHY_THREE_NAME = "aggregator"

private const val LOCAL_ID = 1234L
private const val GLOBAL_ID = LOCAL_ID.toString()
private const val RANDOM_SEED_LENGTH_IN_BYTES = 48

// In the test, use the same set of cert and encryption key for all parties.
private const val DUCHY_CERT_NAME = "cert 1"
private val DUCHY_CERT_DER = TestData.FIXED_SERVER_CERT_DER_FILE.readBytes().toByteString()
private val DUCHY_PRIVATE_KEY_DER = TestData.FIXED_SERVER_KEY_DER_FILE.readBytes().toByteString()
private val ROOT_CERT: X509Certificate = readCertificate(TestData.FIXED_CA_CERT_PEM_FILE)
private val MEASUREMENT_ENCRYPTION_PRIVATE_KEY = TinkPrivateKeyHandle.generateEcies()
private val MEASUREMENT_ENCRYPTION_PUBLIC_KEY =
  MEASUREMENT_ENCRYPTION_PRIVATE_KEY.publicKey.toEncryptionPublicKey()
private val DUCHY_SIGNING_CERT = readCertificate(DUCHY_CERT_DER)
private val DUCHY_SIGNING_KEY =
  SigningKeyHandle(
    DUCHY_SIGNING_CERT,
    readPrivateKey(DUCHY_PRIVATE_KEY_DER, DUCHY_SIGNING_CERT.publicKey.algorithm),
  )

private val DATA_PROVIDER_CERT_DER = TestData.FIXED_SERVER_CERT_DER_FILE.readBytes().toByteString()
private val DATA_PROVIDER_PRIVATE_KEY_DER =
  TestData.FIXED_SERVER_KEY_DER_FILE.readBytes().toByteString()
private val DATA_PROVIDER_SIGNING_CERT = readCertificate(DATA_PROVIDER_CERT_DER)
private val DATA_PROVIDER_SIGNING_KEY =
  SigningKeyHandle(
    DATA_PROVIDER_SIGNING_CERT,
    readPrivateKey(DATA_PROVIDER_PRIVATE_KEY_DER, DATA_PROVIDER_SIGNING_CERT.publicKey.algorithm),
  )

private const val KEK_URI = FakeKmsClient.KEY_URI_PREFIX + "kek"
private val AEAD_KEY_TEMPLATE = KeyTemplates.get("AES128_GCM")
private val KEY_ENCRYPTION_KEY = KeysetHandle.generateNew(AEAD_KEY_TEMPLATE)
private val AEAD = KEY_ENCRYPTION_KEY.getPrimitive(Aead::class.java)

private const val COMPUTATION_PARTICIPANT_1 = "worker_1"
private const val COMPUTATION_PARTICIPANT_2 = "worker_2"
private const val COMPUTATION_PARTICIPANT_3 = "aggregator"

private val TEST_REQUISITION_1 = TestRequisition("111") { SERIALIZED_MEASUREMENT_SPEC }
private val TEST_REQUISITION_2 = TestRequisition("222") { SERIALIZED_MEASUREMENT_SPEC }
private val TEST_REQUISITION_3 = TestRequisition("333") { SERIALIZED_MEASUREMENT_SPEC }

private val MEASUREMENT_SPEC = measurementSpec {
  nonceHashes += TEST_REQUISITION_1.nonceHash
  nonceHashes += TEST_REQUISITION_2.nonceHash
  nonceHashes += TEST_REQUISITION_3.nonceHash
  reachAndFrequency = MeasurementSpec.ReachAndFrequency.getDefaultInstance()
  vidSamplingInterval = MeasurementSpecKt.vidSamplingInterval { width = 0.5f }
}
private val SERIALIZED_MEASUREMENT_SPEC: ByteString = MEASUREMENT_SPEC.toByteString()

private val REQUISITION_1 =
  TEST_REQUISITION_1.toRequisitionMetadata(Requisition.State.FULFILLED, DUCHY_ONE_NAME).copy {
    details =
      details.copy {
        honestMajorityShareShuffle = honestMajorityShareShuffleDetails {
          registerCount = 100
          dataProviderCertificate = "DataProviders/1/Certificates/1"
        }
      }
    path = RequisitionBlobContext(GLOBAL_ID, externalKey.externalRequisitionId).blobKey
    secretSeedCiphertext = "secret_seed_1".toByteStringUtf8()
  }
private val REQUISITION_2 =
  TEST_REQUISITION_2.toRequisitionMetadata(Requisition.State.UNFULFILLED, DUCHY_TWO_NAME)
private val REQUISITION_3 =
  TEST_REQUISITION_3.toRequisitionMetadata(Requisition.State.FULFILLED, DUCHY_ONE_NAME).copy {
    details =
      details.copy {
        honestMajorityShareShuffle = honestMajorityShareShuffleDetails {
          registerCount = 100
          dataProviderCertificate = "DataProviders/3/Certificates/2"
        }
      }
    path = RequisitionBlobContext(GLOBAL_ID, externalKey.externalRequisitionId).blobKey
    secretSeedCiphertext = "secret_seed_3".toByteStringUtf8()
  }
private val REQUISITIONS = listOf(REQUISITION_1, REQUISITION_2, REQUISITION_3)

private val HMSS_PARAMETERS =
  HonestMajorityShareShuffleKt.ComputationDetailsKt.parameters {
    sketchParams = shareShuffleSketchParams {
      bytesPerRegister = 4
      maximumCombinedFrequency = 11
      ringModulus = 13
    }
    dpParams = differentialPrivacyParams {
      epsilon = 1.0
      delta = 1.0
    }
    noiseMechanism = NoiseMechanism.DISCRETE_GAUSSIAN
  }

@RunWith(JUnit4::class)
class HonestMajorityShareShuffleMillTest {
  private val mockComputationControl: ComputationControlCoroutineImplBase = mockService {
    onBlocking { advanceComputation(any()) }
      .thenAnswer {
        val request: Flow<AdvanceComputationRequest> = it.getArgument(0)
        advanceComputationRequests = runBlocking { request.toList() }
        AdvanceComputationResponse.getDefaultInstance()
      }
  }
  private val mockSystemComputations: SystemComputationsCoroutineImplBase = mockService()
  private val mockComputationParticipants: ComputationParticipantsCoroutineImplBase = mockService()
  private val mockComputationLogEntries:
    ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineImplBase =
    mockService()
  private val mockComputationStats: ComputationStatsCoroutineImplBase = mockService()
  private val mockCryptoWorker: HonestMajorityShareShuffleCryptor =
    mock(useConstructor = UseConstructor.parameterless()) {}
  private val mockCertificates: CertificatesGrpcKt.CertificatesCoroutineImplBase = mockService()

  private val fakeComputationDb = FakeComputationsDatabase()

  private lateinit var computationDataClients: ComputationDataClients
  private lateinit var computationStore: ComputationStore
  private lateinit var requisitionStore: RequisitionStore
  private lateinit var privateKeyStore: PrivateKeyStore<TinkKeyId, TinkPrivateKeyHandle>

  private val tempDirectory = TemporaryFolder()

  private val grpcTestServerRule = GrpcTestServerRule {
    val storageClient = FileSystemStorageClient(tempDirectory.root)
    computationStore = ComputationStore(storageClient)
    requisitionStore = RequisitionStore(storageClient)
    val kmsClient = FakeKmsClient().also { it.setAead(KEK_URI, AEAD) }
    privateKeyStore =
      TinkKeyStorageProvider(kmsClient).makeKmsPrivateKeyStore(TinkKeyStore(storageClient), KEK_URI)
    computationDataClients =
      ComputationDataClients.forTesting(
        ComputationsCoroutineStub(channel),
        computationStore,
        requisitionStore,
      )
    addService(mockComputationControl)
    addService(mockSystemComputations)
    addService(mockComputationLogEntries)
    addService(mockComputationParticipants)
    addService(mockComputationStats)
    addService(
      ComputationsService(
        fakeComputationDb,
        systemComputationLogEntriesStub,
        computationStore,
        requisitionStore,
        DUCHY_THREE_NAME,
        Clock.systemUTC(),
      )
    )
    addService(mockCertificates)
  }

  @get:Rule val ruleChain = chainRulesSequentially(tempDirectory, grpcTestServerRule)

  private val workerStub: ComputationControlCoroutineStub by lazy {
    ComputationControlCoroutineStub(grpcTestServerRule.channel)
  }

  private val systemComputationStub: ComputationsGrpcKt.ComputationsCoroutineStub by lazy {
    ComputationsGrpcKt.ComputationsCoroutineStub(grpcTestServerRule.channel)
  }

  private val systemComputationLogEntriesStub:
    ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub by lazy {
    ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub(grpcTestServerRule.channel)
  }

  private val systemComputationParticipantsStub:
    SystemComputationParticipantsCoroutineStub by lazy {
    SystemComputationParticipantsCoroutineStub(grpcTestServerRule.channel)
  }

  private val computationStatsStub: ComputationStatsCoroutineStub by lazy {
    ComputationStatsCoroutineStub(grpcTestServerRule.channel)
  }

  private val certificateStub: CertificatesGrpcKt.CertificatesCoroutineStub by lazy {
    CertificatesGrpcKt.CertificatesCoroutineStub(grpcTestServerRule.channel)
  }

  private lateinit var advanceComputationRequests: List<AdvanceComputationRequest>

  // Just use the same workerStub for all other duchies, since it is not relevant to this test.
  private val workerStubs =
    mapOf(
      DUCHY_ONE_NAME to workerStub,
      DUCHY_TWO_NAME to workerStub,
      DUCHY_THREE_NAME to workerStub,
    )

  private fun createHmssMill(duchyName: String): HonestMajorityShareShuffleMill {
    DuchyInfo.setForTest(setOf(DUCHY_ONE_NAME, DUCHY_TWO_NAME, DUCHY_THREE_NAME))

    val csCertificate = Certificate(DUCHY_CERT_NAME, DUCHY_SIGNING_CERT)

    val duchyCertificates =
      DuchyInfo.entries.values.associateBy({ it.rootCertificateSkid }, { ROOT_CERT })
    val dataProviderCertificates =
      mapOf(DATA_PROVIDER_SIGNING_CERT.authorityKeyIdentifier!! to ROOT_CERT)

    val trustedCertificates = duchyCertificates + dataProviderCertificates

    return HonestMajorityShareShuffleMill(
      millId = duchyName + MILL_ID_SUFFIX,
      duchyId = duchyName,
      signingKey = DUCHY_SIGNING_KEY,
      consentSignalCert = csCertificate,
      trustedCertificates = trustedCertificates,
      dataClients = computationDataClients,
      systemComputationParticipantsClient = systemComputationParticipantsStub,
      systemComputationsClient = systemComputationStub,
      systemComputationLogEntriesClient = systemComputationLogEntriesStub,
      computationStatsClient = computationStatsStub,
      privateKeyStore = privateKeyStore,
      certificateClient = certificateStub,
      workerStubs = workerStubs,
      cryptoWorker = mockCryptoWorker,
      workLockDuration = Duration.ofMinutes(5),
      openTelemetry = GlobalOpenTelemetry.get(),
      requestChunkSizeBytes = 20,
      maximumAttempts = 2,
    )
  }

  private suspend fun getHmssComputationDetails(role: RoleInComputation): ComputationDetails {
    return computationDetails {
      kingdomComputation =
        ComputationDetailsKt.kingdomComputationDetails {
          publicApiVersion = PUBLIC_API_VERSION
          measurementPublicKey = MEASUREMENT_ENCRYPTION_PUBLIC_KEY.toDuchyEncryptionPublicKey()
          measurementSpec = SERIALIZED_MEASUREMENT_SPEC
          participantCount = 3
        }
      honestMajorityShareShuffle =
        HonestMajorityShareShuffleKt.computationDetails {
          this.role = role
          parameters = HMSS_PARAMETERS
          participants +=
            listOf(COMPUTATION_PARTICIPANT_1, COMPUTATION_PARTICIPANT_2, COMPUTATION_PARTICIPANT_3)

          if (
            role == RoleInComputation.FIRST_NON_AGGREGATOR ||
              role == RoleInComputation.SECOND_NON_AGGREGATOR
          ) {
            randomSeed = RANDOM.generateSeed(RANDOM_SEED_LENGTH_IN_BYTES).toByteString()

            val privateKeyHandle = TinkPrivateKeyHandle.generateHpke()
            val privateKeyId = privateKeyStore.write(privateKeyHandle)
            encryptionKeyPair = encryptionKeyPair {
              this.privateKeyId = privateKeyId
              publicKey = encryptionPublicKey {
                format = EncryptionPublicKey.Format.TINK_KEYSET
                data = privateKeyHandle.publicKey.toByteString()
              }
            }
          }
        }
    }
  }

  @Test
  fun `initializationPhase sends params to Kingdom and advance stage`() = runBlocking {
    val computationDetails = getHmssComputationDetails(RoleInComputation.FIRST_NON_AGGREGATOR)
    fakeComputationDb.addComputation(
      LOCAL_ID,
      Stage.INITIALIZED.toProtocolStage(),
      computationDetails = computationDetails,
      requisitions = REQUISITIONS,
    )

    val mill = createHmssMill(DUCHY_ONE_NAME)
    mill.pollAndProcessNextComputation()

    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        computationToken {
          globalComputationId = GLOBAL_ID
          localComputationId = LOCAL_ID
          computationStage = Stage.WAIT_TO_START.toProtocolStage()
          attempt = 1
          version = 2
          this.computationDetails = computationDetails
          requisitions += REQUISITIONS
        }
      )

    val request =
      verifyAndCapture(
        mockComputationParticipants,
        ComputationParticipantsCoroutineImplBase::setParticipantRequisitionParams,
      )
    assertThat(request)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        setParticipantRequisitionParamsRequest {
          name = ComputationParticipantKey(GLOBAL_ID, DUCHY_ONE_NAME).toName()
          this.requisitionParams =
            ComputationParticipantKt.requisitionParams { duchyCertificate = DUCHY_CERT_NAME }
        }
      )
    val signedEncryptionPublicKey = signedMessage {
      setMessage(
        com.google.protobuf.any {
          value = request.requisitionParams.honestMajorityShareShuffle.tinkPublicKey
          typeUrl = ProtoReflection.getTypeUrl(EncryptedMessage.getDescriptor())
        }
      )
      signature = request.requisitionParams.honestMajorityShareShuffle.tinkPublicKeySignature
      signatureAlgorithmOid =
        request.requisitionParams.honestMajorityShareShuffle.tinkPublicKeySignatureAlgorithmOid
    }
    verifyEncryptionPublicKey(signedEncryptionPublicKey, DUCHY_SIGNING_CERT, ROOT_CERT)
  }

  @Test
  fun `The first non-aggregator setupPhase successfully sends seeds to the peer worker`() =
    runBlocking {
      val computationDetails = getHmssComputationDetails(RoleInComputation.FIRST_NON_AGGREGATOR)
      fakeComputationDb.addComputation(
        LOCAL_ID,
        Stage.SETUP_PHASE.toProtocolStage(),
        computationDetails = computationDetails,
        requisitions = REQUISITIONS,
      )

      val mill = createHmssMill(DUCHY_ONE_NAME)
      mill.pollAndProcessNextComputation()

      val updatedToken = fakeComputationDb[LOCAL_ID]

      assertThat(updatedToken)
        .isEqualTo(
          computationToken {
            globalComputationId = GLOBAL_ID
            localComputationId = LOCAL_ID
            computationStage = Stage.WAIT_ON_SHUFFLE_INPUT_PHASE_TWO.toProtocolStage()
            attempt = 1
            version = 2
            this.computationDetails = computationDetails
            requisitions += REQUISITIONS
            blobs += newOutputBlobMetadata(0, "")
          }
        )

      assertThat(getComputationRequestHeader())
        .isEqualTo(
          AdvanceComputationRequestKt.header {
            name = "computations/$GLOBAL_ID"
            honestMajorityShareShuffle = systemHonestMajorityShareShuffle {
              description = SystemHonestMajorityShareShuffle.Description.SHUFFLE_PHASE_INPUT_ONE
            }
          }
        )

      val shufflePhaseInput = ShufflePhaseInput.parseFrom(getComputationRequestBodyContent())
      assertThat(shufflePhaseInput)
        .isEqualTo(
          shufflePhaseInput {
            peerRandomSeed = computationDetails.honestMajorityShareShuffle.randomSeed
            secretSeeds += secretSeed {
              requisitionId = REQUISITION_1.externalKey.externalRequisitionId
              secretSeedCiphertext = REQUISITION_1.secretSeedCiphertext
              registerCount = REQUISITION_1.details.honestMajorityShareShuffle.registerCount
              dataProviderCertificate =
                REQUISITION_1.details.honestMajorityShareShuffle.dataProviderCertificate
            }
            secretSeeds += secretSeed {
              requisitionId = REQUISITION_3.externalKey.externalRequisitionId
              secretSeedCiphertext = REQUISITION_3.secretSeedCiphertext
              registerCount = REQUISITION_3.details.honestMajorityShareShuffle.registerCount
              dataProviderCertificate =
                REQUISITION_3.details.honestMajorityShareShuffle.dataProviderCertificate
            }
          }
        )
    }

  @Test
  fun `The second non-aggregator setupPhase successfully sends seeds to the peer worker`() =
    runBlocking {
      val unfulfilledRequisition1 =
        TEST_REQUISITION_1.toRequisitionMetadata(Requisition.State.UNFULFILLED, DUCHY_ONE_NAME)
      val fulfilledRequisition2 =
        TEST_REQUISITION_1.toRequisitionMetadata(Requisition.State.FULFILLED, DUCHY_TWO_NAME).copy {
          details =
            details.copy {
              honestMajorityShareShuffle = honestMajorityShareShuffleDetails {
                registerCount = 100
                dataProviderCertificate = "DataProviders/2/Certificates/2"
              }
            }
          path = RequisitionBlobContext(GLOBAL_ID, externalKey.externalRequisitionId).blobKey
          secretSeedCiphertext = "secret_seed_2".toByteStringUtf8()
        }
      val unfulfilledRequisition3 =
        TEST_REQUISITION_3.toRequisitionMetadata(Requisition.State.UNFULFILLED, DUCHY_ONE_NAME)

      val requisitions =
        listOf(unfulfilledRequisition1, fulfilledRequisition2, unfulfilledRequisition3)
      val computationDetails = getHmssComputationDetails(RoleInComputation.SECOND_NON_AGGREGATOR)
      fakeComputationDb.addComputation(
        LOCAL_ID,
        Stage.SETUP_PHASE.toProtocolStage(),
        computationDetails = computationDetails,
        requisitions = requisitions,
      )

      val mill = createHmssMill(DUCHY_TWO_NAME)
      mill.pollAndProcessNextComputation()

      assertThat(fakeComputationDb[LOCAL_ID])
        .isEqualTo(
          computationToken {
            globalComputationId = GLOBAL_ID
            localComputationId = LOCAL_ID
            computationStage = Stage.SHUFFLE_PHASE.toProtocolStage()
            attempt = 0
            version = 2
            this.computationDetails = computationDetails
            this.requisitions += requisitions
          }
        )

      assertThat(getComputationRequestHeader())
        .isEqualTo(
          AdvanceComputationRequestKt.header {
            name = "computations/$GLOBAL_ID"
            honestMajorityShareShuffle = systemHonestMajorityShareShuffle {
              description = SystemHonestMajorityShareShuffle.Description.SHUFFLE_PHASE_INPUT_TWO
            }
          }
        )

      val shufflePhaseInput = ShufflePhaseInput.parseFrom(getComputationRequestBodyContent())
      assertThat(shufflePhaseInput)
        .isEqualTo(
          shufflePhaseInput {
            peerRandomSeed = computationDetails.honestMajorityShareShuffle.randomSeed
            secretSeeds += secretSeed {
              requisitionId = fulfilledRequisition2.externalKey.externalRequisitionId
              secretSeedCiphertext = fulfilledRequisition2.secretSeedCiphertext
              registerCount = fulfilledRequisition2.details.honestMajorityShareShuffle.registerCount
              dataProviderCertificate =
                fulfilledRequisition2.details.honestMajorityShareShuffle.dataProviderCertificate
            }
          }
        )
    }

  @Test
  fun `shufflePhase successfully returns results`() = runBlocking {
    val computationDetails = getHmssComputationDetails(RoleInComputation.FIRST_NON_AGGREGATOR)

    val inputBlobPath = ComputationBlobContext(GLOBAL_ID, Stage.SHUFFLE_PHASE.toProtocolStage(), 0L)
    val peerRandomSeed = RANDOM.generateSeed(RANDOM_SEED_LENGTH_IN_BYTES).toByteString()
    val requisitionSeed = randomSeed {
      RANDOM.generateSeed(RANDOM_SEED_LENGTH_IN_BYTES).toByteString()
    }
    val requisitionSignedSeed = signRandomSeed(requisitionSeed, DATA_PROVIDER_SIGNING_KEY)
    val duchyPublicKey =
      computationDetails.honestMajorityShareShuffle.encryptionKeyPair.publicKey
        .toV2AlphaEncryptionPublicKey()
    val requisitionEncryptedSeed = encryptRandomSeed(requisitionSignedSeed, duchyPublicKey)

    val inputBlobData =
      shufflePhaseInput {
          this.peerRandomSeed = peerRandomSeed
          secretSeeds += secretSeed {
            requisitionId = REQUISITION_2.externalKey.externalRequisitionId
            secretSeedCiphertext = requisitionEncryptedSeed.toByteString()
            registerCount = 100
            dataProviderCertificate = "DataProviders/2/Certificates/2"
          }
        }
        .toByteString()
    val inputBlobs =
      listOf(
        computationStageBlobMetadata {
          dependencyType = ComputationBlobDependency.INPUT
          blobId = 0L
          path = inputBlobPath.blobKey
        }
      )
    computationStore.write(inputBlobPath, inputBlobData)

    val requisitionBlobContext1 =
      RequisitionBlobContext(GLOBAL_ID, REQUISITION_1.externalKey.externalRequisitionId)
    val requisitionBlobContext3 =
      RequisitionBlobContext(GLOBAL_ID, REQUISITION_3.externalKey.externalRequisitionId)
    // TODO(@renjiez): Use ShareShuffleSketch from any-sketch-java when it is available..
    val requisitionData1 = shareShuffleSketch { data += listOf(1, 2, 3) }.toByteString()
    val requisitionData3 = shareShuffleSketch { data += listOf(4, 5, 6) }.toByteString()
    requisitionStore.write(requisitionBlobContext1, requisitionData1)
    requisitionStore.write(requisitionBlobContext3, requisitionData3)

    fakeComputationDb.addComputation(
      LOCAL_ID,
      Stage.SHUFFLE_PHASE.toProtocolStage(),
      blobs = inputBlobs,
      computationDetails = computationDetails,
      requisitions = REQUISITIONS,
    )

    var cryptoRequest = CompleteShufflePhaseRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeShufflePhase(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      completeShufflePhaseResponse { combinedSketch += listOf(1, 2, 3) }
    }
    whenever(mockCertificates.getCertificate(any())).thenAnswer {
      certificate {
        name = REQUISITION_2.details.honestMajorityShareShuffle.dataProviderCertificate
        x509Der = DATA_PROVIDER_CERT_DER
        this.subjectKeyIdentifier = DATA_PROVIDER_SIGNING_CERT.subjectKeyIdentifier!!
      }
    }

    val mill = createHmssMill(DUCHY_ONE_NAME)
    mill.pollAndProcessNextComputation()

    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        computationToken {
          globalComputationId = GLOBAL_ID
          localComputationId = LOCAL_ID
          computationStage = Stage.COMPLETE.toProtocolStage()
          attempt = 1
          version = 2
          this.computationDetails =
            computationDetails.copy { endingState = ComputationDetails.CompletedReason.SUCCEEDED }
          requisitions += REQUISITIONS
        }
      )

    assertThat(cryptoRequest)
      .ignoringFields(CompleteShufflePhaseRequest.COMMON_RANDOM_SEED_FIELD_NUMBER)
      .isEqualTo(
        completeShufflePhaseRequest {
          val hmss = computationDetails.honestMajorityShareShuffle
          sketchParams = hmss.parameters.sketchParams
          dpParams = hmss.parameters.dpParams
          noiseMechanism = hmss.parameters.noiseMechanism
          order = CompleteShufflePhaseRequest.NonAggregatorOrder.FIRST

          sketchShares += sketchShare {
            data =
              CompleteShufflePhaseRequestKt.SketchShareKt.shareData { values += listOf(1, 2, 3) }
          }
          sketchShares += sketchShare { seed = requisitionSeed.data }
          sketchShares += sketchShare {
            data =
              CompleteShufflePhaseRequestKt.SketchShareKt.shareData { values += listOf(4, 5, 6) }
          }
        }
      )
  }

  @Test
  fun `shufflePhase throw exception when fail to get data provider certificate`() = runBlocking {
    val computationDetails = getHmssComputationDetails(RoleInComputation.FIRST_NON_AGGREGATOR)

    val inputBlobPath = ComputationBlobContext(GLOBAL_ID, Stage.SHUFFLE_PHASE.toProtocolStage(), 0L)
    val peerRandomSeed = RANDOM.generateSeed(RANDOM_SEED_LENGTH_IN_BYTES).toByteString()
    val requisitionSeed = randomSeed {
      RANDOM.generateSeed(RANDOM_SEED_LENGTH_IN_BYTES).toByteString()
    }
    val requisitionSignedSeed = signRandomSeed(requisitionSeed, DATA_PROVIDER_SIGNING_KEY)
    val duchyPublicKey =
      computationDetails.honestMajorityShareShuffle.encryptionKeyPair.publicKey
        .toV2AlphaEncryptionPublicKey()
    val requisitionEncryptedSeed = encryptRandomSeed(requisitionSignedSeed, duchyPublicKey)

    val inputBlobData =
      shufflePhaseInput {
          this.peerRandomSeed = peerRandomSeed
          secretSeeds += secretSeed {
            requisitionId = REQUISITION_2.externalKey.externalRequisitionId
            secretSeedCiphertext = requisitionEncryptedSeed.toByteString()
            registerCount = 100
            dataProviderCertificate = "DataProviders/2/Certificates/2"
          }
        }
        .toByteString()
    val inputBlobs =
      listOf(
        computationStageBlobMetadata {
          dependencyType = ComputationBlobDependency.INPUT
          blobId = 0L
          path = inputBlobPath.blobKey
        }
      )
    computationStore.write(inputBlobPath, inputBlobData)

    val requisitionBlobContext1 =
      RequisitionBlobContext(GLOBAL_ID, REQUISITION_1.externalKey.externalRequisitionId)
    val requisitionBlobContext3 =
      RequisitionBlobContext(GLOBAL_ID, REQUISITION_3.externalKey.externalRequisitionId)
    // TODO(@renjiez): Use ShareShuffleSketch from any-sketch-java when it is available..
    val requisitionData1 = shareShuffleSketch { data += listOf(1, 2, 3) }.toByteString()
    val requisitionData3 = shareShuffleSketch { data += listOf(4, 5, 6) }.toByteString()
    requisitionStore.write(requisitionBlobContext1, requisitionData1)
    requisitionStore.write(requisitionBlobContext3, requisitionData3)

    fakeComputationDb.addComputation(
      LOCAL_ID,
      Stage.SHUFFLE_PHASE.toProtocolStage(),
      blobs = inputBlobs,
      computationDetails = computationDetails,
      requisitions = REQUISITIONS,
    )

    whenever(mockCertificates.getCertificate(any()))
      .thenThrow(Status.NOT_FOUND.asRuntimeException())

    val mill = createHmssMill(DUCHY_ONE_NAME)
    mill.pollAndProcessNextComputation()

    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        computationToken {
          globalComputationId = GLOBAL_ID
          localComputationId = LOCAL_ID
          computationStage = Stage.COMPLETE.toProtocolStage()
          attempt = 1
          version = 2
          this.computationDetails =
            computationDetails.copy { endingState = ComputationDetails.CompletedReason.FAILED }
          requisitions += REQUISITIONS
        }
      )
  }

  @Test
  fun `aggregationPhase successfully returns results`() = runBlocking {
    val computationDetails = getHmssComputationDetails(RoleInComputation.AGGREGATOR)
    val inputBlobPath1 =
      ComputationBlobContext(GLOBAL_ID, Stage.AGGREGATION_PHASE.toProtocolStage(), 0L)
    val inputBlobData1 = aggregationPhaseInput { combinedSketch += listOf(1, 2, 3) }.toByteString()
    val inputBlobPath2 =
      ComputationBlobContext(GLOBAL_ID, Stage.AGGREGATION_PHASE.toProtocolStage(), 1L)
    val inputBlobData2 = aggregationPhaseInput { combinedSketch += listOf(4, 5, 6) }.toByteString()
    val inputBlobs =
      listOf(
        computationStageBlobMetadata {
          dependencyType = ComputationBlobDependency.INPUT
          blobId = 0L
          path = inputBlobPath1.blobKey
        },
        computationStageBlobMetadata {
          dependencyType = ComputationBlobDependency.INPUT
          blobId = 1L
          path = inputBlobPath2.blobKey
        },
      )
    computationStore.write(inputBlobPath1, inputBlobData1)
    computationStore.write(inputBlobPath2, inputBlobData2)
    fakeComputationDb.addComputation(
      LOCAL_ID,
      Stage.AGGREGATION_PHASE.toProtocolStage(),
      blobs = inputBlobs,
      computationDetails = computationDetails,
      requisitions = REQUISITIONS,
    )

    val expectedReach = 100L
    val expectedFrequency = mapOf(0L to 0.1, 1L to 0.5, 2L to 0.4)
    var cryptoRequest = CompleteAggregationPhaseRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeAggregationPhase(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      completeAggregationPhaseResponse {
        reach = expectedReach
        for (entry in expectedFrequency) {
          frequencyDistribution[entry.key] = entry.value
        }
        elapsedCpuDuration = Duration.ofSeconds(1).toProtoDuration()
      }
    }

    val mill = createHmssMill(DUCHY_THREE_NAME)
    mill.pollAndProcessNextComputation()

    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        computationToken {
          globalComputationId = GLOBAL_ID
          localComputationId = LOCAL_ID
          computationStage = Stage.COMPLETE.toProtocolStage()
          attempt = 1
          version = 2
          this.computationDetails =
            computationDetails.copy { endingState = ComputationDetails.CompletedReason.SUCCEEDED }
          requisitions += REQUISITIONS
        }
      )

    verifyProtoArgument(
        mockSystemComputations,
        SystemComputationsCoroutineImplBase::setComputationResult,
      )
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        setComputationResultRequest {
          name = "computations/$GLOBAL_ID"
          aggregatorCertificate = DUCHY_CERT_NAME
          resultPublicKey = MEASUREMENT_ENCRYPTION_PUBLIC_KEY.toByteString()
        }
      )

    assertThat(cryptoRequest)
      .isEqualTo(
        completeAggregationPhaseRequest {
          val hmss = computationDetails.honestMajorityShareShuffle
          sketchParams = hmss.parameters.sketchParams
          maximumFrequency = hmss.parameters.maximumFrequency
          vidSamplingIntervalWidth = MEASUREMENT_SPEC.vidSamplingInterval.width
          dpParams = hmss.parameters.dpParams
          noiseMechanism = hmss.parameters.noiseMechanism

          sketchShares +=
            CompleteAggregationPhaseRequestKt.shareData { shareVector += listOf(1, 2, 3) }
          sketchShares +=
            CompleteAggregationPhaseRequestKt.shareData { shareVector += listOf(4, 5, 6) }
        }
      )
  }

  fun getComputationRequestHeader(): AdvanceComputationRequest.Header =
    advanceComputationRequests.first().header

  fun getComputationRequestBodyContent(): ByteString {
    return advanceComputationRequests
      .subList(1, advanceComputationRequests.size)
      .map { it.bodyChunk.partialData.toByteArray() }
      .reduce { acc, bytes -> acc + bytes }
      .toByteString()
  }
}
