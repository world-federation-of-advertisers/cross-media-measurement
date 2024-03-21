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

import org.wfanet.measurement.consent.client.dataprovider.verifyEncryptionPublicKey
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import io.opentelemetry.api.GlobalOpenTelemetry
import java.security.SecureRandom
import java.security.cert.X509Certificate
import java.time.Clock
import java.time.Duration
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.UseConstructor
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.common.crypto.PrivateKeyStore
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
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
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.duchy.daemon.mill.Certificate
import org.wfanet.measurement.duchy.daemon.mill.shareshuffle.crypto.HonestMajorityShareShuffleCryptor
import org.wfanet.measurement.duchy.daemon.testing.TestRequisition
import org.wfanet.measurement.duchy.daemon.utils.toDuchyEncryptionPublicKey
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients
import org.wfanet.measurement.duchy.db.computation.testing.FakeComputationsDatabase
import org.wfanet.measurement.duchy.service.internal.computations.ComputationsService
import org.wfanet.measurement.duchy.storage.ComputationStore
import org.wfanet.measurement.duchy.storage.RequisitionBlobContext
import org.wfanet.measurement.duchy.storage.RequisitionStore
import org.wfanet.measurement.duchy.storage.TinkKeyStore
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationDetailsKt
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineImplBase
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.EncryptionPublicKey
import org.wfanet.measurement.internal.duchy.NoiseMechanism
import org.wfanet.measurement.internal.duchy.computationDetails
import org.wfanet.measurement.internal.duchy.config.RoleInComputation
import org.wfanet.measurement.internal.duchy.copy
import org.wfanet.measurement.internal.duchy.differentialPrivacyParams
import org.wfanet.measurement.internal.duchy.encryptionKeyPair
import org.wfanet.measurement.internal.duchy.encryptionPublicKey
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle.Stage
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffleKt
import org.wfanet.measurement.internal.duchy.protocol.shareShuffleSketchParams
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.measurement.system.v1alpha.AdvanceComputationRequest
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
import org.wfanet.measurement.api.v2alpha.SignedMessage
import org.wfanet.measurement.internal.duchy.RequisitionDetailsKt.honestMajorityShareShuffleDetails
import org.wfanet.measurement.internal.duchy.computationToken
import org.wfanet.measurement.system.v1alpha.honestMajorityShareShuffle as systemHonestMajorityShareShuffle
import org.wfanet.measurement.system.v1alpha.HonestMajorityShareShuffle as SystemHonestMajorityShareShuffle
import com.google.protobuf.kotlin.toByteStringUtf8
import org.wfanet.measurement.duchy.service.internal.computations.newOutputBlobMetadata
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffleKt.ShufflePhaseInputKt.secretSeed
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffleKt.shufflePhaseInput
import org.wfanet.measurement.system.v1alpha.AdvanceComputationRequestKt
import org.wfanet.measurement.system.v1alpha.ComputationKey
import org.wfanet.measurement.system.v1alpha.Requisition
import org.wfanet.measurement.system.v1alpha.advanceComputationRequest
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
private val PRIVATE_KEY_DER = TestData.FIXED_SERVER_KEY_DER_FILE.readBytes().toByteString()
private val ROOT_CERT: X509Certificate = readCertificate(TestData.FIXED_CA_CERT_PEM_FILE)
private val MEASUREMENT_ENCRYPTION_PRIVATE_KEY = TinkPrivateKeyHandle.generateEcies()
private val MEASUREMENT_ENCRYPTION_PUBLIC_KEY =
  MEASUREMENT_ENCRYPTION_PRIVATE_KEY.publicKey.toEncryptionPublicKey()
private val DUCHY_SIGNING_CERT = readCertificate(DUCHY_CERT_DER)
private val DUCHY_SIGNING_KEY =
  SigningKeyHandle(
    DUCHY_SIGNING_CERT,
    readPrivateKey(PRIVATE_KEY_DER, DUCHY_SIGNING_CERT.publicKey.algorithm),
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
}
private val SERIALIZED_MEASUREMENT_SPEC: ByteString = MEASUREMENT_SPEC.toByteString()

private val REQUISITION_1 =
  TEST_REQUISITION_1.toRequisitionMetadata(Requisition.State.FULFILLED, DUCHY_ONE_NAME).copy {
    details = details.copy {
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
    details = details.copy {
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
  private val workerStubs = mapOf(DUCHY_TWO_NAME to workerStub, DUCHY_THREE_NAME to workerStub)

  private lateinit var aggregatorMill: HonestMajorityShareShuffleMill
  private lateinit var firstWorkerMill: HonestMajorityShareShuffleMill
  private lateinit var secondWorkerMill: HonestMajorityShareShuffleMill

  @Before
  fun initializeMill() = runBlocking {
    DuchyInfo.setForTest(setOf(DUCHY_ONE_NAME, DUCHY_TWO_NAME, DUCHY_THREE_NAME))

    val csCertificate = Certificate(DUCHY_CERT_NAME, DUCHY_SIGNING_CERT)
    val trustedCertificates =
      DuchyInfo.entries.values.associateBy({ it.rootCertificateSkid }, { ROOT_CERT })

    firstWorkerMill =
      HonestMajorityShareShuffleMill(
        millId = DUCHY_ONE_NAME + MILL_ID_SUFFIX,
        duchyId = DUCHY_ONE_NAME,
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
    secondWorkerMill =
      HonestMajorityShareShuffleMill(
        millId = DUCHY_TWO_NAME + MILL_ID_SUFFIX,
        duchyId = DUCHY_TWO_NAME,
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
    aggregatorMill =
      HonestMajorityShareShuffleMill(
        millId = DUCHY_THREE_NAME + MILL_ID_SUFFIX,
        duchyId = DUCHY_THREE_NAME,
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

  private fun buildAdvanceComputationRequests(
    globalComputationId: String,
    description: SystemHonestMajorityShareShuffle.Description,
    vararg chunkContents: ByteString,
  ): List<AdvanceComputationRequest> {
    val header = advanceComputationRequest {
      header =
        AdvanceComputationRequestKt.header {
          name = ComputationKey(globalComputationId).toName()
          this.honestMajorityShareShuffle = systemHonestMajorityShareShuffle {
            this.description = description
          }
        }
    }
    val body =
      chunkContents.asList().map {
        advanceComputationRequest {
          bodyChunk =
            AdvanceComputationRequestKt.bodyChunk { partialData = it }
        }
      }
    return listOf(header) + body
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

    firstWorkerMill.pollAndProcessNextComputation()

    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(computationToken {
        globalComputationId = GLOBAL_ID
        localComputationId = LOCAL_ID
        computationStage = Stage.WAIT_TO_START.toProtocolStage()
        attempt = 1
        version = 2
        this.computationDetails = computationDetails
        requisitions += REQUISITIONS
        blobs += newOutputBlobMetadata(0, "1")
      })

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
            ComputationParticipantKt.requisitionParams {
              duchyCertificate = DUCHY_CERT_NAME
            }
        }
      )
    val signedEncryptionPublicKey = SignedMessage.parseFrom(request.requisitionParams.honestMajorityShareShuffle.tinkPublicKey)
    verifyEncryptionPublicKey(signedEncryptionPublicKey, DUCHY_SIGNING_CERT, ROOT_CERT)
  }

  @Test
  fun `setupPhase successfully sends seeds to the peer worker`() = runBlocking {
    val computationDetails = getHmssComputationDetails(RoleInComputation.FIRST_NON_AGGREGATOR)
    fakeComputationDb.addComputation(
      LOCAL_ID,
      Stage.SETUP_PHASE.toProtocolStage(),
      computationDetails = computationDetails,
      requisitions = REQUISITIONS,
    )


    firstWorkerMill.pollAndProcessNextComputation()

    val updatedToken = fakeComputationDb[LOCAL_ID]

    assertThat(updatedToken)
      .isEqualTo(computationToken {
        globalComputationId = GLOBAL_ID
        localComputationId = LOCAL_ID
        computationStage = Stage.WAIT_ON_SHUFFLE_INPUT_PHASE_TWO.toProtocolStage()
        attempt = 1
        version = 2
        this.computationDetails = computationDetails
        requisitions += REQUISITIONS
        blobs += newOutputBlobMetadata(0, "")
      })

    val expectedShufflePhaseInput = shufflePhaseInput {
      peerRandomSeed = computationDetails.honestMajorityShareShuffle.randomSeed
      for (requisition in REQUISITIONS) {
        if (requisition.path.isEmpty()) {
          continue
        }
        secretSeeds += secretSeed {
          requisitionId = requisition.externalKey.externalRequisitionId
          secretSeedCiphertext = requisition.secretSeedCiphertext
        }
      }
    }
    assertThat(advanceComputationRequests)
      .containsExactly(expectedShufflePhaseInput.toByteString()).inOrder()
  }

  fun getComputationRequestHeader(): AdvanceComputationRequest.Header =
    advanceComputationRequests.first().header


  fun getComputationRequestBodyContent(): ByteString {

    advanceComputationRequests.concat()

  }

}
