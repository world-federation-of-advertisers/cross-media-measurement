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

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.crypto.tink.Aead
import com.google.crypto.tink.BinaryKeysetWriter
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.StreamingAead
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import java.io.ByteArrayOutputStream
import java.security.GeneralSecurityException
import java.time.Clock
import java.time.Duration
import kotlin.io.path.Path
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.testing.TestData
import org.wfanet.measurement.common.crypto.tink.GCloudWifCredentials
import org.wfanet.measurement.common.crypto.tink.KmsClientFactory
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.DuchyInfo
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients
import org.wfanet.measurement.duchy.db.computation.testing.FakeComputationsDatabase
import org.wfanet.measurement.duchy.mill.Certificate
import org.wfanet.measurement.duchy.mill.MillBase
import org.wfanet.measurement.duchy.mill.trustee.processor.TrusTeeProcessor
import org.wfanet.measurement.duchy.service.internal.computations.ComputationsService
import org.wfanet.measurement.duchy.storage.ComputationStore
import org.wfanet.measurement.duchy.storage.RequisitionBlobContext
import org.wfanet.measurement.duchy.storage.RequisitionStore
import org.wfanet.measurement.duchy.testing.TestRequisition
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.duchy.utils.ReachAndFrequencyResult
import org.wfanet.measurement.duchy.utils.toDuchyEncryptionPublicKey
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationDetailsKt
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineImplBase
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.NoiseMechanism
import org.wfanet.measurement.internal.duchy.RequisitionDetailsKt
import org.wfanet.measurement.internal.duchy.RequisitionDetailsKt.RequisitionProtocolKt.trusTee as requisitionTrusTee
import org.wfanet.measurement.internal.duchy.computationDetails
import org.wfanet.measurement.internal.duchy.computationToken
import org.wfanet.measurement.internal.duchy.config.RoleInComputation
import org.wfanet.measurement.internal.duchy.copy
import org.wfanet.measurement.internal.duchy.differentialPrivacyParams
import org.wfanet.measurement.internal.duchy.protocol.TrusTee.ComputationDetails as TrusTeeDetails
import org.wfanet.measurement.internal.duchy.protocol.TrusTee.Stage
import org.wfanet.measurement.internal.duchy.protocol.TrusTeeKt
import org.wfanet.measurement.measurementconsumer.stats.TrusTeeMethodology
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.ComputationParticipant
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKey
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKt
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub as SystemComputationParticipantsCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt.ComputationsCoroutineImplBase as SystemComputationsCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.Requisition
import org.wfanet.measurement.system.v1alpha.computationParticipant
import org.wfanet.measurement.system.v1alpha.setComputationResultRequest
import org.wfanet.measurement.system.v1alpha.setParticipantRequisitionParamsRequest

@RunWith(JUnit4::class)
class TrusTeeMillTest {
  private val mockProcessor: TrusTeeProcessor = mock()
  private val mockProcessorFactory: TrusTeeProcessor.Factory = mock {
    on { create(any()) }.thenReturn(mockProcessor)
  }

  private val fakeKmsClient = FakeKmsClient()
  private val mockKmsClientFactory: KmsClientFactory<GCloudWifCredentials> = mock {
    on { getKmsClient(any()) }.thenReturn(fakeKmsClient)
  }

  private val mockComputationControl: ComputationControlCoroutineImplBase = mockService()
  private val mockSystemComputations: SystemComputationsCoroutineImplBase = mockService()
  private val mockComputationParticipants: ComputationParticipantsCoroutineImplBase = mockService()
  private val mockComputationLogEntries: ComputationLogEntriesCoroutineImplBase = mockService()
  private val mockComputationStats: ComputationStatsCoroutineImplBase = mockService()

  private val fakeComputationDb = FakeComputationsDatabase()

  private lateinit var computationDataClients: ComputationDataClients
  private lateinit var requisitionStore: RequisitionStore
  private lateinit var computationStore: ComputationStore

  private val tempDirectory = TemporaryFolder()

  private val grpcTestServerRule =
    GrpcTestServerRule(defaultServiceConfig = MillBase.SERVICE_CONFIG) {
      DuchyInfo.setForTest(setOf(DUCHY_ID))

      val storageClient = FileSystemStorageClient(tempDirectory.root)
      requisitionStore = RequisitionStore(storageClient)
      computationStore = ComputationStore(storageClient)
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
          DUCHY_ID,
          clock = Clock.systemUTC(),
        )
      )
    }

  @get:Rule val ruleChain = chainRulesSequentially(tempDirectory, grpcTestServerRule)

  @Before
  fun setUp() {
    // For test only, EDPs share the same fakeKmsClient.
    fakeKmsClient.setAead(KEK_URI_1, KEK_AEAD_1)
    fakeKmsClient.setAead(KEK_URI_2, KEK_AEAD_2)
    fakeKmsClient.setAead(KEK_URI_3, KEK_AEAD_3)
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

  private fun createMill(): TrusTeeMill {
    val csCertificate = Certificate(DUCHY_CERT_NAME, DUCHY_SIGNING_CERT)

    return TrusTeeMill(
      millId = MILL_ID,
      duchyId = DUCHY_ID,
      signingKey = DUCHY_SIGNING_KEY,
      consentSignalCert = csCertificate,
      dataClients = computationDataClients,
      systemComputationParticipantsClient = systemComputationParticipantsStub,
      systemComputationsClient = systemComputationStub,
      systemComputationLogEntriesClient = systemComputationLogEntriesStub,
      computationStatsClient = computationStatsStub,
      workLockDuration = Duration.ofMinutes(5),
      trusTeeProcessorFactory = mockProcessorFactory,
      kmsClientFactory = mockKmsClientFactory,
      attestationTokenPath = ATTESTATION_TOKEN_PATH,
    )
  }

  private fun KeysetHandle.toEncryptedByteString(kekAead: Aead): ByteString {
    val outputStream = ByteArrayOutputStream()
    this.write(BinaryKeysetWriter.withOutputStream(outputStream), kekAead)
    return outputStream.toByteArray().toByteString()
  }

  private fun encryptWithStreamingAead(streamingAead: StreamingAead, data: ByteArray): ByteString {
    val outputStream = ByteArrayOutputStream()
    streamingAead.newEncryptingStream(outputStream, byteArrayOf()).use { encryptingStream ->
      encryptingStream.write(data)
    }
    return outputStream.toByteArray().toByteString()
  }

  private suspend fun writeRequisitionData() {
    val requisitionBlobContext1 =
      RequisitionBlobContext(GLOBAL_ID, REQUISITION_1.externalKey.externalRequisitionId)
    val requisitionBlobContext2 =
      RequisitionBlobContext(GLOBAL_ID, REQUISITION_2.externalKey.externalRequisitionId)
    val requisitionBlobContext3 =
      RequisitionBlobContext(GLOBAL_ID, REQUISITION_3.externalKey.externalRequisitionId)

    val encryptedData1 = encryptWithStreamingAead(DEK_STREAMING_AEAD_1, RAW_DATA_1)
    requisitionStore.write(requisitionBlobContext1, encryptedData1)

    val encryptedData2 = encryptWithStreamingAead(DEK_STREAMING_AEAD_2, RAW_DATA_2)
    requisitionStore.write(requisitionBlobContext2, encryptedData2)

    val encryptedData3 = encryptWithStreamingAead(DEK_STREAMING_AEAD_3, RAW_DATA_3)
    requisitionStore.write(requisitionBlobContext3, encryptedData3)
  }

  @Test
  fun `initialized phase has higher priority to be claimed`() = runBlocking {
    fakeComputationDb.addComputation(
      1L,
      Stage.COMPUTING.toProtocolStage(),
      computationDetails = COMPUTATION_DETAILS,
      requisitions = REQUISITIONS,
    )

    fakeComputationDb.addComputation(
      2L,
      Stage.INITIALIZED.toProtocolStage(),
      computationDetails = COMPUTATION_DETAILS,
      requisitions = REQUISITIONS,
    )
    val mill = createMill()
    mill.claimAndProcessWork()

    assertThat(fakeComputationDb[2]!!.computationStage)
      .isEqualTo(Stage.WAIT_TO_START.toProtocolStage())
    assertThat(fakeComputationDb[1]!!.computationStage).isEqualTo(Stage.COMPUTING.toProtocolStage())
  }

  @Test
  fun `initializationPhase sends params to Kingdom and advance stage`() = runBlocking {
    val computationParticipant = computationParticipant {
      name = ComputationParticipantKey(GLOBAL_ID, DUCHY_ID).toName()
      etag = "entity tag"
      state = ComputationParticipant.State.CREATED
    }
    whenever(mockComputationParticipants.getComputationParticipant(any()))
      .thenReturn(computationParticipant)

    fakeComputationDb.addComputation(
      LOCAL_ID,
      Stage.INITIALIZED.toProtocolStage(),
      computationDetails = COMPUTATION_DETAILS,
      requisitions = REQUISITIONS,
    )

    val mill = createMill()
    mill.claimAndProcessWork()

    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        computationToken {
          globalComputationId = GLOBAL_ID
          localComputationId = LOCAL_ID
          computationStage = Stage.WAIT_TO_START.toProtocolStage()
          attempt = 1
          version = 2
          this.computationDetails = COMPUTATION_DETAILS
          requisitions += REQUISITIONS
        }
      )

    verifyProtoArgument(
        mockComputationParticipants,
        ComputationParticipantsCoroutineImplBase::setParticipantRequisitionParams,
      )
      .isEqualTo(
        setParticipantRequisitionParamsRequest {
          name = computationParticipant.name
          etag = computationParticipant.etag
          requisitionParams =
            ComputationParticipantKt.requisitionParams {
              duchyCertificate = DUCHY_CERT_NAME
              trusTee = ComputationParticipant.RequisitionParams.TrusTee.getDefaultInstance()
            }
        }
      )
  }

  @Test
  fun `initializationPhase skips sending params to Kingdom when already set`(): Unit = runBlocking {
    val computationParticipant = computationParticipant {
      name = ComputationParticipantKey(GLOBAL_ID, DUCHY_ID).toName()
      etag = "entity tag"
      state = ComputationParticipant.State.READY
      requisitionParams = ComputationParticipant.RequisitionParams.getDefaultInstance()
    }
    whenever(mockComputationParticipants.getComputationParticipant(any()))
      .thenReturn(computationParticipant)

    fakeComputationDb.addComputation(
      LOCAL_ID,
      Stage.INITIALIZED.toProtocolStage(),
      computationDetails = COMPUTATION_DETAILS,
      requisitions = REQUISITIONS,
    )

    val mill = createMill()
    mill.claimAndProcessWork()

    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        computationToken {
          globalComputationId = GLOBAL_ID
          localComputationId = LOCAL_ID
          computationStage = Stage.WAIT_TO_START.toProtocolStage()
          attempt = 1
          version = 2
          this.computationDetails = COMPUTATION_DETAILS
          requisitions += REQUISITIONS
        }
      )
    verify(mockComputationParticipants, never()).setParticipantRequisitionParams(any())
  }

  @Test
  fun `computingPhase succeeds`() = runBlocking {
    writeRequisitionData()
    fakeComputationDb.addComputation(
      LOCAL_ID,
      Stage.COMPUTING.toProtocolStage(),
      computationDetails = COMPUTATION_DETAILS,
      requisitions = REQUISITIONS,
    )

    whenever(mockProcessor.addFrequencyVector(any())).thenAnswer {}
    whenever(mockProcessor.computeResult()).thenReturn(MEASUREMENT_RESULT)

    val mill = createMill()
    mill.claimAndProcessWork()

    val finalToken = fakeComputationDb[LOCAL_ID]!!
    assertThat(finalToken.computationStage).isEqualTo(Stage.COMPLETE.toProtocolStage())
    assertThat(finalToken.computationDetails.endingState)
      .isEqualTo(ComputationDetails.CompletedReason.SUCCEEDED)

    val vectorCaptor = argumentCaptor<ByteArray>()
    verify(mockProcessor, times(3)).addFrequencyVector(vectorCaptor.capture())
    verify(mockProcessor, times(1)).computeResult()
    val capturedVectors = vectorCaptor.allValues
    assertThat(capturedVectors).hasSize(3)
    assertThat(capturedVectors[0]).isEqualTo(RAW_DATA_1)
    assertThat(capturedVectors[1]).isEqualTo(RAW_DATA_2)
    assertThat(capturedVectors[2]).isEqualTo(RAW_DATA_3)

    verifyProtoArgument(
        mockSystemComputations,
        SystemComputationsCoroutineImplBase::setComputationResult,
      )
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        setComputationResultRequest {
          name = "computations/${GLOBAL_ID}"
          aggregatorCertificate = DUCHY_CERT_NAME
          resultPublicKey = MEASUREMENT_ENCRYPTION_PUBLIC_KEY.toByteString()
        }
      )
  }

  @Test
  fun `computingPhase fails when requisition data is missing`(): Unit = runBlocking {
    // Data for REQUISITION_2 is deliberately omitted.
    requisitionStore.write(
      RequisitionBlobContext(GLOBAL_ID, REQUISITION_1.externalKey.externalRequisitionId),
      encryptWithStreamingAead(DEK_STREAMING_AEAD_1, RAW_DATA_1),
    )
    requisitionStore.write(
      RequisitionBlobContext(GLOBAL_ID, REQUISITION_3.externalKey.externalRequisitionId),
      encryptWithStreamingAead(DEK_STREAMING_AEAD_3, RAW_DATA_3),
    )

    fakeComputationDb.addComputation(
      LOCAL_ID,
      Stage.COMPUTING.toProtocolStage(),
      computationDetails = COMPUTATION_DETAILS,
      requisitions = REQUISITIONS, // Includes the one with missing data
    )

    val mill = createMill()
    mill.claimAndProcessWork()

    val finalToken = fakeComputationDb[LOCAL_ID]!!
    assertThat(finalToken.computationStage).isEqualTo(Stage.COMPLETE.toProtocolStage())
    assertThat(finalToken.computationDetails.endingState)
      .isEqualTo(ComputationDetails.CompletedReason.FAILED)

    verify(mockProcessor, times(1)).addFrequencyVector(any())
    verify(mockProcessor, never()).computeResult()
    verify(mockSystemComputations, never()).setComputationResult(any())
  }

  @Test
  fun `computingPhase fails when kmsClient creation fails`(): Unit = runBlocking {
    writeRequisitionData()
    fakeComputationDb.addComputation(
      LOCAL_ID,
      Stage.COMPUTING.toProtocolStage(),
      computationDetails = COMPUTATION_DETAILS,
      requisitions = REQUISITIONS,
    )

    whenever(mockKmsClientFactory.getKmsClient(any<GCloudWifCredentials>())).thenAnswer {
      throw GeneralSecurityException("KMS client creation failed for test")
    }

    val mill = createMill()
    mill.claimAndProcessWork()

    val finalToken = fakeComputationDb[LOCAL_ID]!!
    assertThat(finalToken.computationStage).isEqualTo(Stage.COMPLETE.toProtocolStage())
    assertThat(finalToken.computationDetails.endingState)
      .isEqualTo(ComputationDetails.CompletedReason.FAILED)

    verify(mockProcessor, never()).addFrequencyVector(any())
    verify(mockProcessor, never()).computeResult()
    verify(mockSystemComputations, never()).setComputationResult(any())
  }

  @Test
  fun `computingPhase fails when kek not found`(): Unit = runBlocking {
    writeRequisitionData()
    fakeComputationDb.addComputation(
      LOCAL_ID,
      Stage.COMPUTING.toProtocolStage(),
      computationDetails = COMPUTATION_DETAILS,
      requisitions = REQUISITIONS,
    )

    val incompleteKmsClient = FakeKmsClient()
    incompleteKmsClient.setAead(KEK_URI_2, KEK_AEAD_2)
    incompleteKmsClient.setAead(KEK_URI_3, KEK_AEAD_3)
    whenever(mockKmsClientFactory.getKmsClient(any<GCloudWifCredentials>()))
      .thenReturn(incompleteKmsClient)

    val mill = createMill()
    mill.claimAndProcessWork()

    // The attempt fails and the computation is enqueued
    val finalToken = fakeComputationDb[LOCAL_ID]!!
    assertThat(finalToken.computationStage).isEqualTo(Stage.COMPLETE.toProtocolStage())
    assertThat(finalToken.computationDetails.endingState)
      .isEqualTo(ComputationDetails.CompletedReason.FAILED)

    verify(mockProcessor, never()).addFrequencyVector(any())
    verify(mockProcessor, never()).computeResult()
    verify(mockSystemComputations, never()).setComputationResult(any())
  }

  @Test
  fun `computingPhase fails when kek not accessible`(): Unit = runBlocking {
    writeRequisitionData()
    fakeComputationDb.addComputation(
      LOCAL_ID,
      Stage.COMPUTING.toProtocolStage(),
      computationDetails = COMPUTATION_DETAILS,
      requisitions = REQUISITIONS,
    )

    val inaccessibleKmsClient: KmsClient = mock()
    whenever(inaccessibleKmsClient.getAead(KEK_URI_1))
      .thenThrow(GeneralSecurityException("KMS permission denied"))
    whenever(mockKmsClientFactory.getKmsClient(any<GCloudWifCredentials>()))
      .thenReturn(inaccessibleKmsClient)

    val mill = createMill()
    mill.claimAndProcessWork()

    val finalToken = fakeComputationDb[LOCAL_ID]!!
    assertThat(finalToken.computationStage).isEqualTo(Stage.COMPLETE.toProtocolStage())
    assertThat(finalToken.computationDetails.endingState)
      .isEqualTo(ComputationDetails.CompletedReason.FAILED)

    verify(mockProcessor, never()).addFrequencyVector(any())
    verify(mockProcessor, never()).computeResult()
    verify(mockSystemComputations, never()).setComputationResult(any())
  }

  @Test
  fun `computingPhase fails when encrypted_dek decryption fails`(): Unit = runBlocking {
    writeRequisitionData()
    val corruptedRequisition1 =
      REQUISITION_1.copy {
        details =
          details.copy {
            protocol =
              protocol.copy {
                trusTee =
                  trusTee.copy {
                    encryptedDekCiphertext = DEK_KEYSET_HANDLE_1.toEncryptedByteString(KEK_AEAD_2)
                  }
              }
          }
      }

    fakeComputationDb.addComputation(
      LOCAL_ID,
      Stage.COMPUTING.toProtocolStage(),
      computationDetails = COMPUTATION_DETAILS,
      requisitions = listOf(corruptedRequisition1, REQUISITION_2, REQUISITION_3),
    )

    val mill = createMill()
    mill.claimAndProcessWork()

    val finalToken = fakeComputationDb[LOCAL_ID]!!
    assertThat(finalToken.computationStage).isEqualTo(Stage.COMPLETE.toProtocolStage())
    assertThat(finalToken.computationDetails.endingState)
      .isEqualTo(ComputationDetails.CompletedReason.FAILED)

    verify(mockProcessor, never()).addFrequencyVector(any())
    verify(mockProcessor, never()).computeResult()
    verify(mockSystemComputations, never()).setComputationResult(any())
  }

  @Test
  fun `computingPhase fails with transient error from kms`(): Unit = runBlocking {
    writeRequisitionData()
    fakeComputationDb.addComputation(
      LOCAL_ID,
      Stage.COMPUTING.toProtocolStage(),
      computationDetails = COMPUTATION_DETAILS,
      requisitions = REQUISITIONS,
    )

    val transientErrorKmsClient: KmsClient = mock()
    whenever(transientErrorKmsClient.getAead(KEK_URI_1))
      .thenThrow(GeneralSecurityException("KMS is temporarily unavailable"))
    whenever(mockKmsClientFactory.getKmsClient(any<GCloudWifCredentials>()))
      .thenReturn(transientErrorKmsClient)

    val mill = createMill()
    mill.claimAndProcessWork()

    val finalToken = fakeComputationDb[LOCAL_ID]!!
    assertThat(finalToken.computationStage).isEqualTo(Stage.COMPLETE.toProtocolStage())
    assertThat(finalToken.computationDetails.endingState)
      .isEqualTo(ComputationDetails.CompletedReason.FAILED)

    verify(mockProcessor, never()).addFrequencyVector(any())
    verify(mockProcessor, never()).computeResult()
    verify(mockSystemComputations, never()).setComputationResult(any())
  }

  @Test
  fun `computingPhase fails when data decryption fails`(): Unit = runBlocking {
    writeRequisitionData()

    val incorrectlyEncryptedData = encryptWithStreamingAead(DEK_STREAMING_AEAD_2, RAW_DATA_1)
    requisitionStore.write(
      RequisitionBlobContext(GLOBAL_ID, REQUISITION_1.externalKey.externalRequisitionId),
      incorrectlyEncryptedData,
    )

    fakeComputationDb.addComputation(
      LOCAL_ID,
      Stage.COMPUTING.toProtocolStage(),
      computationDetails = COMPUTATION_DETAILS,
      requisitions = REQUISITIONS,
    )

    val mill = createMill()
    mill.claimAndProcessWork()

    val finalToken = fakeComputationDb[LOCAL_ID]!!
    assertThat(finalToken.computationStage).isEqualTo(Stage.COMPLETE.toProtocolStage())
    assertThat(finalToken.computationDetails.endingState)
      .isEqualTo(ComputationDetails.CompletedReason.FAILED)

    verify(mockProcessor, never()).addFrequencyVector(any())
    verify(mockProcessor, never()).computeResult()
    verify(mockSystemComputations, never()).setComputationResult(any())
  }

  @Test
  fun `computingPhase fails when cryptor fails`(): Unit = runBlocking {
    writeRequisitionData()
    fakeComputationDb.addComputation(
      LOCAL_ID,
      Stage.COMPUTING.toProtocolStage(),
      computationDetails = COMPUTATION_DETAILS,
      requisitions = REQUISITIONS,
    )

    whenever(mockProcessor.addFrequencyVector(any())).thenAnswer {}
    whenever(mockProcessor.computeResult())
      .thenThrow(IllegalArgumentException("Test cryptor failure during result computation"))

    val mill = createMill()
    mill.claimAndProcessWork()

    val finalToken = fakeComputationDb[LOCAL_ID]!!
    assertThat(finalToken.computationStage).isEqualTo(Stage.COMPLETE.toProtocolStage())
    assertThat(finalToken.computationDetails.endingState)
      .isEqualTo(ComputationDetails.CompletedReason.FAILED)

    verify(mockProcessor, times(REQUISITIONS.size)).addFrequencyVector(any())
    verify(mockProcessor, times(1)).computeResult()
    verify(mockSystemComputations, never()).setComputationResult(any())
  }

  companion object {
    init {
      AeadConfig.register()
      StreamingAeadConfig.register()
    }

    private const val PUBLIC_API_VERSION = "v2alpha"
    private const val MILL_ID = "test-trustee-mill"
    private const val DUCHY_ID = "aggregator"
    private val ATTESTATION_TOKEN_PATH = Path("attestation/token/path")

    private const val LOCAL_ID = 1234L
    private const val GLOBAL_ID = LOCAL_ID.toString()

    private const val DUCHY_CERT_NAME = "cert 1"
    private val DUCHY_CERT_DER = TestData.FIXED_SERVER_CERT_DER_FILE.readBytes().toByteString()
    private val DUCHY_PRIVATE_KEY_DER =
      TestData.FIXED_SERVER_KEY_DER_FILE.readBytes().toByteString()
    private val MEASUREMENT_ENCRYPTION_PRIVATE_KEY = TinkPrivateKeyHandle.generateEcies()
    private val MEASUREMENT_ENCRYPTION_PUBLIC_KEY =
      MEASUREMENT_ENCRYPTION_PRIVATE_KEY.publicKey.toEncryptionPublicKey()
    private val DUCHY_SIGNING_CERT = readCertificate(DUCHY_CERT_DER)
    private val DUCHY_SIGNING_KEY =
      SigningKeyHandle(
        DUCHY_SIGNING_CERT,
        readPrivateKey(DUCHY_PRIVATE_KEY_DER, DUCHY_SIGNING_CERT.publicKey.algorithm),
      )

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

    private val DEK_KEYSET_HANDLE_1 =
      KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM_HKDF_4KB"))
    private val DEK_KEYSET_HANDLE_2 =
      KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM_HKDF_4KB"))
    private val DEK_KEYSET_HANDLE_3 =
      KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM_HKDF_4KB"))

    private val DEK_STREAMING_AEAD_1: StreamingAead =
      DEK_KEYSET_HANDLE_1.getPrimitive(StreamingAead::class.java)
    private val DEK_STREAMING_AEAD_2: StreamingAead =
      DEK_KEYSET_HANDLE_2.getPrimitive(StreamingAead::class.java)
    private val DEK_STREAMING_AEAD_3: StreamingAead =
      DEK_KEYSET_HANDLE_3.getPrimitive(StreamingAead::class.java)

    private val RAW_DATA_1 = byteArrayOf(1, 0, 1, 0, 1)
    private val RAW_DATA_2 = byteArrayOf(0, 1, 2, 0, 0)
    private val RAW_DATA_3 = byteArrayOf(2, 1, 0, 0, 0)

    private val MEASUREMENT_RESULT =
      ReachAndFrequencyResult(
        reach = 4,
        frequency = mapOf(0L to 0.2, 1L to 0.2, 2L to 0.2, 3L to 0.4, 4L to 0.2, 5L to 0.0),
        methodology = TrusTeeMethodology(5),
      )

    private const val KEK_URI_1 = "fake-kms://kek_uri_1"
    private const val KEK_URI_2 = "fake-kms://kek_uri_2"
    private const val KEK_URI_3 = "fake-kms://kek_uri_3"
    private val KEK_KEYSET_HANDLE_1 = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
    private val KEK_KEYSET_HANDLE_2 = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
    private val KEK_KEYSET_HANDLE_3 = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
    private val KEK_AEAD_1: Aead = KEK_KEYSET_HANDLE_1.getPrimitive(Aead::class.java)
    private val KEK_AEAD_2: Aead = KEK_KEYSET_HANDLE_2.getPrimitive(Aead::class.java)
    private val KEK_AEAD_3: Aead = KEK_KEYSET_HANDLE_3.getPrimitive(Aead::class.java)

    /** Encrypt a dek KeysetHandle and convert into bytes. */
    private fun KeysetHandle.toEncryptedByteString(kekAead: Aead): ByteString {
      val outputStream = ByteArrayOutputStream()
      this.write(BinaryKeysetWriter.withOutputStream(outputStream), kekAead)
      return outputStream.toByteArray().toByteString()
    }

    private val REQUISITION_1 =
      TEST_REQUISITION_1.toRequisitionMetadata(Requisition.State.FULFILLED).copy {
        details =
          details.copy {
            protocol =
              RequisitionDetailsKt.requisitionProtocol {
                trusTee = requisitionTrusTee {
                  encryptedDekCiphertext = DEK_KEYSET_HANDLE_1.toEncryptedByteString(KEK_AEAD_1)
                  kmsKekUri = KEK_URI_1
                  workloadIdentityProvider = "WIP_1"
                  impersonatedServiceAccount = "SA_1"
                }
              }
          }
        path = RequisitionBlobContext(GLOBAL_ID, externalKey.externalRequisitionId).blobKey
      }

    private val REQUISITION_2 =
      TEST_REQUISITION_2.toRequisitionMetadata(Requisition.State.FULFILLED).copy {
        details =
          details.copy {
            protocol =
              RequisitionDetailsKt.requisitionProtocol {
                trusTee = requisitionTrusTee {
                  encryptedDekCiphertext = DEK_KEYSET_HANDLE_2.toEncryptedByteString(KEK_AEAD_2)
                  kmsKekUri = KEK_URI_2
                  workloadIdentityProvider = "WIP_2"
                  impersonatedServiceAccount = "SA_2"
                }
              }
          }
        path = RequisitionBlobContext(GLOBAL_ID, externalKey.externalRequisitionId).blobKey
      }

    private val REQUISITION_3 =
      TEST_REQUISITION_3.toRequisitionMetadata(Requisition.State.FULFILLED).copy {
        details =
          details.copy {
            protocol =
              RequisitionDetailsKt.requisitionProtocol {
                trusTee = requisitionTrusTee {
                  encryptedDekCiphertext = DEK_KEYSET_HANDLE_3.toEncryptedByteString(KEK_AEAD_3)
                  kmsKekUri = KEK_URI_3
                  workloadIdentityProvider = "WIP_3"
                  impersonatedServiceAccount = "SA_3"
                }
              }
          }
        path = RequisitionBlobContext(GLOBAL_ID, externalKey.externalRequisitionId).blobKey
      }

    private val REQUISITIONS = listOf(REQUISITION_1, REQUISITION_2, REQUISITION_3)

    private val TRUSTEE_PARAMETERS =
      TrusTeeKt.ComputationDetailsKt.parameters {
        maximumFrequency = 5
        reachDpParams = differentialPrivacyParams {
          epsilon = 1.1
          delta = 0.1
        }
        frequencyDpParams = differentialPrivacyParams {
          epsilon = 2.1
          delta = 0.1
        }
        noiseMechanism = NoiseMechanism.CONTINUOUS_GAUSSIAN
      }

    private val COMPUTATION_DETAILS = computationDetails {
      kingdomComputation =
        ComputationDetailsKt.kingdomComputationDetails {
          publicApiVersion = PUBLIC_API_VERSION
          measurementPublicKey = MEASUREMENT_ENCRYPTION_PUBLIC_KEY.toDuchyEncryptionPublicKey()
          measurementSpec = SERIALIZED_MEASUREMENT_SPEC
          participantCount = 1
        }
      trusTee =
        TrusTeeKt.computationDetails {
          role = RoleInComputation.AGGREGATOR
          type = TrusTeeDetails.Type.REACH_AND_FREQUENCY
          parameters = TRUSTEE_PARAMETERS
        }
    }
  }
}
