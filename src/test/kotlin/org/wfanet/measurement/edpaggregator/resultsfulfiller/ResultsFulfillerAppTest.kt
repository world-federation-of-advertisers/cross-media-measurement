/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.TinkProtoKeysetFormat
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.Any
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import com.google.protobuf.TypeRegistry
import com.google.type.interval
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Clock
import java.time.Duration
import java.time.LocalDate
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.logging.Logger
import kotlin.random.Random
import kotlin.test.assertFails
import kotlin.test.assertTrue
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.stub
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.FulfillDirectRequisitionRequest
import org.wfanet.measurement.api.v2alpha.GetEventGroupRequest
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.events
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionResponse
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.testing.verifyAndCapture
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.decryptResult
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionsValidator
import org.wfanet.measurement.edpaggregator.requisitionfetcher.SingleRequisitionGrouper
import org.wfanet.measurement.edpaggregator.resultsfulfiller.testing.TestRequisitionStubFactory
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParamsKt.storageParams
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.resultsFulfillerParams
import org.wfanet.measurement.gcloud.pubsub.Publisher
import org.wfanet.measurement.gcloud.pubsub.Subscriber
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorProvider
import org.wfanet.measurement.integration.common.loadEncryptionPrivateKey
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineImplBase
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItemAttempt
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient

class ResultsFulfillerAppTest {
  private lateinit var emulatorClient: GooglePubSubEmulatorClient

  private val workItemsServiceMock = mockService<WorkItemsCoroutineImplBase>()
  private val workItemAttemptsServiceMock = mockService<WorkItemAttemptsCoroutineImplBase>()
  private val requisitionsServiceMock: RequisitionsCoroutineImplBase = mockService {
    onBlocking { fulfillDirectRequisition(any()) }.thenReturn(fulfillDirectRequisitionResponse {})
  }
  private val eventGroupsServiceMock: EventGroupsCoroutineImplBase by lazy {
    mockService {
      onBlocking { getEventGroup(any()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<GetEventGroupRequest>(0)
          eventGroup {
            name = request.name
            eventGroupReferenceId = "some-event-group-reference-id"
          }
        }
    }
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(workItemsServiceMock)
    addService(workItemAttemptsServiceMock)
    addService(requisitionsServiceMock)
    addService(eventGroupsServiceMock)
  }

  private val requisitionsStub: RequisitionsCoroutineStub by lazy {
    RequisitionsCoroutineStub(grpcTestServerRule.channel)
  }
  private val eventGroupsStub: EventGroupsCoroutineStub by lazy {
    EventGroupsCoroutineStub(grpcTestServerRule.channel)
  }

  private val throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofSeconds(1L))

  @Before
  fun setupPubSubResources() {
    runBlocking {
      emulatorClient =
        GooglePubSubEmulatorClient(
          host = pubSubEmulatorProvider.host,
          port = pubSubEmulatorProvider.port,
        )
      emulatorClient.createTopic(PROJECT_ID, TOPIC_ID)
      emulatorClient.createSubscription(PROJECT_ID, SUBSCRIPTION_ID, TOPIC_ID)
    }
  }

  @After
  fun cleanPubSubResources() {
    runBlocking {
      emulatorClient.deleteTopic(PROJECT_ID, TOPIC_ID)
      emulatorClient.deleteSubscription(PROJECT_ID, SUBSCRIPTION_ID)
    }
  }

  @Test
  fun `runWork processes requisition successfully`() = runBlocking {
    val pubSubClient = Subscriber(projectId = PROJECT_ID, googlePubSubClient = emulatorClient)
    val publisher = Publisher<WorkItem>(PROJECT_ID, emulatorClient)
    val workItemsStub = WorkItemsCoroutineStub(grpcTestServerRule.channel)
    val workItemAttemptsStub = WorkItemAttemptsCoroutineStub(grpcTestServerRule.channel)

    val testWorkItemAttempt = workItemAttempt {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
    }
    val workItemParams = createWorkItemParams(ResultsFulfillerParams.NoiseParams.NoiseType.NONE)
    val workItem = createWorkItem(workItemParams)
    workItemAttemptsServiceMock.stub {
      onBlocking { createWorkItemAttempt(any()) } doReturn testWorkItemAttempt
      onBlocking { completeWorkItemAttempt(any()) } doReturn testWorkItemAttempt
      onBlocking { failWorkItemAttempt(any()) } doReturn testWorkItemAttempt
    }
    workItemsServiceMock.stub { onBlocking { failWorkItem(any()) } doReturn workItem }

    val tmpPath = Files.createTempDirectory(null).toFile()

    // Create requisitions storage client
    Files.createDirectories(tmpPath.resolve(REQUISITIONS_BUCKET).toPath())
    val requisitionsStorageClient = SelectedStorageClient(REQUISITIONS_FILE_URI, tmpPath)

    val requisitionValidator =
      RequisitionsValidator(
        fatalRequisitionErrorPredicate =
          fun(requisition: Requisition, refusal: Requisition.Refusal) {},
        privateEncryptionKey = PRIVATE_ENCRYPTION_KEY,
      )
    val groupedRequisitions =
      SingleRequisitionGrouper(
          requisitionsClient = requisitionsStub,
          eventGroupsClient = eventGroupsStub,
          requisitionValidator = requisitionValidator,
          throttler = throttler,
        )
        .groupRequisitions(listOf(REQUISITION))
    // Add requisitions to storage
    requisitionsStorageClient.writeBlob(
      REQUISITIONS_BLOB_KEY,
      Any.pack(groupedRequisitions.single()).toByteString(),
    )

    // Create impressions storage client
    Files.createDirectories(tmpPath.resolve(IMPRESSIONS_BUCKET).toPath())
    val impressionsStorageClient = SelectedStorageClient(IMPRESSIONS_FILE_URI, tmpPath)

    // Set up KMS
    val kmsClient = FakeKmsClient()
    val kekUri = FakeKmsClient.KEY_URI_PREFIX + "kek"
    val kmsKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
    kmsClient.setAead(kekUri, kmsKeyHandle.getPrimitive(Aead::class.java))

    // Set up streaming encryption
    val tinkKeyTemplateType = "AES128_GCM_HKDF_1MB"
    val aeadKeyTemplate = KeyTemplates.get(tinkKeyTemplateType)
    val keyEncryptionHandle = KeysetHandle.generateNew(aeadKeyTemplate)
    val serializedEncryptionKey =
      ByteString.copyFrom(
        TinkProtoKeysetFormat.serializeEncryptedKeyset(
          keyEncryptionHandle,
          kmsClient.getAead(kekUri),
          byteArrayOf(),
        )
      )
    val aeadStorageClient =
      impressionsStorageClient.withEnvelopeEncryption(kmsClient, kekUri, serializedEncryptionKey)

    // Wrap aead client in mesos client
    val mesosRecordIoStorageClient = MesosRecordIoStorageClient(aeadStorageClient)

    val impressions =
      List(100) {
        LABELED_IMPRESSION.copy {
          vid = (it % 80).toLong()
          eventTime = FIRST_EVENT_DATE.atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
        }
      }

    val impressionsFlow = flow {
      impressions.forEach { impression -> emit(impression.toByteString()) }
    }

    // Write impressions to storage
    mesosRecordIoStorageClient.writeBlob(IMPRESSIONS_BLOB_KEY, impressionsFlow)

    // Create the impressions metadata store
    Files.createDirectories(tmpPath.resolve(IMPRESSIONS_METADATA_BUCKET).toPath())
    val impressionsMetadataStorageClient =
      SelectedStorageClient(IMPRESSIONS_METADATA_FILE_URI, tmpPath)

    val encryptedDek =
      EncryptedDek.newBuilder().setKekUri(kekUri).setEncryptedDek(serializedEncryptionKey).build()
    val blobDetails = blobDetails {
      this.blobUri = IMPRESSIONS_FILE_URI
      this.encryptedDek = encryptedDek
    }
    logger.info("Writing Blob $IMPRESSION_METADATA_BLOB_KEY")
    impressionsMetadataStorageClient.writeBlob(
      IMPRESSION_METADATA_BLOB_KEY,
      blobDetails.toByteString(),
    )
    val typeRegistry = TypeRegistry.newBuilder().add(TestEvent.getDescriptor()).build()

    val app =
      ResultsFulfillerApp(
        subscriptionId = SUBSCRIPTION_ID,
        queueSubscriber = pubSubClient,
        parser = WorkItem.parser(),
        workItemsStub,
        workItemAttemptsStub,
        TestRequisitionStubFactory(grpcTestServerRule.channel, EDP_NAME),
        kmsClient,
        typeRegistry,
        getStorageConfig(tmpPath),
        getStorageConfig(tmpPath),
        getStorageConfig(tmpPath),
      )
    app.runWork(Any.pack(workItemParams))

    verifyBlocking(requisitionsServiceMock, times(1)) { fulfillDirectRequisition(any()) }
    val request: FulfillDirectRequisitionRequest =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::fulfillDirectRequisition,
      )
    val result: Measurement.Result = decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()

    assertThat(result.reach.noiseMechanism).isEqualTo(ProtocolConfig.NoiseMechanism.NONE)
    assertTrue(result.reach.hasDeterministicCountDistinct())
    assertThat(result.frequency.noiseMechanism).isEqualTo(ProtocolConfig.NoiseMechanism.NONE)
    assertTrue(result.frequency.hasDeterministicDistribution())

    assertThat(result.reach.value).isEqualTo(80)

    assertThat(result.frequency.relativeFrequencyDistribution)
      .isEqualTo(mapOf(1L to 0.75, 2L to 0.25))
  }

  @Test
  fun `runWork correctly selects continuous gaussian noise`() = runBlocking {
    val pubSubClient = Subscriber(projectId = PROJECT_ID, googlePubSubClient = emulatorClient)
    val publisher = Publisher<WorkItem>(PROJECT_ID, emulatorClient)
    val workItemsStub = WorkItemsCoroutineStub(grpcTestServerRule.channel)
    val workItemAttemptsStub = WorkItemAttemptsCoroutineStub(grpcTestServerRule.channel)

    val testWorkItemAttempt = workItemAttempt {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
    }
    val workItemParams =
      createWorkItemParams(ResultsFulfillerParams.NoiseParams.NoiseType.CONTINUOUS_GAUSSIAN)
    val workItem = createWorkItem(workItemParams)
    workItemAttemptsServiceMock.stub {
      onBlocking { createWorkItemAttempt(any()) } doReturn testWorkItemAttempt
      onBlocking { completeWorkItemAttempt(any()) } doReturn testWorkItemAttempt
      onBlocking { failWorkItemAttempt(any()) } doReturn testWorkItemAttempt
    }
    workItemsServiceMock.stub { onBlocking { failWorkItem(any()) } doReturn workItem }

    val tmpPath = Files.createTempDirectory(null).toFile()

    // Create requisitions storage client
    Files.createDirectories(tmpPath.resolve(REQUISITIONS_BUCKET).toPath())
    val requisitionsStorageClient = SelectedStorageClient(REQUISITIONS_FILE_URI, tmpPath)

    val requisitionValidator =
      RequisitionsValidator(
        fatalRequisitionErrorPredicate =
          fun(requisition: Requisition, refusal: Requisition.Refusal) {},
        privateEncryptionKey = PRIVATE_ENCRYPTION_KEY,
      )
    val requisition2 =
      REQUISITION.copy {
        protocolConfig = protocolConfig {
          protocols +=
            ProtocolConfigKt.protocol {
              direct =
                ProtocolConfigKt.direct {
                  noiseMechanisms += NOISE_MECHANISM
                  deterministicCountDistinct =
                    ProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
                  deterministicDistribution =
                    ProtocolConfig.Direct.DeterministicDistribution.getDefaultInstance()
                }
            }
        }
      }
    val groupedRequisitions =
      SingleRequisitionGrouper(
          requisitionsClient = requisitionsStub,
          eventGroupsClient = eventGroupsStub,
          requisitionValidator = requisitionValidator,
          throttler = throttler,
        )
        .groupRequisitions(listOf(requisition2))
    // Add requisitions to storage
    requisitionsStorageClient.writeBlob(
      REQUISITIONS_BLOB_KEY,
      Any.pack(groupedRequisitions.single()).toByteString(),
    )

    // Create impressions storage client
    Files.createDirectories(tmpPath.resolve(IMPRESSIONS_BUCKET).toPath())
    val impressionsStorageClient = SelectedStorageClient(IMPRESSIONS_FILE_URI, tmpPath)

    // Set up KMS
    val kmsClient = FakeKmsClient()
    val kekUri = FakeKmsClient.KEY_URI_PREFIX + "kek"
    val kmsKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
    kmsClient.setAead(kekUri, kmsKeyHandle.getPrimitive(Aead::class.java))

    // Set up streaming encryption
    val tinkKeyTemplateType = "AES128_GCM_HKDF_1MB"
    val aeadKeyTemplate = KeyTemplates.get(tinkKeyTemplateType)
    val keyEncryptionHandle = KeysetHandle.generateNew(aeadKeyTemplate)
    val serializedEncryptionKey =
      ByteString.copyFrom(
        TinkProtoKeysetFormat.serializeEncryptedKeyset(
          keyEncryptionHandle,
          kmsClient.getAead(kekUri),
          byteArrayOf(),
        )
      )
    val aeadStorageClient =
      impressionsStorageClient.withEnvelopeEncryption(kmsClient, kekUri, serializedEncryptionKey)

    // Wrap aead client in mesos client
    val mesosRecordIoStorageClient = MesosRecordIoStorageClient(aeadStorageClient)

    val impressions =
      List(100) {
        LABELED_IMPRESSION.copy {
          vid = (it % 80).toLong()
          eventTime = FIRST_EVENT_DATE.atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
        }
      }

    val impressionsFlow = flow {
      impressions.forEach { impression -> emit(impression.toByteString()) }
    }

    // Write impressions to storage
    mesosRecordIoStorageClient.writeBlob(IMPRESSIONS_BLOB_KEY, impressionsFlow)

    // Create the impressions metadata store
    Files.createDirectories(tmpPath.resolve(IMPRESSIONS_METADATA_BUCKET).toPath())
    val impressionsMetadataStorageClient =
      SelectedStorageClient(IMPRESSIONS_METADATA_FILE_URI, tmpPath)

    val encryptedDek =
      EncryptedDek.newBuilder().setKekUri(kekUri).setEncryptedDek(serializedEncryptionKey).build()
    val blobDetails = blobDetails {
      this.blobUri = IMPRESSIONS_FILE_URI
      this.encryptedDek = encryptedDek
    }
    logger.info("Writing Blob $IMPRESSION_METADATA_BLOB_KEY")
    impressionsMetadataStorageClient.writeBlob(
      IMPRESSION_METADATA_BLOB_KEY,
      blobDetails.toByteString(),
    )
    val typeRegistry = TypeRegistry.newBuilder().add(TestEvent.getDescriptor()).build()

    val app =
      ResultsFulfillerApp(
        subscriptionId = SUBSCRIPTION_ID,
        queueSubscriber = pubSubClient,
        parser = WorkItem.parser(),
        workItemsStub,
        workItemAttemptsStub,
        TestRequisitionStubFactory(grpcTestServerRule.channel, EDP_NAME),
        kmsClient,
        typeRegistry,
        getStorageConfig(tmpPath),
        getStorageConfig(tmpPath),
        getStorageConfig(tmpPath),
      )
    app.runWork(Any.pack(workItemParams))

    verifyBlocking(requisitionsServiceMock, times(1)) { fulfillDirectRequisition(any()) }
    val request: FulfillDirectRequisitionRequest =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::fulfillDirectRequisition,
      )
    val result: Measurement.Result = decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()

    assertThat(result.reach.noiseMechanism)
      .isEqualTo(ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN)
  }

  @Test
  fun `runWork throws exception if noise is not selected`() = runBlocking {
    val pubSubClient = Subscriber(projectId = PROJECT_ID, googlePubSubClient = emulatorClient)
    val publisher = Publisher<WorkItem>(PROJECT_ID, emulatorClient)
    val workItemsStub = WorkItemsCoroutineStub(grpcTestServerRule.channel)
    val workItemAttemptsStub = WorkItemAttemptsCoroutineStub(grpcTestServerRule.channel)

    val testWorkItemAttempt = workItemAttempt {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
    }
    val workItemParams =
      createWorkItemParams(ResultsFulfillerParams.NoiseParams.NoiseType.UNSPECIFIED)
    val workItem = createWorkItem(workItemParams)
    workItemAttemptsServiceMock.stub {
      onBlocking { createWorkItemAttempt(any()) } doReturn testWorkItemAttempt
      onBlocking { completeWorkItemAttempt(any()) } doReturn testWorkItemAttempt
      onBlocking { failWorkItemAttempt(any()) } doReturn testWorkItemAttempt
    }
    workItemsServiceMock.stub { onBlocking { failWorkItem(any()) } doReturn workItem }

    val tmpPath = Files.createTempDirectory(null).toFile()

    // Create requisitions storage client
    Files.createDirectories(tmpPath.resolve(REQUISITIONS_BUCKET).toPath())
    val requisitionsStorageClient = SelectedStorageClient(REQUISITIONS_FILE_URI, tmpPath)

    val requisitionValidator =
      RequisitionsValidator(
        fatalRequisitionErrorPredicate =
          fun(requisition: Requisition, refusal: Requisition.Refusal) {},
        privateEncryptionKey = PRIVATE_ENCRYPTION_KEY,
      )
    val requisition2 =
      REQUISITION.copy {
        protocolConfig = protocolConfig {
          protocols +=
            ProtocolConfigKt.protocol {
              direct =
                ProtocolConfigKt.direct {
                  noiseMechanisms += NOISE_MECHANISM
                  deterministicCountDistinct =
                    ProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
                  deterministicDistribution =
                    ProtocolConfig.Direct.DeterministicDistribution.getDefaultInstance()
                }
            }
        }
      }
    val groupedRequisitions =
      SingleRequisitionGrouper(
          requisitionsClient = requisitionsStub,
          eventGroupsClient = eventGroupsStub,
          requisitionValidator = requisitionValidator,
          throttler = throttler,
        )
        .groupRequisitions(listOf(requisition2))
    // Add requisitions to storage
    requisitionsStorageClient.writeBlob(
      REQUISITIONS_BLOB_KEY,
      Any.pack(groupedRequisitions.single()).toByteString(),
    )

    // Create impressions storage client
    Files.createDirectories(tmpPath.resolve(IMPRESSIONS_BUCKET).toPath())
    val impressionsStorageClient = SelectedStorageClient(IMPRESSIONS_FILE_URI, tmpPath)

    // Set up KMS
    val kmsClient = FakeKmsClient()
    val kekUri = FakeKmsClient.KEY_URI_PREFIX + "kek"
    val kmsKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
    kmsClient.setAead(kekUri, kmsKeyHandle.getPrimitive(Aead::class.java))

    // Set up streaming encryption
    val tinkKeyTemplateType = "AES128_GCM_HKDF_1MB"
    val aeadKeyTemplate = KeyTemplates.get(tinkKeyTemplateType)
    val keyEncryptionHandle = KeysetHandle.generateNew(aeadKeyTemplate)
    val serializedEncryptionKey =
      ByteString.copyFrom(
        TinkProtoKeysetFormat.serializeEncryptedKeyset(
          keyEncryptionHandle,
          kmsClient.getAead(kekUri),
          byteArrayOf(),
        )
      )
    val aeadStorageClient =
      impressionsStorageClient.withEnvelopeEncryption(kmsClient, kekUri, serializedEncryptionKey)

    // Wrap aead client in mesos client
    val mesosRecordIoStorageClient = MesosRecordIoStorageClient(aeadStorageClient)

    val impressions =
      List(100) {
        LABELED_IMPRESSION.copy {
          vid = (it % 80).toLong()
          eventTime = FIRST_EVENT_DATE.atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
        }
      }

    val impressionsFlow = flow {
      impressions.forEach { impression -> emit(impression.toByteString()) }
    }

    // Write impressions to storage
    mesosRecordIoStorageClient.writeBlob(IMPRESSIONS_BLOB_KEY, impressionsFlow)

    // Create the impressions metadata store
    Files.createDirectories(tmpPath.resolve(IMPRESSIONS_METADATA_BUCKET).toPath())
    val impressionsMetadataStorageClient =
      SelectedStorageClient(IMPRESSIONS_METADATA_FILE_URI, tmpPath)

    val encryptedDek =
      EncryptedDek.newBuilder().setKekUri(kekUri).setEncryptedDek(serializedEncryptionKey).build()
    val blobDetails = blobDetails {
      this.blobUri = IMPRESSIONS_FILE_URI
      this.encryptedDek = encryptedDek
    }
    logger.info("Writing Blob $IMPRESSION_METADATA_BLOB_KEY")
    impressionsMetadataStorageClient.writeBlob(
      IMPRESSION_METADATA_BLOB_KEY,
      blobDetails.toByteString(),
    )
    val typeRegistry = TypeRegistry.newBuilder().add(TestEvent.getDescriptor()).build()

    val app =
      ResultsFulfillerApp(
        subscriptionId = SUBSCRIPTION_ID,
        queueSubscriber = pubSubClient,
        parser = WorkItem.parser(),
        workItemsStub,
        workItemAttemptsStub,
        TestRequisitionStubFactory(grpcTestServerRule.channel, EDP_NAME),
        kmsClient,
        typeRegistry,
        getStorageConfig(tmpPath),
        getStorageConfig(tmpPath),
        getStorageConfig(tmpPath),
      )
    assertFails { app.runWork(Any.pack(workItemParams)) }

    verifyBlocking(requisitionsServiceMock, times(0)) { fulfillDirectRequisition(any()) }
  }

  private fun getStorageConfig(
    tmpPath: File
  ): (ResultsFulfillerParams.StorageParams) -> StorageConfig {
    return { _: ResultsFulfillerParams.StorageParams -> StorageConfig(rootDirectory = tmpPath) }
  }

  private fun createWorkItemParams(
    noiseType: ResultsFulfillerParams.NoiseParams.NoiseType
  ): WorkItemParams {
    return workItemParams {
      appParams =
        resultsFulfillerParams {
            dataProvider = EDP_NAME
            this.storageParams =
              ResultsFulfillerParamsKt.storageParams {
                labeledImpressionsBlobDetailsUriPrefix = IMPRESSIONS_METADATA_FILE_URI_PREFIX
              }
            this.cmmsConnection =
              ResultsFulfillerParamsKt.transportLayerSecurityParams {
                clientCertResourcePath = SECRET_FILES_PATH.resolve("edp1_tls.pem").toString()
                clientPrivateKeyResourcePath = SECRET_FILES_PATH.resolve("edp1_tls.key").toString()
              }
            this.consentParams =
              ResultsFulfillerParamsKt.consentParams {
                resultCsCertDerResourcePath =
                  SECRET_FILES_PATH.resolve("edp1_result_cs_cert.der").toString()
                resultCsPrivateKeyDerResourcePath =
                  SECRET_FILES_PATH.resolve("edp1_result_cs_private.der").toString()
                privateEncryptionKeyResourcePath =
                  SECRET_FILES_PATH.resolve("edp1_enc_private.tink").toString()
                edpCertificateName = DATA_PROVIDER_CERTIFICATE_KEY.toName()
              }
            this.noiseParams = ResultsFulfillerParamsKt.noiseParams { this.noiseType = noiseType }
          }
          .pack()

      dataPathParams =
        WorkItemKt.WorkItemParamsKt.dataPathParams { dataPath = REQUISITIONS_FILE_URI }
    }
  }

  private fun createWorkItem(workItemParams: WorkItemParams): WorkItem {
    val packedWorkItemParams = Any.pack(workItemParams)
    return workItem {
      name = "workItems/workItem"
      this.workItemParams = packedWorkItemParams
    }
  }

  init {
    AeadConfig.register()
    StreamingAeadConfig.register()
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val PROJECT_ID = "test-project"
    private const val SUBSCRIPTION_ID = "test-subscription"
    private const val TOPIC_ID = "test-topic"

    @get:ClassRule @JvmStatic val pubSubEmulatorProvider = GooglePubSubEmulatorProvider()

    private val LAST_EVENT_DATE = LocalDate.now()
    private val FIRST_EVENT_DATE = LAST_EVENT_DATE
    private const val REACH_TOLERANCE = 6.0
    private const val FREQUENCY_DISTRIBUTION_TOLERANCE = 1.0

    private val PERSON = person {
      ageGroup = Person.AgeGroup.YEARS_18_TO_34
      gender = Person.Gender.MALE
      socialGradeGroup = Person.SocialGradeGroup.A_B_C1
    }

    private val TEST_EVENT = testEvent { person = PERSON }

    private val LABELED_IMPRESSION =
      LabeledImpression.newBuilder()
        .setEventTime(Timestamp.getDefaultInstance())
        .setVid(10L)
        .setEvent(TEST_EVENT.pack())
        .build()

    private const val EDP_ID = "someDataProvider"
    private const val EDP_NAME = "dataProviders/$EDP_ID"
    private const val EVENT_GROUP_NAME = "$EDP_NAME/eventGroups/name"
    private const val EDP_DISPLAY_NAME = "edp1"
    private val PRIVATE_ENCRYPTION_KEY =
      loadEncryptionPrivateKey("${EDP_DISPLAY_NAME}_enc_private.tink")
    private val SECRET_FILES_PATH: Path =
      checkNotNull(
        getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )
      )
    private const val MEASUREMENT_CONSUMER_ID = "mc"
    private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
    private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"
    private const val REQUISITION_NAME = "$DATA_PROVIDER_NAME/requisitions/foo"
    private val MC_SIGNING_KEY =
      loadSigningKey(
        "${MEASUREMENT_CONSUMER_ID}_cs_cert.der",
        "${MEASUREMENT_CONSUMER_ID}_cs_private.der",
      )
    private val DATA_PROVIDER_PUBLIC_KEY: EncryptionPublicKey =
      loadPublicKey(SECRET_FILES_PATH.resolve("${EDP_DISPLAY_NAME}_enc_public.tink").toFile())
        .toEncryptionPublicKey()
    private val MC_PUBLIC_KEY: EncryptionPublicKey =
      loadPublicKey(SECRET_FILES_PATH.resolve("mc_enc_public.tink").toFile())
        .toEncryptionPublicKey()
    private val MC_PRIVATE_KEY: TinkPrivateKeyHandle =
      loadPrivateKey(SECRET_FILES_PATH.resolve("mc_enc_private.tink").toFile())
    private val REQUISITION_SPEC = requisitionSpec {
      events = events {
        eventGroups += eventGroupEntry {
          key = EVENT_GROUP_NAME
          value =
            RequisitionSpecKt.EventGroupEntryKt.value {
              collectionInterval = interval {
                startTime = FIRST_EVENT_DATE.atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
                endTime =
                  LAST_EVENT_DATE.plusDays(1)
                    .atStartOfDay()
                    .minusSeconds(1)
                    .toInstant(ZoneOffset.UTC)
                    .toProtoTime()
              }
              filter = eventFilter { expression = "person.gender==1" }
            }
        }
      }
      measurementPublicKey = MC_PUBLIC_KEY.pack()
      nonce = Random.Default.nextLong()
    }
    private val ENCRYPTED_REQUISITION_SPEC =
      encryptRequisitionSpec(
        signRequisitionSpec(REQUISITION_SPEC, MC_SIGNING_KEY),
        DATA_PROVIDER_PUBLIC_KEY,
      )
    private val OUTPUT_DP_PARAMS = differentialPrivacyParams {
      epsilon = 1.0
      delta = 1E-12
    }
    private val MEASUREMENT_SPEC = measurementSpec {
      measurementPublicKey = MC_PUBLIC_KEY.pack()
      reachAndFrequency = reachAndFrequency {
        reachPrivacyParams = OUTPUT_DP_PARAMS
        frequencyPrivacyParams = OUTPUT_DP_PARAMS
        maximumFrequency = 2
      }
      vidSamplingInterval = vidSamplingInterval {
        start = 0.0f
        width = 1.0f
      }
      nonceHashes += Hashing.hashSha256(REQUISITION_SPEC.nonce)
    }

    private val NOISE_MECHANISM =
      listOf(ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN, ProtocolConfig.NoiseMechanism.NONE)

    private val REQUISITION = requisition {
      name = REQUISITION_NAME
      measurement = "$MEASUREMENT_CONSUMER_NAME/measurements/BBBBBBBBBHs"
      state = Requisition.State.UNFULFILLED
      measurementConsumerCertificate = "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAcg"
      this.measurementSpec = signMeasurementSpec(MEASUREMENT_SPEC, MC_SIGNING_KEY)
      encryptedRequisitionSpec = ENCRYPTED_REQUISITION_SPEC
      protocolConfig = protocolConfig {
        protocols +=
          ProtocolConfigKt.protocol {
            direct =
              ProtocolConfigKt.direct {
                noiseMechanisms += NOISE_MECHANISM
                deterministicCountDistinct =
                  ProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
                deterministicDistribution =
                  ProtocolConfig.Direct.DeterministicDistribution.getDefaultInstance()
              }
          }
      }
      dataProviderCertificate = "$DATA_PROVIDER_NAME/certificates/AAAAAAAAAcg"
      dataProviderPublicKey = DATA_PROVIDER_PUBLIC_KEY.pack()
    }

    private val EDP_RESULT_SIGNING_KEY =
      loadSigningKey(
        "${EDP_DISPLAY_NAME}_result_cs_cert.der",
        "${EDP_DISPLAY_NAME}_result_cs_private.der",
      )
    private val DATA_PROVIDER_CERTIFICATE_KEY =
      DataProviderCertificateKey(EDP_ID, externalIdToApiId(8L))

    private fun loadSigningKey(
      certDerFileName: String,
      privateKeyDerFileName: String,
    ): SigningKeyHandle {
      return org.wfanet.measurement.common.crypto.testing.loadSigningKey(
        SECRET_FILES_PATH.resolve(certDerFileName).toFile(),
        SECRET_FILES_PATH.resolve(privateKeyDerFileName).toFile(),
      )
    }

    private const val REQUISITIONS_BUCKET = "requisitions-bucket"
    private const val REQUISITIONS_BLOB_KEY = "requisitions"
    private const val REQUISITIONS_FILE_URI = "file:///$REQUISITIONS_BUCKET/$REQUISITIONS_BLOB_KEY"

    private const val IMPRESSIONS_BUCKET = "impression-bucket"
    private const val IMPRESSIONS_BLOB_KEY = "impressions"
    private const val IMPRESSIONS_FILE_URI = "file:///$IMPRESSIONS_BUCKET/$IMPRESSIONS_BLOB_KEY"

    private const val IMPRESSIONS_METADATA_BUCKET = "impression-metadata-bucket"
    private const val EVENT_GROUP_REFERENCE_ID = "some-event-group-reference-id"
    private val DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    private val IMPRESSION_METADATA_BLOB_KEY =
      "ds/${FIRST_EVENT_DATE}/event-group-reference-id/$EVENT_GROUP_REFERENCE_ID/metadata"

    private val IMPRESSIONS_METADATA_FILE_URI =
      "file:///$IMPRESSIONS_METADATA_BUCKET/$IMPRESSION_METADATA_BLOB_KEY"

    private const val IMPRESSIONS_METADATA_FILE_URI_PREFIX = "file:///$IMPRESSIONS_METADATA_BUCKET"
  }
}
