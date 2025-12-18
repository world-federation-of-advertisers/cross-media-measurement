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
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.TinkProtoKeysetFormat
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.Any
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import com.google.protobuf.TypeRegistry
import com.google.protobuf.timestamp
import com.google.type.interval
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Clock
import java.time.Duration
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.logging.Logger
import kotlin.random.Random
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlinx.coroutines.Dispatchers
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
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.FulfillDirectRequisitionRequest
import org.wfanet.measurement.api.v2alpha.GetEventGroupRequest
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt
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
import org.wfanet.measurement.api.v2alpha.populationSpec
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
import org.wfanet.measurement.computation.KAnonymityParams
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.decryptResult
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionsValidator
import org.wfanet.measurement.edpaggregator.requisitionfetcher.SingleRequisitionGrouper
import org.wfanet.measurement.edpaggregator.requisitionfetcher.testing.TestRequisitionData
import org.wfanet.measurement.edpaggregator.resultsfulfiller.testing.TestRequisitionStubFactory
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.encryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRequisitionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.requisitionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.resultsFulfillerParams
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.InMemoryVidIndexMap
import org.wfanet.measurement.gcloud.pubsub.Subscriber
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorProvider
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
  private val requisitionMetadataServiceMock: RequisitionMetadataServiceCoroutineImplBase =
    mockService {
      onBlocking { startProcessingRequisitionMetadata(any()) }
        .thenReturn(requisitionMetadata { cmmsRequisition = REQUISITION_NAME })
      onBlocking { fulfillRequisitionMetadata(any()) }.thenReturn(requisitionMetadata {})
    }
  private val impressionMetadataServiceMock =
    mockService<ImpressionMetadataServiceCoroutineImplBase>()
  private val requisitionsServiceMock: RequisitionsCoroutineImplBase = mockService {
    onBlocking { fulfillDirectRequisition(any()) }.thenReturn(fulfillDirectRequisitionResponse {})
    onBlocking { getRequisition(any()) }
      .thenReturn(requisition { state = Requisition.State.UNFULFILLED })
  }
  private val eventGroupsServiceMock: EventGroupsCoroutineImplBase by lazy {
    mockService {
      onBlocking { getEventGroup(any()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<GetEventGroupRequest>(0)
          eventGroup {
            name = request.name
            eventGroupReferenceId = EVENT_GROUP_NAME
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
    addService(requisitionMetadataServiceMock)
    addService(impressionMetadataServiceMock)
  }

  private val requisitionsStub: RequisitionsCoroutineStub by lazy {
    RequisitionsCoroutineStub(grpcTestServerRule.channel)
  }
  private val eventGroupsStub: EventGroupsCoroutineStub by lazy {
    EventGroupsCoroutineStub(grpcTestServerRule.channel)
  }
  private val requisitionMetadataStub: RequisitionMetadataServiceCoroutineStub by lazy {
    RequisitionMetadataServiceCoroutineStub(grpcTestServerRule.channel)
  }
  private val impressionMetadataStub: ImpressionMetadataServiceCoroutineStub by lazy {
    ImpressionMetadataServiceCoroutineStub(grpcTestServerRule.channel)
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
    val subscriber =
      Subscriber(
        projectId = PROJECT_ID,
        googlePubSubClient = emulatorClient,
        maxMessages = 1,
        pullIntervalMillis = 100,
        ackDeadlineExtensionIntervalSeconds = 60,
        ackDeadlineExtensionSeconds = 600,
        blockingContext = Dispatchers.IO,
      )
    val workItemsStub = WorkItemsCoroutineStub(grpcTestServerRule.channel)
    val workItemAttemptsStub = WorkItemAttemptsCoroutineStub(grpcTestServerRule.channel)

    whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
      .thenReturn(
        listRequisitionMetadataResponse {
          requisitionMetadata += requisitionMetadata {
            state = RequisitionMetadata.State.STORED
            cmmsCreateTime = timestamp { seconds = 12345 }
            cmmsRequisition = REQUISITION_NAME
            blobUri = "some blob uri"
            blobTypeUrl = "some-blob-type-url"
            groupId = "an-existing-group-id"
            report = "report-name"
          }
        }
      )

    val testWorkItemAttempt = workItemAttempt {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
    }
    val workItemParams =
      createWorkItemParams(
        ResultsFulfillerParams.NoiseParams.NoiseType.NONE,
        kAnonymityParams = null,
      )
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
      RequisitionsValidator(TestRequisitionData.EDP_DATA.privateEncryptionKey)
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

    val kmsClients = getKmsClientMap()
    val kmsClient = kmsClients.getValue(EDP_NAME)

    val serializedEncryptionKey = getSerializedEncryptionKey(kmsClient)
    val mesosRecordIoStorageClient =
      getImpressionStorageClient(tmpPath, kmsClient, serializedEncryptionKey)

    val impressions =
      List(100) {
        LABELED_IMPRESSION.copy {
          vid = (it % 80 + 1).toLong()
          eventTime = FIRST_EVENT_DATE.atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
          event = TEST_EVENT.pack()
        }
      }

    val impressionsFlow = flow {
      impressions.forEach { impression -> emit(impression.toByteString()) }
    }

    // Write impressions to storage
    mesosRecordIoStorageClient.writeBlob(IMPRESSIONS_BLOB_KEY, impressionsFlow)

    writeImpressionMetadata(tmpPath, serializedEncryptionKey)

    val start = FIRST_EVENT_DATE.atStartOfDay().toInstant(ZoneOffset.UTC)
    val end = FIRST_EVENT_DATE.plusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC)

    whenever(impressionMetadataServiceMock.listImpressionMetadata(any()))
      .thenReturn(
        listImpressionMetadataResponse {
          impressionMetadata += impressionMetadata {
            state = ImpressionMetadata.State.ACTIVE
            blobUri = "file:///$IMPRESSIONS_METADATA_BUCKET/$IMPRESSION_METADATA_BLOB_KEY"
            interval = interval {
              startTime = timestamp {
                seconds = start.epochSecond
                nanos = start.nano
              }
              endTime = timestamp {
                seconds = end.epochSecond
                nanos = end.nano
              }
            }
          }
        }
      )

    val app =
      ResultsFulfillerApp(
        subscriptionId = SUBSCRIPTION_ID,
        queueSubscriber = subscriber,
        parser = WorkItem.parser(),
        workItemsStub,
        workItemAttemptsStub,
        requisitionMetadataStub,
        impressionMetadataStub,
        TestRequisitionStubFactory(
          grpcTestServerRule.channel,
          mapOf("some-duchy" to grpcTestServerRule.channel),
        ),
        kmsClients,
        getStorageConfig(tmpPath),
        getStorageConfig(tmpPath),
        getStorageConfig(tmpPath),
        mapOf("some-model-line" to MODEL_LINE_INFO),
        metrics = ResultsFulfillerMetrics.create(),
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
  fun `runWork throws where requisition metadata is not found`() {
    runBlocking {
      val subscriber =
        Subscriber(
          projectId = PROJECT_ID,
          googlePubSubClient = emulatorClient,
          maxMessages = 1,
          pullIntervalMillis = 100,
          ackDeadlineExtensionIntervalSeconds = 60,
          ackDeadlineExtensionSeconds = 600,
          blockingContext = Dispatchers.IO,
        )
      val workItemsStub = WorkItemsCoroutineStub(grpcTestServerRule.channel)
      val workItemAttemptsStub = WorkItemAttemptsCoroutineStub(grpcTestServerRule.channel)

      val testWorkItemAttempt = workItemAttempt {
        name = "workItems/workItem/workItemAttempts/workItemAttempt"
      }
      val workItemParams =
        createWorkItemParams(
          ResultsFulfillerParams.NoiseParams.NoiseType.NONE,
          kAnonymityParams = null,
        )
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
        RequisitionsValidator(TestRequisitionData.EDP_DATA.privateEncryptionKey)
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

      val kmsClients = getKmsClientMap()
      val kmsClient = kmsClients.getValue(EDP_NAME)

      val serializedEncryptionKey = getSerializedEncryptionKey(kmsClient)
      val mesosRecordIoStorageClient =
        getImpressionStorageClient(tmpPath, kmsClient, serializedEncryptionKey)

      val impressions =
        List(100) {
          LABELED_IMPRESSION.copy {
            vid = (it % 80 + 1).toLong()
            eventTime = FIRST_EVENT_DATE.atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
            event = TEST_EVENT.pack()
          }
        }

      val impressionsFlow = flow {
        impressions.forEach { impression -> emit(impression.toByteString()) }
      }

      // Write impressions to storage
      mesosRecordIoStorageClient.writeBlob(IMPRESSIONS_BLOB_KEY, impressionsFlow)

      writeImpressionMetadata(tmpPath, serializedEncryptionKey)

      val app =
        ResultsFulfillerApp(
          subscriptionId = SUBSCRIPTION_ID,
          queueSubscriber = subscriber,
          parser = WorkItem.parser(),
          workItemsStub,
          workItemAttemptsStub,
          requisitionMetadataStub,
          impressionMetadataStub,
          TestRequisitionStubFactory(
            grpcTestServerRule.channel,
            mapOf("some-duchy" to grpcTestServerRule.channel),
          ),
          kmsClients,
          getStorageConfig(tmpPath),
          getStorageConfig(tmpPath),
          getStorageConfig(tmpPath),
          mapOf("some-model-line" to MODEL_LINE_INFO),
          metrics = ResultsFulfillerMetrics.create(),
        )
      assertFailsWith<IllegalArgumentException> { app.runWork(Any.pack(workItemParams)) }
    }
  }

  @Test
  fun `runWork correctly selects continuous gaussian noise`() = runBlocking {
    val subscriber =
      Subscriber(
        projectId = PROJECT_ID,
        googlePubSubClient = emulatorClient,
        maxMessages = 1,
        pullIntervalMillis = 100,
        ackDeadlineExtensionIntervalSeconds = 60,
        ackDeadlineExtensionSeconds = 600,
        blockingContext = Dispatchers.IO,
      )
    val workItemsStub = WorkItemsCoroutineStub(grpcTestServerRule.channel)
    val workItemAttemptsStub = WorkItemAttemptsCoroutineStub(grpcTestServerRule.channel)

    whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
      .thenReturn(
        listRequisitionMetadataResponse {
          requisitionMetadata += requisitionMetadata {
            state = RequisitionMetadata.State.STORED
            cmmsCreateTime = timestamp { seconds = 12345 }
            cmmsRequisition = REQUISITION_NAME
            blobUri = "some blob uri"
            blobTypeUrl = "some-blob-type-url"
            groupId = "an-existing-group-id"
            report = "report-name"
          }
        }
      )

    val testWorkItemAttempt = workItemAttempt {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
    }
    val workItemParams =
      createWorkItemParams(
        ResultsFulfillerParams.NoiseParams.NoiseType.CONTINUOUS_GAUSSIAN,
        kAnonymityParams = null,
      )

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
      RequisitionsValidator(TestRequisitionData.EDP_DATA.privateEncryptionKey)
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

    val kmsClients = getKmsClientMap()
    val kmsClient = kmsClients.getValue(EDP_NAME)

    val serializedEncryptionKey = getSerializedEncryptionKey(kmsClient)
    val mesosRecordIoStorageClient =
      getImpressionStorageClient(tmpPath, kmsClient, serializedEncryptionKey)

    val impressions =
      List(100) {
        LABELED_IMPRESSION.copy {
          vid = (it % 80 + 1).toLong()
          eventTime = FIRST_EVENT_DATE.atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
          event = TEST_EVENT.pack()
        }
      }

    val impressionsFlow = flow {
      impressions.forEach { impression -> emit(impression.toByteString()) }
    }

    // Write impressions to storage
    mesosRecordIoStorageClient.writeBlob(IMPRESSIONS_BLOB_KEY, impressionsFlow)

    writeImpressionMetadata(tmpPath, serializedEncryptionKey)

    val app =
      ResultsFulfillerApp(
        subscriptionId = SUBSCRIPTION_ID,
        queueSubscriber = subscriber,
        parser = WorkItem.parser(),
        workItemsStub,
        workItemAttemptsStub,
        requisitionMetadataStub,
        impressionMetadataStub,
        TestRequisitionStubFactory(
          grpcTestServerRule.channel,
          mapOf("some-duchy" to grpcTestServerRule.channel),
        ),
        kmsClients,
        getStorageConfig(tmpPath),
        getStorageConfig(tmpPath),
        getStorageConfig(tmpPath),
        mapOf("some-model-line" to MODEL_LINE_INFO),
        metrics = ResultsFulfillerMetrics.create(),
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
    val subscriber =
      Subscriber(
        projectId = PROJECT_ID,
        googlePubSubClient = emulatorClient,
        maxMessages = 1,
        pullIntervalMillis = 100,
        ackDeadlineExtensionIntervalSeconds = 60,
        ackDeadlineExtensionSeconds = 600,
        blockingContext = Dispatchers.IO,
      )
    val workItemsStub = WorkItemsCoroutineStub(grpcTestServerRule.channel)
    val workItemAttemptsStub = WorkItemAttemptsCoroutineStub(grpcTestServerRule.channel)

    val testWorkItemAttempt = workItemAttempt {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
    }
    val workItemParams =
      createWorkItemParams(
        ResultsFulfillerParams.NoiseParams.NoiseType.UNSPECIFIED,
        kAnonymityParams = null,
      )
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
      RequisitionsValidator(TestRequisitionData.EDP_DATA.privateEncryptionKey)
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

    val kmsClients = getKmsClientMap()
    val kmsClient = kmsClients.getValue(EDP_NAME)

    val serializedEncryptionKey = getSerializedEncryptionKey(kmsClient)
    val mesosRecordIoStorageClient =
      getImpressionStorageClient(tmpPath, kmsClient, serializedEncryptionKey)

    val impressions =
      List(100) {
        LABELED_IMPRESSION.copy {
          vid = (it % 80 + 1).toLong()
          eventTime = FIRST_EVENT_DATE.atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
          event = TEST_EVENT.pack()
        }
      }

    val impressionsFlow = flow {
      impressions.forEach { impression -> emit(impression.toByteString()) }
    }

    // Write impressions to storage
    mesosRecordIoStorageClient.writeBlob(IMPRESSIONS_BLOB_KEY, impressionsFlow)

    writeImpressionMetadata(tmpPath, serializedEncryptionKey)

    val app =
      ResultsFulfillerApp(
        subscriptionId = SUBSCRIPTION_ID,
        queueSubscriber = subscriber,
        parser = WorkItem.parser(),
        workItemsStub,
        workItemAttemptsStub,
        requisitionMetadataStub,
        impressionMetadataStub,
        TestRequisitionStubFactory(
          grpcTestServerRule.channel,
          mapOf("some-duchy" to grpcTestServerRule.channel),
        ),
        kmsClients,
        getStorageConfig(tmpPath),
        getStorageConfig(tmpPath),
        getStorageConfig(tmpPath),
        mapOf("some-model-line" to MODEL_LINE_INFO),
        metrics = ResultsFulfillerMetrics.create(),
      )
    assertFails { app.runWork(Any.pack(workItemParams)) }

    verifyBlocking(requisitionsServiceMock, times(0)) { fulfillDirectRequisition(any()) }
  }

  @Test
  fun `runWork zeros out results if k-anonymity threshold is not met`() = runBlocking {
    val subscriber =
      Subscriber(
        projectId = PROJECT_ID,
        googlePubSubClient = emulatorClient,
        maxMessages = 1,
        pullIntervalMillis = 100,
        ackDeadlineExtensionIntervalSeconds = 60,
        ackDeadlineExtensionSeconds = 600,
        blockingContext = Dispatchers.IO,
      )
    val workItemsStub = WorkItemsCoroutineStub(grpcTestServerRule.channel)
    val workItemAttemptsStub = WorkItemAttemptsCoroutineStub(grpcTestServerRule.channel)

    whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
      .thenReturn(
        listRequisitionMetadataResponse {
          requisitionMetadata += requisitionMetadata {
            state = RequisitionMetadata.State.STORED
            cmmsCreateTime = timestamp { seconds = 12345 }
            cmmsRequisition = REQUISITION_NAME
            blobUri = "some blob uri"
            blobTypeUrl = "some-blob-type-url"
            groupId = "an-existing-group-id"
            report = "report-name"
          }
        }
      )

    val testWorkItemAttempt = workItemAttempt {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
    }
    val workItemParams =
      createWorkItemParams(
        ResultsFulfillerParams.NoiseParams.NoiseType.NONE,
        kAnonymityParams =
          KAnonymityParams(minUsers = 100, minImpressions = 10, reachMaxFrequencyPerUser = 10),
      )
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
      RequisitionsValidator(TestRequisitionData.EDP_DATA.privateEncryptionKey)
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

    val kmsClients = getKmsClientMap()
    val kmsClient = kmsClients.getValue(EDP_NAME)

    val serializedEncryptionKey = getSerializedEncryptionKey(kmsClient)
    val mesosRecordIoStorageClient =
      getImpressionStorageClient(tmpPath, kmsClient, serializedEncryptionKey)

    val impressions =
      List(100) {
        LABELED_IMPRESSION.copy {
          vid = (it % 80 + 1).toLong()
          eventTime = FIRST_EVENT_DATE.atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
          event = TEST_EVENT.pack()
        }
      }

    val impressionsFlow = flow {
      impressions.forEach { impression -> emit(impression.toByteString()) }
    }

    // Write impressions to storage
    mesosRecordIoStorageClient.writeBlob(IMPRESSIONS_BLOB_KEY, impressionsFlow)

    writeImpressionMetadata(tmpPath, serializedEncryptionKey)

    val app =
      ResultsFulfillerApp(
        subscriptionId = SUBSCRIPTION_ID,
        queueSubscriber = subscriber,
        parser = WorkItem.parser(),
        workItemsStub,
        workItemAttemptsStub,
        requisitionMetadataStub,
        impressionMetadataStub,
        TestRequisitionStubFactory(
          grpcTestServerRule.channel,
          mapOf("some-duchy" to grpcTestServerRule.channel),
        ),
        kmsClients,
        getStorageConfig(tmpPath),
        getStorageConfig(tmpPath),
        getStorageConfig(tmpPath),
        mapOf("some-model-line" to MODEL_LINE_INFO),
        metrics = ResultsFulfillerMetrics.create(),
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
    assertThat(result.reach.value).isEqualTo(0)
  }

  @Test
  fun `runWork returns non-zero results for sufficient k-anonymity`() = runBlocking {
    val subscriber =
      Subscriber(
        projectId = PROJECT_ID,
        googlePubSubClient = emulatorClient,
        maxMessages = 1,
        pullIntervalMillis = 100,
        ackDeadlineExtensionIntervalSeconds = 60,
        ackDeadlineExtensionSeconds = 600,
        blockingContext = Dispatchers.IO,
      )
    val workItemsStub = WorkItemsCoroutineStub(grpcTestServerRule.channel)
    val workItemAttemptsStub = WorkItemAttemptsCoroutineStub(grpcTestServerRule.channel)

    whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
      .thenReturn(
        listRequisitionMetadataResponse {
          requisitionMetadata += requisitionMetadata {
            state = RequisitionMetadata.State.STORED
            cmmsCreateTime = timestamp { seconds = 12345 }
            cmmsRequisition = REQUISITION_NAME
            blobUri = "some blob uri"
            blobTypeUrl = "some-blob-type-url"
            groupId = "an-existing-group-id"
            report = "report-name"
          }
        }
      )

    val testWorkItemAttempt = workItemAttempt {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
    }
    val workItemParams =
      createWorkItemParams(
        ResultsFulfillerParams.NoiseParams.NoiseType.NONE,
        kAnonymityParams =
          KAnonymityParams(minUsers = 10, minImpressions = 10, reachMaxFrequencyPerUser = 10),
      )
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
      RequisitionsValidator(TestRequisitionData.EDP_DATA.privateEncryptionKey)
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

    val kmsClients = getKmsClientMap()
    val kmsClient = kmsClients.getValue(EDP_NAME)

    val serializedEncryptionKey = getSerializedEncryptionKey(kmsClient)
    val mesosRecordIoStorageClient =
      getImpressionStorageClient(tmpPath, kmsClient, serializedEncryptionKey)

    val impressions =
      List(100) {
        LABELED_IMPRESSION.copy {
          vid = (it % 80 + 1).toLong()
          eventTime = FIRST_EVENT_DATE.atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
          event = TEST_EVENT.pack()
        }
      }

    val impressionsFlow = flow {
      impressions.forEach { impression -> emit(impression.toByteString()) }
    }

    // Write impressions to storage
    mesosRecordIoStorageClient.writeBlob(IMPRESSIONS_BLOB_KEY, impressionsFlow)

    writeImpressionMetadata(tmpPath, serializedEncryptionKey)

    val start = FIRST_EVENT_DATE.atStartOfDay().toInstant(ZoneOffset.UTC)
    val end = FIRST_EVENT_DATE.plusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC)

    whenever(impressionMetadataServiceMock.listImpressionMetadata(any()))
      .thenReturn(
        listImpressionMetadataResponse {
          impressionMetadata += impressionMetadata {
            state = ImpressionMetadata.State.ACTIVE
            blobUri = "file:///$IMPRESSIONS_METADATA_BUCKET/$IMPRESSION_METADATA_BLOB_KEY"
            interval = interval {
              startTime = timestamp {
                seconds = start.epochSecond
                nanos = start.nano
              }
              endTime = timestamp {
                seconds = end.epochSecond
                nanos = end.nano
              }
            }
          }
        }
      )

    val app =
      ResultsFulfillerApp(
        subscriptionId = SUBSCRIPTION_ID,
        queueSubscriber = subscriber,
        parser = WorkItem.parser(),
        workItemsStub,
        workItemAttemptsStub,
        requisitionMetadataStub,
        impressionMetadataStub,
        TestRequisitionStubFactory(
          grpcTestServerRule.channel,
          mapOf("some-duchy" to grpcTestServerRule.channel),
        ),
        kmsClients,
        getStorageConfig(tmpPath),
        getStorageConfig(tmpPath),
        getStorageConfig(tmpPath),
        mapOf("some-model-line" to MODEL_LINE_INFO),
        metrics = ResultsFulfillerMetrics.create(),
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
  fun `runWork throws errors if k-anonymity params not set up correctly`() = runBlocking {
    val subscriber =
      Subscriber(
        projectId = PROJECT_ID,
        googlePubSubClient = emulatorClient,
        maxMessages = 1,
        pullIntervalMillis = 100,
        ackDeadlineExtensionIntervalSeconds = 60,
        ackDeadlineExtensionSeconds = 600,
        blockingContext = Dispatchers.IO,
      )
    val workItemsStub = WorkItemsCoroutineStub(grpcTestServerRule.channel)
    val workItemAttemptsStub = WorkItemAttemptsCoroutineStub(grpcTestServerRule.channel)

    val testWorkItemAttempt = workItemAttempt {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
    }
    val workItemParams =
      createWorkItemParams(
        ResultsFulfillerParams.NoiseParams.NoiseType.NONE,
        kAnonymityParams =
          KAnonymityParams(minUsers = 0, minImpressions = 10, reachMaxFrequencyPerUser = 10),
      )
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
      RequisitionsValidator(TestRequisitionData.EDP_DATA.privateEncryptionKey)
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

    val kmsClients = getKmsClientMap()
    val kmsClient = kmsClients.getValue(EDP_NAME)

    val serializedEncryptionKey = getSerializedEncryptionKey(kmsClient)
    val mesosRecordIoStorageClient =
      getImpressionStorageClient(tmpPath, kmsClient, serializedEncryptionKey)

    val impressions =
      List(100) {
        LABELED_IMPRESSION.copy {
          vid = (it % 80 + 1).toLong()
          eventTime = FIRST_EVENT_DATE.atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
          event = TEST_EVENT.pack()
        }
      }

    val impressionsFlow = flow {
      impressions.forEach { impression -> emit(impression.toByteString()) }
    }

    // Write impressions to storage
    mesosRecordIoStorageClient.writeBlob(IMPRESSIONS_BLOB_KEY, impressionsFlow)

    writeImpressionMetadata(tmpPath, serializedEncryptionKey)
    val typeRegistry = TypeRegistry.newBuilder().add(TestEvent.getDescriptor()).build()

    val start = FIRST_EVENT_DATE.atStartOfDay().toInstant(ZoneOffset.UTC)
    val end = FIRST_EVENT_DATE.plusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC)

    whenever(impressionMetadataServiceMock.listImpressionMetadata(any()))
      .thenReturn(
        listImpressionMetadataResponse {
          impressionMetadata += impressionMetadata {
            state = ImpressionMetadata.State.ACTIVE
            blobUri = "file:///$IMPRESSIONS_METADATA_BUCKET/$IMPRESSION_METADATA_BLOB_KEY"
            interval = interval {
              startTime = timestamp {
                seconds = start.epochSecond
                nanos = start.nano
              }
              endTime = timestamp {
                seconds = end.epochSecond
                nanos = end.nano
              }
            }
          }
        }
      )

    val app =
      ResultsFulfillerApp(
        subscriptionId = SUBSCRIPTION_ID,
        queueSubscriber = subscriber,
        parser = WorkItem.parser(),
        workItemsStub,
        workItemAttemptsStub,
        requisitionMetadataStub,
        impressionMetadataStub,
        TestRequisitionStubFactory(
          grpcTestServerRule.channel,
          mapOf("some-duchy" to grpcTestServerRule.channel),
        ),
        kmsClients,
        getStorageConfig(tmpPath),
        getStorageConfig(tmpPath),
        getStorageConfig(tmpPath),
        mapOf("some-model-line" to MODEL_LINE_INFO),
        metrics = ResultsFulfillerMetrics.create(),
      )

    assertFails { app.runWork(Any.pack(workItemParams)) }

    verifyBlocking(requisitionsServiceMock, times(0)) { fulfillDirectRequisition(any()) }
  }

  private suspend fun writeImpressionMetadata(tmpPath: File, serializedEncryptionKey: ByteString) {
    // Create the impressions metadata store
    Files.createDirectories(tmpPath.resolve(IMPRESSIONS_METADATA_BUCKET).toPath())
    // Create symlink so both impression-metadata-bucket/ds and impression-metadata-bucketds paths
    // work
    val bucketDsPath = tmpPath.resolve("${IMPRESSIONS_METADATA_BUCKET}ds").toPath()
    val bucketWithDsPath = tmpPath.resolve(IMPRESSIONS_METADATA_BUCKET).resolve("ds").toPath()
    Files.createDirectories(bucketWithDsPath.parent)
    Files.createSymbolicLink(bucketDsPath, bucketWithDsPath)
    val impressionsMetadataStorageClient =
      SelectedStorageClient(IMPRESSIONS_METADATA_FILE_URI, tmpPath)

    val encryptedDek = encryptedDek {
      this.kekUri = KEK_URI
      typeUrl = "type.googleapis.com/google.crypto.tink.Keyset"
      protobufFormat = EncryptedDek.ProtobufFormat.BINARY
      ciphertext = serializedEncryptionKey
    }

    val blobDetails = blobDetails {
      this.blobUri = IMPRESSIONS_FILE_URI
      this.encryptedDek = encryptedDek
      this.eventGroupReferenceId = EVENT_GROUP_NAME
    }
    logger.info("Writing Blob $IMPRESSION_METADATA_BLOB_KEY")
    impressionsMetadataStorageClient.writeBlob(
      IMPRESSION_METADATA_BLOB_KEY,
      blobDetails.toByteString(),
    )
  }

  private fun getKmsClientMap(): MutableMap<String, KmsClient> {
    // Set up KMS
    val kmsClients = mutableMapOf<String, KmsClient>()
    val kmsClient = FakeKmsClient()
    val kmsKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
    kmsClient.setAead(KEK_URI, kmsKeyHandle.getPrimitive(Aead::class.java))
    kmsClients[EDP_NAME] = kmsClient
    return kmsClients
  }

  private fun getSerializedEncryptionKey(kmsClient: KmsClient): ByteString {
    // Set up streaming encryption
    val tinkKeyTemplateType = "AES128_GCM_HKDF_1MB"
    val aeadKeyTemplate = KeyTemplates.get(tinkKeyTemplateType)
    val keyEncryptionHandle = KeysetHandle.generateNew(aeadKeyTemplate)
    val serializedEncryptionKey =
      ByteString.copyFrom(
        TinkProtoKeysetFormat.serializeEncryptedKeyset(
          keyEncryptionHandle,
          kmsClient.getAead(KEK_URI),
          byteArrayOf(),
        )
      )
    return serializedEncryptionKey
  }

  private fun getImpressionStorageClient(
    tmpPath: File,
    kmsClient: KmsClient,
    serializedEncryptionKey: ByteString,
  ): MesosRecordIoStorageClient {
    // Create impressions storage client
    Files.createDirectories(tmpPath.resolve(IMPRESSIONS_BUCKET).toPath())
    val impressionsStorageClient = SelectedStorageClient(IMPRESSIONS_FILE_URI, tmpPath)

    val aeadStorageClient =
      impressionsStorageClient.withEnvelopeEncryption(kmsClient, KEK_URI, serializedEncryptionKey)

    // Wrap aead client in mesos client
    return MesosRecordIoStorageClient(aeadStorageClient)
  }

  private fun getStorageConfig(
    tmpPath: File
  ): (ResultsFulfillerParams.StorageParams) -> StorageConfig {
    return { _: ResultsFulfillerParams.StorageParams -> StorageConfig(rootDirectory = tmpPath) }
  }

  private fun createWorkItemParams(
    noiseType: ResultsFulfillerParams.NoiseParams.NoiseType,
    kAnonymityParams: KAnonymityParams?,
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
            if (kAnonymityParams != null) {
              this.kAnonymityParams =
                ResultsFulfillerParamsKt.kAnonymityParams {
                  this.minImpressions = kAnonymityParams.minImpressions
                  this.minUsers = kAnonymityParams.minUsers
                  this.reachMaxFrequencyPerUser = kAnonymityParams.reachMaxFrequencyPerUser
                }
            }
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
      reportingMetadata = MeasurementSpecKt.reportingMetadata { report = "some-report" }
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
      modelLine = "some-model-line"
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
    private val IMPRESSION_METADATA_BLOB_KEY =
      "ds/${FIRST_EVENT_DATE}/model-line/some-model-line/event-group-reference-id/$EVENT_GROUP_NAME/metadata"

    private val IMPRESSIONS_METADATA_FILE_URI =
      "file:///$IMPRESSIONS_METADATA_BUCKET/$IMPRESSION_METADATA_BLOB_KEY"

    private const val IMPRESSIONS_METADATA_FILE_URI_PREFIX = "file:///$IMPRESSIONS_METADATA_BUCKET"

    private val POPULATION_SPEC = populationSpec {
      subpopulations +=
        PopulationSpecKt.subPopulation {
          vidRanges +=
            PopulationSpecKt.vidRange {
              startVid = 1
              endVidInclusive = 1000
            }
        }
    }
    private val KEK_URI = FakeKmsClient.KEY_URI_PREFIX + "kek"
    private val MODEL_LINE_INFO =
      ModelLineInfo(
        eventDescriptor = TestEvent.getDescriptor(),
        populationSpec = POPULATION_SPEC,
        vidIndexMap = InMemoryVidIndexMap.build(POPULATION_SPEC),
      )
  }
}
