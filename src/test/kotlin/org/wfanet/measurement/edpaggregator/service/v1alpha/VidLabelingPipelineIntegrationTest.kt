// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.service.v1alpha

import com.google.auth.oauth2.IdTokenProvider
import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.aead.AeadConfig
import com.google.devtools.build.runfiles.Runfiles
import com.google.protobuf.Any as ProtoAny
import com.google.protobuf.ByteString
import com.google.protobuf.Message
import com.google.protobuf.Parser
import com.google.protobuf.Struct
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
import io.grpc.CallOptions
import io.grpc.Channel as GrpcChannel
import io.grpc.ClientCall
import io.grpc.ClientInterceptor
import io.grpc.ClientInterceptors
import io.grpc.ForwardingClientCall
import io.grpc.ForwardingClientCallListener
import io.grpc.Metadata
import io.grpc.MethodDescriptor
import io.grpc.Status
import java.io.File
import java.io.IOException
import java.net.InetSocketAddress
import java.net.URI
import java.nio.file.Paths
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.RawLocalFileSystem
import org.junit.After
import org.junit.Before
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.rules.TestRule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.GetModelLineRequest
import org.wfanet.measurement.api.v2alpha.ListModelLinesRequest
import org.wfanet.measurement.api.v2alpha.ListModelRolloutsRequest
import org.wfanet.measurement.api.v2alpha.ListModelShardsRequest
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelShardKt.modelBlob
import org.wfanet.measurement.api.v2alpha.ModelShardsGrpcKt
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt
import org.wfanet.measurement.api.v2alpha.ReplaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.listModelLinesResponse
import org.wfanet.measurement.api.v2alpha.listModelRolloutsResponse
import org.wfanet.measurement.api.v2alpha.listModelShardsResponse
import org.wfanet.measurement.api.v2alpha.modelLine
import org.wfanet.measurement.api.v2alpha.modelRollout
import org.wfanet.measurement.api.v2alpha.modelShard
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.config.securecomputation.WatchedPathKt.httpEndpointSink
import org.wfanet.measurement.config.securecomputation.watchedPath
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityBlobs
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySync
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySyncLeaseRunner
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySyncTaskPublisher
import org.wfanet.measurement.edpaggregator.dataavailability.GrpcDataAvailabilitySyncLeaseClient
import org.wfanet.measurement.edpaggregator.deploy.gcloud.dataavailability.DataAvailabilitySyncTaskProcessor
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.DataAvailabilitySyncTaskPublicationRunner
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.InternalApiServices as EdpaInternalApiServices
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata as EdpaSchemata
import org.wfanet.measurement.edpaggregator.rawimpressions.GENERATION_PATH_PREFIX
import org.wfanet.measurement.edpaggregator.rawimpressions.RawImpressionFileMetadata
import org.wfanet.measurement.edpaggregator.rawimpressions.readEventDateFromFooter
import org.wfanet.measurement.edpaggregator.resultsfulfiller.StorageEventReader
import org.wfanet.measurement.edpaggregator.subpoolassigner.SubpoolAssignerApp
import org.wfanet.measurement.edpaggregator.subpoolassigner.VirtualPeoplePoolEmitLabeler
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceAttributes
import org.wfanet.measurement.edpaggregator.testing.TestEncryptedStorage
import org.wfanet.measurement.edpaggregator.testing.VidLabelingRpcThrottlersTestHelper
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncLeaseServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncTask
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncTaskServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidateServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.SubpoolAssignerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperationServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.ageBucket
import org.wfanet.measurement.edpaggregator.v1alpha.ageRange
import org.wfanet.measurement.edpaggregator.v1alpha.approveUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.bucketLookup
import org.wfanet.measurement.edpaggregator.v1alpha.enumLookup
import org.wfanet.measurement.edpaggregator.v1alpha.labelerInputFieldMapping
import org.wfanet.measurement.edpaggregator.v1alpha.listDataAvailabilitySyncTasksRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRankIndexBlobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listUploadHealingOperationsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.scalarColumn
import org.wfanet.measurement.edpaggregator.v1alpha.subpoolAssignerParams
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelerParams
import org.wfanet.measurement.edpaggregator.vidlabeler.LabeledImpressionsBlobKeys
import org.wfanet.measurement.edpaggregator.vidlabeler.ParquetImpressionConverter
import org.wfanet.measurement.edpaggregator.vidlabeler.PopulationAttributeWriter
import org.wfanet.measurement.edpaggregator.vidlabeler.VidLabelerApp
import org.wfanet.measurement.edpaggregator.vidlabeler.VirtualPeopleVidAssigner
import org.wfanet.measurement.edpaggregator.vidlabeling.RawImpressionBlobMetadata
import org.wfanet.measurement.edpaggregator.vidlabeling.RawImpressionUploadManifestClassifier
import org.wfanet.measurement.edpaggregator.vidlabeling.RequestIds
import org.wfanet.measurement.edpaggregator.vidlabeling.VidLabelingDispatchSequencer
import org.wfanet.measurement.edpaggregator.vidlabeling.VidLabelingDispatcher
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.DoneBlobReplayer
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.EvictUploader
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.GrpcCorrectionCandidateCleaner
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.GrpcHealingOperationStore
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.RawImpressionUploadCorrectionPlanner
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.RecoveryExecutor
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.VidLabelingHealingController
import org.wfanet.measurement.edpaggregator.vidrankbuilder.VidRankBuilderApp
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidateServiceGrpcKt as InternalCorrectionCandidateServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperationServiceGrpcKt as InternalUploadHealingOperationServiceGrpcKt
import org.wfanet.measurement.queue.MessageConsumer
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CompleteWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.EnsureWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.FailWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.GetWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttempt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcher
import org.wfanet.measurement.securecomputation.datawatcher.WatchedBlobs
import org.wfanet.measurement.securecomputation.datawatcher.testing.DataWatcherSubscribingStorageClient
import org.wfanet.measurement.securecomputation.deploy.gcloud.testing.TestIdTokenProvider
import org.wfanet.measurement.securecomputation.service.WorkItemInvalidStateException
import org.wfanet.measurement.storage.BlobChangedException
import org.wfanet.measurement.storage.BlobMetadataStorageClient
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.ParquetEncryptionConfig
import org.wfanet.measurement.storage.ParquetStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.measurement.storage.parquetRow
import org.wfanet.measurement.storage.parquetValue
import org.wfanet.virtualpeople.common.Gender

@RunWith(JUnit4::class)
class VidLabelingPipelineIntegrationTest {
  private val tempFolder = TemporaryFolder()
  private val edpaDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, EdpaSchemata.EDP_AGGREGATOR_CHANGELOG_PATH)
  private val workItemTransport = InProcessWorkItemTransport()
  private val edpaInternalServer = GrpcTestServerRule {
    EdpaInternalApiServices.build(edpaDatabase.databaseClient, EmptyCoroutineContext)
      .toList()
      .forEach { addService(it) }
  }

  private val modelLines =
    listOf(
      modelLine {
        name = MEMOIZED_MODEL_LINE
        type = ModelLine.Type.PROD
        activeStartTime =
          EVENT_DATE.minusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant().toProtoTime()
        activeEndTime =
          EVENT_DATE.plusDays(10).atStartOfDay(ZoneOffset.UTC).toInstant().toProtoTime()
      },
      modelLine {
        name = DIRECT_MODEL_LINE
        type = ModelLine.Type.PROD
        activeStartTime =
          EVENT_DATE.minusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant().toProtoTime()
        activeEndTime =
          EVENT_DATE.plusDays(10).atStartOfDay(ZoneOffset.UTC).toInstant().toProtoTime()
      },
    )
  private val modelLinesService =
    object : ModelLinesGrpcKt.ModelLinesCoroutineImplBase() {
      override suspend fun listModelLines(request: ListModelLinesRequest) = listModelLinesResponse {
        modelLines += this@VidLabelingPipelineIntegrationTest.modelLines
      }

      override suspend fun getModelLine(request: GetModelLineRequest): ModelLine =
        modelLines.single { it.name == request.name }
    }
  private val modelRolloutsService =
    object : ModelRolloutsGrpcKt.ModelRolloutsCoroutineImplBase() {
      override suspend fun listModelRollouts(request: ListModelRolloutsRequest) =
        listModelRolloutsResponse {
          modelRollouts += modelRollout {
            modelRelease =
              if (request.parent == MEMOIZED_MODEL_LINE) MEMOIZED_RELEASE else DIRECT_RELEASE
          }
        }
    }
  private val modelShardsService =
    object : ModelShardsGrpcKt.ModelShardsCoroutineImplBase() {
      override suspend fun listModelShards(request: ListModelShardsRequest) =
        listModelShardsResponse {
          modelShards += modelShard {
            name = "$DATA_PROVIDER/modelShards/memoized"
            modelRelease = MEMOIZED_RELEASE
            modelBlob = modelBlob { modelBlobPath = modelBlobUri }
            memoizedVidAssignmentEnabled = true
          }
          modelShards += modelShard {
            name = "$DATA_PROVIDER/modelShards/direct"
            modelRelease = DIRECT_RELEASE
            modelBlob = modelBlob { modelBlobPath = modelBlobUri }
          }
        }
    }
  private val dataProvidersService = RecordingDataProvidersService()
  private val edpaPublicServer = GrpcTestServerRule {
    Services.build(edpaInternalServer.channel).toList().forEach { addService(it) }
    addService(modelLinesService)
    addService(modelRolloutsService)
    addService(modelShardsService)
    addService(dataProvidersService)
  }
  private val workItemPublicServer = GrpcTestServerRule {
    addService(workItemTransport.workItemsService)
    addService(workItemTransport.workItemAttemptsService)
  }

  @get:Rule
  val ruleChain: TestRule =
    chainRulesSequentially(
      tempFolder,
      edpaDatabase,
      edpaInternalServer,
      edpaPublicServer,
      workItemPublicServer,
    )

  private lateinit var rootKey: String
  private lateinit var fileBucket: String
  private lateinit var fileStorageRoot: File
  private lateinit var rawPrefix: String
  private lateinit var outputPrefix: String
  private lateinit var externalOutputPrefix: String
  private lateinit var modelBlobUri: String
  private lateinit var fileStorage: ConditionalOperationStorageClient
  private lateinit var mapStorage: UriNormalizingStorageClient
  private lateinit var rawEventStorage: DataWatcherSubscribingStorageClient
  private lateinit var outputEventStorage: DataWatcherSubscribingStorageClient
  private lateinit var metadataStorage: RecordingBlobMetadataStorageClient
  private lateinit var kmsClient: KmsClient
  private lateinit var rawWatcher: DataWatcher
  private lateinit var outputWatcher: DataWatcher
  private lateinit var httpServer: HttpServer
  private lateinit var appScope: CoroutineScope
  private val appJobs = mutableListOf<Job>()
  private val endpointFailure = AtomicReference<Throwable?>()
  private val externalAvailabilityDeliveries = AtomicInteger()
  private val taskRpcFaults = LostSuccessfulResponseInterceptor()
  private val availabilityClock = MutableClock(Clock.systemUTC().instant().plus(Duration.ofDays(1)))
  private val availabilityTransport = InProcessAvailabilityTaskTransport()

  private lateinit var uploadsStub:
    RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
  private lateinit var filesStub:
    RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
  private lateinit var modelLineRowsStub:
    RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
  private lateinit var rankIndexBlobsStub:
    RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
  private lateinit var impressionMetadataStub:
    ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
  private lateinit var availabilityTasksStub:
    DataAvailabilitySyncTaskServiceGrpcKt.DataAvailabilitySyncTaskServiceCoroutineStub
  private lateinit var faultingAvailabilityTasksStub:
    DataAvailabilitySyncTaskServiceGrpcKt.DataAvailabilitySyncTaskServiceCoroutineStub
  private lateinit var correctionDetectionStub:
    InternalCorrectionCandidateServiceGrpcKt.RawImpressionUploadCorrectionCandidateServiceCoroutineStub
  private lateinit var correctionCandidatesStub:
    RawImpressionUploadCorrectionCandidateServiceGrpcKt.RawImpressionUploadCorrectionCandidateServiceCoroutineStub
  private lateinit var operationsStub:
    UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineStub
  private lateinit var workItemsStub: WorkItemsGrpcKt.WorkItemsCoroutineStub
  private lateinit var workItemAttemptsStub: WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
  private lateinit var dispatchSequencer: VidLabelingDispatchSequencer
  private lateinit var internalDataAvailabilitySync: DataAvailabilitySync
  private lateinit var availabilityTaskProcessor: DataAvailabilitySyncTaskProcessor
  private lateinit var availabilityPublicationRunner: DataAvailabilitySyncTaskPublicationRunner

  @Before
  fun setUp() {
    AeadConfig.register()
    GenerationMatchedTestHadoopFileSystem.resetRecordedReads()
    val absoluteRoot = tempFolder.root.toPath().toAbsolutePath().toString().removePrefix("/")
    fileBucket = absoluteRoot.substringBefore('/')
    rootKey = absoluteRoot.substringAfter('/')
    fileStorageRoot = File("/$fileBucket")
    rawPrefix = "gs://$fileBucket/$rootKey/raw"
    outputPrefix = "file:///$fileBucket/$rootKey/output"
    externalOutputPrefix = "gs://$fileBucket/$rootKey/output/external"
    modelBlobUri = "file:///$fileBucket/$rootKey/models/model.riegeli"
    fileStorage = GenerationEnforcingStorageClient(FileSystemStorageClient(fileStorageRoot))
    mapStorage = UriNormalizingStorageClient(fileStorage)
    metadataStorage = RecordingBlobMetadataStorageClient(fileStorage)
    kmsClient = TestEncryptedStorage.buildFakeKmsClient(KEK_URI, keyTemplate = "AES128_GCM")

    runBlocking {
      val modelPath =
        Paths.get(
          Runfiles.preload()
            .unmapped()
            .rlocation(
              "_main/src/main/resources/testing/labeler/" +
                "memoized_reference_test_model_riegeli_list"
            )
        )
      fileStorage.writeBlob(
        SelectedStorageClient.parseBlobUri(modelBlobUri).key,
        flowOf(ByteString.copyFrom(java.nio.file.Files.readAllBytes(modelPath))),
      )
    }

    uploadsStub =
      RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub(
        edpaPublicServer.channel
      )
    filesStub =
      RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub(
        edpaPublicServer.channel
      )
    modelLineRowsStub =
      RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub(
        edpaPublicServer.channel
      )
    rankIndexBlobsStub =
      RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub(edpaPublicServer.channel)
    impressionMetadataStub =
      ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub(
        edpaPublicServer.channel
      )
    availabilityTasksStub =
      DataAvailabilitySyncTaskServiceGrpcKt.DataAvailabilitySyncTaskServiceCoroutineStub(
        edpaPublicServer.channel
      )
    faultingAvailabilityTasksStub =
      DataAvailabilitySyncTaskServiceGrpcKt.DataAvailabilitySyncTaskServiceCoroutineStub(
        ClientInterceptors.intercept(edpaPublicServer.channel, taskRpcFaults)
      )
    correctionDetectionStub =
      InternalCorrectionCandidateServiceGrpcKt
        .RawImpressionUploadCorrectionCandidateServiceCoroutineStub(edpaInternalServer.channel)
    correctionCandidatesStub =
      RawImpressionUploadCorrectionCandidateServiceGrpcKt
        .RawImpressionUploadCorrectionCandidateServiceCoroutineStub(edpaPublicServer.channel)
    operationsStub =
      UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineStub(
        edpaPublicServer.channel
      )
    workItemsStub = WorkItemsGrpcKt.WorkItemsCoroutineStub(workItemPublicServer.channel)
    workItemAttemptsStub =
      WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub(workItemPublicServer.channel)

    val modelLineConfig = modelLineConfig()
    dispatchSequencer =
      VidLabelingDispatchSequencer(
        rawImpressionUploadStub = uploadsStub,
        rawImpressionUploadModelLineStub = modelLineRowsStub,
        workItemsStub = workItemsStub,
        poolAssignmentJobStub =
          PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub(
            edpaPublicServer.channel
          ),
        modelRolloutsStub =
          ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub(edpaPublicServer.channel),
        modelShardsStub = ModelShardsGrpcKt.ModelShardsCoroutineStub(edpaPublicServer.channel),
        modelLinesStub = ModelLinesGrpcKt.ModelLinesCoroutineStub(edpaPublicServer.channel),
        dataProviderName = DATA_PROVIDER,
        vidLabelerParamsTemplate = vidLabelerParamsTemplate(),
        subpoolAssignerParamsTemplate = subpoolAssignerParamsTemplate(),
        queueName = VID_LABELER_QUEUE,
        poolAssignerQueueName = POOL_ASSIGNER_QUEUE,
        numberOfShards = 1,
        modelLineConfigs =
          mapOf(MEMOIZED_MODEL_LINE to modelLineConfig, DIRECT_MODEL_LINE to modelLineConfig),
        rawImpressionUploadFileStub = filesStub,
        vidLabelingJobStub =
          VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub(edpaPublicServer.channel),
        maxFileBatchSizeBytes = 10_000_000L,
        rpcThrottlers = VidLabelingRpcThrottlersTestHelper.alwaysReady(),
      )

    val dispatcherFactory = { exchange: HttpExchange ->
      VidLabelingDispatcher(
        storageClient = fileStorage,
        rawImpressionUploadStub = uploadsStub,
        rawImpressionUploadFilesStub = filesStub,
        correctionDetectionStub = correctionDetectionStub,
        rawImpressionUploadModelLineStub = modelLineRowsStub,
        rankIndexBlobStub = rankIndexBlobsStub,
        modelLinesStub = ModelLinesGrpcKt.ModelLinesCoroutineStub(edpaPublicServer.channel),
        dispatchSequencer = dispatchSequencer,
        dataProviderName = DATA_PROVIDER,
        modelSuiteName = MODEL_SUITE,
        overrideModelLines =
          exchange.requestHeaders.getFirst(OVERRIDE_MODEL_LINES_HEADER)?.split(',').orEmpty(),
        recoverySourceUpload = exchange.requestHeaders.getFirst(RECOVERY_SOURCE_UPLOAD_HEADER),
        recoveryOperationId = exchange.requestHeaders.getFirst(EVICTION_OPERATION_ID_HEADER),
        modelLineConfigs =
          mapOf(MEMOIZED_MODEL_LINE to modelLineConfig, DIRECT_MODEL_LINE to modelLineConfig),
        readEventDate = { uri -> readEventDate(uri) },
        readBlobMetadata = { key -> blobMetadata(key) },
        rpcThrottlers = VidLabelingRpcThrottlersTestHelper.alwaysReady(),
        clock = Clock.fixed(NOW, ZoneOffset.UTC),
      )
    }

    internalDataAvailabilitySync =
      DataAvailabilitySync(
        edpImpressionPath = SelectedStorageClient.parseBlobUri(outputPrefix).key,
        storageClient = metadataStorage,
        dataProvidersStub =
          DataProvidersGrpcKt.DataProvidersCoroutineStub(edpaPublicServer.channel),
        impressionMetadataServiceStub = impressionMetadataStub,
        dataProviderName = DATA_PROVIDER,
        throttler = ImmediateThrottler,
        impressionMetadataBatchSize = 100,
        modelLineMap = emptyMap(),
        errorIfGapsExist = false,
      )
    val externalDataAvailabilitySync =
      DataAvailabilitySync(
        edpImpressionPath = SelectedStorageClient.parseBlobUri(externalOutputPrefix).key,
        storageClient = metadataStorage,
        dataProvidersStub =
          DataProvidersGrpcKt.DataProvidersCoroutineStub(edpaPublicServer.channel),
        impressionMetadataServiceStub = impressionMetadataStub,
        dataProviderName = DATA_PROVIDER,
        throttler = ImmediateThrottler,
        impressionMetadataBatchSize = 100,
        modelLineMap = emptyMap(),
        errorIfGapsExist = false,
      )

    httpServer = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
    httpServer.createContext(
      "/raw",
      SuspendingHttpHandler(endpointFailure::set) { exchange ->
        dispatcherFactory(exchange)
          .upload(
            checkNotNull(exchange.requestHeaders.getFirst(DATA_WATCHER_PATH_HEADER)),
            checkNotNull(exchange.requestHeaders.getFirst(DATA_WATCHER_GENERATION_HEADER)).toLong(),
          )
      },
    )
    httpServer.createContext(
      "/labeled",
      SuspendingHttpHandler(endpointFailure::set) { exchange ->
        externalAvailabilityDeliveries.incrementAndGet()
        val leaseRunner =
          DataAvailabilitySyncLeaseRunner(
            GrpcDataAvailabilitySyncLeaseClient(
              DataAvailabilitySyncLeaseServiceGrpcKt.DataAvailabilitySyncLeaseServiceCoroutineStub(
                edpaPublicServer.channel
              )
            )
          )
        leaseRunner.run(DATA_PROVIDER) { lease ->
          externalDataAvailabilitySync.sync(
            checkNotNull(exchange.requestHeaders.getFirst(DATA_WATCHER_PATH_HEADER)),
            dataAvailabilitySyncLease = lease.name,
            ensureLeaseActive = lease::invoke,
            doneBlobGeneration =
              checkNotNull(exchange.requestHeaders.getFirst(DATA_WATCHER_GENERATION_HEADER))
                .toLong(),
          )
        }
      },
    )
    httpServer.start()

    val endpoint = "http://127.0.0.1:${httpServer.address.port}"
    val idTokenProvider: IdTokenProvider = TestIdTokenProvider()
    rawWatcher =
      DataWatcher(
        workItemsStub,
        listOf(
          watchedPath {
            identifier = "raw-upload"
            sourcePathRegex = "${Regex.escape(rawPrefix)}/.+/done"
            httpEndpointSink = httpEndpointSink {
              endpointUri = "$endpoint/raw"
              appParams = Struct.getDefaultInstance()
            }
          }
        ),
        idTokenProvider = idTokenProvider,
      )
    outputWatcher =
      DataWatcher(
        workItemsStub,
        listOf(
          watchedPath {
            identifier = "labeled-output"
            sourcePathRegex = "${Regex.escape(externalOutputPrefix)}/model-line/.+/done"
            httpEndpointSink = httpEndpointSink {
              endpointUri = "$endpoint/labeled"
              appParams = Struct.getDefaultInstance()
            }
          }
        ),
        idTokenProvider = idTokenProvider,
      )
    rawEventStorage = DataWatcherSubscribingStorageClient(fileStorage, "gs://$fileBucket/")
    outputEventStorage = DataWatcherSubscribingStorageClient(fileStorage, "gs://$fileBucket/")
    rawEventStorage.subscribe(rawWatcher)
    outputEventStorage.subscribe(outputWatcher)

    availabilityPublicationRunner =
      DataAvailabilitySyncTaskPublicationRunner(
        edpaDatabase.databaseClient,
        availabilityTransport,
        clock = availabilityClock,
        pollInterval = Duration.ofMillis(1),
        leaseDuration = Duration.ofMinutes(1),
        initialRetryDelay = Duration.ofSeconds(1),
        maxRetryDelay = Duration.ofSeconds(1),
      )
    availabilityTaskProcessor =
      DataAvailabilitySyncTaskProcessor(
        faultingAvailabilityTasksStub,
        DataAvailabilitySyncLeaseRunner(
          GrpcDataAvailabilitySyncLeaseClient(
            DataAvailabilitySyncLeaseServiceGrpcKt.DataAvailabilitySyncLeaseServiceCoroutineStub(
              edpaPublicServer.channel
            )
          )
        ),
        buildDataAvailabilitySync = { internalDataAvailabilitySync },
        verifyDoneObject = { task ->
          val key = SelectedStorageClient.parseBlobUri(task.doneBlobUri).key
          val generation = checkNotNull(fileStorage.getFreshnessToken(key)).toLong()
          check(generation == task.doneBlobGeneration)
        },
      )

    startApplications()
  }

  @After
  fun tearDown() = runBlocking {
    appJobs.forEach { it.cancelAndJoin() }
    workItemTransport.close()
    if (::httpServer.isInitialized) {
      httpServer.stop(0)
    }
  }

  @Test
  fun `raw uploads run through both pipelines and data availability`() = runBlocking {
    workItemTransport.forceNextAcknowledgementRedelivery()
    availabilityTransport.duplicateNextDelivery = true
    availabilityTransport.redeliverAfterNextFailure = true
    taskRpcFaults.loseNextCreateResponse.set(true)
    taskRpcFaults.loseNextMarkRunningResponse.set(true)
    val initialFile = writeRawFile("day-1", "initial.parquet", listOf("person-1"))
    val firstDoneGeneration = finalizeRawUpload("day-1")
    awaitPipelineIdle()

    val initialUploads = listUploads()
    assertThat(initialUploads).hasSize(1)
    assertThat(initialUploads.single().doneBlobGeneration).isEqualTo(firstDoneGeneration)
    assertThat(listUploadFiles(initialUploads.single().name).single().blobGeneration)
      .isEqualTo(generationOf(initialFile))
    assertCompletedForBothPaths(initialUploads.single())
    assertReadablePeople(MEMOIZED_MODEL_LINE, setOf("person-1"))
    assertReadablePeople(DIRECT_MODEL_LINE, setOf("person-1"))
    assertThat(workItemTransport.forcedRetryCount).isEqualTo(1)
    assertThat(workItemTransport.forcedRetryWorkItemNames).hasSize(1)

    val workItemsAfterInitial = workItemTransport.publishedCount
    val metadataAfterInitial = listMetadata()
    val memoizedOutputsAfterInitial = outputGenerations(MEMOIZED_MODEL_LINE)
    val directOutputsAfterInitial = outputGenerations(DIRECT_MODEL_LINE)
    val rankIndexesAfterInitial = listRankIndexBlobs(initialUploads.single().name)
    assertThat(metadataAfterInitial).hasSize(2)
    assertThat(memoizedOutputsAfterInitial).hasSize(1)
    assertThat(directOutputsAfterInitial).hasSize(1)
    assertThat(rankIndexesAfterInitial).hasSize(2)
    val initialTasks = listAvailabilityTasks(initialUploads.single().name)
    assertThat(initialTasks).hasSize(2)
    assertThat(initialTasks.map { it.state }.toSet())
      .containsExactly(DataAvailabilitySyncTask.State.SUCCEEDED)
    assertThat(initialTasks.map { it.attemptCount }.toSet()).containsExactly(1, 2)
    assertAvailabilityTaskIdentities(initialUploads.single(), EVENT_DATE)
    assertThat(taskRpcFaults.droppedCreateResponses.get()).isEqualTo(1)
    assertThat(taskRpcFaults.droppedMarkRunningResponses.get()).isEqualTo(1)
    assertThat(availabilityTransport.deliveryCounts.values.any { it == 1 }).isTrue()
    assertThat(availabilityTransport.deliveryCounts.values.any { it >= 2 }).isTrue()
    assertThat(externalAvailabilityDeliveries.get()).isEqualTo(0)
    rawWatcher.receivePath(
      "$rawPrefix/day-1/done",
      mapOf(DataWatcher.GENERATION_METADATA_KEY to firstDoneGeneration.toString()),
    )
    awaitPipelineIdle()
    assertThat(listUploads()).hasSize(1)
    assertThat(workItemTransport.publishedCount).isEqualTo(workItemsAfterInitial)
    assertThat(listMetadata()).containsExactlyElementsIn(metadataAfterInitial)
    assertThat(outputGenerations(MEMOIZED_MODEL_LINE)).isEqualTo(memoizedOutputsAfterInitial)
    assertThat(outputGenerations(DIRECT_MODEL_LINE)).isEqualTo(directOutputsAfterInitial)
    assertThat(listRankIndexBlobs(initialUploads.single().name))
      .containsExactlyElementsIn(rankIndexesAfterInitial)

    val additiveFile = writeRawFile("day-1", "additional.parquet", listOf("person-2"))
    val additiveFileGeneration = generationOf(additiveFile)
    availabilityTransport.redeliverAfterNextFailure = true
    taskRpcFaults.loseNextMarkSucceededResponse.set(true)
    val additiveDoneGeneration = finalizeRawUpload("day-1")
    awaitPipelineIdle()
    val revisions = listUploads().filter { it.doneBlobUri == "$rawPrefix/day-1/done" }
    assertThat(revisions).hasSize(2)
    val additive = revisions.single { it.doneBlobGeneration == additiveDoneGeneration }
    assertThat(additive.replacesRawImpressionUpload).isEqualTo(initialUploads.single().name)
    assertThat(listUploadFiles(additive.name).map { it.blobUri to it.blobGeneration })
      .containsExactly(additiveFile to additiveFileGeneration)
    val additiveTasks = listAvailabilityTasks(additive.name)
    assertThat(additiveTasks.map { it.state }.toSet())
      .containsExactly(DataAvailabilitySyncTask.State.SUCCEEDED)
    assertAvailabilityTaskIdentities(additive, EVENT_DATE)
    assertThat(taskRpcFaults.droppedMarkSucceededResponses.get()).isEqualTo(1)
    assertThat(additiveTasks.any { availabilityTransport.deliveryCounts.getValue(it.name) >= 2 })
      .isTrue()

    val independentFile =
      writeRawFile("day-1/advertiser-a", "independent.parquet", listOf("person-3"))
    val independentFileGeneration = generationOf(independentFile)
    val independentDoneGeneration = finalizeRawUpload("day-1/advertiser-a")
    awaitPipelineIdle()
    val independent =
      listUploads().single {
        it.doneBlobUri == "$rawPrefix/day-1/advertiser-a/done" &&
          it.doneBlobGeneration == independentDoneGeneration
      }
    assertThat(independent.replacesRawImpressionUpload).isEmpty()
    assertThat(listUploadFiles(independent.name).map { it.blobUri to it.blobGeneration })
      .containsExactly(independentFile to independentFileGeneration)
    assertReadablePeople(MEMOIZED_MODEL_LINE, setOf("person-1", "person-2", "person-3"))
    assertReadablePeople(DIRECT_MODEL_LINE, setOf("person-1", "person-2", "person-3"))
    assertAvailabilityPublished(setOf(MEMOIZED_MODEL_LINE, DIRECT_MODEL_LINE), setOf(EVENT_DATE))
    assertThat(externalAvailabilityDeliveries.get()).isEqualTo(0)
    assertEveryRegisteredRawGenerationWasRead()
  }

  @Test
  fun `missing task publication is retried without relabeling`() = runBlocking {
    availabilityTransport.failBeforeNextPublication = true
    writeRawFile("day-1", "input.parquet", listOf("person-1"))
    finalizeRawUpload("day-1")
    workItemTransport.awaitIdle()
    val upload = listUploads().single()

    assertThat(availabilityPublicationRunner.publishPendingTasks(limit = 1)).isEqualTo(0)

    val failedTask =
      listAvailabilityTasks(upload.name).single {
        it.state == DataAvailabilitySyncTask.State.FAILED
      }
    assertThat(failedTask.failureCategory)
      .isEqualTo(DataAvailabilitySyncTask.FailureCategory.PUBLICATION)
    assertThat(listMetadata()).isEmpty()

    availabilityClock.advance(Duration.ofSeconds(2))
    drainAvailabilityTasks()

    assertThat(listAvailabilityTasks(upload.name).map { it.state }.toSet())
      .containsExactly(DataAvailabilitySyncTask.State.SUCCEEDED)
    assertThat(listMetadata()).hasSize(2)
    assertReadablePeople(MEMOIZED_MODEL_LINE, setOf("person-1"))
    assertReadablePeople(DIRECT_MODEL_LINE, setOf("person-1"))
  }

  @Test
  fun `lost publication response recovers through the outbox`() =
    runBlocking<Unit> {
      writeRawFile("day-1", "input.parquet", listOf("person-1"))
      finalizeRawUpload("day-1")
      workItemTransport.awaitIdle()
      val upload = listUploads().single()
      availabilityTransport.loseNextResponse = true

      assertThat(availabilityPublicationRunner.publishPendingTasks(limit = 1)).isEqualTo(0)
      val deliveredTaskName = availabilityTransport.takeNext()
      assertThat(runCatching { availabilityTaskProcessor.process(deliveredTaskName) }.isFailure)
        .isTrue()
      val failedTask = listAvailabilityTasks(upload.name).single { it.name == deliveredTaskName }
      assertThat(failedTask.state).isEqualTo(DataAvailabilitySyncTask.State.FAILED)
      assertThat(failedTask.failureCategory)
        .isEqualTo(DataAvailabilitySyncTask.FailureCategory.PUBLICATION)

      availabilityClock.advance(Duration.ofSeconds(2))
      drainAvailabilityTasks()

      assertThat(listAvailabilityTasks(upload.name).map { it.state }.toSet())
        .containsExactly(DataAvailabilitySyncTask.State.SUCCEEDED)
      assertThat(availabilityTransport.lostResponseTaskNames).containsExactly(deliveredTaskName)
    }

  @Test
  fun `task failure before metadata creation recovers by republication`() = runBlocking {
    val inputFile = writeRawFile("day-1", "input.parquet", listOf("person-1"))
    finalizeRawUpload("day-1")
    workItemTransport.awaitIdle()
    val upload = listUploads().single()
    val directTask =
      listAvailabilityTasks(upload.name).single { it.cmmsModelLine == DIRECT_MODEL_LINE }
    val directSidecarKey = blobKey(sidecarUri(inputFile, DIRECT_MODEL_LINE, EVENT_DATE))
    val validSidecar = checkNotNull(fileStorage.getBlob(directSidecarKey)).read().flatten()
    fileStorage.writeBlob(directSidecarKey, flowOf(ByteString.copyFromUtf8("invalid sidecar")))
    publishTask(directTask.name)
    availabilityTaskProcessor.process(directTask.name)

    val failedTask =
      listAvailabilityTasks(upload.name).single { it.cmmsModelLine == DIRECT_MODEL_LINE }
    assertThat(failedTask.state).isEqualTo(DataAvailabilitySyncTask.State.FAILED)
    assertThat(failedTask.failureCategory)
      .isEqualTo(DataAvailabilitySyncTask.FailureCategory.SYNCHRONIZATION)
    assertThat(failedTask.attemptCount).isEqualTo(1)
    assertThat(listMetadata().none { it.modelLine == DIRECT_MODEL_LINE }).isTrue()

    fileStorage.writeBlob(directSidecarKey, flowOf(validSidecar))
    availabilityClock.advance(Duration.ofHours(2))
    drainAvailabilityTasks()

    val recoveredTask =
      listAvailabilityTasks(upload.name).single { it.cmmsModelLine == DIRECT_MODEL_LINE }
    assertThat(recoveredTask.state).isEqualTo(DataAvailabilitySyncTask.State.SUCCEEDED)
    assertThat(recoveredTask.attemptCount).isEqualTo(2)
    assertThat(listMetadata().any { it.modelLine == DIRECT_MODEL_LINE }).isTrue()
  }

  @Test
  fun `task with only stale sidecars fails until its output is restored`() = runBlocking {
    writeRawFile("day-1", "initial.parquet", listOf("person-1"))
    finalizeRawUpload("day-1")
    awaitPipelineIdle()

    val additiveFile = writeRawFile("day-1", "additive.parquet", listOf("person-2"))
    val additiveDoneGeneration = finalizeRawUpload("day-1")
    workItemTransport.awaitIdle()
    val additiveUpload = listUploads().single { it.doneBlobGeneration == additiveDoneGeneration }
    val directTask =
      listAvailabilityTasks(additiveUpload.name).single { it.cmmsModelLine == DIRECT_MODEL_LINE }
    val sidecarKey = blobKey(sidecarUri(additiveFile, DIRECT_MODEL_LINE, EVENT_DATE))
    val sidecarBytes = checkNotNull(fileStorage.getBlob(sidecarKey)).read().flatten()
    checkNotNull(fileStorage.getBlob(sidecarKey)).delete()
    publishTask(directTask.name)
    availabilityTaskProcessor.process(directTask.name)

    val failedTask =
      listAvailabilityTasks(additiveUpload.name).single { it.cmmsModelLine == DIRECT_MODEL_LINE }
    assertThat(failedTask.state).isEqualTo(DataAvailabilitySyncTask.State.FAILED)
    assertThat(failedTask.failureCategory)
      .isEqualTo(DataAvailabilitySyncTask.FailureCategory.SYNCHRONIZATION)
    assertThat(
        listMetadata().none {
          it.rawImpressionUpload == additiveUpload.name && it.modelLine == DIRECT_MODEL_LINE
        }
      )
      .isTrue()

    fileStorage.writeBlob(sidecarKey, flowOf(sidecarBytes))
    availabilityClock.advance(Duration.ofHours(2))
    drainAvailabilityTasks()

    val recoveredTask =
      listAvailabilityTasks(additiveUpload.name).single { it.cmmsModelLine == DIRECT_MODEL_LINE }
    assertThat(recoveredTask.state).isEqualTo(DataAvailabilitySyncTask.State.SUCCEEDED)
    assertThat(
        listMetadata().any {
          it.rawImpressionUpload == additiveUpload.name && it.modelLine == DIRECT_MODEL_LINE
        }
      )
      .isTrue()
  }

  @Test
  fun `overlapping task delivery does not reclaim a running leased attempt`() = runBlocking {
    writeRawFile("day-1", "input.parquet", listOf("person-1"))
    finalizeRawUpload("day-1")
    workItemTransport.awaitIdle()
    val upload = listUploads().single()
    val directTask =
      listAvailabilityTasks(upload.name).single { it.cmmsModelLine == DIRECT_MODEL_LINE }
    publishTask(directTask.name)
    metadataStorage.pauseNextSyncStart()

    val firstDelivery =
      async(Dispatchers.Default) { availabilityTaskProcessor.process(directTask.name) }
    metadataStorage.awaitPausedSyncStart()
    val duplicateResult =
      async(Dispatchers.Default) {
          runCatching { availabilityTaskProcessor.process(directTask.name) }
        }
        .await()

    assertThat(duplicateResult.isSuccess).isTrue()
    assertThat(runCatching { availabilityTaskProcessor.process(directTask.name) }.isSuccess)
      .isTrue()
    val running = listAvailabilityTasks(upload.name).single { it.name == directTask.name }
    assertThat(running.state).isEqualTo(DataAvailabilitySyncTask.State.RUNNING)
    assertThat(running.attemptCount).isEqualTo(1)

    metadataStorage.releasePausedSyncStart()
    firstDelivery.await()
    availabilityTaskProcessor.process(directTask.name)

    val succeeded = listAvailabilityTasks(upload.name).single { it.name == directTask.name }
    assertThat(succeeded.state).isEqualTo(DataAvailabilitySyncTask.State.SUCCEEDED)
    assertThat(succeeded.attemptCount).isEqualTo(1)
    val doneKey = blobKey(succeeded.doneBlobUri)
    val doneBlob = checkNotNull(metadataStorage.getBlob(doneKey))
    assertThat(DataAvailabilityBlobs.isSynced(doneBlob)).isTrue()
    assertThat(DataAvailabilityBlobs.isDataAvailabilityPublished(doneBlob)).isTrue()
  }

  @Test
  fun `external watched path uses DataWatcher while internal output uses tasks`() = runBlocking {
    val internalDoneKey =
      blobKey(LabeledImpressionsBlobKeys.forDoneUri(outputPrefix, DIRECT_MODEL_LINE, EVENT_DATE))
    outputEventStorage.writeBlob(internalDoneKey, flowOf(ByteString.EMPTY))
    assertThat(externalAvailabilityDeliveries.get()).isEqualTo(0)

    val externalDataUri =
      "$externalOutputPrefix/model-line/direct/$EVENT_DATE/external-output.riegeli"
    val externalSidecarUri = "$externalDataUri.metadata.binpb"
    outputEventStorage.writeBlob(blobKey(externalDataUri), flowOf(ByteString.EMPTY))
    outputEventStorage.writeBlob(
      blobKey(externalSidecarUri),
      flowOf(
        blobDetails {
            blobUri = externalDataUri
            eventGroupReferenceId = "external-event-group"
            modelLine = DIRECT_MODEL_LINE
            interval =
              com.google.type.interval {
                startTime = EVENT_DATE.atStartOfDay(ZoneOffset.UTC).toInstant().toProtoTime()
                endTime =
                  EVENT_DATE.plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant().toProtoTime()
              }
          }
          .toByteString()
      ),
    )
    val externalDoneKey = blobKey("$externalOutputPrefix/model-line/direct/$EVENT_DATE/done")
    outputEventStorage.writeBlob(externalDoneKey, flowOf(ByteString.EMPTY))
    endpointFailure.getAndSet(null)?.let {
      throw AssertionError("External finalized-object delivery failed", it)
    }

    assertThat(externalAvailabilityDeliveries.get()).isEqualTo(1)
    val metadata = listMetadata().single()
    assertThat(metadata.modelLine).isEqualTo(DIRECT_MODEL_LINE)
    assertThat(metadata.rawImpressionUpload).isEmpty()
    assertThat(metadata.state).isEqualTo(ImpressionMetadata.State.ACTIVE)
  }

  @Test
  fun `corrected output synchronizes only after healing releases the fence`() = runBlocking {
    writeRawFile("day-1", "input.parquet", listOf("stale-person"))
    val originalDoneGeneration = finalizeRawUpload("day-1")
    workItemTransport.awaitIdle()
    val original = listUploads().single { it.doneBlobGeneration == originalDoneGeneration }
    assertCompletedForBothPaths(original)
    assertThat(listAvailabilityTasks(original.name).map { it.state }.toSet())
      .containsExactly(DataAvailabilitySyncTask.State.PENDING)

    writeRawFile("day-1", "input.parquet", listOf("corrected-person"))
    val correctionDoneGeneration = finalizeRawUpload("day-1")
    workItemTransport.awaitIdle()
    val correction = listUploads().single { it.doneBlobGeneration == correctionDoneGeneration }
    assertThat(correction.state).isEqualTo(RawImpressionUpload.State.CORRECTION_REQUIRED)

    val controller = buildHealingController()
    assertThat(controller.run().failedDataProviders).isEqualTo(0)
    val draft = listHealingOperations().single()
    approveHealingOperation(draft, RawImpressionUploadCorrectionCandidate.Decision.DECISION_CORRECT)

    assertThat(controller.run().failedDataProviders).isEqualTo(0)
    assertThat(listAvailabilityTasks(original.name).map { it.state }.toSet())
      .containsExactly(DataAvailabilitySyncTask.State.SUPERSEDED)

    workItemTransport.awaitIdle()
    endpointFailure.getAndSet(null)?.let {
      throw AssertionError("Corrected revision replay failed", it)
    }
    val replacement = listUploads().single { it.replacesRawImpressionUpload == original.name }
    assertCompletedForBothPaths(replacement)
    assertThat(listAvailabilityTasks(replacement.name).map { it.state }.toSet())
      .containsExactly(DataAvailabilitySyncTask.State.PENDING)
    assertThat(listMetadata()).isEmpty()

    assertThat(controller.run().failedDataProviders).isEqualTo(0)
    assertThat(listHealingOperations().single().state)
      .isEqualTo(UploadHealingOperation.State.COMPLETE)
    drainAvailabilityTasks()

    assertThat(listAvailabilityTasks(replacement.name).map { it.state }.toSet())
      .containsExactly(DataAvailabilitySyncTask.State.SUCCEEDED)
    assertThat(listMetadata().map { it.rawImpressionUpload }.toSet())
      .containsExactly(replacement.name)
    Unit
  }

  @Test
  fun `no-replacement correction cancels pending availability tasks`() = runBlocking {
    writeRawFile("day-1", "input.parquet", listOf("stale-person"))
    val originalDoneGeneration = finalizeRawUpload("day-1")
    workItemTransport.awaitIdle()
    val original = listUploads().single { it.doneBlobGeneration == originalDoneGeneration }
    assertCompletedForBothPaths(original)
    assertThat(listAvailabilityTasks(original.name).map { it.state }.toSet())
      .containsExactly(DataAvailabilitySyncTask.State.PENDING)
    assertThat(listMetadata()).isEmpty()

    writeRawFile("day-1", "input.parquet", listOf("replacement-person"))
    val correctionDoneGeneration = finalizeRawUpload("day-1")
    workItemTransport.awaitIdle()
    val correction = listUploads().single { it.doneBlobGeneration == correctionDoneGeneration }
    assertThat(correction.state).isEqualTo(RawImpressionUpload.State.CORRECTION_REQUIRED)
    assertThat(listModelLines(correction.name)).isEmpty()

    val controller = buildHealingController()
    assertThat(controller.run().failedDataProviders).isEqualTo(0)
    val draft = listHealingOperations().single()
    assertThat(draft.state).isEqualTo(UploadHealingOperation.State.APPROVAL_REQUIRED)
    approveHealingOperation(
      draft,
      RawImpressionUploadCorrectionCandidate.Decision.DECISION_NO_REPLACEMENT,
    )

    assertThat(controller.run().failedDataProviders).isEqualTo(0)

    val completed = listHealingOperations().single()
    assertThat(completed.state).isEqualTo(UploadHealingOperation.State.COMPLETE)
    assertThat(listAvailabilityTasks(original.name).map { it.state }.toSet())
      .containsExactly(DataAvailabilitySyncTask.State.CANCELLED)
    assertThat(availabilityPublicationRunner.publishPendingTasks()).isEqualTo(0)
    assertThat(listMetadata()).isEmpty()
  }

  private fun buildHealingController(): VidLabelingHealingController {
    val labeledOutputPrefix = canonicalGcsUri(outputPrefix)
    val evictUploader =
      EvictUploader(
        uploadsStub,
        modelLineRowsStub,
        rankIndexBlobsStub,
        filesStub,
        impressionMetadataStub,
        labeledOutputPrefix,
        getBlobGeneration = { blobUri ->
          fileStorage.getFreshnessToken(blobKey(blobUri))?.toLong()
        },
        deleteBlob = { blobUri, generation ->
          val key = blobKey(blobUri)
          val currentGeneration = fileStorage.getFreshnessToken(key)?.toLong()
          if (currentGeneration != generation) {
            false
          } else {
            val blob = fileStorage.getBlob(key)
            if (blob == null) false
            else {
              blob.delete()
              true
            }
          }
        },
      )
    val operationStore =
      GrpcHealingOperationStore(
        operationsStub,
        InternalUploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineStub(
          edpaInternalServer.channel
        ),
      )
    return VidLabelingHealingController(
      listOf(
        VidLabelingHealingController.DataProviderConfig(
          DATA_PROVIDER,
          labeledOutputPrefix,
          Duration.ofDays(3650),
        )
      ),
      correctionCandidatesStub,
      GrpcCorrectionCandidateCleaner(correctionDetectionStub),
      operationStore,
      uploadsStub,
      filesStub,
      modelLineRowsStub,
      rankIndexBlobsStub,
      plannerFactory = {
        RawImpressionUploadCorrectionPlanner(
          planCorrection = { owners, cutoff, operationId ->
            evictUploader.planCorrection(owners, cutoff, operationId)
          }
        )
      },
      evictionExecutorFactory = { evictUploader },
      manifestReader = { doneBlobUri, doneBlobGeneration ->
        val revision =
          listUploads().single {
            it.doneBlobUri == doneBlobUri && it.doneBlobGeneration == doneBlobGeneration
          }
        listUploadFiles(revision.name).map {
          RawImpressionUploadManifestClassifier.File(it.blobUri, it.blobGeneration, it.eventDate)
        }
      },
      doneBlobReplayerFactory = {
        DoneBlobReplayer { request ->
          val operationId = request.uploadHealingOperation.substringAfterLast('/')
          rawWatcher.receivePath(
            request.doneBlobUri,
            mapOf(
              DataWatcher.GENERATION_METADATA_KEY to request.doneBlobGeneration.toString(),
              WatchedBlobs.OVERRIDE_MODEL_LINES_KEY to request.cmmsModelLines.joinToString(","),
              WatchedBlobs.RECOVERY_SOURCE_UPLOAD_KEY to request.sourceRawImpressionUpload,
              WatchedBlobs.EVICTION_OPERATION_ID_KEY to operationId,
            ),
          )
        }
      },
      recoveryExecutorFactory = {
        RecoveryExecutor { _, _ -> error("no-replacement healing must not recover an upload") }
      },
    )
  }

  private suspend fun listHealingOperations(): List<UploadHealingOperation> =
    operationsStub
      .listUploadHealingOperations(listUploadHealingOperationsRequest { parent = DATA_PROVIDER })
      .uploadHealingOperationsList

  private suspend fun approveHealingOperation(
    operation: UploadHealingOperation,
    decision: RawImpressionUploadCorrectionCandidate.Decision,
  ) {
    operationsStub.approveUploadHealingOperation(
      approveUploadHealingOperationRequest {
        name = operation.name
        candidateDecisions +=
          org.wfanet.measurement.edpaggregator.v1alpha.ApproveUploadHealingOperationRequestKt
            .candidateDecision {
              rawImpressionUploadCorrectionCandidate =
                operation.rawImpressionUploadCorrectionCandidatesList.single()
              this.decision = decision
            }
        etag = operation.etag
        requestId = "123e4567-e89b-42d3-a456-426614174099"
      }
    )
  }

  private fun startApplications() {
    val rawParquetClient = { storageConfig: StorageConfig, kms: KmsClient ->
      parquetClient(
        kms,
        if (storageConfig.blobPrefix?.startsWith("gs://") == true) {
          Path("gs://$fileBucket/")
        } else {
          Path("file:///$fileBucket")
        },
      )
    }
    val modelBytes: suspend (String) -> ByteString = { uri ->
      val key = SelectedStorageClient.parseBlobUri(uri).key
      checkNotNull(fileStorage.getBlob(key)).read().flatten()
    }
    val throttlers = VidLabelingRpcThrottlersTestHelper.alwaysReady()
    val apps =
      listOf(
        SubpoolAssignerApp(
          subscriptionId = POOL_ASSIGNER_QUEUE,
          queueSubscriber = workItemTransport,
          parser = WorkItem.parser(),
          workItemsClient = workItemsStub,
          workItemAttemptsClient = workItemAttemptsStub,
          vidRankBuilderQueue = RANK_BUILDER_QUEUE,
          kmsClients = mapOf(DATA_PROVIDER to kmsClient),
          getSubpoolMapStorageConfig = {
            StorageConfig(rootDirectory = fileStorageRoot, blobPrefix = it.blobPrefix)
          },
          getRawImpressionsStorageConfig = {
            StorageConfig(rootDirectory = fileStorageRoot, blobPrefix = it.blobPrefix)
          },
          getModelStorageConfig = {
            StorageConfig(rootDirectory = fileStorageRoot, blobPrefix = it.blobPrefix)
          },
          rawImpressionUploadsStub = uploadsStub,
          rawImpressionUploadModelLinesStub = modelLineRowsStub,
          rankerJobsStub =
            RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub(edpaPublicServer.channel),
          poolAssignmentJobsStub =
            PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub(
              edpaPublicServer.channel
            ),
          rawImpressionUploadFilesStub = filesStub,
          buildParquetStorageClient = rawParquetClient,
          buildSubpoolMapStorageClient = { mapStorage },
          loadPoolEmitLabeler = { _, uri ->
            VirtualPeoplePoolEmitLabeler.fromCompiledNodeBlob(modelBytes(uri))
          },
          getSubpoolMapKekUri = { KEK_URI },
          rpcThrottlers = throttlers,
        ),
        VidRankBuilderApp(
          subscriptionId = RANK_BUILDER_QUEUE,
          queueSubscriber = workItemTransport,
          parser = WorkItem.parser(),
          workItemsClient = workItemsStub,
          workItemAttemptsClient = workItemAttemptsStub,
          kmsClients = mapOf(DATA_PROVIDER to kmsClient),
          retentionDaysByDataProvider = mapOf(DATA_PROVIDER to 30),
          rankerJobsStub =
            RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub(edpaPublicServer.channel),
          rankIndexBlobsStub = rankIndexBlobsStub,
          rawImpressionUploadModelLinesStub = modelLineRowsStub,
          vidLabelingJobsStub =
            VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub(
              edpaPublicServer.channel
            ),
          rawImpressionUploadFilesStub = filesStub,
          vidLabelerQueue = VID_LABELER_QUEUE,
          rpcThrottlers = throttlers,
          buildSubpoolMapStorageClient = { mapStorage },
          buildVidRankMapStorageClient = { mapStorage },
          today = { EVENT_DATE.plusDays(2) },
          rankStripes = 1,
          maxInFlightRecords = 2,
        ),
        VidLabelerApp(
          subscriptionId = VID_LABELER_QUEUE,
          queueSubscriber = workItemTransport,
          parser = WorkItem.parser(),
          workItemsClient = workItemsStub,
          workItemAttemptsClient = workItemAttemptsStub,
          kmsClients = mapOf(DATA_PROVIDER to kmsClient),
          encryptKekUris = mapOf(DATA_PROVIDER to KEK_URI),
          getStorageConfig = {
            StorageConfig(rootDirectory = File("/"), blobPrefix = it.impressionsBlobPrefix)
          },
          vidLabelingJobsStub =
            VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub(
              edpaPublicServer.channel
            ),
          rawImpressionUploadModelLinesStub = modelLineRowsStub,
          dataAvailabilitySyncTasksStub = faultingAvailabilityTasksStub,
          rankIndexBlobsStub = rankIndexBlobsStub,
          rawImpressionUploadFilesStub = filesStub,
          buildParquetStorageClient = rawParquetClient,
          buildVidRankMapStorageClient = { mapStorage },
          loadAssigner = { _, uri ->
            VirtualPeopleVidAssigner.fromCompiledNodeBlob(modelBytes(uri))
          },
          buildImpressionConverter = { _, _ ->
            ParquetImpressionConverter(
              TestEvent.getDescriptor(),
              PopulationAttributeWriter(TestEvent.getDescriptor(), POPULATION_SPEC),
            )
          },
          rpcThrottlers = throttlers,
          writeDoneBlobToStorage = { _, doneUri ->
            val doneKey = SelectedStorageClient.parseBlobUri(doneUri).key
            val blob =
              outputEventStorage.writeBlob(doneKey, flowOf(ByteString.EMPTY))
                as ConditionalOperationStorageClient.Blob
            "gs://$fileBucket/$doneKey" to blob.freshnessToken.toLong()
          },
        ),
      )
    appScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    appJobs += apps.map { appScope.launch { it.run() } }
  }

  private suspend fun writeRawFile(
    folder: String,
    fileName: String,
    personIds: List<String>,
    eventDate: LocalDate = EVENT_DATE,
  ): String {
    val key = "$rootKey/raw/$folder/$fileName"
    val eventTimeMicros = eventDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli() * 1_000L
    parquetClient(kmsClient)
      .writeBlob(
        key,
        flow {
          for ((index, personId) in personIds.withIndex()) {
            emit(
              parquetRow {
                  columns[EVENT_ID_COLUMN] = parquetValue { stringValue = "$personId-$index" }
                  columns[EVENT_TIME_COLUMN] = parquetValue { int64Value = eventTimeMicros + index }
                  columns[PERSON_ID_COLUMN] = parquetValue { stringValue = personId }
                  columns[GENDER_COLUMN] = parquetValue { stringValue = "MALE" }
                  columns[AGE_GROUP_COLUMN] = parquetValue { stringValue = "YEARS_18_TO_34" }
                }
                .toByteString()
            )
          }
        },
        mapOf(RawImpressionFileMetadata.EVENT_DATE_KEY to eventDate.toString()),
      )
    return "gs://$fileBucket/$key"
  }

  private suspend fun finalizeRawUpload(folder: String): Long {
    val key = "$rootKey/raw/$folder/done"
    val blob = rawEventStorage.writeBlob(key, flowOf(ByteString.EMPTY))
    endpointFailure.getAndSet(null)?.let {
      throw AssertionError("Finalized-object delivery failed", it)
    }
    return (blob as ConditionalOperationStorageClient.Blob).freshnessToken.toLong()
  }

  private fun parquetClient(
    kms: KmsClient,
    root: Path = Path("file:///$fileBucket"),
  ): ParquetStorageClient =
    ParquetStorageClient(
      Configuration().apply {
        set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        set("fs.gs.impl", GenerationMatchedTestHadoopFileSystem::class.java.name)
        setBoolean("fs.gs.impl.disable.cache", true)
        set("parquet.encryption.uniform.key", KEK_URI)
        setBoolean("parquet.encryption.plaintext.footer", true)
      },
      root,
      encryptionConfig = ParquetEncryptionConfig(kmsProvider = { kms }),
    )

  private suspend fun readEventDate(blobUri: String): LocalDate =
    readEventDateFromFooter(parquetClient(kmsClient, Path("gs://$fileBucket/")), blobUri)

  private suspend fun blobMetadata(key: String): RawImpressionBlobMetadata {
    val blob = checkNotNull(fileStorage.getBlob(key))
    val generation =
      checkNotNull((blob as? ConditionalOperationStorageClient.Blob)?.freshnessToken).toLong()
    // Each FileSystem overwrite is a new logical object generation. Its update time therefore
    // models GCS's per-generation create time rather than the inode's original create time.
    return RawImpressionBlobMetadata(generation, blob.size, blob.updateTime)
  }

  private suspend fun generationOf(blobUri: String): Long =
    checkNotNull(fileStorage.getFreshnessToken(SelectedStorageClient.parseBlobUri(blobUri).key))
      .toLong()

  private suspend fun listUploads(): List<RawImpressionUpload> =
    uploadsStub
      .listRawImpressionUploads(
        listRawImpressionUploadsRequest {
          parent = DATA_PROVIDER
          filter = ListRawImpressionUploadsRequestKt.filter {}
        }
      )
      .rawImpressionUploadsList

  private suspend fun listUploadFiles(upload: String) =
    filesStub
      .listRawImpressionUploadFiles(listRawImpressionUploadFilesRequest { parent = upload })
      .rawImpressionUploadFilesList

  private suspend fun listModelLines(upload: String) =
    modelLineRowsStub
      .listRawImpressionUploadModelLines(
        listRawImpressionUploadModelLinesRequest { parent = upload }
      )
      .rawImpressionUploadModelLinesList

  private suspend fun listRankIndexBlobs(upload: String, showDeleted: Boolean = false) =
    rankIndexBlobsStub
      .listRankIndexBlobs(
        listRankIndexBlobsRequest {
          parent = upload
          this.showDeleted = showDeleted
        }
      )
      .rankIndexBlobsList

  private suspend fun assertSnapshotsEvicted(upload: String, originals: List<RankIndexBlob>) {
    val afterEviction = listRankIndexBlobs(upload, showDeleted = true).associateBy { it.name }
    assertThat(afterEviction.keys).containsExactlyElementsIn(originals.map { it.name })
    for (original in originals) {
      val current = afterEviction.getValue(original.name)
      if (original.blobType == RankIndexBlob.BlobType.SNAPSHOT) {
        assertThat(current.hasDeleteTime()).isTrue()
      } else {
        assertThat(current.hasDeleteTime()).isFalse()
      }
    }
  }

  private suspend fun listMetadata(showDeleted: Boolean = false): List<ImpressionMetadata> =
    impressionMetadataStub
      .listImpressionMetadata(
        listImpressionMetadataRequest {
          parent = DATA_PROVIDER
          this.showDeleted = showDeleted
        }
      )
      .impressionMetadataList

  private suspend fun listAvailabilityTasks(upload: String): List<DataAvailabilitySyncTask> =
    availabilityTasksStub
      .listDataAvailabilitySyncTasks(listDataAvailabilitySyncTasksRequest { parent = upload })
      .dataAvailabilitySyncTasksList

  private suspend fun assertAvailabilityTaskIdentities(
    upload: RawImpressionUpload,
    eventDate: LocalDate,
  ) {
    val tasks = listAvailabilityTasks(upload.name)
    for (task in tasks) {
      val expectedDoneUri =
        canonicalGcsUri(
          LabeledImpressionsBlobKeys.forDoneUri(outputPrefix, task.cmmsModelLine, eventDate)
        )
      val expectedGeneration =
        checkNotNull(fileStorage.getFreshnessToken(blobKey(expectedDoneUri))).toLong()
      val expectedPathHash = VidLabelingTraceAttributes.gcsObjectPathHash(expectedDoneUri)
      val expectedId = RequestIds.forDataAvailabilitySyncTask(expectedPathHash, expectedGeneration)
      assertThat(task.name).isEqualTo("${upload.name}/dataAvailabilitySyncTasks/$expectedId")
      assertThat(task.doneBlobUri).isEqualTo(expectedDoneUri)
      assertThat(task.doneBlobGeneration).isEqualTo(expectedGeneration)
      assertThat(task.doneBlobPathHash).isEqualTo(expectedPathHash)
      assertThat(task.eventDate.year).isEqualTo(eventDate.year)
      assertThat(task.eventDate.month).isEqualTo(eventDate.monthValue)
      assertThat(task.eventDate.day).isEqualTo(eventDate.dayOfMonth)
    }
  }

  private suspend fun assertEveryRegisteredRawGenerationWasRead() {
    val registeredFiles = listUploads().flatMap { listUploadFiles(it.name) }
    for (file in registeredFiles) {
      assertThat(GenerationMatchedTestHadoopFileSystem.recordedGenerations(file.blobUri))
        .contains(file.blobGeneration)
    }
  }

  private fun sidecarUri(inputBlobUri: String, modelLine: String, eventDate: LocalDate): String =
    LabeledImpressionsBlobKeys.forInputUri(outputPrefix, inputBlobUri, modelLine, eventDate) +
      ".metadata.binpb"

  private fun blobKey(blobUri: String): String = SelectedStorageClient.parseBlobUri(blobUri).key

  private fun canonicalGcsUri(blobUri: String): String {
    val parsed = SelectedStorageClient.parseBlobUri(blobUri)
    return "gs://${parsed.bucket}/${parsed.key}"
  }

  private suspend fun snapshotOutputArtifacts(
    metadata: Collection<ImpressionMetadata>
  ): Map<String, OutputArtifact> = metadata.associate { it.blobUri to snapshotOutputArtifact(it) }

  private suspend fun snapshotOutputArtifact(metadata: ImpressionMetadata): OutputArtifact {
    val sidecarBlob = checkNotNull(fileStorage.getBlob(blobKey(metadata.blobUri)))
    val dataUri = BlobDetails.parseFrom(sidecarBlob.read().flatten()).blobUri
    return OutputArtifact(
      dataUri,
      checkNotNull(fileStorage.getFreshnessToken(blobKey(metadata.blobUri))),
      checkNotNull(fileStorage.getFreshnessToken(blobKey(dataUri))),
    )
  }

  private suspend fun assertMetadataMatchesSidecar(metadata: ImpressionMetadata) {
    val details =
      BlobDetails.parseFrom(
        checkNotNull(fileStorage.getBlob(blobKey(metadata.blobUri))).read().flatten()
      )
    assertThat(metadata.modelLine).isEqualTo(details.modelLine)
    assertThat(metadata.interval).isEqualTo(details.interval)
    assertThat(metadata.eventGroupReferenceId).isEqualTo(details.eventGroupReferenceId)
    assertThat(metadata.entityKeysList.map { it.entityType to it.entityId })
      .containsExactlyElementsIn(
        details.entityKeysList.flatMap { group ->
          group.entityIdsList.map { entityId -> group.entityType to entityId }
        }
      )
  }

  private suspend fun assertAvailabilityPublished(
    modelLines: Set<String>,
    eventDates: Set<LocalDate>,
  ) {
    assertThat(dataProvidersService.requests).isNotEmpty()
    assertThat(dataProvidersService.requests.map { it.name }.toSet()).containsExactly(DATA_PROVIDER)
    val publishedIntervals =
      dataProvidersService.requests.last().dataAvailabilityIntervalsList.associate {
        it.key to it.value
      }
    assertThat(publishedIntervals.keys).containsExactlyElementsIn(modelLines)
    val expectedStart = eventDates.min().atStartOfDay(ZoneOffset.UTC).toInstant().toProtoTime()
    val expectedEnd = eventDates.max().atStartOfDay(ZoneOffset.UTC).toInstant().toProtoTime()
    for (modelLine in modelLines) {
      val interval = publishedIntervals.getValue(modelLine)
      assertThat(interval.startTime).isEqualTo(expectedStart)
      assertThat(interval.endTime).isEqualTo(expectedEnd)
    }

    for (modelLine in modelLines) {
      for (eventDate in eventDates) {
        val doneKey =
          blobKey(LabeledImpressionsBlobKeys.forDoneUri(outputPrefix, modelLine, eventDate))
        val doneBlob = checkNotNull(metadataStorage.getBlob(doneKey))
        assertThat(DataAvailabilityBlobs.isSynced(doneBlob)).isTrue()
        assertThat(DataAvailabilityBlobs.isDataAvailabilityPublished(doneBlob)).isTrue()
      }
    }
    for (metadata in listMetadata()) {
      val sidecarBlob = checkNotNull(metadataStorage.getBlob(blobKey(metadata.blobUri)))
      assertThat(sidecarBlob.metadata)
        .containsEntry(WatchedBlobs.IMPRESSION_METADATA_RESOURCE_ID_KEY, metadata.name)
      assertThat(DataAvailabilityBlobs.isSynced(sidecarBlob)).isTrue()
    }
  }

  private suspend fun assertCompletedForBothPaths(upload: RawImpressionUpload) {
    assertThat(listModelLines(upload.name).associate { it.cmmsModelLine to it.state })
      .containsExactly(
        MEMOIZED_MODEL_LINE,
        RawImpressionUploadModelLine.State.COMPLETED,
        DIRECT_MODEL_LINE,
        RawImpressionUploadModelLine.State.COMPLETED,
      )
    assertThat(
        rankIndexBlobsStub
          .listRankIndexBlobs(listRankIndexBlobsRequest { parent = upload.name })
          .rankIndexBlobsList
          .map { it.cmmsModelLine }
      )
      .contains(MEMOIZED_MODEL_LINE)
  }

  private suspend fun assertReadablePeople(modelLine: String, expectedPeople: Set<String>) {
    val metadata = listMetadata().filter { it.modelLine == modelLine }
    val labeledEvents =
      metadata.flatMap { row ->
        val sidecarKey = SelectedStorageClient.parseBlobUri(row.blobUri).key
        val details =
          BlobDetails.parseFrom(checkNotNull(fileStorage.getBlob(sidecarKey)).read().flatten())
        StorageEventReader(
            blobDetails = details,
            kmsClient = kmsClient,
            impressionsStorageConfig = StorageConfig(rootDirectory = File("/")),
            descriptor = TestEvent.getDescriptor(),
          )
          .readEvents()
          .toList()
          .flatten()
      }
    val people =
      labeledEvents.flatMap { event ->
        event.entityKeys.filter { it.entityType == "person" }.map { it.entityId }
      }
    assertThat(people).containsExactlyElementsIn(expectedPeople)
    val vids = labeledEvents.map { it.vid }
    assertThat(vids.all { it in EXPECTED_VID_RANGE }).isTrue()
  }

  private suspend fun awaitPipelineIdle() {
    workItemTransport.awaitIdle()
    drainAvailabilityTasks()
    endpointFailure.getAndSet(null)?.let {
      throw AssertionError("Finalized-object delivery failed", it)
    }
  }

  private suspend fun drainAvailabilityTasks() {
    while (true) {
      val publishedCount = availabilityPublicationRunner.publishPendingTasks()
      availabilityTransport.drain(availabilityTaskProcessor::process)
      if (publishedCount == 0 && availabilityTransport.pendingCount == 0) return
    }
  }

  private suspend fun publishTask(taskName: String) {
    while (true) {
      check(availabilityPublicationRunner.publishPendingTasks() == 1)
      val publishedTaskName = availabilityTransport.takeNext()
      if (publishedTaskName == taskName) return
      availabilityTaskProcessor.process(publishedTaskName)
    }
  }

  private suspend fun outputGenerations(modelLine: String): Map<String, String> =
    listMetadata()
      .filter { it.modelLine == modelLine }
      .associate { row ->
        val dataUri =
          BlobDetails.parseFrom(
              checkNotNull(fileStorage.getBlob(SelectedStorageClient.parseBlobUri(row.blobUri).key))
                .read()
                .flatten()
            )
            .blobUri
        dataUri to
          checkNotNull(
            fileStorage.getFreshnessToken(SelectedStorageClient.parseBlobUri(dataUri).key)
          )
      }

  private fun vidLabelerParamsTemplate(): VidLabelerParams = vidLabelerParams {
    dataProvider = DATA_PROVIDER
    rawImpressionsStorageParams =
      VidLabelerParamsKt.storageParams { impressionsBlobPrefix = rawPrefix }
    vidLabeledImpressionsStorageParams =
      VidLabelerParamsKt.storageParams { impressionsBlobPrefix = outputPrefix }
    modelStorageParams =
      VidLabelerParamsKt.storageParams {
        impressionsBlobPrefix = modelBlobUri.substringBeforeLast('/')
      }
  }

  private fun subpoolAssignerParamsTemplate() = subpoolAssignerParams {
    dataProvider = DATA_PROVIDER
    rawImpressionStorageParams = SubpoolAssignerParamsKt.storageParams { blobPrefix = rawPrefix }
    vidLabeledImpressionsStorageParams =
      SubpoolAssignerParamsKt.storageParams { blobPrefix = outputPrefix }
    subpoolMapStorageParams =
      SubpoolAssignerParamsKt.storageParams { blobPrefix = "file:///$fileBucket/$rootKey/subpool" }
    vidRankMapStorageParams =
      SubpoolAssignerParamsKt.storageParams { blobPrefix = "file:///$fileBucket/$rootKey/rank" }
    modelStorageParams =
      SubpoolAssignerParamsKt.storageParams { blobPrefix = modelBlobUri.substringBeforeLast('/') }
    maxFileBatchSizeBytes = 10_000_000L
  }

  private fun modelLineConfig(): VidLabelerParams.ModelLineConfig =
    VidLabelerParamsKt.modelLineConfig {
      labelerInputFieldMapping += labelerInputFieldMapping {
        fieldPath = "event_id.id"
        scalar = scalarColumn { column = EVENT_ID_COLUMN }
      }
      labelerInputFieldMapping += labelerInputFieldMapping {
        fieldPath = "timestamp_usec"
        scalar = scalarColumn { column = EVENT_TIME_COLUMN }
      }
      labelerInputFieldMapping += labelerInputFieldMapping {
        fieldPath = "profile_info.proprietary_id_space_1_user_info.user_id"
        scalar = scalarColumn { column = PERSON_ID_COLUMN }
      }
      labelerInputFieldMapping += labelerInputFieldMapping {
        fieldPath = "profile_info.proprietary_id_space_1_user_info.demo.demo_bucket.gender"
        enumLookup = enumLookup {
          column = GENDER_COLUMN
          lookupTable["MALE"] = Gender.GENDER_MALE.name
        }
      }
      labelerInputFieldMapping += labelerInputFieldMapping {
        fieldPath = "profile_info.proprietary_id_space_1_user_info.demo.demo_bucket.age"
        ageRange = ageRange {
          bucketLookup = bucketLookup {
            column = AGE_GROUP_COLUMN
            bucketTable["YEARS_18_TO_34"] = ageBucket {
              minAge = 16
              maxAge = 34
            }
          }
        }
      }
      optionalEntityKeyFieldMapping["person"] = PERSON_ID_COLUMN
      populationSpecBlobUri = "file:///unused/population-spec"
      eventTemplateDescriptorBlobUri = "file:///unused/event-template-descriptor"
      eventTemplateType = TestEvent.getDescriptor().fullName
    }

  private class RecordingDataProvidersService :
    DataProvidersGrpcKt.DataProvidersCoroutineImplBase() {
    val requests = mutableListOf<ReplaceDataAvailabilityIntervalsRequest>()

    override suspend fun replaceDataAvailabilityIntervals(
      request: ReplaceDataAvailabilityIntervalsRequest
    ): DataProvider {
      synchronized(requests) { requests += request }
      return dataProvider {
        name = request.name
        dataAvailabilityIntervals += request.dataAvailabilityIntervalsList
      }
    }
  }

  private class InProcessAvailabilityTaskTransport : DataAvailabilitySyncTaskPublisher {
    private val pending = ArrayDeque<String>()
    val deliveryCounts = mutableMapOf<String, Int>()
    val lostResponseTaskNames = mutableSetOf<String>()
    var failBeforeNextPublication = false
    var loseNextResponse = false
    var duplicateNextDelivery = false
    var redeliverAfterNextFailure = false
    var beforeNextRedelivery: (() -> Unit)? = null

    val pendingCount: Int
      get() = pending.size

    fun takeNext(): String = pending.removeFirst()

    override suspend fun publish(taskName: String) {
      if (failBeforeNextPublication) {
        failBeforeNextPublication = false
        throw IOException("injected publication failure")
      }
      pending.addLast(taskName)
      if (duplicateNextDelivery) {
        duplicateNextDelivery = false
        pending.addLast(taskName)
      }
      if (loseNextResponse) {
        loseNextResponse = false
        lostResponseTaskNames += taskName
        throw IOException("injected lost publication response")
      }
    }

    suspend fun drain(process: suspend (String) -> Unit) {
      while (pending.isNotEmpty()) {
        val taskName = pending.removeFirst()
        deliveryCounts[taskName] = deliveryCounts.getOrDefault(taskName, 0) + 1
        try {
          process(taskName)
        } catch (e: Exception) {
          pending.addFirst(taskName)
          if (redeliverAfterNextFailure) {
            redeliverAfterNextFailure = false
            beforeNextRedelivery?.invoke()
            beforeNextRedelivery = null
            continue
          }
          throw e
        }
      }
    }
  }

  private class LostSuccessfulResponseInterceptor : ClientInterceptor {
    val loseNextCreateResponse = AtomicBoolean()
    val loseNextMarkRunningResponse = AtomicBoolean()
    val loseNextMarkSucceededResponse = AtomicBoolean()
    val droppedCreateResponses = AtomicInteger()
    val droppedMarkRunningResponses = AtomicInteger()
    val droppedMarkSucceededResponses = AtomicInteger()

    override fun <ReqT : Any, RespT : Any> interceptCall(
      method: MethodDescriptor<ReqT, RespT>,
      callOptions: CallOptions,
      next: GrpcChannel,
    ): ClientCall<ReqT, RespT> {
      val fault =
        when (method.bareMethodName) {
          "CreateDataAvailabilitySyncTask" -> loseNextCreateResponse to droppedCreateResponses
          "MarkDataAvailabilitySyncTaskRunning" ->
            loseNextMarkRunningResponse to droppedMarkRunningResponses
          "MarkDataAvailabilitySyncTaskSucceeded" ->
            loseNextMarkSucceededResponse to droppedMarkSucceededResponses
          else -> return next.newCall(method, callOptions)
        }
      return object :
        ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
          next.newCall(method, callOptions)
        ) {
        override fun start(responseListener: Listener<RespT>, headers: Metadata) {
          super.start(
            object :
              ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(
                responseListener
              ) {
              private var response: RespT? = null

              override fun onMessage(message: RespT) {
                response = message
              }

              override fun onClose(status: Status, trailers: Metadata) {
                if (status.isOk && fault.first.compareAndSet(true, false)) {
                  fault.second.incrementAndGet()
                  super.onClose(
                    Status.UNAVAILABLE.withDescription("injected lost successful response"),
                    trailers,
                  )
                  return
                }
                response?.let { super.onMessage(it) }
                super.onClose(status, trailers)
              }
            },
            headers,
          )
        }
      }
    }
  }

  private class MutableClock(private var instant: Instant) : Clock() {
    override fun instant(): Instant = instant

    override fun getZone() = ZoneOffset.UTC

    override fun withZone(zone: java.time.ZoneId): Clock = this

    fun advance(duration: Duration) {
      instant = instant.plus(duration)
    }
  }

  private data class OutputArtifact(
    val dataUri: String,
    val sidecarGeneration: String,
    val dataGeneration: String,
  )

  private class RecordingBlobMetadataStorageClient(private val delegate: StorageClient) :
    BlobMetadataStorageClient, StorageClient by delegate {
    private data class BlobVersion(val blobKey: String, val freshnessToken: String)

    private val metadata = ConcurrentHashMap<BlobVersion, Map<String, String>>()
    private val pauseNextSyncStart = AtomicBoolean()
    private var pausedSyncStart = CompletableDeferred<Unit>()
    private var pausedSyncRelease = CompletableDeferred<Unit>()

    fun pauseNextSyncStart() {
      check(pauseNextSyncStart.compareAndSet(false, true))
      pausedSyncStart = CompletableDeferred()
      pausedSyncRelease = CompletableDeferred()
    }

    suspend fun awaitPausedSyncStart() {
      pausedSyncStart.await()
    }

    fun releasePausedSyncStart() {
      pausedSyncRelease.complete(Unit)
    }

    override suspend fun getBlob(blobKey: String): StorageClient.Blob? =
      delegate.getBlob(blobKey)?.let(::withMetadata)

    override suspend fun listBlobs(prefix: String?): Flow<StorageClient.Blob> =
      delegate.listBlobs(prefix).map(::withMetadata)

    override suspend fun updateBlobMetadata(
      blobKey: String,
      customCreateTime: Instant?,
      metadata: Map<String, String>,
    ) {
      if (
        DataAvailabilityBlobs.SYNC_ID_KEY in metadata &&
          pauseNextSyncStart.compareAndSet(true, false)
      ) {
        pausedSyncStart.complete(Unit)
        pausedSyncRelease.await()
      }
      val blob = checkNotNull(delegate.getBlob(blobKey)) { "Blob does not exist: $blobKey" }
      this.metadata.compute(versionOf(blob)) { _, current -> current.orEmpty() + metadata }
    }

    private fun withMetadata(blob: StorageClient.Blob): StorageClient.Blob =
      object : StorageClient.Blob by blob {
        override val storageClient: StorageClient
          get() = this@RecordingBlobMetadataStorageClient

        override val metadata: Map<String, String>
          get() = this@RecordingBlobMetadataStorageClient.metadata[versionOf(blob)].orEmpty()
      }

    private fun versionOf(blob: StorageClient.Blob): BlobVersion =
      BlobVersion(
        blob.blobKey,
        (blob as? ConditionalOperationStorageClient.Blob)?.freshnessToken
          ?: "${blob.createTime}:${blob.updateTime}",
      )
  }

  /** Adds generation-precondition semantics to the single-process filesystem test backend. */
  private class GenerationEnforcingStorageClient(
    private val delegate: ConditionalOperationStorageClient
  ) : ConditionalOperationStorageClient {
    override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob =
      wrap(delegate.writeBlob(blobKey, content))

    override suspend fun getBlob(blobKey: String): StorageClient.Blob? =
      delegate.getBlob(blobKey)?.let { wrap(it) }

    override suspend fun listBlobs(prefix: String?): Flow<StorageClient.Blob> =
      delegate.listBlobs(prefix).map { wrap(it) }

    override suspend fun listBlobKeysAndPrefixes(prefix: String): Flow<String> =
      delegate.listBlobKeysAndPrefixes(prefix)

    override suspend fun getFreshnessToken(blobKey: String): String? =
      delegate.getFreshnessToken(blobKey)

    override suspend fun writeBlobIfUnchanged(
      blob: StorageClient.Blob,
      content: Flow<ByteString>,
    ): StorageClient.Blob {
      require(blob is ConditionalOperationStorageClient.Blob && blob.storageClient === this)
      return writeBlobIfUnchanged(blob.blobKey, blob.freshnessToken, content)
    }

    override suspend fun writeBlobIfUnchanged(
      blobKey: String,
      freshnessToken: String,
      content: Flow<ByteString>,
    ): StorageClient.Blob = wrap(delegate.writeBlobIfUnchanged(blobKey, freshnessToken, content))

    override suspend fun writeBlobIfNotFound(
      blobKey: String,
      content: Flow<ByteString>,
    ): StorageClient.Blob = wrap(delegate.writeBlobIfNotFound(blobKey, content))

    private suspend fun wrap(blob: StorageClient.Blob): StorageClient.Blob {
      val freshnessToken = (blob as ConditionalOperationStorageClient.Blob).freshnessToken
      return object : ConditionalOperationStorageClient.Blob {
        override val storageClient: StorageClient
          get() = this@GenerationEnforcingStorageClient

        override val blobKey: String = blob.blobKey
        override val size: Long = blob.size
        override val createTime: Instant = blob.createTime
        override val updateTime: Instant = blob.updateTime
        override val metadata: Map<String, String> = blob.metadata
        override val freshnessToken: String = freshnessToken

        override fun read(): Flow<ByteString> = flow {
          verifyGeneration()
          emitAll(blob.read())
        }

        override suspend fun delete() {
          verifyGeneration()
          blob.delete()
        }

        private suspend fun verifyGeneration() {
          val current = delegate.getFreshnessToken(blobKey)
          if (current != freshnessToken) {
            throw BlobChangedException(
              "Blob $blobKey is at generation $current, not $freshnessToken"
            )
          }
        }
      }
    }
  }

  private class UriNormalizingStorageClient(
    private val delegate: ConditionalOperationStorageClient
  ) : ConditionalOperationStorageClient {
    override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob =
      delegate.writeBlob(normalize(blobKey), content)

    override suspend fun getBlob(blobKey: String): StorageClient.Blob? =
      delegate.getBlob(normalize(blobKey))

    override suspend fun listBlobs(prefix: String?): Flow<StorageClient.Blob> =
      delegate.listBlobs(prefix?.let(::normalize))

    override suspend fun getFreshnessToken(blobKey: String): String? =
      delegate.getFreshnessToken(normalize(blobKey))

    override suspend fun writeBlobIfUnchanged(
      blob: StorageClient.Blob,
      content: Flow<ByteString>,
    ): StorageClient.Blob =
      delegate.writeBlobIfUnchanged(
        normalize(blob.blobKey),
        (blob as ConditionalOperationStorageClient.Blob).freshnessToken,
        content,
      )

    override suspend fun writeBlobIfUnchanged(
      blobKey: String,
      freshnessToken: String,
      content: Flow<ByteString>,
    ): StorageClient.Blob =
      delegate.writeBlobIfUnchanged(normalize(blobKey), freshnessToken, content)

    override suspend fun writeBlobIfNotFound(
      blobKey: String,
      content: Flow<ByteString>,
    ): StorageClient.Blob = delegate.writeBlobIfNotFound(normalize(blobKey), content)

    private fun normalize(blobKey: String): String =
      if ("://" in blobKey) SelectedStorageClient.parseBlobUri(blobKey).key else blobKey
  }

  private class SuspendingHttpHandler(
    private val onFailure: (Throwable) -> Unit,
    private val block: suspend (HttpExchange) -> Unit,
  ) : HttpHandler {
    override fun handle(exchange: HttpExchange) {
      try {
        runBlocking { block(exchange) }
        exchange.sendResponseHeaders(200, -1)
      } catch (e: Exception) {
        onFailure(e)
        exchange.sendResponseHeaders(500, 0)
        exchange.responseBody.use { it.write(e.stackTraceToString().toByteArray()) }
        return
      }
      exchange.close()
    }
  }

  private class InProcessWorkItemTransport : QueueSubscriber {
    private val redeliveryScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val channels =
      ConcurrentHashMap<String, Channel<QueueSubscriber.QueueMessage<WorkItem>>>()
    private val workItems = ConcurrentHashMap<String, WorkItem>()
    private val attempts = ConcurrentHashMap<String, WorkItemAttempt>()
    private val attemptParents = ConcurrentHashMap<String, String>()
    private val failures = ConcurrentHashMap<String, String>()
    private val pending = AtomicInteger()
    private val changes = MutableStateFlow(0L)
    private val sequence = AtomicInteger()
    private val forceAcknowledgementRedelivery = AtomicInteger()
    private val forcedRetries = AtomicInteger()
    private val forcedRetryWorkItems = ConcurrentHashMap.newKeySet<String>()
    val publishedCount: Int
      get() = sequence.get()

    val forcedRetryCount: Int
      get() = forcedRetries.get()

    val forcedRetryWorkItemNames: Set<String>
      get() = forcedRetryWorkItems.toSet()

    fun forceNextAcknowledgementRedelivery() {
      check(forceAcknowledgementRedelivery.compareAndSet(0, 1))
    }

    val workItemsService =
      object : WorkItemsGrpcKt.WorkItemsCoroutineImplBase() {
        override suspend fun createWorkItem(request: CreateWorkItemRequest): WorkItem {
          val name = "workItems/${request.workItemId}"
          val created =
            request.workItem
              .toBuilder()
              .setName(name)
              .setState(WorkItem.State.QUEUED)
              .setGeneration(1L)
              .build()
          if (workItems.putIfAbsent(name, created) != null) {
            throw Status.ALREADY_EXISTS.asRuntimeException()
          }
          publish(created.queue, created)
          return created
        }

        override suspend fun ensureWorkItem(request: EnsureWorkItemRequest): WorkItem {
          val name = "workItems/${request.workItemId}"
          val existing = workItems[name]
          if (existing != null) return existing
          return createWorkItem(
            CreateWorkItemRequest.newBuilder()
              .setWorkItemId(request.workItemId)
              .setWorkItem(request.workItem)
              .build()
          )
        }

        override suspend fun getWorkItem(request: GetWorkItemRequest): WorkItem =
          workItems[request.name] ?: throw Status.NOT_FOUND.asRuntimeException()
      }

    val workItemAttemptsService =
      object : WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase() {
        override suspend fun createWorkItemAttempt(
          request: CreateWorkItemAttemptRequest
        ): WorkItemAttempt {
          val item = workItems[request.parent] ?: throw Status.NOT_FOUND.asRuntimeException()
          if (item.state != WorkItem.State.QUEUED) {
            if (item.state == WorkItem.State.SUCCEEDED && item.name in forcedRetryWorkItems) {
              forcedRetries.incrementAndGet()
            }
            throw WorkItemInvalidStateException(item.name, item.state.name)
              .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
          }
          check(
            !request.hasExpectedWorkItemGeneration() ||
              request.expectedWorkItemGeneration == item.generation
          )
          workItems[request.parent] = item.toBuilder().setState(WorkItem.State.RUNNING).build()
          val attempt =
            WorkItemAttempt.newBuilder()
              .setName("${request.parent}/workItemAttempts/${request.workItemAttemptId}")
              .setState(WorkItemAttempt.State.ACTIVE)
              .build()
          attempts[attempt.name] = attempt
          attemptParents[attempt.name] = request.parent
          return attempt
        }

        override suspend fun completeWorkItemAttempt(
          request: CompleteWorkItemAttemptRequest
        ): WorkItemAttempt {
          val attempt = attempts[request.name] ?: throw Status.NOT_FOUND.asRuntimeException()
          val completed = attempt.toBuilder().setState(WorkItemAttempt.State.SUCCEEDED).build()
          attempts[request.name] = completed
          val parent = request.name.substringBefore("/workItemAttempts/")
          workItems.computeIfPresent(parent) { _, item ->
            item.toBuilder().setState(WorkItem.State.SUCCEEDED).build()
          }
          return completed
        }

        override suspend fun failWorkItemAttempt(
          request: FailWorkItemAttemptRequest
        ): WorkItemAttempt {
          val attempt = attempts[request.name] ?: throw Status.NOT_FOUND.asRuntimeException()
          val failed = attempt.toBuilder().setState(WorkItemAttempt.State.FAILED).build()
          attempts[request.name] = failed
          val parent = checkNotNull(attemptParents[request.name])
          workItems.computeIfPresent(parent) { _, item ->
            item.toBuilder().setState(WorkItem.State.QUEUED).build()
          }
          failures[parent] = request.errorMessage
          return failed
        }
      }

    private suspend fun publish(queueName: String, workItem: WorkItem) {
      pending.incrementAndGet()
      enqueue(queueName, workItem)
      signalChange()
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T : Message> subscribe(
      subscriptionId: String,
      parser: Parser<T>,
    ): ReceiveChannel<QueueSubscriber.QueueMessage<T>> =
      channel(subscriptionId) as ReceiveChannel<QueueSubscriber.QueueMessage<T>>

    private fun enqueue(queueName: String, workItem: WorkItem) {
      val ackId = "in-process-${sequence.incrementAndGet()}"
      val consumer =
        object : MessageConsumer {
          override fun ack() {
            if (forceAcknowledgementRedelivery.compareAndSet(1, 0)) {
              forcedRetryWorkItems += workItem.name
              scheduleRedelivery(queueName, workItem, "$ackId-forced-retry", this)
              return
            }
            failures.remove(workItem.name)
            pending.decrementAndGet()
            signalChange()
          }

          override fun nack() {
            if (attempts.values.count { attemptParents[it.name] == workItem.name } >= 3) {
              pending.decrementAndGet()
            } else {
              scheduleRedelivery(queueName, workItem, "$ackId-retry", this)
            }
            signalChange()
          }
        }
      channel(queueName).trySend(QueueSubscriber.QueueMessage(workItem, ackId, consumer))
    }

    private fun scheduleRedelivery(
      queueName: String,
      workItem: WorkItem,
      ackId: String,
      consumer: MessageConsumer,
    ) {
      redeliveryScope.launch {
        delay(REDELIVERY_DELAY_MILLIS)
        channel(queueName).send(QueueSubscriber.QueueMessage(workItem, ackId, consumer))
        signalChange()
      }
    }

    private fun channel(queueName: String) =
      channels.computeIfAbsent(queueName) { Channel(Channel.UNLIMITED) }

    private fun signalChange() {
      changes.value = changes.value + 1
    }

    suspend fun awaitIdle() {
      withTimeout(120_000L) {
        while (true) {
          val observed = changes.value
          if (pending.get() == 0) break
          changes.first { it != observed }
        }
      }
      check(failures.isEmpty()) { failures.values.joinToString(separator = "\n") }
    }

    override fun close() {
      redeliveryScope.cancel()
      channels.values.forEach { it.close() }
    }
  }

  private object ImmediateThrottler : Throttler {
    override suspend fun <T> onReady(block: suspend () -> T): T = block()
  }

  companion object {
    init {
      AeadConfig.register()
    }

    @ClassRule @JvmField val spannerEmulator = SpannerEmulatorRule()

    private const val DATA_PROVIDER = "dataProviders/dp1"
    private const val MODEL_SUITE = "modelProviders/mp1/modelSuites/ms1"
    private const val MEMOIZED_MODEL_LINE = "$MODEL_SUITE/modelLines/memoized"
    private const val DIRECT_MODEL_LINE = "$MODEL_SUITE/modelLines/direct"
    private const val MEMOIZED_RELEASE = "$MODEL_SUITE/modelReleases/memoized"
    private const val DIRECT_RELEASE = "$MODEL_SUITE/modelReleases/direct"
    private const val POOL_ASSIGNER_QUEUE = "queues/pool-assigner"
    private const val RANK_BUILDER_QUEUE = "queues/rank-builder"
    private const val VID_LABELER_QUEUE = "queues/vid-labeler"
    private const val KEK_URI = "fake-kms://vid-labeling"
    private const val EVENT_ID_COLUMN = "event_id"
    private const val EVENT_TIME_COLUMN = "event_time_usec"
    private const val PERSON_ID_COLUMN = "person_id"
    private const val GENDER_COLUMN = "person_gender"
    private const val AGE_GROUP_COLUMN = "person_age_group"
    private const val DATA_WATCHER_PATH_HEADER = "X-DataWatcher-Path"
    private const val DATA_WATCHER_GENERATION_HEADER = "X-DataWatcher-Generation"
    private const val OVERRIDE_MODEL_LINES_HEADER = "X-Override-Model-Lines"
    private const val RECOVERY_SOURCE_UPLOAD_HEADER = "X-Recovery-Source-Upload"
    private const val EVICTION_OPERATION_ID_HEADER = "X-Eviction-Operation-Id"
    private const val REDELIVERY_DELAY_MILLIS = 50L
    private val EXPECTED_VID_RANGE = 10_000L..10_099L
    private val EVENT_DATE: LocalDate = LocalDate.of(2026, 9, 1)
    private val NOW: Instant = EVENT_DATE.plusDays(2).atStartOfDay(ZoneOffset.UTC).toInstant()

    private val POPULATION_SPEC: PopulationSpec = populationSpec {
      val buckets =
        listOf(
          Triple(Person.Gender.MALE, Person.AgeGroup.YEARS_18_TO_34, 10_000L),
          Triple(Person.Gender.MALE, Person.AgeGroup.YEARS_35_TO_54, 10_100L),
          Triple(Person.Gender.MALE, Person.AgeGroup.YEARS_55_PLUS, 10_200L),
          Triple(Person.Gender.FEMALE, Person.AgeGroup.YEARS_18_TO_34, 10_300L),
          Triple(Person.Gender.FEMALE, Person.AgeGroup.YEARS_35_TO_54, 10_400L),
          Triple(Person.Gender.FEMALE, Person.AgeGroup.YEARS_55_PLUS, 10_500L),
        )
      for ((gender, age, start) in buckets) {
        subpopulations +=
          PopulationSpecKt.subPopulation {
            attributes +=
              ProtoAny.pack(
                person {
                  this.gender = gender
                  ageGroup = age
                  socialGradeGroup = Person.SocialGradeGroup.A_B_C1
                }
              )
            vidRanges +=
              PopulationSpecKt.vidRange {
                startVid = start
                endVidInclusive = start + 99L
              }
          }
      }
    }
  }
}

/**
 * Test-only `gs://` filesystem that maps the bucket to a local root and enforces the generation
 * qualifier used by the production raw-impression readers.
 */
class GenerationMatchedTestHadoopFileSystem : RawLocalFileSystem() {
  private val localFileSystem = RawLocalFileSystem()
  private var fileSystemUri: URI? = null

  override fun initialize(name: URI, configuration: Configuration) {
    fileSystemUri = URI(name.scheme, name.authority, "/", null, null)
    super.initialize(URI.create("file:///"), configuration)
    localFileSystem.initialize(URI.create("file:///"), configuration)
  }

  override fun getUri(): URI = fileSystemUri ?: URI.create("gs:///")

  override fun open(path: Path, bufferSize: Int): FSDataInputStream =
    localFileSystem.open(toLocalPath(path), bufferSize)

  override fun getFileStatus(path: Path): FileStatus =
    localFileSystem.getFileStatus(toLocalPath(path)).also { it.path = path }

  override fun pathToFile(path: Path): File = toLocalPath(path).toUri().let(::File)

  private fun toLocalPath(path: Path): Path {
    val uri = path.toUri()
    if (uri.scheme == "file") return path
    val rawPath = uri.path
    val generationAndPath =
      rawPath.takeIf { it.startsWith(GENERATION_PATH_PREFIX) }?.removePrefix(GENERATION_PATH_PREFIX)
        ?: throw IOException("Raw-impression read is missing a generation qualifier: $path")
    val generation = generationAndPath.substringBefore('/').toLongOrNull()
    val objectPath = generationAndPath.substringAfter('/', missingDelimiterValue = "")
    if (generation == null || generation <= 0L || objectPath.isEmpty()) {
      throw IOException("Invalid raw-impression generation in path: $path")
    }
    val blobUri = "gs://${checkNotNull(uri.authority)}/$objectPath"
    generationReads.computeIfAbsent(blobUri) { ConcurrentHashMap.newKeySet() }.add(generation)
    val file = File("/${checkNotNull(uri.authority)}/${objectPath.trimStart('/')}")
    if (file.lastModified() != generation) {
      throw IOException(
        "Raw-impression object $file no longer matches its registered generation $generation"
      )
    }
    return Path(file.toURI())
  }

  companion object {
    private val generationReads = ConcurrentHashMap<String, MutableSet<Long>>()

    fun resetRecordedReads() {
      generationReads.clear()
    }

    fun recordedGenerations(blobUri: String): Set<Long> = generationReads[blobUri].orEmpty()
  }
}
