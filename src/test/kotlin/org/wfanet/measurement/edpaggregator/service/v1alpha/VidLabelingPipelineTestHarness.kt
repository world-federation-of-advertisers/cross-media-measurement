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
import io.grpc.ForwardingServerCall
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.ServerInterceptors
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
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
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
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.config.securecomputation.WatchedPathKt.httpEndpointSink
import org.wfanet.measurement.config.securecomputation.watchedPath
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityBlobs
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySync
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySyncLeaseRunner
import org.wfanet.measurement.edpaggregator.dataavailability.GrpcDataAvailabilitySyncLeaseClient
import org.wfanet.measurement.edpaggregator.deploy.gcloud.dataavailability.DataAvailabilitySyncWorkItem
import org.wfanet.measurement.edpaggregator.deploy.gcloud.dataavailability.DataAvailabilitySyncWorkItemProcessor
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.InternalApiServices as EdpaInternalApiServices
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata as EdpaSchemata
import org.wfanet.measurement.edpaggregator.rawimpressions.EventIdDigest
import org.wfanet.measurement.edpaggregator.rawimpressions.EventIdDigestExtractor
import org.wfanet.measurement.edpaggregator.rawimpressions.GENERATION_PATH_PREFIX
import org.wfanet.measurement.edpaggregator.rawimpressions.RankIndexStore
import org.wfanet.measurement.edpaggregator.rawimpressions.RawImpressionFileMetadata
import org.wfanet.measurement.edpaggregator.rawimpressions.SubpoolFingerprintsStore
import org.wfanet.measurement.edpaggregator.rawimpressions.readEventDateFromFooter
import org.wfanet.measurement.edpaggregator.resultsfulfiller.StorageEventReader
import org.wfanet.measurement.edpaggregator.subpoolassigner.SubpoolAssignerApp
import org.wfanet.measurement.edpaggregator.subpoolassigner.VirtualPeoplePoolEmitLabeler
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceAttributes
import org.wfanet.measurement.edpaggregator.testing.TestEncryptedStorage
import org.wfanet.measurement.edpaggregator.testing.VidLabelingRpcThrottlersTestHelper
import org.wfanet.measurement.edpaggregator.tools.DispatchFailer
import org.wfanet.measurement.edpaggregator.tools.FailedDispatchRetrier
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncLeaseServiceGrpcKt
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
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFile
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
import org.wfanet.measurement.edpaggregator.v1alpha.VidRankBuilderParams
import org.wfanet.measurement.edpaggregator.v1alpha.ageBucket
import org.wfanet.measurement.edpaggregator.v1alpha.ageRange
import org.wfanet.measurement.edpaggregator.v1alpha.approveUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.bucketLookup
import org.wfanet.measurement.edpaggregator.v1alpha.enumLookup
import org.wfanet.measurement.edpaggregator.v1alpha.getRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.edpaggregator.v1alpha.labelerInputFieldMapping
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRankIndexBlobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadCorrectionCandidatesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listUploadHealingOperationsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineAvailabilitySynchronizedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.retryUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.scalarColumn
import org.wfanet.measurement.edpaggregator.v1alpha.subpoolAssignerParams
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelerParams
import org.wfanet.measurement.edpaggregator.vidlabeler.LabeledImpressionsBlobKeys
import org.wfanet.measurement.edpaggregator.vidlabeler.ParquetImpressionConverter
import org.wfanet.measurement.edpaggregator.vidlabeler.PopulationAttributeWriter
import org.wfanet.measurement.edpaggregator.vidlabeler.VidLabelerApp
import org.wfanet.measurement.edpaggregator.vidlabeler.VirtualPeopleVidAssigner
import org.wfanet.measurement.edpaggregator.vidlabeling.DataAvailabilitySyncWorkItems
import org.wfanet.measurement.edpaggregator.vidlabeling.RawImpressionBlobMetadata
import org.wfanet.measurement.edpaggregator.vidlabeling.RawImpressionUploadManifestClassifier
import org.wfanet.measurement.edpaggregator.vidlabeling.RequestIds
import org.wfanet.measurement.edpaggregator.vidlabeling.VidLabelingDispatchSequencer
import org.wfanet.measurement.edpaggregator.vidlabeling.VidLabelingDispatcher
import org.wfanet.measurement.edpaggregator.vidlabeling.VidLabelingMonitor
import org.wfanet.measurement.edpaggregator.vidlabeling.WorkItemIds
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.DoneBlobReplayer
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.EvictUploader
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.GrpcCorrectionCandidateCleaner
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.GrpcHealingOperationStore
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.RawImpressionUploadCorrectionPlanner
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.RecoverUploader
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.VidLabelingHealingController
import org.wfanet.measurement.edpaggregator.vidrankbuilder.EventIdDigestBytes
import org.wfanet.measurement.edpaggregator.vidrankbuilder.LastSeenDayBytes
import org.wfanet.measurement.edpaggregator.vidrankbuilder.SubpoolRanker
import org.wfanet.measurement.edpaggregator.vidrankbuilder.SubpoolRetention
import org.wfanet.measurement.edpaggregator.vidrankbuilder.VidRankBuilder
import org.wfanet.measurement.edpaggregator.vidrankbuilder.VidRankBuilderApp
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidate as InternalCandidate
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidateServiceGrpcKt as InternalCorrectionCandidateServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperationServiceGrpcKt as InternalUploadHealingOperationServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.createQuarantinedRawImpressionUploadRequest
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUpload as internalRawImpressionUpload
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUploadCorrectionCandidate as internalCandidate
import org.wfanet.measurement.internal.edpaggregator.registerDetectedRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.queue.MessageConsumer
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CompleteWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.EnsureWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.FailWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.FailWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.GetWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.RenewWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttempt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcher
import org.wfanet.measurement.securecomputation.datawatcher.WatchedBlobs
import org.wfanet.measurement.securecomputation.datawatcher.testing.DataWatcherSubscribingStorageClient
import org.wfanet.measurement.securecomputation.deploy.gcloud.testing.TestIdTokenProvider
import org.wfanet.measurement.securecomputation.service.WorkItemGenerationMismatchException
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

data class PipelineHarnessConfig(
  val extraModelLines: List<ModelLineFixture> = emptyList(),
  val initiallyVisibleModelLines: Set<String>? = null,
  val numberOfShards: Int = 1,
  val rankStripes: Int = 1,
  val maxFileBatchSizeBytes: Long = 10_000_000L,
  val retentionDays: Int = 30,
  val initialToday: LocalDate = LocalDate.of(2026, 9, 3),
)

data class ModelLineFixture(
  val name: String,
  val release: String,
  val memoized: Boolean,
  val activeStart: Instant,
  val activeEnd: Instant? = null,
)

data class RawEventFixture(
  val eventId: String,
  val personId: String,
  val eventDate: LocalDate,
  val gender: String = "MALE",
  val ageGroup: String = "YEARS_18_TO_34",
)

data class RankEntryFixture(
  val digest: EventIdDigest,
  val poolOffset: Long,
  val rank: Int,
  val lastSeen: LocalDate,
)

@RunWith(JUnit4::class)
abstract class VidLabelingPipelineTestHarness(
  protected val pipelineConfig: PipelineHarnessConfig = PipelineHarnessConfig()
) {
  protected val tempFolder = TemporaryFolder()
  protected val edpaDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, EdpaSchemata.EDP_AGGREGATOR_CHANGELOG_PATH)
  protected val workItemTransport = InProcessWorkItemTransport()
  protected val edpaInternalServer = GrpcTestServerRule {
    EdpaInternalApiServices.build(edpaDatabase.databaseClient, EmptyCoroutineContext)
      .toList()
      .forEach { addService(it) }
  }

  protected val modelLineFixtures =
    listOf(
      ModelLineFixture(
        MEMOIZED_MODEL_LINE,
        MEMOIZED_RELEASE,
        memoized = true,
        EVENT_DATE.minusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant(),
        EVENT_DATE.plusDays(10).atStartOfDay(ZoneOffset.UTC).toInstant(),
      ),
      ModelLineFixture(
        DIRECT_MODEL_LINE,
        DIRECT_RELEASE,
        memoized = false,
        EVENT_DATE.minusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant(),
        EVENT_DATE.plusDays(10).atStartOfDay(ZoneOffset.UTC).toInstant(),
      ),
    ) + pipelineConfig.extraModelLines
  protected val visibleModelLineNames = ConcurrentHashMap.newKeySet<String>()
  protected val today = AtomicReference(pipelineConfig.initialToday)
  protected val modelLines =
    modelLineFixtures.map { fixture ->
      modelLine {
        name = fixture.name
        type = ModelLine.Type.PROD
        activeStartTime = fixture.activeStart.toProtoTime()
        fixture.activeEnd?.let { activeEndTime = it.toProtoTime() }
      }
    }
  protected val modelLinesService =
    object : ModelLinesGrpcKt.ModelLinesCoroutineImplBase() {
      override suspend fun listModelLines(request: ListModelLinesRequest) = listModelLinesResponse {
        modelLines +=
          this@VidLabelingPipelineTestHarness.modelLines.filter { it.name in visibleModelLineNames }
      }

      override suspend fun getModelLine(request: GetModelLineRequest): ModelLine =
        modelLines.single { it.name == request.name }
    }
  protected val modelRolloutsService =
    object : ModelRolloutsGrpcKt.ModelRolloutsCoroutineImplBase() {
      override suspend fun listModelRollouts(request: ListModelRolloutsRequest) =
        listModelRolloutsResponse {
          modelRollouts += modelRollout {
            modelRelease = modelLineFixtures.single { it.name == request.parent }.release
          }
        }
    }
  protected val modelShardsService =
    object : ModelShardsGrpcKt.ModelShardsCoroutineImplBase() {
      override suspend fun listModelShards(request: ListModelShardsRequest) =
        listModelShardsResponse {
          modelShards +=
            modelLineFixtures.map { fixture ->
              modelShard {
                name = "$DATA_PROVIDER/modelShards/${fixture.name.substringAfterLast('/')}"
                modelRelease = fixture.release
                modelBlob = modelBlob { modelBlobPath = modelBlobUri }
                memoizedVidAssignmentEnabled = fixture.memoized
              }
            }
        }
    }
  protected val dataProvidersService = RecordingDataProvidersService()
  protected val edpaBeforeCallFaults = BeforeCallFaultInterceptor()
  protected val edpaPublicServer = GrpcTestServerRule {
    Services.build(edpaInternalServer.channel).toList().forEach {
      addService(ServerInterceptors.intercept(it, edpaBeforeCallFaults))
    }
    addService(modelLinesService)
    addService(modelRolloutsService)
    addService(modelShardsService)
    addService(dataProvidersService)
  }
  protected val workItemRpcFaults = BeforeCallFaultInterceptor()
  protected val workItemPublicServer = GrpcTestServerRule {
    addService(ServerInterceptors.intercept(workItemTransport.workItemsService, workItemRpcFaults))
    addService(
      ServerInterceptors.intercept(workItemTransport.workItemAttemptsService, workItemRpcFaults)
    )
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

  protected lateinit var rootKey: String
  protected lateinit var fileBucket: String
  protected lateinit var fileStorageRoot: File
  protected lateinit var rawPrefix: String
  protected lateinit var outputPrefix: String
  protected lateinit var externalOutputPrefix: String
  protected lateinit var modelBlobUri: String
  protected lateinit var kekUri: String
  protected lateinit var fileStorage: ConditionalOperationStorageClient
  protected lateinit var mapStorage: UriNormalizingStorageClient
  protected lateinit var rawEventStorage: DataWatcherSubscribingStorageClient
  protected lateinit var outputEventStorage: DataWatcherSubscribingStorageClient
  protected lateinit var metadataStorage: RecordingBlobMetadataStorageClient
  protected lateinit var kmsClient: KmsClient
  protected lateinit var rawWatcher: DataWatcher
  protected lateinit var outputWatcher: DataWatcher
  protected lateinit var httpServer: HttpServer
  protected lateinit var appScope: CoroutineScope
  protected val appJobs = mutableListOf<Job>()
  protected val endpointFailure = AtomicReference<Throwable?>()
  protected val externalAvailabilityDeliveries = AtomicInteger()
  private val outputDeleteHook = AtomicReference<OutputDeleteHook?>()

  protected lateinit var uploadsStub:
    RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
  protected lateinit var filesStub:
    RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
  protected lateinit var modelLineRowsStub:
    RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
  protected lateinit var rankIndexBlobsStub:
    RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
  protected lateinit var impressionMetadataStub:
    ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
  protected lateinit var correctionDetectionStub:
    InternalCorrectionCandidateServiceGrpcKt.RawImpressionUploadCorrectionCandidateServiceCoroutineStub
  protected lateinit var correctionCandidatesStub:
    RawImpressionUploadCorrectionCandidateServiceGrpcKt.RawImpressionUploadCorrectionCandidateServiceCoroutineStub
  protected lateinit var operationsStub:
    UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineStub
  protected lateinit var workItemsStub: WorkItemsGrpcKt.WorkItemsCoroutineStub
  protected lateinit var workItemAttemptsStub: WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
  protected lateinit var dispatchSequencer: VidLabelingDispatchSequencer
  protected lateinit var internalDataAvailabilitySync: DataAvailabilitySync
  private lateinit var availabilityWorkItemProcessor: DataAvailabilitySyncWorkItemProcessor

  @Before
  fun setUp() {
    AeadConfig.register()
    GenerationMatchedTestHadoopFileSystem.resetRecordedReads()
    visibleModelLineNames.clear()
    visibleModelLineNames +=
      pipelineConfig.initiallyVisibleModelLines ?: modelLineFixtures.map { it.name }
    today.set(pipelineConfig.initialToday)
    val absoluteRoot = tempFolder.root.toPath().toAbsolutePath().toString().removePrefix("/")
    fileBucket = absoluteRoot.substringBefore('/')
    rootKey = absoluteRoot.substringAfter('/')
    fileStorageRoot = File("/$fileBucket")
    rawPrefix = "gs://$fileBucket/$rootKey/raw"
    outputPrefix = "file:///$fileBucket/$rootKey/output"
    externalOutputPrefix = "gs://$fileBucket/$rootKey/output/external"
    modelBlobUri = "file:///$fileBucket/$rootKey/models/model.riegeli"
    kekUri = "fake-kms://vid-labeling/$rootKey"
    fileStorage = GenerationEnforcingStorageClient(FileSystemStorageClient(fileStorageRoot))
    mapStorage = UriNormalizingStorageClient(fileStorage)
    metadataStorage = RecordingBlobMetadataStorageClient(fileStorage)
    kmsClient = TestEncryptedStorage.buildFakeKmsClient(kekUri, keyTemplate = "AES128_GCM")

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
        numberOfShards = pipelineConfig.numberOfShards,
        modelLineConfigs = modelLineFixtures.associate { it.name to modelLineConfig },
        rawImpressionUploadFileStub = filesStub,
        vidLabelingJobStub =
          VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub(edpaPublicServer.channel),
        maxFileBatchSizeBytes = pipelineConfig.maxFileBatchSizeBytes,
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
        modelLineConfigs = modelLineFixtures.associate { it.name to modelLineConfig },
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

    availabilityWorkItemProcessor =
      DataAvailabilitySyncWorkItemProcessor(
        workItemsStub,
        workItemAttemptsStub,
        DataAvailabilitySyncLeaseRunner(
          GrpcDataAvailabilitySyncLeaseClient(
            DataAvailabilitySyncLeaseServiceGrpcKt.DataAvailabilitySyncLeaseServiceCoroutineStub(
              edpaPublicServer.channel
            )
          )
        ),
        synchronize = { workItem, lease, onStage ->
          val rawImpressionBlobUris =
            listUploadFiles(workItem.appParams.triggeringRawImpressionUpload)
              .filter { it.eventDate.toLocalDate() == workItem.eventDate }
              .map { it.blobUri }
          check(rawImpressionBlobUris.isNotEmpty())
          internalDataAvailabilitySync.sync(
            workItem.doneBlobUri,
            dataAvailabilitySyncLease = lease.name,
            doneBlobGeneration = workItem.doneBlobGeneration,
            discoveryMode =
              DataAvailabilitySync.DiscoveryMode.VidLabelerOutputs(
                rawImpressionUpload = workItem.appParams.triggeringRawImpressionUpload,
                rawImpressionBlobUris = rawImpressionBlobUris,
                modelLine = workItem.appParams.modelLine,
                eventDate = workItem.eventDate,
              ),
            onStage = onStage,
            ensureLeaseActive = lease::invoke,
          )
        },
        verifyDoneObject = { workItem ->
          val key = SelectedStorageClient.parseBlobUri(workItem.doneBlobUri).key
          val generation = checkNotNull(fileStorage.getFreshnessToken(key)).toLong()
          check(generation == workItem.doneBlobGeneration)
        },
        markAvailabilitySynchronized = { workItem ->
          modelLineRowsStub.markRawImpressionUploadModelLineAvailabilitySynchronized(
            markRawImpressionUploadModelLineAvailabilitySynchronizedRequest {
              name = workItem.rawImpressionUploadModelLineName
              eventDate = workItem.appParams.eventDate
              requestId =
                RequestIds.forMarkRawImpressionUploadModelLineAvailabilitySynchronized(
                  workItem.rawImpressionUploadModelLineName,
                  workItem.eventDate.toString(),
                )
            }
          )
        },
        activeAttemptRetryDelay = { delay(1L) },
        attemptLeaseRenewalDelay = { delay(1_000L) },
        attemptUpdateRetryDelay = {},
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

  protected fun buildHealingController(
    correctionRetention: Duration = Duration.ofDays(3650),
    dataProviderNames: List<String> = listOf(DATA_PROVIDER),
  ): VidLabelingHealingController {
    val evictUploader =
      EvictUploader(
        uploadsStub,
        modelLineRowsStub,
        rankIndexBlobsStub,
        filesStub,
        impressionMetadataStub,
        outputPrefix,
        getBlobGeneration = { blobUri ->
          fileStorage.getFreshnessToken(blobKey(blobUri))?.toLong()
        },
        deleteBlob = { blobUri, generation ->
          val key = blobKey(blobUri)
          val blob = fileStorage.getBlob(key) as? ConditionalOperationStorageClient.Blob
          if (blob?.freshnessToken?.toLong() != generation) {
            false
          } else {
            outputDeleteHook
              .get()
              ?.takeIf { it.blobUri == blobUri }
              ?.let { hook -> if (outputDeleteHook.compareAndSet(hook, null)) hook.block() }
            try {
              blob.delete()
              true
            } catch (_: BlobChangedException) {
              false
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
    val recoverUploader =
      RecoverUploader(uploadsStub, modelLineRowsStub, rankIndexBlobsStub) {
        doneBlobUri,
        expectedGeneration,
        metadata ->
        val key = blobKey(doneBlobUri)
        check(fileStorage.getFreshnessToken(key)?.toLong() == expectedGeneration)
        val blob = fileStorage.writeBlob(key, flowOf(ByteString.EMPTY))
        val generation =
          checkNotNull((blob as? ConditionalOperationStorageClient.Blob)?.freshnessToken).toLong()
        rawWatcher.receivePath(
          doneBlobUri,
          metadata + (DataWatcher.GENERATION_METADATA_KEY to generation.toString()),
        )
        generation
      }
    return VidLabelingHealingController(
      dataProviderNames.map { dataProviderName ->
        VidLabelingHealingController.DataProviderConfig(
          dataProviderName,
          if (dataProviderName == DATA_PROVIDER) outputPrefix
          else "$outputPrefix/${dataProviderName.substringAfterLast('/')}",
          correctionRetention,
        )
      },
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
      manifestReader = { doneBlobUri, doneBlobGeneration, persistedManifest ->
        val doneKey = blobKey(doneBlobUri)
        check(generationOf(doneBlobUri) == doneBlobGeneration)
        val prefix =
          doneKey.substringBeforeLast('/', missingDelimiterValue = "").let {
            if (it.isEmpty()) "" else "$it/"
          }
        val persistedByIdentity = persistedManifest.associateBy { it.blobUri to it.blobGeneration }
        val manifest =
          fileStorage
            .listBlobs(prefix)
            .toList()
            .filterNot { it.blobKey.substringAfterLast('/').equals("done", ignoreCase = true) }
            .map { blob ->
              val blobUri = "gs://$fileBucket/${blob.blobKey}"
              val generation =
                checkNotNull((blob as? ConditionalOperationStorageClient.Blob)?.freshnessToken)
                  .toLong()
              RawImpressionUploadManifestClassifier.File(
                blobUri,
                generation,
                persistedByIdentity[blobUri to generation]?.eventDate
                  ?: com.google.type.Date.getDefaultInstance(),
              )
            }
        check(generationOf(doneBlobUri) == doneBlobGeneration)
        manifest
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
      recoveryExecutorFactory = { recoverUploader },
    )
  }

  protected fun beforeOutputDelete(blobUri: String, block: suspend () -> Unit) {
    check(outputDeleteHook.compareAndSet(null, OutputDeleteHook(blobUri, block)))
  }

  protected fun buildVidLabelingMonitor(
    stalenessThreshold: Duration = Duration.ZERO,
    rawInputQuietPeriod: Duration = Duration.ZERO,
  ): VidLabelingMonitor =
    VidLabelingMonitor(
      rawImpressionUploadStub = uploadsStub,
      rawImpressionUploadModelLineStub = modelLineRowsStub,
      correctionCandidateStub = correctionCandidatesStub,
      dispatchSequencer = dispatchSequencer,
      dataProviderName = DATA_PROVIDER,
      stalenessThreshold = stalenessThreshold,
      rawImpressionsStorageRootUri = "gs://$fileBucket/",
      rawImpressionsBlobPrefix = "$rootKey/raw",
      rawInputQuietPeriod = rawInputQuietPeriod,
      rawImpressionsExcludedBlobPrefixes =
        setOf("$rootKey/output", "$rootKey/rank", "$rootKey/models", "$rootKey/tmp"),
      rawImpressionsStorageClientProvider = { fileStorage },
      rawImpressionUploadFileStub = filesStub,
      vidLabeledImpressionsStorageClientProvider = {
        PrefixingStorageClient(fileStorage, SelectedStorageClient.parseBlobUri(outputPrefix).key)
      },
      poolAssignmentJobStub =
        PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub(
          edpaPublicServer.channel
        ),
      rankerJobStub =
        RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub(edpaPublicServer.channel),
      vidLabelingJobStub =
        VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub(edpaPublicServer.channel),
      workItemsStub = workItemsStub,
      vidLabeledImpressionsBlobPrefix = SelectedStorageClient.parseBlobUri(outputPrefix).key,
      rpcThrottlers = VidLabelingRpcThrottlersTestHelper.alwaysReady(),
    )

  protected fun buildDispatchFailer(): DispatchFailer =
    DispatchFailer(uploadsStub, modelLineRowsStub)

  protected fun buildFailedDispatchRetrier(): FailedDispatchRetrier =
    FailedDispatchRetrier(
      modelLineRowsStub,
      PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub(
        edpaPublicServer.channel
      ),
      RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub(edpaPublicServer.channel),
      VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub(edpaPublicServer.channel),
      workItemsStub,
      VidLabelingRpcThrottlersTestHelper.alwaysReady(),
    )

  protected suspend fun runRankBuilderDirect(workItem: WorkItem): VidRankBuilder.Result {
    val params =
      workItem.workItemParams
        .unpack(WorkItemParams::class.java)
        .appParams
        .unpack(VidRankBuilderParams::class.java)
    val throttlers = VidLabelingRpcThrottlersTestHelper.alwaysReady()
    val rankIndexStore = RankIndexStore(mapStorage, kmsClient)
    val runDate = today.get()
    val retention =
      SubpoolRetention(
        rankIndexBlobsStub,
        rankIndexStore,
        params.dataProvider,
        params.modelLine,
        pipelineConfig.retentionDays,
        runDate,
        throttlers,
      )
    val subpoolRanker =
      SubpoolRanker(
        SubpoolFingerprintsStore(mapStorage, kmsClient),
        rankIndexStore,
        rankIndexBlobsStub,
        modelLineRowsStub,
        retention,
        params.dataProvider,
        params.rawImpressionUpload,
        params.modelLine,
        SelectedStorageClient.parseBlobUri(params.vidRankMapStorageParams.blobPrefix).key,
        params.encryptedSubpoolMapsDek.kekUri,
        params.encryptedSubpoolMapsDek,
        params.maxEventDate,
        pipelineConfig.retentionDays,
        runDate,
        throttlers,
        workerDispatcher = Dispatchers.Default,
        stripes = pipelineConfig.rankStripes,
        maxInFlightRecords = 2,
      )
    return VidRankBuilder(
        subpoolRanker,
        RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub(edpaPublicServer.channel),
        modelLineRowsStub,
        VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub(edpaPublicServer.channel),
        filesStub,
        workItemsStub,
        params.rawImpressionUpload,
        params.modelLine,
        params.rankerJob,
        params.subpoolMapBlobUrisMap,
        params.subpoolRankedSizesMap,
        vidLabelerParamsFromRankBuilder(params),
        VID_LABELER_QUEUE,
        params.maxFileBatchSizeBytes,
        throttlers,
      )
      .run()
  }

  private fun vidLabelerParamsFromRankBuilder(params: VidRankBuilderParams): VidLabelerParams =
    vidLabelerParams {
      dataProvider = params.dataProvider
      rawImpressionsStorageParams =
        VidLabelerParamsKt.storageParams {
          gcsProjectId = params.rawImpressionStorageParams.gcsProjectId
          impressionsBlobPrefix = params.rawImpressionStorageParams.blobPrefix
        }
      vidLabeledImpressionsStorageParams =
        VidLabelerParamsKt.storageParams {
          gcsProjectId = params.vidLabeledImpressionsStorageParams.gcsProjectId
          impressionsBlobPrefix = params.vidLabeledImpressionsStorageParams.blobPrefix
        }
      modelLineConfigs[params.modelLine] =
        VidLabelerParamsKt.modelLineConfig {
          labelerInputFieldMapping.addAll(params.labelerInputFieldMappingList)
          eventTemplateFieldMapping.putAll(params.eventTemplateFieldMappingMap)
          eventTemplateDescriptorBlobUri = params.eventTemplateDescriptorBlobUri
          eventTemplateType = params.eventTemplateType
          populationSpecBlobUri = params.populationSpecBlobUri
          requiredEntityKeyFieldMapping.putAll(params.requiredEntityKeyFieldMappingMap)
          optionalEntityKeyFieldMapping.putAll(params.optionalEntityKeyFieldMappingMap)
          if (params.hasActiveStartTime()) activeStartTime = params.activeStartTime
          if (params.hasActiveEndTime()) activeEndTime = params.activeEndTime
        }
      modelLines += params.modelLine
      modelBlobPaths[params.modelLine] = params.modelBlobPath
      modelStorageParams =
        VidLabelerParamsKt.storageParams {
          gcsProjectId = params.modelStorageParams.gcsProjectId
          impressionsBlobPrefix = params.modelStorageParams.blobPrefix
        }
      memoizedParams =
        VidLabelerParamsKt.memoizedParams {
          vidRankMapStorageParams =
            VidLabelerParamsKt.storageParams {
              gcsProjectId = params.vidRankMapStorageParams.gcsProjectId
              impressionsBlobPrefix = params.vidRankMapStorageParams.blobPrefix
            }
        }
    }

  protected suspend fun listHealingOperations(
    dataProviderName: String = DATA_PROVIDER
  ): List<UploadHealingOperation> =
    operationsStub
      .listUploadHealingOperations(listUploadHealingOperationsRequest { parent = dataProviderName })
      .uploadHealingOperationsList

  protected suspend fun listCorrectionCandidates(
    dataProviderName: String = DATA_PROVIDER
  ): List<RawImpressionUploadCorrectionCandidate> =
    correctionCandidatesStub
      .listRawImpressionUploadCorrectionCandidates(
        listRawImpressionUploadCorrectionCandidatesRequest { parent = dataProviderName }
      )
      .rawImpressionUploadCorrectionCandidatesList

  protected suspend fun registerSyntheticCorrectionCandidate(
    dataProviderResourceId: String,
    candidateId: String,
  ) {
    val doneBlobUri = "gs://synthetic-$dataProviderResourceId/raw/done"
    val quarantined =
      correctionDetectionStub.createQuarantinedRawImpressionUpload(
        createQuarantinedRawImpressionUploadRequest {
          this.dataProviderResourceId = dataProviderResourceId
          rawImpressionUpload = internalRawImpressionUpload {
            this.doneBlobUri = doneBlobUri
            doneBlobGeneration = 2L
            doneBlobCreateTime = Clock.systemUTC().instant().toProtoTime()
          }
          rawImpressionUploadCorrectionCandidateId = candidateId
          requestId = UUID.randomUUID().toString()
        }
      )
    val blobUri = "gs://synthetic-$dataProviderResourceId/raw/input.parquet"
    val prior =
      org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidateKt
        .manifestEntry {
          this.blobUri = blobUri
          blobGeneration = 1L
          outputSourceRawImpressionUploadResourceId = "prior-$candidateId"
        }
    val current =
      org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidateKt
        .manifestEntry {
          this.blobUri = blobUri
          blobGeneration = 2L
          outputSourceRawImpressionUploadResourceId = quarantined.rawImpressionUploadResourceId
        }
    val comparison =
      org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidateKt
        .manifestComparison {
          priorManifest += prior
          currentManifest += current
          differences +=
            org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidateKt
              .manifestDifference {
                type = InternalCandidate.ManifestDifference.Type.TYPE_EDITED
                this.prior = prior
                this.current = current
              }
        }
    correctionDetectionStub.registerDetectedRawImpressionUploadCorrectionCandidate(
      registerDetectedRawImpressionUploadCorrectionCandidateRequest {
        this.dataProviderResourceId = dataProviderResourceId
        rawImpressionUploadCorrectionCandidateId = candidateId
        rawImpressionUploadCorrectionCandidate = internalCandidate {
          rawImpressionUploadResourceId = quarantined.rawImpressionUploadResourceId
          classification = InternalCandidate.Classification.CLASSIFICATION_EDITED
          priorManifestDigest =
            RawImpressionUploadManifestClassifier()
              .digest(listOf(RawImpressionUploadManifestClassifier.File(blobUri, 1L)))
          currentManifestDigest =
            RawImpressionUploadManifestClassifier()
              .digest(listOf(RawImpressionUploadManifestClassifier.File(blobUri, 2L)))
          manifestComparison = comparison
          expireTime = Clock.systemUTC().instant().plus(Duration.ofDays(1)).toProtoTime()
        }
        requestId = UUID.randomUUID().toString()
      }
    )
  }

  protected suspend fun getCorrectionCandidate(
    name: String
  ): RawImpressionUploadCorrectionCandidate =
    correctionCandidatesStub.getRawImpressionUploadCorrectionCandidate(
      getRawImpressionUploadCorrectionCandidateRequest { this.name = name }
    )

  protected suspend fun approveHealingOperation(
    operation: UploadHealingOperation,
    decision: RawImpressionUploadCorrectionCandidate.Decision,
  ): UploadHealingOperation =
    approveHealingOperation(
      operation,
      operation.rawImpressionUploadCorrectionCandidatesList.associateWith { decision },
    )

  protected suspend fun retryHealingOperation(
    operation: UploadHealingOperation,
    requestId: String = "123e4567-e89b-42d3-a456-426614174092",
  ): UploadHealingOperation =
    operationsStub.retryUploadHealingOperation(
      retryUploadHealingOperationRequest {
        name = operation.name
        etag = operation.etag
        this.requestId = requestId
      }
    )

  protected suspend fun approveHealingOperation(
    operation: UploadHealingOperation,
    decisions: Map<String, RawImpressionUploadCorrectionCandidate.Decision>,
    requestId: String = "123e4567-e89b-42d3-a456-426614174099",
    etag: String = operation.etag,
  ): UploadHealingOperation =
    operationsStub.approveUploadHealingOperation(
      approveUploadHealingOperationRequest {
        name = operation.name
        candidateDecisions +=
          decisions.map { (candidate, decision) ->
            org.wfanet.measurement.edpaggregator.v1alpha.ApproveUploadHealingOperationRequestKt
              .candidateDecision {
                rawImpressionUploadCorrectionCandidate = candidate
                this.decision = decision
              }
          }
        this.etag = etag
        this.requestId = requestId
      }
    )

  protected fun startApplications() {
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
          getSubpoolMapKekUri = { kekUri },
          rpcThrottlers = throttlers,
        ),
        VidRankBuilderApp(
          subscriptionId = RANK_BUILDER_QUEUE,
          queueSubscriber = workItemTransport,
          parser = WorkItem.parser(),
          workItemsClient = workItemsStub,
          workItemAttemptsClient = workItemAttemptsStub,
          kmsClients = mapOf(DATA_PROVIDER to kmsClient),
          retentionDaysByDataProvider = mapOf(DATA_PROVIDER to pipelineConfig.retentionDays),
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
          today = today::get,
          rankStripes = pipelineConfig.rankStripes,
          maxInFlightRecords = 2,
        ),
        VidLabelerApp(
          subscriptionId = VID_LABELER_QUEUE,
          queueSubscriber = workItemTransport,
          parser = WorkItem.parser(),
          workItemsClient = workItemsStub,
          workItemAttemptsClient = workItemAttemptsStub,
          kmsClients = mapOf(DATA_PROVIDER to kmsClient),
          encryptKekUris = mapOf(DATA_PROVIDER to kekUri),
          getStorageConfig = {
            StorageConfig(rootDirectory = File("/"), blobPrefix = it.impressionsBlobPrefix)
          },
          vidLabelingJobsStub =
            VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub(
              edpaPublicServer.channel
            ),
          rawImpressionUploadModelLinesStub = modelLineRowsStub,
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
        ),
      )
    appScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    appJobs += apps.map { appScope.launch { it.run() } }
    appJobs +=
      appScope.launch {
        val messages =
          workItemTransport.subscribe(DataAvailabilitySyncWorkItems.QUEUE, WorkItem.parser())
        for (message in messages) {
          try {
            availabilityWorkItemProcessor.process(DataAvailabilitySyncWorkItem.parse(message.body))
            message.ack()
          } catch (e: CancellationException) {
            throw e
          } catch (_: Exception) {
            message.nack()
          }
        }
      }
  }

  protected suspend fun writeRawFile(
    folder: String,
    fileName: String,
    personIds: List<String>,
    eventDate: LocalDate = EVENT_DATE,
    gender: String = "MALE",
    ageGroup: String = "YEARS_18_TO_34",
  ): String =
    writeRawEvents(
      folder,
      fileName,
      personIds.mapIndexed { index, personId ->
        RawEventFixture("$personId-$index", personId, eventDate, gender, ageGroup)
      },
    )

  protected suspend fun writeRawEvents(
    folder: String,
    fileName: String,
    events: List<RawEventFixture>,
  ): String {
    require(events.isNotEmpty())
    val eventDates = events.mapTo(mutableSetOf()) { it.eventDate }
    require(eventDates.size == 1) { "one Parquet file must have exactly one event date" }
    val eventDate = eventDates.single()
    val key = "$rootKey/raw/$folder/$fileName"
    val eventTimeMicros = eventDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli() * 1_000L
    parquetClient(kmsClient)
      .writeBlob(
        key,
        flow {
          for ((index, event) in events.withIndex()) {
            emit(
              parquetRow {
                  columns[EVENT_ID_COLUMN] = parquetValue { stringValue = event.eventId }
                  columns[EVENT_TIME_COLUMN] = parquetValue { int64Value = eventTimeMicros + index }
                  columns[PERSON_ID_COLUMN] = parquetValue { stringValue = event.personId }
                  columns[GENDER_COLUMN] = parquetValue { stringValue = event.gender }
                  columns[AGE_GROUP_COLUMN] = parquetValue { stringValue = event.ageGroup }
                }
                .toByteString()
            )
          }
        },
        mapOf(RawImpressionFileMetadata.EVENT_DATE_KEY to eventDate.toString()),
      )
    return "gs://$fileBucket/$key"
  }

  protected fun withholdNextPoolAssignmentPublications(count: Int = 1) {
    workItemTransport.dropNextPublications(POOL_ASSIGNER_QUEUE, count)
  }

  protected fun withholdNextVidLabelerPublications(count: Int = 1) {
    workItemTransport.dropNextPublications(VID_LABELER_QUEUE, count)
  }

  protected fun withholdNextRankBuilderPublications(count: Int = 1) {
    workItemTransport.dropNextPublications(RANK_BUILDER_QUEUE, count)
  }

  protected fun withholdNextAvailabilityPublications(count: Int = 1) {
    workItemTransport.dropNextPublications(DataAvailabilitySyncWorkItems.QUEUE, count)
  }

  protected suspend fun republishQueuedPoolAssignments() {
    workItemTransport.republishQueuedWorkItems(POOL_ASSIGNER_QUEUE)
  }

  protected suspend fun republishQueuedVidLabelers() {
    workItemTransport.republishQueuedWorkItems(VID_LABELER_QUEUE)
  }

  protected suspend fun republishQueuedRankBuilders() {
    workItemTransport.republishQueuedWorkItems(RANK_BUILDER_QUEUE)
  }

  protected suspend fun republishQueuedAvailability() {
    workItemTransport.republishQueuedWorkItems(DataAvailabilitySyncWorkItems.QUEUE)
  }

  protected fun duplicateNextPoolAssignmentDelivery() {
    workItemTransport.duplicateNextDelivery(POOL_ASSIGNER_QUEUE)
  }

  protected fun duplicateNextRankBuilderDelivery() {
    workItemTransport.duplicateNextDelivery(RANK_BUILDER_QUEUE)
  }

  protected fun duplicateNextVidLabelerDelivery() {
    workItemTransport.duplicateNextDelivery(VID_LABELER_QUEUE)
  }

  protected fun holdNextPoolAssignmentRedelivery() {
    workItemTransport.holdNextRedelivery(POOL_ASSIGNER_QUEUE)
  }

  protected suspend fun awaitHeldPoolAssignmentRedelivery() {
    workItemTransport.awaitHeldRedelivery()
  }

  protected fun releaseHeldPoolAssignmentRedelivery() {
    workItemTransport.releaseHeldRedelivery()
  }

  protected fun holdNextRankBuilderRedelivery() {
    workItemTransport.holdNextRedelivery(RANK_BUILDER_QUEUE)
  }

  protected suspend fun awaitHeldRankBuilderRedelivery() {
    workItemTransport.awaitHeldRedelivery()
  }

  protected fun releaseHeldRankBuilderRedelivery() {
    workItemTransport.releaseHeldRedelivery()
  }

  protected fun holdNextVidLabelerRedelivery() {
    workItemTransport.holdNextRedelivery(VID_LABELER_QUEUE)
  }

  protected suspend fun awaitHeldVidLabelerRedelivery() {
    workItemTransport.awaitHeldRedelivery()
  }

  protected fun releaseHeldVidLabelerRedelivery() {
    workItemTransport.releaseHeldRedelivery()
  }

  protected fun setVisibleModelLines(vararg modelLineNames: String) {
    require(modelLineNames.all { requested -> modelLineFixtures.any { it.name == requested } })
    visibleModelLineNames.clear()
    visibleModelLineNames += modelLineNames
  }

  protected fun setToday(date: LocalDate) {
    today.set(date)
  }

  protected suspend fun finalizeRawUpload(folder: String): Long {
    val key = "$rootKey/raw/$folder/done"
    val blob = rawEventStorage.writeBlob(key, flowOf(ByteString.EMPTY))
    endpointFailure.getAndSet(null)?.let {
      throw AssertionError("Finalized-object delivery failed", it)
    }
    return (blob as ConditionalOperationStorageClient.Blob).freshnessToken.toLong()
  }

  protected fun parquetClient(
    kms: KmsClient,
    root: Path = Path("file:///$fileBucket"),
  ): ParquetStorageClient =
    ParquetStorageClient(
      Configuration().apply {
        set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        set("fs.gs.impl", GenerationMatchedTestHadoopFileSystem::class.java.name)
        setBoolean("fs.gs.impl.disable.cache", true)
        set("parquet.encryption.uniform.key", kekUri)
        setBoolean("parquet.encryption.plaintext.footer", true)
      },
      root,
      encryptionConfig = ParquetEncryptionConfig(kmsProvider = { kms }),
    )

  protected suspend fun readEventDate(blobUri: String): LocalDate =
    readEventDateFromFooter(parquetClient(kmsClient, Path("gs://$fileBucket/")), blobUri)

  protected suspend fun blobMetadata(key: String): RawImpressionBlobMetadata {
    val blob = checkNotNull(fileStorage.getBlob(key))
    val generation =
      checkNotNull((blob as? ConditionalOperationStorageClient.Blob)?.freshnessToken).toLong()
    // Each FileSystem overwrite is a new logical object generation. Its update time therefore
    // models GCS's per-generation create time rather than the inode's original create time.
    return RawImpressionBlobMetadata(generation, blob.size, blob.updateTime)
  }

  protected suspend fun generationOf(blobUri: String): Long =
    checkNotNull(fileStorage.getFreshnessToken(SelectedStorageClient.parseBlobUri(blobUri).key))
      .toLong()

  protected suspend fun listUploads(): List<RawImpressionUpload> = buildList {
    var pageToken = ""
    do {
      val response =
        uploadsStub.listRawImpressionUploads(
          listRawImpressionUploadsRequest {
            parent = DATA_PROVIDER
            filter = ListRawImpressionUploadsRequestKt.filter {}
            this.pageToken = pageToken
          }
        )
      addAll(response.rawImpressionUploadsList)
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
  }

  protected suspend fun listUploadFiles(upload: String): List<RawImpressionUploadFile> = buildList {
    var pageToken = ""
    do {
      val response =
        filesStub.listRawImpressionUploadFiles(
          listRawImpressionUploadFilesRequest {
            parent = upload
            this.pageToken = pageToken
          }
        )
      addAll(response.rawImpressionUploadFilesList)
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
  }

  protected suspend fun listModelLines(upload: String): List<RawImpressionUploadModelLine> =
    buildList {
      var pageToken = ""
      do {
        val response =
          modelLineRowsStub.listRawImpressionUploadModelLines(
            listRawImpressionUploadModelLinesRequest {
              parent = upload
              this.pageToken = pageToken
            }
          )
        addAll(response.rawImpressionUploadModelLinesList)
        pageToken = response.nextPageToken
      } while (pageToken.isNotEmpty())
    }

  protected suspend fun listRankIndexBlobs(
    upload: String,
    showDeleted: Boolean = false,
  ): List<RankIndexBlob> = buildList {
    var pageToken = ""
    do {
      val response =
        rankIndexBlobsStub.listRankIndexBlobs(
          listRankIndexBlobsRequest {
            parent = upload
            this.showDeleted = showDeleted
            this.pageToken = pageToken
          }
        )
      addAll(response.rankIndexBlobsList)
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
  }

  protected suspend fun rankEntries(
    upload: RawImpressionUpload,
    blobType: RankIndexBlob.BlobType,
  ): List<RankEntryFixture> {
    val store = RankIndexStore(mapStorage, kmsClient)
    return buildList {
      for (blob in listRankIndexBlobs(upload.name).filter { it.blobType == blobType }) {
        store.readBlob(blob.blobUri, blob.encryptedDek, blob.blobChecksum).collect { record ->
          repeat(record.ranksCount) { index ->
            val digestOffset = index * EventIdDigestBytes.WIDTH
            add(
              RankEntryFixture(
                EventIdDigest(
                  EventIdDigestBytes.readHi(record.fingerprints, digestOffset),
                  EventIdDigestBytes.readLo(record.fingerprints, digestOffset + 8),
                ),
                record.poolOffset,
                record.getRanks(index),
                LocalDate.ofEpochDay(
                  LastSeenDayBytes.read(record.lastSeenDays, index * LastSeenDayBytes.WIDTH)
                    .toLong()
                ),
              )
            )
          }
        }
      }
    }
  }

  protected fun digest(eventId: String): EventIdDigest =
    EventIdDigestExtractor().extract(ByteString.copyFromUtf8(eventId))

  protected suspend fun assertSnapshotsEvicted(upload: String, originals: List<RankIndexBlob>) {
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

  protected suspend fun listMetadata(showDeleted: Boolean = false): List<ImpressionMetadata> =
    buildList {
      var pageToken = ""
      do {
        val response =
          impressionMetadataStub.listImpressionMetadata(
            listImpressionMetadataRequest {
              parent = DATA_PROVIDER
              this.showDeleted = showDeleted
              this.pageToken = pageToken
            }
          )
        addAll(response.impressionMetadataList)
        pageToken = response.nextPageToken
      } while (pageToken.isNotEmpty())
    }

  protected fun listAvailabilityTasks(upload: String): List<AvailabilityWorkItemRecord> =
    workItemTransport
      .workItemsForQueue(DataAvailabilitySyncWorkItems.QUEUE)
      .map { workItem ->
        val input = DataAvailabilitySyncWorkItem.parse(workItem)
        AvailabilityWorkItemRecord(
          workItem = workItem,
          attemptCount = workItemTransport.attemptCount(workItem.name),
          cmmsModelLine = input.appParams.modelLine,
          rawImpressionUpload = input.appParams.triggeringRawImpressionUpload,
          doneBlobUri = input.doneBlobUri,
          doneBlobGeneration = input.doneBlobGeneration,
          eventDate = input.appParams.eventDate,
        )
      }
      .filter { it.rawImpressionUpload == upload }

  protected fun poolAssignmentWorkItems(): List<WorkItem> =
    workItemTransport.workItemsForQueue(POOL_ASSIGNER_QUEUE)

  protected fun rankBuilderWorkItems(): List<WorkItem> =
    workItemTransport.workItemsForQueue(RANK_BUILDER_QUEUE)

  protected fun vidLabelerWorkItems(): List<WorkItem> =
    workItemTransport.workItemsForQueue(VID_LABELER_QUEUE)

  protected suspend fun processAvailabilityTask(task: AvailabilityWorkItemRecord) {
    availabilityWorkItemProcessor.process(DataAvailabilitySyncWorkItem.parse(task.workItem))
  }

  protected suspend fun assertAvailabilityTaskIdentities(
    upload: RawImpressionUpload,
    eventDate: LocalDate,
  ) {
    val tasks = listAvailabilityTasks(upload.name)
    for (task in tasks) {
      val expectedDoneUri =
        LabeledImpressionsBlobKeys.forDoneUri(outputPrefix, task.cmmsModelLine, eventDate)
      val expectedGeneration =
        checkNotNull(fileStorage.getFreshnessToken(blobKey(expectedDoneUri))).toLong()
      val expectedPathHash = VidLabelingTraceAttributes.gcsObjectPathHash(expectedDoneUri)
      val expectedId = WorkItemIds.forDataAvailabilitySync(expectedPathHash, expectedGeneration)
      assertThat(task.name).isEqualTo("workItems/$expectedId")
      assertThat(task.doneBlobUri).isEqualTo(expectedDoneUri)
      assertThat(task.doneBlobGeneration).isEqualTo(expectedGeneration)
      assertThat(task.doneBlobPathHash).isEqualTo(expectedPathHash)
      assertThat(task.eventDate.year).isEqualTo(eventDate.year)
      assertThat(task.eventDate.month).isEqualTo(eventDate.monthValue)
      assertThat(task.eventDate.day).isEqualTo(eventDate.dayOfMonth)
    }
  }

  protected suspend fun assertEveryRegisteredRawGenerationWasRead() {
    val registeredFiles = listUploads().flatMap { listUploadFiles(it.name) }
    for (file in registeredFiles) {
      assertThat(GenerationMatchedTestHadoopFileSystem.recordedGenerations(file.blobUri))
        .contains(file.blobGeneration)
    }
  }

  protected fun sidecarUri(inputBlobUri: String, modelLine: String, eventDate: LocalDate): String =
    LabeledImpressionsBlobKeys.forInputUri(outputPrefix, inputBlobUri, modelLine, eventDate) +
      ".metadata.binpb"

  protected fun blobKey(blobUri: String): String = SelectedStorageClient.parseBlobUri(blobUri).key

  protected suspend fun snapshotOutputArtifacts(
    metadata: Collection<ImpressionMetadata>
  ): Map<String, OutputArtifact> = metadata.associate { it.blobUri to snapshotOutputArtifact(it) }

  protected suspend fun snapshotOutputArtifact(metadata: ImpressionMetadata): OutputArtifact {
    val sidecarBlob = checkNotNull(fileStorage.getBlob(blobKey(metadata.blobUri)))
    val dataUri = BlobDetails.parseFrom(sidecarBlob.read().flatten()).blobUri
    return OutputArtifact(
      dataUri,
      checkNotNull(fileStorage.getFreshnessToken(blobKey(metadata.blobUri))),
      checkNotNull(fileStorage.getFreshnessToken(blobKey(dataUri))),
    )
  }

  protected suspend fun assertMetadataMatchesSidecar(metadata: ImpressionMetadata) {
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

  protected suspend fun assertAvailabilityPublished(
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
    val metadata = listMetadata().filter { it.modelLine in modelLines }
    val expectedStart = metadata.minBy { it.interval.startTime.toInstant() }.interval.startTime
    val expectedEnd = metadata.maxBy { it.interval.endTime.toInstant() }.interval.endTime
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

  protected suspend fun assertCompletedForBothPaths(upload: RawImpressionUpload) {
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

  protected suspend fun assertReadablePeople(modelLine: String, expectedPeople: Set<String>) {
    val labeledPeople = readLabeledPeople(modelLine)
    assertThat(labeledPeople.map { it.personId }).containsExactlyElementsIn(expectedPeople)
    assertThat(labeledPeople.all { it.vid in EXPECTED_VID_RANGE }).isTrue()
  }

  protected suspend fun readLabeledPeople(modelLine: String): List<LabeledPersonImpression> {
    val metadata = listMetadata().filter { it.modelLine == modelLine }
    return metadata
      .flatMap { row ->
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
      .flatMap { event ->
        event.entityKeys
          .filter { it.entityType == "person" }
          .map { entityKey ->
            LabeledPersonImpression(
              personId = entityKey.entityId,
              vid = event.vid,
              eventDate = event.timestamp.atZone(ZoneOffset.UTC).toLocalDate(),
            )
          }
      }
  }

  protected suspend fun awaitPipelineIdle(allowFailures: Boolean = false) {
    workItemTransport.awaitIdle(allowFailures)
    endpointFailure.getAndSet(null)?.let {
      throw AssertionError("Finalized-object delivery failed", it)
    }
  }

  protected suspend fun drainSequencer() {
    do {
      val result = dispatchSequencer.dispatchNext()
      awaitPipelineIdle()
    } while (result.dispatchedUpload != null || result.queuedUploads > 0)
  }

  protected suspend fun outputGenerations(modelLine: String): Map<String, String> =
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

  protected suspend fun labeledOutputGeneration(
    inputBlobUri: String,
    modelLine: String,
    eventDate: LocalDate,
  ): String {
    val details =
      BlobDetails.parseFrom(
        checkNotNull(fileStorage.getBlob(blobKey(sidecarUri(inputBlobUri, modelLine, eventDate))))
          .read()
          .flatten()
      )
    return checkNotNull(
      fileStorage.getFreshnessToken(SelectedStorageClient.parseBlobUri(details.blobUri).key)
    )
  }

  protected fun vidLabelerParamsTemplate(): VidLabelerParams = vidLabelerParams {
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

  protected fun subpoolAssignerParamsTemplate() = subpoolAssignerParams {
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
    maxFileBatchSizeBytes = pipelineConfig.maxFileBatchSizeBytes
  }

  protected fun modelLineConfig(): VidLabelerParams.ModelLineConfig =
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
          lookupTable["FEMALE"] = Gender.GENDER_FEMALE.name
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
            bucketTable["YEARS_35_TO_54"] = ageBucket {
              minAge = 35
              maxAge = 54
            }
            bucketTable["YEARS_55_PLUS"] = ageBucket {
              minAge = 55
              maxAge = 99
            }
          }
        }
      }
      optionalEntityKeyFieldMapping["person"] = PERSON_ID_COLUMN
      populationSpecBlobUri = "file:///unused/population-spec"
      eventTemplateDescriptorBlobUri = "file:///unused/event-template-descriptor"
      eventTemplateType = TestEvent.getDescriptor().fullName
    }

  protected class RecordingDataProvidersService :
    DataProvidersGrpcKt.DataProvidersCoroutineImplBase() {
    val requests = mutableListOf<ReplaceDataAvailabilityIntervalsRequest>()
    private val lostResponsesAfterCommit = AtomicInteger()

    fun loseNextResponseAfterCommit() {
      lostResponsesAfterCommit.incrementAndGet()
    }

    override suspend fun replaceDataAvailabilityIntervals(
      request: ReplaceDataAvailabilityIntervalsRequest
    ): DataProvider {
      synchronized(requests) { requests += request }
      if (lostResponsesAfterCommit.getAndUpdate { count -> maxOf(0, count - 1) } > 0) {
        throw Status.UNAVAILABLE.withDescription("injected response loss after commit")
          .asRuntimeException()
      }
      return dataProvider {
        name = request.name
        dataAvailabilityIntervals += request.dataAvailabilityIntervalsList
      }
    }
  }

  protected class BeforeCallFaultInterceptor : ServerInterceptor {
    private enum class Stage {
      BEFORE_HANDLER,
      AFTER_COMMIT_BEFORE_RESPONSE,
    }

    private data class Fault(
      val fullMethodName: String,
      val stage: Stage,
      var skip: Int,
      var remaining: Int,
    )

    private var fault: Fault? = null

    @Synchronized
    fun arm(fullMethodName: String, skip: Int = 0, count: Int = 1) {
      arm(fullMethodName, Stage.BEFORE_HANDLER, skip, count)
    }

    @Synchronized
    fun armAfterCommit(fullMethodName: String, skip: Int = 0, count: Int = 1) {
      arm(fullMethodName, Stage.AFTER_COMMIT_BEFORE_RESPONSE, skip, count)
    }

    private fun arm(fullMethodName: String, stage: Stage, skip: Int, count: Int) {
      require(skip >= 0)
      require(count > 0)
      check(fault == null)
      fault = Fault(fullMethodName, stage, skip, count)
    }

    @Synchronized
    fun clear() {
      fault = null
    }

    override fun <ReqT : Any, RespT : Any> interceptCall(
      call: ServerCall<ReqT, RespT>,
      headers: Metadata,
      next: ServerCallHandler<ReqT, RespT>,
    ): ServerCall.Listener<ReqT> {
      val stage =
        synchronized(this) {
          val current = fault
          when {
            current == null || current.fullMethodName != call.methodDescriptor.fullMethodName ->
              null
            current.skip > 0 -> {
              current.skip--
              null
            }
            else -> {
              current.remaining--
              if (current.remaining == 0) fault = null
              current.stage
            }
          }
        }
      return when (stage) {
        null -> next.startCall(call, headers)
        Stage.BEFORE_HANDLER -> {
          call.close(Status.UNAVAILABLE.withDescription("injected before-call failure"), Metadata())
          object : ServerCall.Listener<ReqT>() {}
        }
        Stage.AFTER_COMMIT_BEFORE_RESPONSE -> {
          val forwardingCall =
            object : ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
              override fun close(status: Status, trailers: Metadata) {
                if (status.isOk) {
                  super.close(
                    Status.UNAVAILABLE.withDescription("injected response loss after commit"),
                    trailers,
                  )
                } else {
                  super.close(status, trailers)
                }
              }
            }
          next.startCall(forwardingCall, headers)
        }
      }
    }
  }

  protected data class OutputArtifact(
    val dataUri: String,
    val sidecarGeneration: String,
    val dataGeneration: String,
  )

  private data class OutputDeleteHook(val blobUri: String, val block: suspend () -> Unit)

  protected data class LabeledPersonImpression(
    val personId: String,
    val vid: Long,
    val eventDate: LocalDate,
  )

  protected data class AvailabilityWorkItemRecord(
    val workItem: WorkItem,
    val attemptCount: Int,
    val cmmsModelLine: String,
    val rawImpressionUpload: String,
    val doneBlobUri: String,
    val doneBlobGeneration: Long,
    val eventDate: com.google.type.Date,
  ) {
    val name: String
      get() = workItem.name

    val state: WorkItem.State
      get() = workItem.state

    val doneBlobPathHash: String
      get() = VidLabelingTraceAttributes.gcsObjectPathHash(doneBlobUri)
  }

  protected class RecordingBlobMetadataStorageClient(private val delegate: StorageClient) :
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

  private class PrefixingStorageClient(
    private val delegate: StorageClient,
    private val prefix: String,
  ) : StorageClient {
    override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob =
      delegate.writeBlob("${prefix.trimEnd('/')}/$blobKey", content)

    override suspend fun getBlob(blobKey: String): StorageClient.Blob? =
      delegate.getBlob("${prefix.trimEnd('/')}/$blobKey")

    override suspend fun listBlobs(prefix: String?): Flow<StorageClient.Blob> =
      delegate.listBlobs(
        if (prefix.isNullOrEmpty()) "${this.prefix.trimEnd('/')}/"
        else "${this.prefix.trimEnd('/')}/$prefix"
      )
  }

  /** Adds generation-precondition semantics to the single-process filesystem test backend. */
  protected class GenerationEnforcingStorageClient(
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

  protected class UriNormalizingStorageClient(
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

  protected class SuspendingHttpHandler(
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

  protected class InProcessWorkItemTransport : QueueSubscriber {
    private data class RedeliveryGate(
      val queueName: String,
      val held: CompletableDeferred<Unit> = CompletableDeferred(),
      val release: CompletableDeferred<Unit> = CompletableDeferred(),
    )

    private val stateMutex = Mutex()
    private val clock = Clock.systemUTC()
    private val redeliveryScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val channels =
      ConcurrentHashMap<String, Channel<QueueSubscriber.QueueMessage<WorkItem>>>()
    private val workItems = ConcurrentHashMap<String, WorkItem>()
    private val attempts = ConcurrentHashMap<String, WorkItemAttempt>()
    private val attemptParents = ConcurrentHashMap<String, String>()
    private val currentAttemptByWorkItem = ConcurrentHashMap<String, String>()
    private val failures = ConcurrentHashMap<String, String>()
    private val pending = AtomicInteger()
    private val changes = MutableStateFlow(0L)
    private val sequence = AtomicInteger()
    private val forceAcknowledgementRedelivery = AtomicInteger()
    private val forcedRetries = AtomicInteger()
    private val forcedRetryWorkItems = ConcurrentHashMap.newKeySet<String>()
    private val droppedPublications = ConcurrentHashMap<String, AtomicInteger>()
    private val lostPublicationResponses = ConcurrentHashMap<String, AtomicInteger>()
    private val duplicatedDeliveries = ConcurrentHashMap<String, AtomicInteger>()
    private val withheldWorkItems = ConcurrentHashMap.newKeySet<String>()
    private val deliveryCounts = ConcurrentHashMap<String, AtomicInteger>()
    private val lostResponseWorkItemNames = ConcurrentHashMap.newKeySet<String>()
    private val beforeNextRedelivery = AtomicReference<(suspend () -> Unit)?>(null)
    private val redeliveryGate = AtomicReference<RedeliveryGate?>()
    val publishedCount: Int
      get() = sequence.get()

    val forcedRetryCount: Int
      get() = forcedRetries.get()

    val forcedRetryWorkItemNames: Set<String>
      get() = forcedRetryWorkItems.toSet()

    fun forceNextAcknowledgementRedelivery() {
      check(forceAcknowledgementRedelivery.compareAndSet(0, 1))
    }

    fun dropNextPublications(queueName: String, count: Int = 1) {
      check(count > 0)
      droppedPublications.computeIfAbsent(queueName) { AtomicInteger() }.addAndGet(count)
    }

    fun loseNextPublicationResponse(queueName: String, count: Int = 1) {
      check(count > 0)
      lostPublicationResponses.computeIfAbsent(queueName) { AtomicInteger() }.addAndGet(count)
    }

    fun duplicateNextDelivery(queueName: String, count: Int = 1) {
      check(count > 0)
      duplicatedDeliveries.computeIfAbsent(queueName) { AtomicInteger() }.addAndGet(count)
    }

    fun beforeNextRedelivery(block: suspend () -> Unit) {
      check(beforeNextRedelivery.compareAndSet(null, block))
    }

    fun holdNextRedelivery(queueName: String) {
      check(redeliveryGate.compareAndSet(null, RedeliveryGate(queueName)))
    }

    suspend fun awaitHeldRedelivery() {
      checkNotNull(redeliveryGate.get()).held.await()
    }

    fun releaseHeldRedelivery() {
      checkNotNull(redeliveryGate.get()).release.complete(Unit)
    }

    fun workItemsForQueue(queueName: String): List<WorkItem> =
      workItems.values.filter { it.queue == queueName }.sortedBy { it.name }

    fun attemptCount(workItemName: String): Int = attemptParents.values.count { it == workItemName }

    fun deliveryCount(workItemName: String): Int = deliveryCounts[workItemName]?.get() ?: 0

    fun lostResponseWorkItemNames(): Set<String> = lostResponseWorkItemNames.toSet()

    suspend fun republishWorkItem(workItemName: String) {
      val workItem = checkNotNull(workItems[workItemName])
      check(workItem.state == WorkItem.State.QUEUED)
      withheldWorkItems.remove(workItemName)
      publish(workItem.queue, workItem)
    }

    suspend fun redeliverWorkItem(workItemName: String) {
      val workItem = checkNotNull(workItems[workItemName])
      publish(workItem.queue, workItem)
    }

    suspend fun republishQueuedWorkItems(queueName: String) {
      for (workItem in workItemsForQueue(queueName).filter { it.state == WorkItem.State.QUEUED }) {
        republishWorkItem(workItem.name)
      }
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
          publishCreatedWorkItem(created)
          return created
        }

        override suspend fun ensureWorkItem(request: EnsureWorkItemRequest): WorkItem {
          val name = "workItems/${request.workItemId}"
          var created = false
          val ensured =
            checkNotNull(
              workItems.compute(name) { _, existing ->
                existing
                  ?: request.workItem
                    .toBuilder()
                    .setName(name)
                    .setState(WorkItem.State.QUEUED)
                    .setGeneration(1L)
                    .build()
                    .also { created = true }
              }
            )
          if (
            ensured.queue != request.workItem.queue ||
              ensured.workItemParams != request.workItem.workItemParams ||
              ensured.serializationKey != request.workItem.serializationKey
          ) {
            throw Status.ALREADY_EXISTS.withDescription("WorkItem immutable fields do not match")
              .asRuntimeException()
          }
          if (created) publishCreatedWorkItem(ensured)
          return ensured
        }

        override suspend fun getWorkItem(request: GetWorkItemRequest): WorkItem =
          workItems[request.name] ?: throw Status.NOT_FOUND.asRuntimeException()

        override suspend fun failWorkItem(request: FailWorkItemRequest): WorkItem {
          return stateMutex.withLock {
            val item = workItems[request.name] ?: throw Status.NOT_FOUND.asRuntimeException()
            if (
              request.hasExpectedWorkItemGeneration() &&
                request.expectedWorkItemGeneration != item.generation
            ) {
              throw WorkItemGenerationMismatchException(
                  item.name,
                  request.expectedWorkItemGeneration.toString(),
                  item.generation.toString(),
                )
                .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
            }
            currentAttemptByWorkItem.remove(item.name)?.let { attemptName ->
              attempts.computeIfPresent(attemptName) { _, attempt ->
                attempt
                  .toBuilder()
                  .setState(WorkItemAttempt.State.FAILED)
                  .setUpdateTime(clock.instant().toProtoTime())
                  .build()
              }
            }
            item.toBuilder().setState(WorkItem.State.FAILED).build().also {
              workItems[request.name] = it
            }
          }
        }
      }

    private suspend fun publishCreatedWorkItem(created: WorkItem) {
      if (consume(droppedPublications, created.queue)) {
        withheldWorkItems += created.name
      } else {
        publish(created.queue, created)
      }
      if (consume(lostPublicationResponses, created.queue)) {
        lostResponseWorkItemNames += created.name
        throw Status.UNAVAILABLE.withDescription("injected lost publication response")
          .asRuntimeException()
      }
    }

    val workItemAttemptsService =
      object : WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase() {
        override suspend fun createWorkItemAttempt(
          request: CreateWorkItemAttemptRequest
        ): WorkItemAttempt =
          stateMutex.withLock {
            val item = workItems[request.parent] ?: throw Status.NOT_FOUND.asRuntimeException()
            if (
              request.hasExpectedWorkItemGeneration() &&
                request.expectedWorkItemGeneration != item.generation
            ) {
              throw WorkItemGenerationMismatchException(
                  item.name,
                  request.expectedWorkItemGeneration.toString(),
                  item.generation.toString(),
                )
                .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
            }
            val now = clock.instant()
            val activeAttemptName = currentAttemptByWorkItem[item.name]
            val activeAttempt = activeAttemptName?.let { attempts[it] }
            if (
              activeAttempt != null &&
                activeAttempt.state == WorkItemAttempt.State.ACTIVE &&
                activeAttempt.leaseExpirationTime.toInstant().isAfter(now)
            ) {
              throw WorkItemInvalidStateException(item.name, WorkItem.State.RUNNING.name)
                .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
            }
            if (activeAttempt != null && activeAttempt.state == WorkItemAttempt.State.ACTIVE) {
              attempts[activeAttempt.name] =
                activeAttempt
                  .toBuilder()
                  .setState(WorkItemAttempt.State.FAILED)
                  .setUpdateTime(now.toProtoTime())
                  .build()
              currentAttemptByWorkItem.remove(item.name, activeAttempt.name)
            }
            if (item.state !in setOf(WorkItem.State.QUEUED, WorkItem.State.RUNNING)) {
              if (item.state == WorkItem.State.SUCCEEDED && item.name in forcedRetryWorkItems) {
                forcedRetries.incrementAndGet()
              }
              throw WorkItemInvalidStateException(item.name, item.state.name)
                .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
            }
            workItems[request.parent] = item.toBuilder().setState(WorkItem.State.RUNNING).build()
            val attempt =
              WorkItemAttempt.newBuilder()
                .setName("${request.parent}/workItemAttempts/${request.workItemAttemptId}")
                .setState(WorkItemAttempt.State.ACTIVE)
                .setAttemptNumber(attemptParents.values.count { it == request.parent } + 1)
                .setCreateTime(now.toProtoTime())
                .setUpdateTime(now.toProtoTime())
                .setLeaseExpirationTime(now.plus(WORK_ITEM_ATTEMPT_LEASE_DURATION).toProtoTime())
                .build()
            attempts[attempt.name] = attempt
            attemptParents[attempt.name] = request.parent
            currentAttemptByWorkItem[request.parent] = attempt.name
            attempt
          }

        override suspend fun completeWorkItemAttempt(
          request: CompleteWorkItemAttemptRequest
        ): WorkItemAttempt =
          stateMutex.withLock {
            val attempt = requireActiveAttempt(request.name, requireUnexpired = true)
            val parent = checkNotNull(attemptParents[request.name])
            if (currentAttemptByWorkItem[parent] != request.name) {
              throw Status.FAILED_PRECONDITION.withDescription("WorkItemAttempt is not current")
                .asRuntimeException()
            }
            val completed =
              attempt
                .toBuilder()
                .setState(WorkItemAttempt.State.SUCCEEDED)
                .setUpdateTime(clock.instant().toProtoTime())
                .build()
            attempts[request.name] = completed
            currentAttemptByWorkItem.remove(parent, request.name)
            val item = workItems[parent] ?: throw Status.NOT_FOUND.asRuntimeException()
            workItems[parent] = item.toBuilder().setState(WorkItem.State.SUCCEEDED).build()
            completed
          }

        override suspend fun renewWorkItemAttempt(
          request: RenewWorkItemAttemptRequest
        ): WorkItemAttempt =
          stateMutex.withLock {
            val attempt = requireActiveAttempt(request.name, requireUnexpired = true)
            val parent = checkNotNull(attemptParents[request.name])
            if (currentAttemptByWorkItem[parent] != request.name) {
              throw Status.FAILED_PRECONDITION.withDescription("WorkItemAttempt is not current")
                .asRuntimeException()
            }
            val now = clock.instant()
            attempt
              .toBuilder()
              .setUpdateTime(now.toProtoTime())
              .setLeaseExpirationTime(now.plus(WORK_ITEM_ATTEMPT_LEASE_DURATION).toProtoTime())
              .build()
              .also { attempts[request.name] = it }
          }

        override suspend fun failWorkItemAttempt(
          request: FailWorkItemAttemptRequest
        ): WorkItemAttempt =
          stateMutex.withLock {
            val attempt = attempts[request.name] ?: throw Status.NOT_FOUND.asRuntimeException()
            if (attempt.state == WorkItemAttempt.State.FAILED) return@withLock attempt
            if (attempt.state != WorkItemAttempt.State.ACTIVE) {
              throw Status.FAILED_PRECONDITION.withDescription("WorkItemAttempt is not ACTIVE")
                .asRuntimeException()
            }
            val failed =
              attempt
                .toBuilder()
                .setState(WorkItemAttempt.State.FAILED)
                .setErrorMessage(request.errorMessage)
                .setUpdateTime(clock.instant().toProtoTime())
                .build()
            attempts[request.name] = failed
            val parent = checkNotNull(attemptParents[request.name])
            currentAttemptByWorkItem.remove(parent, request.name)
            failures[parent] = request.errorMessage
            failed
          }

        private fun requireActiveAttempt(
          attemptName: String,
          requireUnexpired: Boolean,
        ): WorkItemAttempt {
          val attempt = attempts[attemptName] ?: throw Status.NOT_FOUND.asRuntimeException()
          if (attempt.state != WorkItemAttempt.State.ACTIVE) {
            throw Status.FAILED_PRECONDITION.withDescription("WorkItemAttempt is not ACTIVE")
              .asRuntimeException()
          }
          if (
            requireUnexpired && !attempt.leaseExpirationTime.toInstant().isAfter(clock.instant())
          ) {
            throw Status.FAILED_PRECONDITION.withDescription("WorkItemAttempt lease expired")
              .asRuntimeException()
          }
          return attempt
        }
      }

    private suspend fun publish(queueName: String, workItem: WorkItem) {
      enqueue(queueName, workItem)
      if (consume(duplicatedDeliveries, queueName)) {
        enqueue(queueName, workItem)
      }
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T : Message> subscribe(
      subscriptionId: String,
      parser: Parser<T>,
    ): ReceiveChannel<QueueSubscriber.QueueMessage<T>> =
      channel(subscriptionId) as ReceiveChannel<QueueSubscriber.QueueMessage<T>>

    private fun enqueue(queueName: String, workItem: WorkItem) {
      pending.incrementAndGet()
      deliveryCounts.computeIfAbsent(workItem.name) { AtomicInteger() }.incrementAndGet()
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
              redeliveryScope.launch {
                stateMutex.withLock {
                  workItems.computeIfPresent(workItem.name) { _, item ->
                    item.toBuilder().setState(WorkItem.State.FAILED).build()
                  }
                  currentAttemptByWorkItem.remove(workItem.name)
                }
                pending.decrementAndGet()
                signalChange()
              }
            } else {
              scheduleRedelivery(queueName, workItem, "$ackId-retry", this)
            }
            signalChange()
          }
        }
      channel(queueName).trySend(QueueSubscriber.QueueMessage(workItem, ackId, consumer))
      signalChange()
    }

    private fun scheduleRedelivery(
      queueName: String,
      workItem: WorkItem,
      ackId: String,
      consumer: MessageConsumer,
    ) {
      redeliveryScope.launch {
        beforeNextRedelivery.getAndSet(null)?.invoke()
        redeliveryGate
          .get()
          ?.takeIf { it.queueName == queueName }
          ?.let { gate ->
            gate.held.complete(Unit)
            gate.release.await()
            redeliveryGate.compareAndSet(gate, null)
          }
        delay(REDELIVERY_DELAY_MILLIS)
        deliveryCounts.computeIfAbsent(workItem.name) { AtomicInteger() }.incrementAndGet()
        channel(queueName).send(QueueSubscriber.QueueMessage(workItem, ackId, consumer))
        signalChange()
      }
    }

    private fun consume(counters: ConcurrentHashMap<String, AtomicInteger>, key: String): Boolean {
      val counter = counters[key] ?: return false
      while (true) {
        val current = counter.get()
        if (current <= 0) return false
        if (counter.compareAndSet(current, current - 1)) return true
      }
    }

    private fun channel(queueName: String) =
      channels.computeIfAbsent(queueName) { Channel(Channel.UNLIMITED) }

    private fun signalChange() {
      changes.update { it + 1L }
    }

    suspend fun awaitIdle(allowFailures: Boolean = false) {
      withTimeout(120_000L) {
        while (true) {
          val observed = changes.value
          if (pending.get() == 0) break
          changes.first { it != observed }
        }
      }
      if (!allowFailures) {
        check(failures.isEmpty()) { failures.values.joinToString(separator = "\n") }
      }
    }

    override fun close() {
      redeliveryScope.cancel()
      channels.values.forEach { it.close() }
    }

    companion object {
      private val WORK_ITEM_ATTEMPT_LEASE_DURATION = Duration.ofMinutes(5)
    }
  }

  protected object ImmediateThrottler : Throttler {
    override suspend fun <T> onReady(block: suspend () -> T): T = block()
  }

  companion object {
    init {
      AeadConfig.register()
    }

    @ClassRule @JvmField val spannerEmulator = SpannerEmulatorRule()

    private const val DATA_PROVIDER = "dataProviders/dp1"
    private const val MODEL_SUITE = "modelProviders/mp1/modelSuites/ms1"
    const val MEMOIZED_MODEL_LINE = "$MODEL_SUITE/modelLines/memoized"
    const val DIRECT_MODEL_LINE = "$MODEL_SUITE/modelLines/direct"
    private const val MEMOIZED_RELEASE = "$MODEL_SUITE/modelReleases/memoized"
    private const val DIRECT_RELEASE = "$MODEL_SUITE/modelReleases/direct"
    private const val POOL_ASSIGNER_QUEUE = "queues/pool-assigner"
    private const val RANK_BUILDER_QUEUE = "queues/rank-builder"
    private const val VID_LABELER_QUEUE = "queues/vid-labeler"
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
    val EVENT_DATE: LocalDate = LocalDate.of(2026, 9, 1)
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
  protected val localFileSystem = RawLocalFileSystem()
  protected var fileSystemUri: URI? = null

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

  protected fun toLocalPath(path: Path): Path {
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
