/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.vidlabeling

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Timestamp
import com.google.protobuf.util.Timestamps
import com.google.type.date
import io.grpc.Status
import io.grpc.StatusException
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.data.MetricData
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter
import java.time.Clock
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.util.logging.Handler
import java.util.logging.LogRecord
import java.util.logging.Logger
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.atLeastOnce
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelShardsGrpcKt
import org.wfanet.measurement.api.v2alpha.listModelLinesResponse
import org.wfanet.measurement.api.v2alpha.listModelRolloutsResponse
import org.wfanet.measurement.api.v2alpha.listModelShardsResponse
import org.wfanet.measurement.api.v2alpha.modelLine
import org.wfanet.measurement.api.v2alpha.modelRollout
import org.wfanet.measurement.api.v2alpha.modelShard
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.BlobUris
import org.wfanet.measurement.edpaggregator.VidLabelingRpcThrottlers
import org.wfanet.measurement.edpaggregator.rawimpressions.generationMatchedBlobUri
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceLogging
import org.wfanet.measurement.edpaggregator.testing.VidLabelingRpcThrottlersTestHelper
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.CreateRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.LabelerInputFieldMapping
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.MarkRawImpressionUploadRegistrationCompleteRequest
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.ScalarColumn
import org.wfanet.measurement.edpaggregator.v1alpha.SubpoolAssignerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRawImpressionUploadFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.listRankIndexBlobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelerParams
import org.wfanet.measurement.internal.edpaggregator.ActivateQuarantinedRawImpressionUploadRequest
import org.wfanet.measurement.internal.edpaggregator.CreateQuarantinedRawImpressionUploadRequest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidate as InternalCandidate
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidateServiceGrpcKt as InternalCandidateGrpcKt
import org.wfanet.measurement.internal.edpaggregator.RegisterDetectedRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.ResolveDetectedRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUpload as internalRawImpressionUpload
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUploadCorrectionCandidate as internalCandidate
import org.wfanet.measurement.internal.edpaggregator.registerDetectedRawImpressionUploadCorrectionCandidateResponse
import org.wfanet.measurement.internal.edpaggregator.resolveDetectedRawImpressionUploadCorrectionCandidateResponse
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient

@RunWith(JUnit4::class)
class VidLabelingDispatcherTest {
  private val logRecords = mutableListOf<LogRecord>()
  private val rootLogger = Logger.getLogger("")
  private val logHandler =
    object : Handler() {
      override fun publish(record: LogRecord) {
        logRecords += record
      }

      override fun flush() {}

      override fun close() {}
    }

  @Before
  fun initLogging() {
    logRecords.clear()
    rootLogger.addHandler(logHandler)
  }

  @After
  fun cleanupLogging() {
    rootLogger.removeHandler(logHandler)
  }

  private val modelLinesService: ModelLinesGrpcKt.ModelLinesCoroutineImplBase = mockService()
  private val modelRolloutsService: ModelRolloutsGrpcKt.ModelRolloutsCoroutineImplBase =
    mockService()
  private val modelShardsService: ModelShardsGrpcKt.ModelShardsCoroutineImplBase = mockService()
  private val rawImpressionUploadService:
    RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineImplBase =
    mockService()
  private val rawImpressionUploadFileService:
    RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineImplBase =
    mockService()
  private val correctionCandidateService:
    InternalCandidateGrpcKt.RawImpressionUploadCorrectionCandidateServiceCoroutineImplBase =
    mockService()
  private val rawImpressionUploadModelLineService:
    RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase =
    mockService()
  private val rankIndexBlobService:
    RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineImplBase =
    mockService()
  private val workItemsService: WorkItemsGrpcKt.WorkItemsCoroutineImplBase = mockService()
  private val poolAssignmentJobService:
    PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineImplBase =
    mockService()
  private val vidLabelingJobService:
    VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineImplBase =
    mockService()
  private val storageClient: StorageClient = mock()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(modelLinesService)
    addService(modelRolloutsService)
    addService(modelShardsService)
    addService(rawImpressionUploadService)
    addService(rawImpressionUploadFileService)
    addService(correctionCandidateService)
    addService(rawImpressionUploadModelLineService)
    addService(rankIndexBlobService)
    addService(workItemsService)
    addService(poolAssignmentJobService)
    addService(vidLabelingJobService)
  }

  private val poolAssignmentJobStub by lazy {
    PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub(grpcTestServerRule.channel)
  }

  private val modelLinesStub by lazy {
    ModelLinesGrpcKt.ModelLinesCoroutineStub(grpcTestServerRule.channel)
  }

  private val modelRolloutsStub by lazy {
    ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub(grpcTestServerRule.channel)
  }

  private val modelShardsStub by lazy {
    ModelShardsGrpcKt.ModelShardsCoroutineStub(grpcTestServerRule.channel)
  }

  private val rawImpressionUploadStub by lazy {
    RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub(
      grpcTestServerRule.channel
    )
  }

  private val rawImpressionUploadFilesStub by lazy {
    RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub(
      grpcTestServerRule.channel
    )
  }

  private val correctionCandidateStub by lazy {
    InternalCandidateGrpcKt.RawImpressionUploadCorrectionCandidateServiceCoroutineStub(
      grpcTestServerRule.channel
    )
  }

  private val rawImpressionUploadModelLineStub by lazy {
    RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub(
      grpcTestServerRule.channel
    )
  }

  private val rankIndexBlobStub by lazy {
    RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub(grpcTestServerRule.channel)
  }

  private val workItemsStub by lazy {
    WorkItemsGrpcKt.WorkItemsCoroutineStub(grpcTestServerRule.channel)
  }

  private val vidLabelingJobStub by lazy {
    VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub(grpcTestServerRule.channel)
  }

  private val fixedClock: Clock = Clock.fixed(FIXED_NOW, ZoneId.of("UTC"))

  private data class MetricsTestEnvironment(
    val metrics: VidLabelingDispatcherMetrics,
    val metricExporter: InMemoryMetricExporter,
    val metricReader: PeriodicMetricReader,
    val openTelemetry: OpenTelemetrySdk,
  ) {
    fun close() {
      openTelemetry.close()
      GlobalOpenTelemetry.resetForTest()
      Instrumentation.resetForTest()
    }
  }

  private fun createMetricsEnvironment(): MetricsTestEnvironment {
    GlobalOpenTelemetry.resetForTest()
    Instrumentation.resetForTest()
    val metricExporter = InMemoryMetricExporter.create()
    val metricReader = PeriodicMetricReader.create(metricExporter)
    val meterProvider = SdkMeterProvider.builder().registerMetricReader(metricReader).build()
    val openTelemetry =
      OpenTelemetrySdk.builder().setMeterProvider(meterProvider).buildAndRegisterGlobal()
    val meter = meterProvider.get("vid-labeling-dispatcher-test")
    return MetricsTestEnvironment(
      VidLabelingDispatcherMetrics(meter),
      metricExporter,
      metricReader,
      openTelemetry,
    )
  }

  private fun createSequencer(
    modelLineConfigs: Map<String, VidLabelerParams.ModelLineConfig> = DEFAULT_MODEL_LINE_CONFIGS,
    rpcThrottlers: VidLabelingRpcThrottlers = VidLabelingRpcThrottlersTestHelper.alwaysReady(),
  ): VidLabelingDispatchSequencer {
    return VidLabelingDispatchSequencer(
      rawImpressionUploadStub = rawImpressionUploadStub,
      rawImpressionUploadModelLineStub = rawImpressionUploadModelLineStub,
      workItemsStub = workItemsStub,
      poolAssignmentJobStub = poolAssignmentJobStub,
      modelRolloutsStub = modelRolloutsStub,
      modelShardsStub = modelShardsStub,
      modelLinesStub = modelLinesStub,
      dataProviderName = DATA_PROVIDER_NAME,
      vidLabelerParamsTemplate = vidLabelerParams {},
      subpoolAssignerParamsTemplate = SubpoolAssignerParams.getDefaultInstance(),
      queueName = QUEUE_NAME,
      poolAssignerQueueName = POOL_ASSIGNER_QUEUE_NAME,
      numberOfShards = NUMBER_OF_SHARDS,
      modelLineConfigs = modelLineConfigs,
      rawImpressionUploadFileStub = rawImpressionUploadFilesStub,
      vidLabelingJobStub = vidLabelingJobStub,
      maxFileBatchSizeBytes = MAX_FILE_BATCH_SIZE_BYTES,
      rpcThrottlers = rpcThrottlers,
    )
  }

  private fun createDispatcher(
    overrideModelLines: List<String> = emptyList(),
    recoverySourceUpload: String? = null,
    recoveryOperationId: String? = null,
    modelLineConfigs: Map<String, VidLabelerParams.ModelLineConfig> = DEFAULT_MODEL_LINE_CONFIGS,
    readEventDate: suspend (String) -> LocalDate = { EVENT_DATE },
    readBlobMetadata: suspend (String) -> RawImpressionBlobMetadata = {
      RawImpressionBlobMetadata(RAW_BLOB_GENERATION, 100L, RAW_BLOB_CREATE_TIME)
    },
    readDoneBlobMetadata: suspend () -> RawImpressionBlobMetadata = {
      RawImpressionBlobMetadata(DONE_BLOB_GENERATION, 0L, DONE_BLOB_CREATE_TIME)
    },
    metrics: VidLabelingDispatcherMetrics = VidLabelingDispatcherMetrics(),
    rpcThrottlers: VidLabelingRpcThrottlers = VidLabelingRpcThrottlersTestHelper.alwaysReady(),
  ): VidLabelingDispatcher {
    return VidLabelingDispatcher(
      storageClient = storageClient,
      rawImpressionUploadStub = rawImpressionUploadStub,
      rawImpressionUploadFilesStub = rawImpressionUploadFilesStub,
      correctionDetectionStub = correctionCandidateStub,
      rawImpressionUploadModelLineStub = rawImpressionUploadModelLineStub,
      rankIndexBlobStub = rankIndexBlobStub,
      modelLinesStub = modelLinesStub,
      dispatchSequencer = createSequencer(modelLineConfigs, rpcThrottlers),
      dataProviderName = DATA_PROVIDER_NAME,
      modelSuiteName = MODEL_SUITE_NAME,
      overrideModelLines = overrideModelLines,
      recoverySourceUpload = recoverySourceUpload,
      recoveryOperationId = recoveryOperationId,
      modelLineConfigs = modelLineConfigs,
      readEventDate = readEventDate,
      readBlobMetadata = { blobKey ->
        if (blobKey.substringAfterLast("/").equals("done", ignoreCase = true)) {
          readDoneBlobMetadata()
        } else {
          readBlobMetadata(blobKey)
        }
      },
      rpcThrottlers = rpcThrottlers,
      clock = fixedClock,
      metrics = metrics,
    )
  }

  private fun createMockBlob(key: String, size: Long = 100L): StorageClient.Blob {
    val blob: StorageClient.Blob = mock()
    whenever(blob.blobKey).thenReturn(key)
    whenever(blob.size).thenReturn(size)
    return blob
  }

  private fun previousUpload(
    generation: Long = 100L,
    nameSuffix: String = "previous",
  ): RawImpressionUpload = rawImpressionUpload {
    name = "$DATA_PROVIDER_NAME/rawImpressionUploads/$nameSuffix"
    state = RawImpressionUpload.State.COMPLETED
    doneBlobUri = DONE_BLOB_PATH
    doneBlobGeneration = generation
    doneBlobCreateTime =
      DONE_BLOB_CREATE_TIME.minusSeconds(DONE_BLOB_GENERATION - generation).toProtoTime()
    registrationComplete = true
  }

  private fun manifestFile(
    uploadName: String,
    blobUri: String,
    generation: Long,
  ): RawImpressionUploadFile = rawImpressionUploadFile {
    name = "$uploadName/files/${generation}"
    this.blobUri = blobUri
    blobGeneration = generation
    eventDate = date {
      year = EVENT_DATE.year
      month = EVENT_DATE.monthValue
      day = EVENT_DATE.dayOfMonth
    }
  }

  private class RecordingThrottler : Throttler {
    var onReadyCalls = 0
    var inOnReady = false
      private set

    override suspend fun <T> onReady(block: suspend () -> T): T {
      onReadyCalls++
      inOnReady = true
      return try {
        block()
      } finally {
        inOnReady = false
      }
    }
  }

  private suspend fun stubRecoverySource(
    source: RawImpressionUpload,
    latestSourceRevision: RawImpressionUpload = source,
    predecessorModelLineState: RawImpressionUploadModelLine.State =
      RawImpressionUploadModelLine.State.COMPLETED,
    recoveryPredecessorUpload: String = RECOVERY_PREDECESSOR_UPLOAD,
    recoveryAction: RawImpressionUploadModelLine.RecoveryAction =
      RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY,
    metadataReadThrottler: RecordingThrottler? = null,
    paginateRecoveryReads: Boolean = false,
  ) {
    val predecessor = rawImpressionUpload {
      name = RECOVERY_PREDECESSOR_UPLOAD
      doneBlobUri = RECOVERY_PREDECESSOR_DONE_BLOB_PATH
      doneBlobGeneration = DONE_BLOB_GENERATION - 2
      doneBlobCreateTime = DONE_BLOB_CREATE_TIME.minusSeconds(3).toProtoTime()
    }
    val predecessorReplacement = rawImpressionUpload {
      name = RECOVERY_PREDECESSOR_REPLACEMENT
      doneBlobUri = RECOVERY_PREDECESSOR_DONE_BLOB_PATH
      doneBlobGeneration = DONE_BLOB_GENERATION - 1
      doneBlobCreateTime = DONE_BLOB_CREATE_TIME.minusSeconds(2).toProtoTime()
      replacesRawImpressionUpload = predecessor.name
    }
    whenever(rawImpressionUploadService.getRawImpressionUpload(any())).thenAnswer { invocation ->
      check(metadataReadThrottler == null || metadataReadThrottler.inOnReady)
      val request =
        invocation.getArgument<
          org.wfanet.measurement.edpaggregator.v1alpha.GetRawImpressionUploadRequest
        >(
          0
        )
      when (request.name) {
        source.name -> source
        predecessor.name -> predecessor
        else -> error("Unexpected upload: ${request.name}")
      }
    }
    whenever(rawImpressionUploadService.listRawImpressionUploads(any())).thenAnswer { invocation ->
      val request = invocation.getArgument<ListRawImpressionUploadsRequest>(0)
      listRawImpressionUploadsResponse {
        if (request.filter.doneBlobUri == predecessor.doneBlobUri) {
          rawImpressionUploads += predecessor
          rawImpressionUploads += predecessorReplacement
        } else {
          rawImpressionUploads += latestSourceRevision
        }
      }
    }
    whenever(rawImpressionUploadModelLineService.listRawImpressionUploadModelLines(any()))
      .thenAnswer { invocation ->
        check(metadataReadThrottler == null || metadataReadThrottler.inOnReady)
        val request =
          invocation.getArgument<
            org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequest
          >(
            0
          )
        listRawImpressionUploadModelLinesResponse {
          if (request.pageToken.isEmpty())
            when (request.parent) {
              source.name -> rawImpressionUploadModelLines += rawImpressionUploadModelLine {
                  name = "${source.name}/rawImpressionUploadModelLines/rml1"
                  cmmsModelLine = MODEL_LINE_1
                  state = RawImpressionUploadModelLine.State.FAILED
                  failureReason = RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT
                  this.recoveryAction = recoveryAction
                  evictionOperationId = EVICTION_OPERATION_ID
                  if (recoveryPredecessorUpload.isNotEmpty()) {
                    recoveryPredecessorRawImpressionUpload = recoveryPredecessorUpload
                  }
                }
              predecessorReplacement.name -> rawImpressionUploadModelLines +=
                  rawImpressionUploadModelLine {
                    name = "${predecessorReplacement.name}/rawImpressionUploadModelLines/rml1"
                    cmmsModelLine = MODEL_LINE_1
                    state = predecessorModelLineState
                  }
            }
          if (paginateRecoveryReads && request.pageToken.isEmpty()) {
            nextPageToken = "next"
          }
        }
      }
    whenever(rankIndexBlobService.listRankIndexBlobs(any())).thenAnswer { invocation ->
      check(metadataReadThrottler == null || metadataReadThrottler.inOnReady)
      val request =
        invocation.getArgument<
          org.wfanet.measurement.edpaggregator.v1alpha.ListRankIndexBlobsRequest
        >(
          0
        )
      listRankIndexBlobsResponse {
        if (request.pageToken.isEmpty()) {
          rankIndexBlobs += rankIndexBlob {
            name = "${request.parent}/rankIndexBlobs/snapshot"
            blobType = RankIndexBlob.BlobType.SNAPSHOT
            cmmsModelLine = MODEL_LINE_1
            if (request.parent == source.name) deleteTime = Timestamp.getDefaultInstance()
          }
          if (paginateRecoveryReads && request.showDeleted) {
            nextPageToken = "next"
          }
        }
      }
    }
  }

  private suspend fun stubRawImpressionUploadCreation() {
    whenever(rawImpressionUploadService.createRawImpressionUpload(any()))
      .thenReturn(
        RawImpressionUpload.newBuilder()
          .setName("$DATA_PROVIDER_NAME/rawImpressionUploads/$RAW_IMPRESSION_UPLOAD_ID")
          .setDoneBlobUri(DONE_BLOB_PATH)
          .setEtag(UPLOAD_ETAG)
          .build()
      )
    whenever(rawImpressionUploadFileService.batchCreateRawImpressionUploadFiles(any()))
      .thenReturn(batchCreateRawImpressionUploadFilesResponse {})
    whenever(rawImpressionUploadFileService.listRawImpressionUploadFiles(any()))
      .thenReturn(listRawImpressionUploadFilesResponse {})
    whenever(rawImpressionUploadModelLineService.batchCreateRawImpressionUploadModelLines(any()))
      .thenReturn(batchCreateRawImpressionUploadModelLinesResponse {})
    whenever(
        rawImpressionUploadService.markRawImpressionUploadRegistrationComplete(
          any<MarkRawImpressionUploadRegistrationCompleteRequest>()
        )
      )
      .thenReturn(
        RawImpressionUpload.newBuilder()
          .setName("$DATA_PROVIDER_NAME/rawImpressionUploads/$RAW_IMPRESSION_UPLOAD_ID")
          .setRegistrationComplete(true)
          .setEtag("completed-$UPLOAD_ETAG")
          .build()
      )
    // The post-registration fast path lists uploads; default to none so dispatch is a no-op unless
    // a test overrides this.
    whenever(rawImpressionUploadService.listRawImpressionUploads(any()))
      .thenReturn(listRawImpressionUploadsResponse {})
  }

  private suspend fun stubCorrectionCandidateRegistration() {
    whenever(correctionCandidateService.createQuarantinedRawImpressionUpload(any()))
      .thenReturn(
        internalRawImpressionUpload {
          dataProviderResourceId = DATA_PROVIDER_NAME.substringAfter("dataProviders/")
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_ID
          etag = UPLOAD_ETAG
        }
      )
    whenever(
        correctionCandidateService.registerDetectedRawImpressionUploadCorrectionCandidate(any())
      )
      .thenAnswer { invocation ->
        val request =
          invocation.getArgument<RegisterDetectedRawImpressionUploadCorrectionCandidateRequest>(0)
        registerDetectedRawImpressionUploadCorrectionCandidateResponse {
          rawImpressionUploadCorrectionCandidate = internalCandidate {
            dataProviderResourceId = request.dataProviderResourceId
            rawImpressionUploadCorrectionCandidateId =
              request.rawImpressionUploadCorrectionCandidateId
            rawImpressionUploadResourceId =
              request.rawImpressionUploadCorrectionCandidate.rawImpressionUploadResourceId
            classification = request.rawImpressionUploadCorrectionCandidate.classification
          }
          newlyCreated = true
        }
      }
    whenever(
        correctionCandidateService.resolveDetectedRawImpressionUploadCorrectionCandidate(any())
      )
      .thenReturn(
        resolveDetectedRawImpressionUploadCorrectionCandidateResponse { newlyResolved = true }
      )
  }

  private suspend fun stubFullResolutionChain(vararg modelLineNames: String) {
    whenever(modelLinesService.listModelLines(any()))
      .thenReturn(
        listModelLinesResponse {
          modelLines +=
            modelLineNames.map { name ->
              modelLine {
                this.name = name
                type = ModelLine.Type.PROD
                activeStartTime = Timestamps.fromMillis(FIXED_NOW.toEpochMilli() - 86400000)
                activeEndTime = Timestamps.fromMillis(FIXED_NOW.toEpochMilli() + 86400000)
              }
            }
        }
      )

    whenever(modelRolloutsService.listModelRollouts(any()))
      .thenReturn(
        listModelRolloutsResponse {
          modelRollouts += modelRollout { modelRelease = MODEL_RELEASE_NAME }
        }
      )

    whenever(modelShardsService.listModelShards(any()))
      .thenReturn(
        listModelShardsResponse {
          modelShards += modelShard {
            name = "$DATA_PROVIDER_NAME/modelShards/ms1"
            modelRelease = MODEL_RELEASE_NAME
            modelBlob =
              org.wfanet.measurement.api.v2alpha.ModelShardKt.modelBlob {
                modelBlobPath = MODEL_BLOB_PATH
              }
          }
        }
      )
  }

  private suspend fun stubOverrideResolutionChain() {
    whenever(modelRolloutsService.listModelRollouts(any()))
      .thenReturn(
        listModelRolloutsResponse {
          modelRollouts += modelRollout { modelRelease = MODEL_RELEASE_NAME }
        }
      )

    whenever(modelShardsService.listModelShards(any()))
      .thenReturn(
        listModelShardsResponse {
          modelShards += modelShard {
            name = "$DATA_PROVIDER_NAME/modelShards/ms1"
            modelRelease = MODEL_RELEASE_NAME
            modelBlob =
              org.wfanet.measurement.api.v2alpha.ModelShardKt.modelBlob {
                modelBlobPath = MODEL_BLOB_PATH
              }
          }
        }
      )
  }

  @Test
  fun `initial upload rejects an empty directory`() = runBlocking {
    whenever(storageClient.listBlobs(any())).thenReturn(emptyFlow())

    val error =
      assertFailsWith<IllegalArgumentException> {
        createDispatcher().upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)
      }

    assertThat(error).hasMessageThat().contains("cannot be empty")
    verifyBlocking(rawImpressionUploadService, never()) { createRawImpressionUpload(any()) }
    verifyBlocking(modelLinesService, never()) { listModelLines(any()) }
  }

  @Test
  fun `upload lists only the done marker directory`() = runBlocking {
    whenever(storageClient.listBlobs(any())).thenReturn(emptyFlow())

    assertFailsWith<IllegalArgumentException> {
      createDispatcher().upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)
    }

    verify(storageClient)
      .listBlobs(
        SelectedStorageClient.parseBlobUri(DONE_BLOB_PATH).key.substringBeforeLast("/") + "/"
      )
    Unit
  }

  @Test
  fun `edited manifest is quarantined without model-line work`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file.parquet")
    val blobUri =
      BlobUris.buildUri(SelectedStorageClient.parseBlobUri(DONE_BLOB_PATH), blob.blobKey)
    val previous = previousUpload()
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubRawImpressionUploadCreation()
    stubCorrectionCandidateRegistration()
    whenever(rawImpressionUploadService.listRawImpressionUploads(any()))
      .thenReturn(listRawImpressionUploadsResponse { rawImpressionUploads += previous })
    whenever(rawImpressionUploadFileService.listRawImpressionUploadFiles(any()))
      .thenReturn(
        listRawImpressionUploadFilesResponse {
          rawImpressionUploadFiles += manifestFile(previous.name, blobUri, 10L)
        }
      )

    val outcome =
      createDispatcher(
          readBlobMetadata = { RawImpressionBlobMetadata(20L, 100L, RAW_BLOB_CREATE_TIME) }
        )
        .upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

    assertThat(outcome).isEqualTo(VidLabelingDispatcher.UploadOutcome.CORRECTION_REQUIRED)
    assertThat(
        logRecords
          .single { it.message.contains("event=edpa.vid_labeling.upload_registered ") }
          .message
      )
      .contains("xmm.outcome=correction_required")
    val uploadRequest = argumentCaptor<CreateQuarantinedRawImpressionUploadRequest>()
    verifyBlocking(correctionCandidateService) {
      createQuarantinedRawImpressionUpload(uploadRequest.capture())
    }
    assertThat(uploadRequest.firstValue.rawImpressionUploadCorrectionCandidateId).isNotEmpty()
    val candidate = argumentCaptor<RegisterDetectedRawImpressionUploadCorrectionCandidateRequest>()
    verifyBlocking(correctionCandidateService) {
      registerDetectedRawImpressionUploadCorrectionCandidate(candidate.capture())
    }
    assertThat(candidate.firstValue.rawImpressionUploadCorrectionCandidate.classification)
      .isEqualTo(InternalCandidate.Classification.CLASSIFICATION_EDITED)
    assertThat(candidate.firstValue.rawImpressionUploadCorrectionCandidate.priorManifestDigest)
      .isNotEqualTo(
        candidate.firstValue.rawImpressionUploadCorrectionCandidate.currentManifestDigest
      )
    val manifestComparison =
      candidate.firstValue.rawImpressionUploadCorrectionCandidate.manifestComparison
    assertThat(
        manifestComparison.priorManifestList.single().outputSourceRawImpressionUploadResourceId
      )
      .isEqualTo("previous")
    assertThat(
        manifestComparison.currentManifestList.single().outputSourceRawImpressionUploadResourceId
      )
      .isEqualTo(RAW_IMPRESSION_UPLOAD_ID)
    assertThat(manifestComparison.currentManifestList.single().eventDate)
      .isEqualTo(EVENT_DATE_PROTO)
    assertThat(candidate.firstValue.rawImpressionUploadCorrectionCandidate.currentManifestDigest)
      .isEqualTo(
        RawImpressionUploadManifestClassifier()
          .digest(
            listOf(RawImpressionUploadManifestClassifier.File(blobUri, 20L, EVENT_DATE_PROTO))
          )
      )
    assertThat(manifestComparison.differencesList.single().type)
      .isEqualTo(InternalCandidate.ManifestDifference.Type.TYPE_EDITED)
    verifyBlocking(rawImpressionUploadService, never()) {
      markRawImpressionUploadRegistrationComplete(any())
    }
    verifyBlocking(rawImpressionUploadModelLineService, never()) {
      batchCreateRawImpressionUploadModelLines(any())
    }
    verifyBlocking(modelLinesService, never()) { listModelLines(any()) }
  }

  @Test
  fun `event date move uses the dated mixed classification`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file.parquet")
    val blobUri =
      BlobUris.buildUri(SelectedStorageClient.parseBlobUri(DONE_BLOB_PATH), blob.blobKey)
    val previous = previousUpload()
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubRawImpressionUploadCreation()
    stubCorrectionCandidateRegistration()
    whenever(rawImpressionUploadService.listRawImpressionUploads(any()))
      .thenReturn(listRawImpressionUploadsResponse { rawImpressionUploads += previous })
    whenever(rawImpressionUploadFileService.listRawImpressionUploadFiles(any()))
      .thenReturn(
        listRawImpressionUploadFilesResponse {
          rawImpressionUploadFiles += manifestFile(previous.name, blobUri, 10L)
        }
      )
    val movedDate = EVENT_DATE.plusDays(1)

    createDispatcher(
        readEventDate = { movedDate },
        readBlobMetadata = { RawImpressionBlobMetadata(20L, 100L, RAW_BLOB_CREATE_TIME) },
      )
      .upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

    val candidate = argumentCaptor<RegisterDetectedRawImpressionUploadCorrectionCandidateRequest>()
    verifyBlocking(correctionCandidateService) {
      registerDetectedRawImpressionUploadCorrectionCandidate(candidate.capture())
    }
    val registered = candidate.firstValue.rawImpressionUploadCorrectionCandidate
    assertThat(registered.classification)
      .isEqualTo(InternalCandidate.Classification.CLASSIFICATION_MIXED)
    assertThat(registered.manifestComparison.differencesList.single().type)
      .isEqualTo(InternalCandidate.ManifestDifference.Type.TYPE_EVENT_DATE_CHANGED)
    assertThat(registered.manifestComparison.currentManifestList.single().eventDate)
      .isEqualTo(
        date {
          year = movedDate.year
          month = movedDate.monthValue
          day = movedDate.dayOfMonth
        }
      )
  }

  @Test
  fun `non-additive manifest acquires fence before footer failure`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file.parquet")
    val blobUri =
      BlobUris.buildUri(SelectedStorageClient.parseBlobUri(DONE_BLOB_PATH), blob.blobKey)
    val previous = previousUpload()
    var fenceAcquired = false
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubCorrectionCandidateRegistration()
    whenever(correctionCandidateService.createQuarantinedRawImpressionUpload(any())).thenAnswer {
      fenceAcquired = true
      internalRawImpressionUpload {
        dataProviderResourceId = DATA_PROVIDER_NAME.substringAfter("dataProviders/")
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_ID
        etag = UPLOAD_ETAG
      }
    }
    whenever(rawImpressionUploadService.listRawImpressionUploads(any()))
      .thenReturn(listRawImpressionUploadsResponse { rawImpressionUploads += previous })
    whenever(rawImpressionUploadFileService.listRawImpressionUploadFiles(any()))
      .thenReturn(
        listRawImpressionUploadFilesResponse {
          rawImpressionUploadFiles += manifestFile(previous.name, blobUri, 10L)
        }
      )

    val error =
      assertFailsWith<IllegalStateException> {
        createDispatcher(
            readEventDate = {
              check(fenceAcquired)
              error("invalid footer")
            },
            readBlobMetadata = { RawImpressionBlobMetadata(20L, 100L, RAW_BLOB_CREATE_TIME) },
          )
          .upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)
      }

    assertThat(error).hasMessageThat().contains("invalid footer")
    verifyBlocking(correctionCandidateService) { createQuarantinedRawImpressionUpload(any()) }
    verifyBlocking(correctionCandidateService, never()) {
      registerDetectedRawImpressionUploadCorrectionCandidate(any())
    }
  }

  @Test
  fun `empty replacement manifest is quarantined as removed`() = runBlocking {
    val previous = previousUpload()
    val blobUri = "file:///test-bucket/$FOLDER_PREFIX/file.parquet"
    whenever(storageClient.listBlobs(any())).thenReturn(emptyFlow())
    stubRawImpressionUploadCreation()
    stubCorrectionCandidateRegistration()
    whenever(rawImpressionUploadService.listRawImpressionUploads(any()))
      .thenReturn(listRawImpressionUploadsResponse { rawImpressionUploads += previous })
    whenever(rawImpressionUploadFileService.listRawImpressionUploadFiles(any()))
      .thenReturn(
        listRawImpressionUploadFilesResponse {
          rawImpressionUploadFiles += manifestFile(previous.name, blobUri, 10L)
        }
      )

    createDispatcher().upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

    val candidate = argumentCaptor<RegisterDetectedRawImpressionUploadCorrectionCandidateRequest>()
    verifyBlocking(correctionCandidateService) {
      registerDetectedRawImpressionUploadCorrectionCandidate(candidate.capture())
    }
    assertThat(candidate.firstValue.rawImpressionUploadCorrectionCandidate.classification)
      .isEqualTo(InternalCandidate.Classification.CLASSIFICATION_REMOVED)
    assertThat(
        candidate.firstValue.rawImpressionUploadCorrectionCandidate.manifestComparison
          .currentManifestList
      )
      .isEmpty()
    assertThat(
        candidate.firstValue.rawImpressionUploadCorrectionCandidate.manifestComparison
          .differencesList
          .single()
          .type
      )
      .isEqualTo(InternalCandidate.ManifestDifference.Type.TYPE_REMOVED)
    verifyBlocking(rawImpressionUploadFileService, never()) {
      batchCreateRawImpressionUploadFiles(any())
    }
    verifyBlocking(rawImpressionUploadModelLineService, never()) {
      batchCreateRawImpressionUploadModelLines(any())
    }
  }

  @Test
  fun `rewrite after complete no-replacement deletion uses the empty boundary`() = runBlocking {
    val old = previousUpload(generation = 90L, nameSuffix = "old")
    val removed =
      previousUpload(generation = 100L, nameSuffix = "removed").copy {
        state = RawImpressionUpload.State.REMOVED_WITHOUT_REPLACEMENT
        replacesRawImpressionUpload = old.name
      }
    val oldBlobUri = "file:///test-bucket/$FOLDER_PREFIX/file.parquet"
    whenever(storageClient.listBlobs(any())).thenReturn(emptyFlow())
    whenever(rawImpressionUploadService.listRawImpressionUploads(any()))
      .thenReturn(
        listRawImpressionUploadsResponse {
          rawImpressionUploads += old
          rawImpressionUploads += removed
        }
      )
    whenever(rawImpressionUploadFileService.listRawImpressionUploadFiles(any())).thenAnswer {
      invocation ->
      val parent = invocation.getArgument<ListRawImpressionUploadFilesRequest>(0).parent
      listRawImpressionUploadFilesResponse {
        if (parent == old.name) {
          rawImpressionUploadFiles += manifestFile(old.name, oldBlobUri, 10L)
        }
      }
    }

    val outcome = createDispatcher().upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

    assertThat(outcome).isEqualTo(VidLabelingDispatcher.UploadOutcome.NO_WORK)
    verifyBlocking(correctionCandidateService, never()) {
      createQuarantinedRawImpressionUpload(any())
    }
    verifyBlocking(correctionCandidateService, never()) {
      registerDetectedRawImpressionUploadCorrectionCandidate(any())
    }
    verifyBlocking(rawImpressionUploadService, never()) { createRawImpressionUpload(any()) }
  }

  @Test
  fun `mixed manifest stores the complete current snapshot`() = runBlocking {
    val edited = createMockBlob("$FOLDER_PREFIX/edited.parquet")
    val added = createMockBlob("$FOLDER_PREFIX/added.parquet")
    val doneUri = SelectedStorageClient.parseBlobUri(DONE_BLOB_PATH)
    val editedUri = BlobUris.buildUri(doneUri, edited.blobKey)
    val addedUri = BlobUris.buildUri(doneUri, added.blobKey)
    val removedUri = "file:///test-bucket/$FOLDER_PREFIX/removed.parquet"
    val previous = previousUpload()
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(edited, added))
    stubRawImpressionUploadCreation()
    stubCorrectionCandidateRegistration()
    whenever(rawImpressionUploadService.listRawImpressionUploads(any()))
      .thenReturn(listRawImpressionUploadsResponse { rawImpressionUploads += previous })
    whenever(rawImpressionUploadFileService.listRawImpressionUploadFiles(any()))
      .thenReturn(
        listRawImpressionUploadFilesResponse {
          rawImpressionUploadFiles += manifestFile(previous.name, editedUri, 10L)
          rawImpressionUploadFiles += manifestFile(previous.name, removedUri, 11L)
        }
      )
    val generations = mapOf(edited.blobKey to 20L, added.blobKey to 30L)

    createDispatcher(
        readBlobMetadata = {
          RawImpressionBlobMetadata(generations.getValue(it), 100L, RAW_BLOB_CREATE_TIME)
        }
      )
      .upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

    val candidate = argumentCaptor<RegisterDetectedRawImpressionUploadCorrectionCandidateRequest>()
    verifyBlocking(correctionCandidateService) {
      registerDetectedRawImpressionUploadCorrectionCandidate(candidate.capture())
    }
    assertThat(candidate.firstValue.rawImpressionUploadCorrectionCandidate.classification)
      .isEqualTo(InternalCandidate.Classification.CLASSIFICATION_MIXED)
    assertThat(
        candidate.firstValue.rawImpressionUploadCorrectionCandidate.manifestComparison
          .currentManifestList
          .map { it.blobUri }
      )
      .containsExactly(editedUri, addedUri)
    val files = argumentCaptor<BatchCreateRawImpressionUploadFilesRequest>()
    verifyBlocking(rawImpressionUploadFileService) {
      batchCreateRawImpressionUploadFiles(files.capture())
    }
    assertThat(files.firstValue.requestsList.map { it.rawImpressionUploadFile.blobUri })
      .containsExactly(editedUri, addedUri)
    Unit
  }

  @Test
  fun `newer correction supersedes the previous candidate`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file.parquet")
    val blobUri =
      BlobUris.buildUri(SelectedStorageClient.parseBlobUri(DONE_BLOB_PATH), blob.blobKey)
    val healthy = previousUpload(generation = 90L, nameSuffix = "healthy")
    val quarantined =
      previousUpload(generation = 100L, nameSuffix = "quarantined").copy {
        state = RawImpressionUpload.State.CORRECTION_REQUIRED
        replacesRawImpressionUpload = healthy.name
      }
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubRawImpressionUploadCreation()
    stubCorrectionCandidateRegistration()
    whenever(rawImpressionUploadService.listRawImpressionUploads(any()))
      .thenReturn(
        listRawImpressionUploadsResponse {
          rawImpressionUploads += healthy
          rawImpressionUploads += quarantined
        }
      )
    whenever(rawImpressionUploadFileService.listRawImpressionUploadFiles(any())).thenAnswer {
      invocation ->
      val parent = invocation.getArgument<ListRawImpressionUploadFilesRequest>(0).parent
      listRawImpressionUploadFilesResponse {
        rawImpressionUploadFiles +=
          manifestFile(parent, blobUri, if (parent == healthy.name) 10L else 20L)
      }
    }

    createDispatcher(
        readBlobMetadata = { RawImpressionBlobMetadata(30L, 100L, RAW_BLOB_CREATE_TIME) }
      )
      .upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

    val candidate = argumentCaptor<RegisterDetectedRawImpressionUploadCorrectionCandidateRequest>()
    verifyBlocking(correctionCandidateService) {
      registerDetectedRawImpressionUploadCorrectionCandidate(candidate.capture())
    }
    assertThat(candidate.firstValue.supersededRawImpressionUploadCorrectionCandidateId)
      .isEqualTo(
        RequestIds.forRawImpressionUploadCorrectionCandidate(
          quarantined.doneBlobUri,
          quarantined.doneBlobGeneration,
        )
      )
  }

  @Test
  fun `healthy no-op revision resolves the pending correction`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file.parquet")
    val blobUri =
      BlobUris.buildUri(SelectedStorageClient.parseBlobUri(DONE_BLOB_PATH), blob.blobKey)
    val healthy = previousUpload(generation = 90L, nameSuffix = "healthy")
    val quarantined =
      previousUpload(generation = 100L, nameSuffix = "quarantined").copy {
        state = RawImpressionUpload.State.CORRECTION_REQUIRED
        replacesRawImpressionUpload = healthy.name
      }
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubCorrectionCandidateRegistration()
    whenever(rawImpressionUploadService.listRawImpressionUploads(any()))
      .thenReturn(
        listRawImpressionUploadsResponse {
          rawImpressionUploads += healthy
          rawImpressionUploads += quarantined
        }
      )
    whenever(rawImpressionUploadFileService.listRawImpressionUploadFiles(any())).thenAnswer {
      invocation ->
      val parent = invocation.getArgument<ListRawImpressionUploadFilesRequest>(0).parent
      listRawImpressionUploadFilesResponse {
        rawImpressionUploadFiles +=
          manifestFile(parent, blobUri, if (parent == healthy.name) 10L else 20L)
      }
    }

    createDispatcher(
        readBlobMetadata = { RawImpressionBlobMetadata(10L, 100L, RAW_BLOB_CREATE_TIME) }
      )
      .upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

    val request = argumentCaptor<ResolveDetectedRawImpressionUploadCorrectionCandidateRequest>()
    verifyBlocking(correctionCandidateService) {
      resolveDetectedRawImpressionUploadCorrectionCandidate(request.capture())
    }
    assertThat(request.firstValue.rawImpressionUploadCorrectionCandidateId)
      .isEqualTo(
        RequestIds.forRawImpressionUploadCorrectionCandidate(
          quarantined.doneBlobUri,
          quarantined.doneBlobGeneration,
        )
      )
    verifyBlocking(rawImpressionUploadService, never()) { createRawImpressionUpload(any()) }
  }

  @Test
  fun `healthy append resolves the pending correction and registers only additions`() =
    runBlocking<Unit> {
      val retained = createMockBlob("$FOLDER_PREFIX/retained.parquet")
      val added = createMockBlob("$FOLDER_PREFIX/added.parquet")
      val doneUri = SelectedStorageClient.parseBlobUri(DONE_BLOB_PATH)
      val retainedUri = BlobUris.buildUri(doneUri, retained.blobKey)
      val addedUri = BlobUris.buildUri(doneUri, added.blobKey)
      val healthy = previousUpload(generation = 90L, nameSuffix = "healthy")
      val quarantined =
        previousUpload(generation = 100L, nameSuffix = "quarantined").copy {
          state = RawImpressionUpload.State.CORRECTION_REQUIRED
          replacesRawImpressionUpload = healthy.name
        }
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(retained, added))
      stubRawImpressionUploadCreation()
      stubCorrectionCandidateRegistration()
      whenever(rawImpressionUploadService.listRawImpressionUploads(any()))
        .thenReturn(
          listRawImpressionUploadsResponse {
            rawImpressionUploads += healthy
            rawImpressionUploads += quarantined
          },
          listRawImpressionUploadsResponse {
            rawImpressionUploads += healthy
            rawImpressionUploads += quarantined
          },
        )
      whenever(rawImpressionUploadFileService.listRawImpressionUploadFiles(any())).thenAnswer {
        invocation ->
        val request = invocation.getArgument<ListRawImpressionUploadFilesRequest>(0)
        listRawImpressionUploadFilesResponse {
          rawImpressionUploadFiles +=
            when (request.parent) {
              healthy.name -> listOf(manifestFile(healthy.name, retainedUri, 10L))
              quarantined.name ->
                listOf(
                  manifestFile(quarantined.name, retainedUri, 20L),
                  manifestFile(quarantined.name, addedUri, 30L),
                )
              "$DATA_PROVIDER_NAME/rawImpressionUploads/-" ->
                listOf(
                  manifestFile(healthy.name, retainedUri, 10L),
                  manifestFile(quarantined.name, retainedUri, 20L),
                  manifestFile(quarantined.name, addedUri, 30L),
                )
              else -> emptyList()
            }
        }
      }
      whenever(modelLinesService.listModelLines(any())).thenReturn(listModelLinesResponse {})
      val generations = mapOf(retained.blobKey to 10L, added.blobKey to 30L)

      createDispatcher(
          readBlobMetadata = {
            RawImpressionBlobMetadata(generations.getValue(it), 100L, RAW_BLOB_CREATE_TIME)
          }
        )
        .upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

      verifyBlocking(correctionCandidateService) {
        resolveDetectedRawImpressionUploadCorrectionCandidate(any())
      }
      val files = argumentCaptor<BatchCreateRawImpressionUploadFilesRequest>()
      verifyBlocking(rawImpressionUploadFileService) {
        batchCreateRawImpressionUploadFiles(files.capture())
      }
      assertThat(files.firstValue.requestsList.map { it.rawImpressionUploadFile.blobUri })
        .containsExactly(addedUri)
    }

  @Test
  fun `fast path does not dispatch an upload deferred by a correction fence`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubRawImpressionUploadCreation()
    whenever(rawImpressionUploadService.createRawImpressionUpload(any()))
      .thenReturn(
        rawImpressionUpload {
          name = "$DATA_PROVIDER_NAME/rawImpressionUploads/deferred"
          doneBlobUri = DONE_BLOB_PATH
          doneBlobGeneration = DONE_BLOB_GENERATION
          etag = UPLOAD_ETAG
          processingDeferred = true
        }
      )
    stubFullResolutionChain(MODEL_LINE_1)

    createDispatcher().upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

    val requests = argumentCaptor<ListRawImpressionUploadsRequest>()
    verifyBlocking(rawImpressionUploadService, times(2)) {
      listRawImpressionUploads(requests.capture())
    }
    assertThat(requests.allValues.all { it.filter.stateInList.isEmpty() }).isTrue()
    verifyBlocking(workItemsService, never()) { createWorkItem(any()) }
  }

  @Test
  fun `upload creates a RawImpressionUploadFile for each blob`() =
    runBlocking<Unit> {
      val blob1 = createMockBlob("$FOLDER_PREFIX/file1.parquet", size = 111L)
      val blob2 = createMockBlob("$FOLDER_PREFIX/file2.parquet", size = 222L)
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob1, blob2))
      stubRawImpressionUploadCreation()
      stubFullResolutionChain(MODEL_LINE_1)

      val dispatcher =
        createDispatcher(
          readBlobMetadata = { blobKey ->
            RawImpressionBlobMetadata(
              RAW_BLOB_GENERATION,
              when (blobKey) {
                blob1.blobKey -> 111L
                blob2.blobKey -> 222L
                else -> error("Unexpected blob: $blobKey")
              },
              RAW_BLOB_CREATE_TIME,
            )
          }
        )
      val outcome = dispatcher.upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

      assertThat(outcome).isEqualTo(VidLabelingDispatcher.UploadOutcome.SUCCEEDED)
      val requestCaptor = argumentCaptor<BatchCreateRawImpressionUploadFilesRequest>()
      verifyBlocking(rawImpressionUploadFileService) {
        batchCreateRawImpressionUploadFiles(requestCaptor.capture())
      }
      val request = requestCaptor.firstValue
      val uploadName = "$DATA_PROVIDER_NAME/rawImpressionUploads/$RAW_IMPRESSION_UPLOAD_ID"
      assertThat(request.parent).isEqualTo(uploadName)
      assertThat(request.requestsList.map { it.parent }).containsExactly(uploadName, uploadName)
      val bucket = SelectedStorageClient.parseBlobUri(DONE_BLOB_PATH).bucket
      val blobUris = request.requestsList.map { it.rawImpressionUploadFile.blobUri }
      assertThat(blobUris)
        .containsExactly(
          "file:///$bucket/$FOLDER_PREFIX/file1.parquet",
          "file:///$bucket/$FOLDER_PREFIX/file2.parquet",
        )
      // size_bytes is captured with the object's generation.
      assertThat(request.requestsList.map { it.rawImpressionUploadFile.sizeBytes })
        .containsExactly(111L, 222L)
      // event_date is populated from each file's plaintext Parquet footer (readEventDate seam).
      assertThat(request.requestsList.map { it.rawImpressionUploadFile.eventDate })
        .containsExactly(EVENT_DATE_PROTO, EVENT_DATE_PROTO)
      val completionCaptor = argumentCaptor<MarkRawImpressionUploadRegistrationCompleteRequest>()
      verifyBlocking(rawImpressionUploadService) {
        markRawImpressionUploadRegistrationComplete(completionCaptor.capture())
      }
      assertThat(completionCaptor.firstValue.etag).isEqualTo(UPLOAD_ETAG)
      assertThat(completionCaptor.firstValue.requestId)
        .isEqualTo(RequestIds.forRawImpressionUploadRegistrationComplete(uploadName, UPLOAD_ETAG))
    }

  @Test
  fun `upload rejects a new URI created after the done object`() = runBlocking {
    val earlyBlob = createMockBlob("$FOLDER_PREFIX/early.parquet")
    val lateBlob = createMockBlob("$FOLDER_PREFIX/late.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(earlyBlob, lateBlob))
    stubRawImpressionUploadCreation()

    val outcome =
      createDispatcher(
          readBlobMetadata = { blobKey ->
            RawImpressionBlobMetadata(
              generation = RAW_BLOB_GENERATION,
              sizeBytes = 100L,
              createTime =
                if (blobKey == lateBlob.blobKey) {
                  DONE_BLOB_CREATE_TIME.plusSeconds(1)
                } else {
                  RAW_BLOB_CREATE_TIME
                },
            )
          }
        )
        .upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

    assertThat(outcome).isEqualTo(VidLabelingDispatcher.UploadOutcome.INVALID_DONE_MARKER)
    verifyBlocking(rawImpressionUploadService, never()) { createRawImpressionUpload(any()) }
    verifyBlocking(rawImpressionUploadFileService, never()) {
      batchCreateRawImpressionUploadFiles(any())
    }
    verifyBlocking(rawImpressionUploadModelLineService, never()) {
      batchCreateRawImpressionUploadModelLines(any())
    }
    verifyBlocking(modelLinesService, never()) { listModelLines(any()) }
  }

  @Test
  fun `upload rejects a same URI overwrite created after the done object`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file.parquet")
    val blobUri =
      BlobUris.buildUri(SelectedStorageClient.parseBlobUri(DONE_BLOB_PATH), blob.blobKey)
    val previousUpload = rawImpressionUpload {
      name = "$DATA_PROVIDER_NAME/rawImpressionUploads/previous"
      doneBlobUri = DONE_BLOB_PATH
      doneBlobGeneration = DONE_BLOB_GENERATION - 1
      doneBlobCreateTime = DONE_BLOB_CREATE_TIME.minusSeconds(2).toProtoTime()
      state = RawImpressionUpload.State.COMPLETED
      registrationComplete = true
    }
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubRawImpressionUploadCreation()
    whenever(rawImpressionUploadService.listRawImpressionUploads(any()))
      .thenReturn(listRawImpressionUploadsResponse { rawImpressionUploads += previousUpload })
    whenever(rawImpressionUploadFileService.listRawImpressionUploadFiles(any()))
      .thenReturn(
        listRawImpressionUploadFilesResponse {
          rawImpressionUploadFiles += rawImpressionUploadFile {
            name = "${previousUpload.name}/files/file"
            this.blobUri = blobUri
            blobGeneration = RAW_BLOB_GENERATION - 1
          }
        }
      )

    val outcome =
      createDispatcher(
          readBlobMetadata = {
            RawImpressionBlobMetadata(
              generation = RAW_BLOB_GENERATION,
              sizeBytes = 100L,
              createTime = DONE_BLOB_CREATE_TIME.plusSeconds(1),
            )
          }
        )
        .upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

    assertThat(outcome).isEqualTo(VidLabelingDispatcher.UploadOutcome.INVALID_DONE_MARKER)
    verifyBlocking(rawImpressionUploadService, never()) { createRawImpressionUpload(any()) }
    verifyBlocking(rawImpressionUploadFileService, never()) { listRawImpressionUploadFiles(any()) }
    verifyBlocking(rawImpressionUploadFileService, never()) {
      batchCreateRawImpressionUploadFiles(any())
    }
    verifyBlocking(rawImpressionUploadModelLineService, never()) {
      batchCreateRawImpressionUploadModelLines(any())
    }
    verifyBlocking(modelLinesService, never()) { listModelLines(any()) }
  }

  @Test
  fun `upload rejects files with different footer event dates before creating file resources`() =
    runBlocking<Unit> {
      val blob1 = createMockBlob("$FOLDER_PREFIX/file1.parquet")
      val blob2 = createMockBlob("$FOLDER_PREFIX/file2.parquet")
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob1, blob2))
      stubRawImpressionUploadCreation()

      val exception =
        assertFailsWith<IllegalStateException> {
          createDispatcher(
              readEventDate = { blobUri ->
                if (blobUri.contains("file1.parquet")) EVENT_DATE else EVENT_DATE.plusDays(1)
              }
            )
            .upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)
        }

      assertThat(exception)
        .hasMessageThat()
        .contains("contains files with multiple footer event dates")
      verifyBlocking(rawImpressionUploadFileService, never()) {
        batchCreateRawImpressionUploadFiles(any())
      }
      verifyBlocking(rawImpressionUploadService, never()) {
        markRawImpressionUploadRegistrationComplete(any())
      }
    }

  @Test
  fun `upload routes Kingdom and metadata RPCs through their throttlers`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubRawImpressionUploadCreation()
    stubFullResolutionChain(MODEL_LINE_1)
    val kingdom = RecordingThrottler()
    val metadataRead = RecordingThrottler()
    val metadataWrite = RecordingThrottler()

    createDispatcher(
        rpcThrottlers =
          VidLabelingRpcThrottlers(
            kingdom,
            metadataRead,
            metadataWrite,
            VidLabelingRpcThrottlersTestHelper.alwaysReady().controlPlane,
          )
      )
      .upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

    assertThat(kingdom.onReadyCalls).isGreaterThan(0)
    assertThat(metadataRead.onReadyCalls).isGreaterThan(0)
    assertThat(metadataWrite.onReadyCalls).isGreaterThan(0)
  }

  @Test
  fun `classification avoids global registered-file scans`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file.parquet")
    val blobUri =
      BlobUris.buildUri(SelectedStorageClient.parseBlobUri(DONE_BLOB_PATH), blob.blobKey)
    val currentUpload =
      RawImpressionUpload.newBuilder()
        .setName("$DATA_PROVIDER_NAME/rawImpressionUploads/$RAW_IMPRESSION_UPLOAD_ID")
        .setDoneBlobUri(DONE_BLOB_PATH)
        .setDoneBlobGeneration(DONE_BLOB_GENERATION)
        .setDoneBlobCreateTime(DONE_BLOB_CREATE_TIME.toProtoTime())
        .setState(RawImpressionUpload.State.CREATED)
        .setEtag(UPLOAD_ETAG)
        .build()
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubRawImpressionUploadCreation()
    whenever(rawImpressionUploadService.listRawImpressionUploads(any()))
      .thenReturn(listRawImpressionUploadsResponse { rawImpressionUploads += currentUpload })
    whenever(rawImpressionUploadFileService.listRawImpressionUploadFiles(any())).thenAnswer {
      invocation ->
      val request = invocation.getArgument<ListRawImpressionUploadFilesRequest>(0)
      if (request.parent.endsWith("/-")) {
        listRawImpressionUploadFilesResponse {
          rawImpressionUploadFiles += rawImpressionUploadFile {
            name = "$DATA_PROVIDER_NAME/rawImpressionUploads/historical/files/file"
            this.blobUri = blobUri
            blobGeneration = RAW_BLOB_GENERATION
          }
        }
      } else {
        listRawImpressionUploadFilesResponse {}
      }
    }
    val metadataRead = RecordingThrottler()

    createDispatcher(
        rpcThrottlers =
          VidLabelingRpcThrottlers(
            VidLabelingRpcThrottlersTestHelper.alwaysReady().kingdom,
            metadataRead,
            VidLabelingRpcThrottlersTestHelper.alwaysReady().metadataWrite,
            VidLabelingRpcThrottlersTestHelper.alwaysReady().controlPlane,
          )
      )
      .upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

    assertThat(metadataRead.onReadyCalls).isEqualTo(2)
    verifyBlocking(rawImpressionUploadFileService, never()) { listRawImpressionUploadFiles(any()) }
  }

  @Test
  fun `upload throttles every page of chunked historical-file lookups`() = runBlocking {
    val blobs = (1..101).map { createMockBlob("$FOLDER_PREFIX/file$it.parquet") }
    val previousUploadName = "$DATA_PROVIDER_NAME/rawImpressionUploads/previous"
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(*blobs.toTypedArray()))
    whenever(rawImpressionUploadService.listRawImpressionUploads(any()))
      .thenReturn(
        listRawImpressionUploadsResponse {
          rawImpressionUploads +=
            RawImpressionUpload.newBuilder()
              .setName(previousUploadName)
              .setDoneBlobUri(DONE_BLOB_PATH)
              .setDoneBlobGeneration(100L)
              .setDoneBlobCreateTime(DONE_BLOB_CREATE_TIME.minusSeconds(1).toProtoTime())
              .setState(RawImpressionUpload.State.COMPLETED)
              .setRegistrationComplete(true)
              .build()
        }
      )
    whenever(rawImpressionUploadFileService.listRawImpressionUploadFiles(any())).thenAnswer {
      invocation ->
      val request = invocation.getArgument<ListRawImpressionUploadFilesRequest>(0)
      if (request.pageToken.isEmpty()) {
        listRawImpressionUploadFilesResponse {
          rawImpressionUploadFiles +=
            blobs.mapIndexed { index, blob ->
              rawImpressionUploadFile {
                name = "$previousUploadName/files/file-$index"
                blobUri =
                  BlobUris.buildUri(
                    SelectedStorageClient.parseBlobUri(DONE_BLOB_PATH),
                    blob.blobKey,
                  )
                blobGeneration = RAW_BLOB_GENERATION
              }
            }
          nextPageToken = "next"
        }
      } else {
        listRawImpressionUploadFilesResponse {}
      }
    }
    val metadataRead = RecordingThrottler()

    createDispatcher(
        rpcThrottlers =
          VidLabelingRpcThrottlers(
            VidLabelingRpcThrottlersTestHelper.alwaysReady().kingdom,
            metadataRead,
            VidLabelingRpcThrottlersTestHelper.alwaysReady().metadataWrite,
            VidLabelingRpcThrottlersTestHelper.alwaysReady().controlPlane,
          )
      )
      .upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

    assertThat(metadataRead.onReadyCalls).isEqualTo(3)
    val requestCaptor = argumentCaptor<ListRawImpressionUploadFilesRequest>()
    verifyBlocking(rawImpressionUploadFileService, times(2)) {
      listRawImpressionUploadFiles(requestCaptor.capture())
    }
    assertThat(requestCaptor.allValues.map { it.pageToken }).containsExactly("", "next").inOrder()
    verifyBlocking(rawImpressionUploadService, never()) { createRawImpressionUpload(any()) }
  }

  @Test
  fun `upload reads footer from captured blob generation`() = runBlocking {
    val blobKey = "edp1/2024-01-15/file1.parquet"
    val doneBlobPath = "gs://test-bucket/edp1/2024-01-15/done"
    val blob = createMockBlob(blobKey)
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubRawImpressionUploadCreation()
    stubFullResolutionChain(MODEL_LINE_1)
    var footerPath = ""

    val dispatcher =
      createDispatcher(
        readBlobMetadata = {
          RawImpressionBlobMetadata(RAW_BLOB_GENERATION, 123L, RAW_BLOB_CREATE_TIME)
        },
        readEventDate = {
          footerPath = it
          EVENT_DATE
        },
      )
    dispatcher.upload(doneBlobPath, DONE_BLOB_GENERATION)

    assertThat(footerPath)
      .isEqualTo(generationMatchedBlobUri("gs://test-bucket/$blobKey", RAW_BLOB_GENERATION))
  }

  @Test
  fun `upload with override model lines skips ListModelLines API`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubRawImpressionUploadCreation()
    stubOverrideResolutionChain()

    val dispatcher = createDispatcher(overrideModelLines = listOf(MODEL_LINE_1))
    dispatcher.upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

    verifyBlocking(modelLinesService, never()) { listModelLines(any()) }

    val requestCaptor = argumentCaptor<BatchCreateRawImpressionUploadModelLinesRequest>()
    verifyBlocking(rawImpressionUploadModelLineService) {
      batchCreateRawImpressionUploadModelLines(requestCaptor.capture())
    }
    val request = requestCaptor.firstValue
    assertThat(request.requestsList).hasSize(1)
    assertThat(request.requestsList[0].rawImpressionUploadModelLine.cmmsModelLine)
      .isEqualTo(MODEL_LINE_1)
  }

  @Test
  fun `upload accepts recovery override for latest failed memoized source`() = runBlocking {
    val sourceUploadName = "$DATA_PROVIDER_NAME/rawImpressionUploads/source-upload"
    val source = rawImpressionUpload {
      name = sourceUploadName
      state = RawImpressionUpload.State.FAILED
      doneBlobUri = DONE_BLOB_PATH
      doneBlobGeneration = DONE_BLOB_GENERATION + 100
      doneBlobCreateTime = DONE_BLOB_CREATE_TIME.minusSeconds(1).toProtoTime()
    }
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    val metadataRead = RecordingThrottler()
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubRecoverySource(source, metadataReadThrottler = metadataRead, paginateRecoveryReads = true)
    whenever(rawImpressionUploadService.createRawImpressionUpload(any()))
      .thenReturn(
        rawImpressionUpload {
          name = "$DATA_PROVIDER_NAME/rawImpressionUploads/$RAW_IMPRESSION_UPLOAD_ID"
          doneBlobUri = DONE_BLOB_PATH
          doneBlobGeneration = DONE_BLOB_GENERATION
          doneBlobCreateTime = DONE_BLOB_CREATE_TIME.toProtoTime()
        }
      )
    whenever(rawImpressionUploadFileService.batchCreateRawImpressionUploadFiles(any()))
      .thenReturn(batchCreateRawImpressionUploadFilesResponse {})
    whenever(rawImpressionUploadModelLineService.batchCreateRawImpressionUploadModelLines(any()))
      .thenReturn(batchCreateRawImpressionUploadModelLinesResponse {})
    stubOverrideResolutionChain()

    val dispatcher =
      createDispatcher(
        overrideModelLines = listOf(MODEL_LINE_1),
        recoverySourceUpload = sourceUploadName,
        recoveryOperationId = EVICTION_OPERATION_ID,
        rpcThrottlers =
          VidLabelingRpcThrottlers(
            VidLabelingRpcThrottlersTestHelper.alwaysReady().kingdom,
            metadataRead,
            VidLabelingRpcThrottlersTestHelper.alwaysReady().metadataWrite,
            VidLabelingRpcThrottlersTestHelper.alwaysReady().controlPlane,
          ),
      )
    dispatcher.upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

    val requestCaptor = argumentCaptor<CreateRawImpressionUploadRequest>()
    verifyBlocking(rawImpressionUploadService) {
      createRawImpressionUpload(requestCaptor.capture())
    }
    assertThat(requestCaptor.firstValue.rawImpressionUpload.uploadHealingOperation)
      .isEqualTo("$DATA_PROVIDER_NAME/uploadHealingOperations/$EVICTION_OPERATION_ID")
    assertThat(metadataRead.onReadyCalls).isGreaterThan(0)
  }

  @Test
  fun `upload activates the persisted quarantined manifest without relisting files`() =
    runBlocking {
      val sourceUploadName = "$DATA_PROVIDER_NAME/rawImpressionUploads/source-upload"
      val source = rawImpressionUpload {
        name = sourceUploadName
        state = RawImpressionUpload.State.FAILED
        doneBlobUri = DONE_BLOB_PATH
        doneBlobGeneration = DONE_BLOB_GENERATION + 100
        doneBlobCreateTime = DONE_BLOB_CREATE_TIME.minusSeconds(1).toProtoTime()
      }
      val candidate = rawImpressionUpload {
        name = "$DATA_PROVIDER_NAME/rawImpressionUploads/candidate-upload"
        state = RawImpressionUpload.State.CORRECTION_REQUIRED
        doneBlobUri = DONE_BLOB_PATH
        doneBlobGeneration = DONE_BLOB_GENERATION
        doneBlobCreateTime = DONE_BLOB_CREATE_TIME.toProtoTime()
        replacesRawImpressionUpload = source.name
        registrationComplete = true
      }
      stubRecoverySource(
        source,
        latestSourceRevision = candidate,
        recoveryPredecessorUpload = "",
        recoveryAction = RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION,
      )
      whenever(correctionCandidateService.activateQuarantinedRawImpressionUpload(any()))
        .thenReturn(
          internalRawImpressionUpload {
            dataProviderResourceId = "edp123"
            rawImpressionUploadResourceId = "candidate-upload"
            state =
              org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState
                .RAW_IMPRESSION_UPLOAD_STATE_CREATED
            doneBlobUri = DONE_BLOB_PATH
            doneBlobGeneration = DONE_BLOB_GENERATION
            doneBlobCreateTime = DONE_BLOB_CREATE_TIME.toProtoTime()
            registrationComplete = false
            etag = UPLOAD_ETAG
            evictionOperationId = EVICTION_OPERATION_ID
          }
        )
      whenever(rawImpressionUploadModelLineService.batchCreateRawImpressionUploadModelLines(any()))
        .thenReturn(batchCreateRawImpressionUploadModelLinesResponse {})
      stubOverrideResolutionChain()

      createDispatcher(
          overrideModelLines = listOf(MODEL_LINE_1),
          recoverySourceUpload = sourceUploadName,
          recoveryOperationId = EVICTION_OPERATION_ID,
        )
        .upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

      val request = argumentCaptor<ActivateQuarantinedRawImpressionUploadRequest>()
      verifyBlocking(correctionCandidateService) {
        activateQuarantinedRawImpressionUpload(request.capture())
      }
      assertThat(request.firstValue.rawImpressionUploadResourceId).isEqualTo("candidate-upload")
      assertThat(request.firstValue.sourceRawImpressionUploadResourceId).isEqualTo("source-upload")
      assertThat(request.firstValue.requestId)
        .isEqualTo(
          RequestIds.forActivateQuarantinedRawImpressionUpload(
            candidate.name,
            EVICTION_OPERATION_ID,
          )
        )
      verify(storageClient, never()).listBlobs(any())
      verifyBlocking(rawImpressionUploadFileService, never()) {
        batchCreateRawImpressionUploadFiles(any())
      }
      verifyBlocking(rawImpressionUploadService, never()) { createRawImpressionUpload(any()) }
      verifyBlocking(rawImpressionUploadModelLineService) {
        batchCreateRawImpressionUploadModelLines(any())
      }
    }

  @Test
  fun `upload accepts root recovery when no predecessor remains`() = runBlocking {
    val sourceUploadName = "$DATA_PROVIDER_NAME/rawImpressionUploads/source-upload"
    val source = rawImpressionUpload {
      name = sourceUploadName
      state = RawImpressionUpload.State.FAILED
      doneBlobUri = DONE_BLOB_PATH
      doneBlobGeneration = DONE_BLOB_GENERATION + 100
      doneBlobCreateTime = DONE_BLOB_CREATE_TIME.minusSeconds(1).toProtoTime()
    }
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubRecoverySource(source, recoveryPredecessorUpload = "")
    whenever(rawImpressionUploadService.createRawImpressionUpload(any()))
      .thenReturn(
        rawImpressionUpload {
          name = "$DATA_PROVIDER_NAME/rawImpressionUploads/$RAW_IMPRESSION_UPLOAD_ID"
          doneBlobUri = DONE_BLOB_PATH
          doneBlobGeneration = DONE_BLOB_GENERATION
          doneBlobCreateTime = DONE_BLOB_CREATE_TIME.toProtoTime()
        }
      )
    whenever(rawImpressionUploadFileService.batchCreateRawImpressionUploadFiles(any()))
      .thenReturn(batchCreateRawImpressionUploadFilesResponse {})
    whenever(rawImpressionUploadModelLineService.batchCreateRawImpressionUploadModelLines(any()))
      .thenReturn(batchCreateRawImpressionUploadModelLinesResponse {})
    stubOverrideResolutionChain()

    createDispatcher(
        overrideModelLines = listOf(MODEL_LINE_1),
        recoverySourceUpload = sourceUploadName,
        recoveryOperationId = EVICTION_OPERATION_ID,
      )
      .upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

    val requestCaptor = argumentCaptor<CreateRawImpressionUploadRequest>()
    verifyBlocking(rawImpressionUploadService) {
      createRawImpressionUpload(requestCaptor.capture())
    }
    assertThat(requestCaptor.firstValue.rawImpressionUpload.uploadHealingOperation)
      .isEqualTo("$DATA_PROVIDER_NAME/uploadHealingOperations/$EVICTION_OPERATION_ID")
  }

  @Test
  fun `upload rejects recovery until predecessor replacement completes`() = runBlocking {
    val sourceUploadName = "$DATA_PROVIDER_NAME/rawImpressionUploads/source-upload"
    val source = rawImpressionUpload {
      name = sourceUploadName
      state = RawImpressionUpload.State.FAILED
      doneBlobUri = DONE_BLOB_PATH
      doneBlobGeneration = DONE_BLOB_GENERATION + 100
      doneBlobCreateTime = DONE_BLOB_CREATE_TIME.minusSeconds(1).toProtoTime()
    }
    stubRecoverySource(
      source,
      predecessorModelLineState = RawImpressionUploadModelLine.State.RANKING,
    )
    val dispatcher =
      createDispatcher(
        overrideModelLines = listOf(MODEL_LINE_1),
        recoverySourceUpload = sourceUploadName,
        recoveryOperationId = EVICTION_OPERATION_ID,
      )

    val error =
      assertFailsWith<IllegalStateException> {
        dispatcher.upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)
      }

    assertThat(error).hasMessageThat().contains("has not been replaced by a completed upload")
    verifyBlocking(rawImpressionUploadService, never()) { createRawImpressionUpload(any()) }
  }

  @Test
  fun `upload rejects EDP correction until its predecessor recovery completes`() = runBlocking {
    val sourceUploadName = "$DATA_PROVIDER_NAME/rawImpressionUploads/source-upload"
    val predecessor = rawImpressionUpload {
      name = RECOVERY_PREDECESSOR_UPLOAD
      doneBlobUri = RECOVERY_PREDECESSOR_DONE_BLOB_PATH
      doneBlobGeneration = DONE_BLOB_GENERATION - 2
      doneBlobCreateTime = DONE_BLOB_CREATE_TIME.minusSeconds(3).toProtoTime()
    }
    val predecessorRecovery = rawImpressionUpload {
      name = RECOVERY_PREDECESSOR_REPLACEMENT
      doneBlobUri = RECOVERY_PREDECESSOR_DONE_BLOB_PATH
      doneBlobGeneration = DONE_BLOB_GENERATION - 1
      doneBlobCreateTime = DONE_BLOB_CREATE_TIME.minusSeconds(2).toProtoTime()
      replacesRawImpressionUpload = predecessor.name
    }
    val source = rawImpressionUpload {
      name = sourceUploadName
      state = RawImpressionUpload.State.FAILED
      doneBlobUri = DONE_BLOB_PATH
      doneBlobGeneration = DONE_BLOB_GENERATION + 100
      doneBlobCreateTime = DONE_BLOB_CREATE_TIME.minusSeconds(1).toProtoTime()
    }
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    whenever(rawImpressionUploadService.getRawImpressionUpload(any())).thenReturn(predecessor)
    whenever(rawImpressionUploadService.listRawImpressionUploads(any())).thenAnswer { invocation ->
      val request = invocation.getArgument<ListRawImpressionUploadsRequest>(0)
      listRawImpressionUploadsResponse {
        if (request.filter.doneBlobUri == RECOVERY_PREDECESSOR_DONE_BLOB_PATH) {
          rawImpressionUploads += predecessor
          rawImpressionUploads += predecessorRecovery
        } else {
          rawImpressionUploads += source
        }
      }
    }
    whenever(rawImpressionUploadModelLineService.listRawImpressionUploadModelLines(any()))
      .thenAnswer { invocation ->
        val request =
          invocation.getArgument<
            org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequest
          >(
            0
          )
        listRawImpressionUploadModelLinesResponse {
          rawImpressionUploadModelLines += rawImpressionUploadModelLine {
            name = "${request.parent}/rawImpressionUploadModelLines/rml1"
            cmmsModelLine = MODEL_LINE_1
            state =
              if (request.parent == source.name) RawImpressionUploadModelLine.State.FAILED
              else RawImpressionUploadModelLine.State.RANKING
            if (request.parent == source.name) {
              failureReason = RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT
              recoveryAction =
                RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION
              evictionOperationId = EVICTION_OPERATION_ID
              recoveryPredecessorRawImpressionUpload = predecessor.name
            }
          }
        }
      }

    val error =
      assertFailsWith<IllegalStateException> {
        createDispatcher().upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)
      }

    assertThat(error).hasMessageThat().contains("has not been replaced by a completed upload")
    verifyBlocking(rawImpressionUploadService, never()) { createRawImpressionUpload(any()) }
  }

  @Test
  fun `upload authorizes an EDP correction with its eviction operation`() = runBlocking {
    val sourceUploadName = "$DATA_PROVIDER_NAME/rawImpressionUploads/source-upload"
    val source = rawImpressionUpload {
      name = sourceUploadName
      state = RawImpressionUpload.State.FAILED
      doneBlobUri = DONE_BLOB_PATH
      doneBlobGeneration = DONE_BLOB_GENERATION + 100
      doneBlobCreateTime = DONE_BLOB_CREATE_TIME.minusSeconds(1).toProtoTime()
    }
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubRawImpressionUploadCreation()
    whenever(rawImpressionUploadService.listRawImpressionUploads(any()))
      .thenReturn(listRawImpressionUploadsResponse { rawImpressionUploads += source })
    whenever(rawImpressionUploadModelLineService.listRawImpressionUploadModelLines(any()))
      .thenReturn(
        listRawImpressionUploadModelLinesResponse {
          rawImpressionUploadModelLines += rawImpressionUploadModelLine {
            name = "$sourceUploadName/rawImpressionUploadModelLines/rml1"
            cmmsModelLine = MODEL_LINE_1
            state = RawImpressionUploadModelLine.State.FAILED
            failureReason = RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT
            recoveryAction =
              RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION
            evictionOperationId = EVICTION_OPERATION_ID
          }
        }
      )
    stubFullResolutionChain(MODEL_LINE_1)

    createDispatcher().upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

    val requestCaptor = argumentCaptor<CreateRawImpressionUploadRequest>()
    verifyBlocking(rawImpressionUploadService) {
      createRawImpressionUpload(requestCaptor.capture())
    }
    assertThat(requestCaptor.firstValue.rawImpressionUpload.uploadHealingOperation)
      .isEqualTo("$DATA_PROVIDER_NAME/uploadHealingOperations/$EVICTION_OPERATION_ID")
  }

  @Test
  fun `EDP correction retains evicted model lines after their active windows end`() = runBlocking {
    val sourceUploadName = "$DATA_PROVIDER_NAME/rawImpressionUploads/source-upload"
    val source = rawImpressionUpload {
      name = sourceUploadName
      state = RawImpressionUpload.State.FAILED
      doneBlobUri = DONE_BLOB_PATH
      doneBlobGeneration = DONE_BLOB_GENERATION + 100
      doneBlobCreateTime = DONE_BLOB_CREATE_TIME.minusSeconds(1).toProtoTime()
    }
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubRawImpressionUploadCreation()
    whenever(rawImpressionUploadService.listRawImpressionUploads(any())).thenAnswer { invocation ->
      val request = invocation.getArgument<ListRawImpressionUploadsRequest>(0)
      listRawImpressionUploadsResponse {
        if (request.filter.doneBlobUri == DONE_BLOB_PATH) rawImpressionUploads += source
      }
    }
    whenever(rawImpressionUploadModelLineService.listRawImpressionUploadModelLines(any()))
      .thenReturn(
        listRawImpressionUploadModelLinesResponse {
          for ((id, modelLine) in listOf("rml1" to MODEL_LINE_1, "rml2" to MODEL_LINE_2)) {
            rawImpressionUploadModelLines += rawImpressionUploadModelLine {
              name = "$sourceUploadName/rawImpressionUploadModelLines/$id"
              cmmsModelLine = modelLine
              state = RawImpressionUploadModelLine.State.FAILED
              failureReason = RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT
              recoveryAction =
                RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION
              evictionOperationId = EVICTION_OPERATION_ID
            }
          }
        }
      )
    // No ListModelLines response is configured: the replacement must use the persisted evicted
    // rows even though those model lines are no longer returned as active by the VID Repository.
    stubOverrideResolutionChain()

    createDispatcher().upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

    val requestCaptor = argumentCaptor<BatchCreateRawImpressionUploadModelLinesRequest>()
    verifyBlocking(rawImpressionUploadModelLineService) {
      batchCreateRawImpressionUploadModelLines(requestCaptor.capture())
    }
    assertThat(
        requestCaptor.firstValue.requestsList.map { it.rawImpressionUploadModelLine.cmmsModelLine }
      )
      .containsExactly(MODEL_LINE_1, MODEL_LINE_2)
      .inOrder()
    verifyBlocking(modelLinesService, never()) { listModelLines(any()) }
  }

  @Test
  fun `EDP upload cannot replace a permanently removed upload`() = runBlocking {
    val sourceUploadName = "$DATA_PROVIDER_NAME/rawImpressionUploads/source-upload"
    val source = rawImpressionUpload {
      name = sourceUploadName
      state = RawImpressionUpload.State.FAILED
      doneBlobUri = DONE_BLOB_PATH
      doneBlobGeneration = DONE_BLOB_GENERATION + 100
      doneBlobCreateTime = DONE_BLOB_CREATE_TIME.minusSeconds(1).toProtoTime()
    }
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    whenever(rawImpressionUploadService.listRawImpressionUploads(any()))
      .thenReturn(listRawImpressionUploadsResponse { rawImpressionUploads += source })
    whenever(rawImpressionUploadModelLineService.listRawImpressionUploadModelLines(any()))
      .thenReturn(
        listRawImpressionUploadModelLinesResponse {
          rawImpressionUploadModelLines += rawImpressionUploadModelLine {
            name = "$sourceUploadName/rawImpressionUploadModelLines/rml1"
            cmmsModelLine = MODEL_LINE_1
            state = RawImpressionUploadModelLine.State.FAILED
            failureReason = RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT
            recoveryAction =
              RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_NO_REPLACEMENT
            evictionOperationId = EVICTION_OPERATION_ID
          }
        }
      )

    val error =
      assertFailsWith<IllegalStateException> {
        createDispatcher().upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)
      }

    assertThat(error).hasMessageThat().contains("permanently removed")
    verifyBlocking(rawImpressionUploadService, never()) { createRawImpressionUpload(any()) }
  }

  @Test
  fun `upload resumes an already registered recovery generation`() = runBlocking {
    val sourceUploadName = "$DATA_PROVIDER_NAME/rawImpressionUploads/source-upload"
    val source = rawImpressionUpload {
      name = sourceUploadName
      state = RawImpressionUpload.State.FAILED
      doneBlobUri = DONE_BLOB_PATH
      doneBlobGeneration = DONE_BLOB_GENERATION + 100
      doneBlobCreateTime = DONE_BLOB_CREATE_TIME.minusSeconds(1).toProtoTime()
    }
    val registeredRecovery = rawImpressionUpload {
      name = "$DATA_PROVIDER_NAME/rawImpressionUploads/$RAW_IMPRESSION_UPLOAD_ID"
      doneBlobUri = DONE_BLOB_PATH
      doneBlobGeneration = DONE_BLOB_GENERATION
      doneBlobCreateTime = DONE_BLOB_CREATE_TIME.toProtoTime()
      replacesRawImpressionUpload = sourceUploadName
      uploadHealingOperation = "$DATA_PROVIDER_NAME/uploadHealingOperations/$EVICTION_OPERATION_ID"
    }
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubRecoverySource(source, registeredRecovery)
    whenever(rawImpressionUploadService.createRawImpressionUpload(any())).thenAnswer {
      throw StatusException(Status.ALREADY_EXISTS)
    }
    whenever(rawImpressionUploadFileService.batchCreateRawImpressionUploadFiles(any()))
      .thenReturn(batchCreateRawImpressionUploadFilesResponse {})
    whenever(rawImpressionUploadModelLineService.batchCreateRawImpressionUploadModelLines(any()))
      .thenReturn(batchCreateRawImpressionUploadModelLinesResponse {})
    stubOverrideResolutionChain()

    val dispatcher =
      createDispatcher(
        overrideModelLines = listOf(MODEL_LINE_1),
        recoverySourceUpload = sourceUploadName,
        recoveryOperationId = EVICTION_OPERATION_ID,
      )
    dispatcher.upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

    verifyBlocking(rawImpressionUploadFileService) { batchCreateRawImpressionUploadFiles(any()) }
    verifyBlocking(rawImpressionUploadModelLineService) {
      batchCreateRawImpressionUploadModelLines(any())
    }
  }

  @Test
  fun `upload accepts newer recovery after prior recovery stopped after upload creation`() =
    runBlocking {
      val sourceUploadName = "$DATA_PROVIDER_NAME/rawImpressionUploads/source-upload"
      val source = rawImpressionUpload {
        name = sourceUploadName
        state = RawImpressionUpload.State.FAILED
        doneBlobUri = DONE_BLOB_PATH
        doneBlobGeneration = DONE_BLOB_GENERATION + 200
        doneBlobCreateTime = DONE_BLOB_CREATE_TIME.minusSeconds(2).toProtoTime()
      }
      val incompleteRecovery = rawImpressionUpload {
        name = "$DATA_PROVIDER_NAME/rawImpressionUploads/incomplete-recovery"
        state = RawImpressionUpload.State.CREATED
        doneBlobUri = DONE_BLOB_PATH
        doneBlobGeneration = DONE_BLOB_GENERATION + 100
        doneBlobCreateTime = DONE_BLOB_CREATE_TIME.minusSeconds(1).toProtoTime()
        replacesRawImpressionUpload = sourceUploadName
        registrationComplete = false
      }
      val newRecovery = rawImpressionUpload {
        name = "$DATA_PROVIDER_NAME/rawImpressionUploads/$RAW_IMPRESSION_UPLOAD_ID"
        state = RawImpressionUpload.State.CREATED
        doneBlobUri = DONE_BLOB_PATH
        doneBlobGeneration = DONE_BLOB_GENERATION
        doneBlobCreateTime = DONE_BLOB_CREATE_TIME.toProtoTime()
        replacesRawImpressionUpload = incompleteRecovery.name
        etag = UPLOAD_ETAG
      }
      val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
      stubRawImpressionUploadCreation()
      stubRecoverySource(source, incompleteRecovery)
      whenever(rawImpressionUploadService.createRawImpressionUpload(any())).thenReturn(newRecovery)
      stubOverrideResolutionChain()

      createDispatcher(
          overrideModelLines = listOf(MODEL_LINE_1),
          recoverySourceUpload = sourceUploadName,
          recoveryOperationId = EVICTION_OPERATION_ID,
        )
        .upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

      val createCaptor = argumentCaptor<CreateRawImpressionUploadRequest>()
      verifyBlocking(rawImpressionUploadService) {
        createRawImpressionUpload(createCaptor.capture())
      }
      assertThat(createCaptor.firstValue.rawImpressionUpload.doneBlobGeneration)
        .isEqualTo(DONE_BLOB_GENERATION)
      verifyBlocking(rawImpressionUploadFileService) { batchCreateRawImpressionUploadFiles(any()) }
      verifyBlocking(rawImpressionUploadModelLineService) {
        batchCreateRawImpressionUploadModelLines(any())
      }
      verifyBlocking(rawImpressionUploadService) {
        markRawImpressionUploadRegistrationComplete(any())
      }
    }

  @Test
  fun `upload ignores stale recovery event after newer generation is registered`() = runBlocking {
    val sourceUploadName = "$DATA_PROVIDER_NAME/rawImpressionUploads/source-upload"
    val source = rawImpressionUpload {
      name = sourceUploadName
      state = RawImpressionUpload.State.FAILED
      doneBlobUri = DONE_BLOB_PATH
      doneBlobGeneration = DONE_BLOB_GENERATION + 100
      doneBlobCreateTime = DONE_BLOB_CREATE_TIME.minusSeconds(1).toProtoTime()
    }
    val newerRecovery = rawImpressionUpload {
      name = "$DATA_PROVIDER_NAME/rawImpressionUploads/newer-recovery"
      doneBlobUri = DONE_BLOB_PATH
      doneBlobGeneration = DONE_BLOB_GENERATION - 100
      doneBlobCreateTime = DONE_BLOB_CREATE_TIME.plusSeconds(1).toProtoTime()
      replacesRawImpressionUpload = sourceUploadName
    }
    whenever(rawImpressionUploadService.getRawImpressionUpload(any())).thenReturn(source)
    whenever(rawImpressionUploadService.listRawImpressionUploads(any()))
      .thenReturn(listRawImpressionUploadsResponse { rawImpressionUploads += newerRecovery })

    val dispatcher =
      createDispatcher(
        overrideModelLines = listOf(MODEL_LINE_1),
        recoverySourceUpload = sourceUploadName,
        recoveryOperationId = EVICTION_OPERATION_ID,
      )
    dispatcher.upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

    verifyBlocking(storageClient, never()) { listBlobs(any()) }
    verifyBlocking(rawImpressionUploadService, never()) { createRawImpressionUpload(any()) }
  }

  @Test
  fun `upload rejects recovery override when source row is not failed`() = runBlocking {
    val sourceUploadName = "$DATA_PROVIDER_NAME/rawImpressionUploads/source-upload"
    val source = rawImpressionUpload {
      name = sourceUploadName
      state = RawImpressionUpload.State.COMPLETED
      doneBlobUri = DONE_BLOB_PATH
      doneBlobGeneration = DONE_BLOB_GENERATION + 100
      doneBlobCreateTime = DONE_BLOB_CREATE_TIME.minusSeconds(1).toProtoTime()
    }
    whenever(rawImpressionUploadService.getRawImpressionUpload(any())).thenReturn(source)
    whenever(rawImpressionUploadService.listRawImpressionUploads(any()))
      .thenReturn(listRawImpressionUploadsResponse { rawImpressionUploads += source })
    whenever(rawImpressionUploadModelLineService.listRawImpressionUploadModelLines(any()))
      .thenReturn(
        listRawImpressionUploadModelLinesResponse {
          rawImpressionUploadModelLines += rawImpressionUploadModelLine {
            name = "$sourceUploadName/rawImpressionUploadModelLines/rml1"
            cmmsModelLine = MODEL_LINE_1
            state = RawImpressionUploadModelLine.State.COMPLETED
          }
        }
      )
    val dispatcher =
      createDispatcher(
        overrideModelLines = listOf(MODEL_LINE_1),
        recoverySourceUpload = sourceUploadName,
        recoveryOperationId = EVICTION_OPERATION_ID,
      )

    val error =
      assertFailsWith<IllegalArgumentException> {
        dispatcher.upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)
      }

    assertThat(error).hasMessageThat().contains("FAILED source model-line rows")
    verifyBlocking(rawImpressionUploadService, never()) { createRawImpressionUpload(any()) }
  }

  @Test
  fun `upload with no active model lines creates upload and files but no model lines`() =
    runBlocking {
      val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
      stubRawImpressionUploadCreation()
      whenever(modelLinesService.listModelLines(any())).thenReturn(listModelLinesResponse {})
      val metadataWrite = RecordingThrottler()

      val dispatcher =
        createDispatcher(
          rpcThrottlers =
            VidLabelingRpcThrottlersTestHelper.alwaysReady().copy(metadataWrite = metadataWrite)
        )
      val outcome = dispatcher.upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

      verifyBlocking(rawImpressionUploadService) { createRawImpressionUpload(any()) }
      verifyBlocking(rawImpressionUploadFileService) { batchCreateRawImpressionUploadFiles(any()) }
      verifyBlocking(rawImpressionUploadModelLineService, never()) {
        batchCreateRawImpressionUploadModelLines(any())
      }
      verifyBlocking(rawImpressionUploadService) {
        markRawImpressionUploadRegistrationComplete(any())
      }
      assertThat(outcome).isEqualTo(VidLabelingDispatcher.UploadOutcome.NO_WORK)
      assertThat(metadataWrite.onReadyCalls).isEqualTo(3)
    }

  @Test
  fun `upload wraps registration completion RPC failure`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubRawImpressionUploadCreation()
    whenever(modelLinesService.listModelLines(any())).thenReturn(listModelLinesResponse {})
    whenever(rawImpressionUploadService.markRawImpressionUploadRegistrationComplete(any()))
      .thenAnswer {
        throw StatusException(Status.UNAVAILABLE.withDescription("metadata store unavailable"))
      }

    val exception =
      assertFailsWith<Exception> { createDispatcher().upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION) }

    assertThat(exception).hasMessageThat().contains("Error marking RawImpressionUpload")
    assertThat(exception).hasCauseThat().isInstanceOf(StatusException::class.java)
  }

  @Test
  fun `upload excludes done marker from file list`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    val doneBlob = createMockBlob("$FOLDER_PREFIX/done")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob, doneBlob))
    stubRawImpressionUploadCreation()
    stubFullResolutionChain(MODEL_LINE_1)

    val dispatcher = createDispatcher()
    dispatcher.upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

    val requestCaptor = argumentCaptor<BatchCreateRawImpressionUploadFilesRequest>()
    verifyBlocking(rawImpressionUploadFileService) {
      batchCreateRawImpressionUploadFiles(requestCaptor.capture())
    }
    assertThat(requestCaptor.firstValue.requestsList).hasSize(1)
    assertThat(
        requestCaptor.firstValue.requestsList.single().rawImpressionUploadFile.blobGeneration
      )
      .isEqualTo(RAW_BLOB_GENERATION)
  }

  @Test
  fun `upload propagates exception on ListModelLines failure`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubRawImpressionUploadCreation()
    whenever(modelLinesService.listModelLines(any())).thenAnswer {
      throw StatusException(Status.UNAVAILABLE.withDescription("VID Repo unavailable"))
    }

    val dispatcher = createDispatcher()
    val exception =
      assertFailsWith<Exception> { dispatcher.upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION) }
    assertThat(exception).hasMessageThat().contains("Error listing model lines")
    assertThat(exception).hasCauseThat().isInstanceOf(StatusException::class.java)
  }

  @Test
  fun `upload skips model line when no rollout found`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubRawImpressionUploadCreation()
    whenever(modelLinesService.listModelLines(any()))
      .thenReturn(
        listModelLinesResponse {
          modelLines += modelLine {
            name = MODEL_LINE_1
            type = ModelLine.Type.PROD
            activeStartTime = Timestamps.fromMillis(FIXED_NOW.toEpochMilli() - 86400000)
            activeEndTime = Timestamps.fromMillis(FIXED_NOW.toEpochMilli() + 86400000)
          }
        }
      )
    whenever(modelRolloutsService.listModelRollouts(any())).thenReturn(listModelRolloutsResponse {})

    val dispatcher = createDispatcher()
    dispatcher.upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

    verifyBlocking(rawImpressionUploadModelLineService, never()) {
      batchCreateRawImpressionUploadModelLines(any())
    }
  }

  @Test
  fun `redelivery of complete same-generation upload skips file inspection`() =
    runBlocking<Unit> {
      val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
      val uploadName = "$DATA_PROVIDER_NAME/rawImpressionUploads/$RAW_IMPRESSION_UPLOAD_ID"
      whenever(rawImpressionUploadService.listRawImpressionUploads(any()))
        .thenReturn(
          listRawImpressionUploadsResponse {
            rawImpressionUploads +=
              RawImpressionUpload.newBuilder()
                .setName(uploadName)
                .setDoneBlobUri(DONE_BLOB_PATH)
                .setDoneBlobGeneration(DONE_BLOB_GENERATION)
                .setDoneBlobCreateTime(DONE_BLOB_CREATE_TIME.toProtoTime())
                .setRegistrationComplete(true)
                .setEtag("completed-$UPLOAD_ETAG")
                .build()
          }
        )

      var rawBlobMetadataReads = 0
      val dispatcher =
        createDispatcher(
          readBlobMetadata = {
            rawBlobMetadataReads++
            RawImpressionBlobMetadata(RAW_BLOB_GENERATION, 100L, RAW_BLOB_CREATE_TIME)
          }
        )
      dispatcher.upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

      assertThat(rawBlobMetadataReads).isEqualTo(0)
      verifyBlocking(rawImpressionUploadService, never()) { createRawImpressionUpload(any()) }
      verifyBlocking(rawImpressionUploadFileService, never()) {
        listRawImpressionUploadFiles(any())
      }
      verifyBlocking(rawImpressionUploadFileService, never()) {
        batchCreateRawImpressionUploadFiles(any())
      }
      verifyBlocking(rawImpressionUploadModelLineService, never()) {
        batchCreateRawImpressionUploadModelLines(any())
      }
      verifyBlocking(rawImpressionUploadService, never()) {
        markRawImpressionUploadRegistrationComplete(any())
      }
    }

  @Test
  fun `upload with different generation produces different request ID`() =
    runBlocking<Unit> {
      val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
      stubRawImpressionUploadCreation()
      stubFullResolutionChain(MODEL_LINE_1)

      var liveGeneration = 123L
      var liveCreateTime = DONE_BLOB_CREATE_TIME
      val dispatcher =
        createDispatcher(
          readDoneBlobMetadata = { RawImpressionBlobMetadata(liveGeneration, 0L, liveCreateTime) }
        )
      dispatcher.upload(DONE_BLOB_PATH, doneBlobGeneration = 123L)

      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
      liveGeneration = 456L
      liveCreateTime = DONE_BLOB_CREATE_TIME.plusSeconds(1)
      dispatcher.upload(DONE_BLOB_PATH, doneBlobGeneration = 456L)

      val requestCaptor = argumentCaptor<CreateRawImpressionUploadRequest>()
      verifyBlocking(rawImpressionUploadService, times(2)) {
        createRawImpressionUpload(requestCaptor.capture())
      }
      assertThat(requestCaptor.allValues[0].requestId)
        .isNotEqualTo(requestCaptor.allValues[1].requestId)
      assertThat(requestCaptor.allValues.map { it.rawImpressionUpload.doneBlobGeneration })
        .containsExactly(123L, 456L)
    }

  @Test
  fun `additive replacement registers only added object versions`() =
    runBlocking<Unit> {
      val blob1 = createMockBlob("$FOLDER_PREFIX/file1.parquet")
      val blob2 = createMockBlob("$FOLDER_PREFIX/file2.parquet")
      val blob3 = createMockBlob("$FOLDER_PREFIX/file3.parquet")
      val doneBlobUri = SelectedStorageClient.parseBlobUri(DONE_BLOB_PATH)
      val blob1Uri = BlobUris.buildUri(doneBlobUri, blob1.blobKey)
      val blob2Uri = BlobUris.buildUri(doneBlobUri, blob2.blobKey)
      val blob3Uri = BlobUris.buildUri(doneBlobUri, blob3.blobKey)
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob1, blob2, blob3))
      stubRawImpressionUploadCreation()
      stubFullResolutionChain(MODEL_LINE_1)
      whenever(rawImpressionUploadService.listRawImpressionUploads(any())).thenAnswer { invocation
        ->
        val request = invocation.getArgument<ListRawImpressionUploadsRequest>(0)
        if (request.filter.doneBlobUri == DONE_BLOB_PATH) {
          listRawImpressionUploadsResponse {
            rawImpressionUploads +=
              RawImpressionUpload.newBuilder()
                .setName("$DATA_PROVIDER_NAME/rawImpressionUploads/previous")
                .setDoneBlobUri(DONE_BLOB_PATH)
                .setDoneBlobGeneration(100L)
                .setDoneBlobCreateTime(DONE_BLOB_CREATE_TIME.minusSeconds(1).toProtoTime())
                .setState(RawImpressionUpload.State.COMPLETED)
                .setRegistrationComplete(true)
                .build()
          }
        } else {
          listRawImpressionUploadsResponse {}
        }
      }
      whenever(rawImpressionUploadFileService.listRawImpressionUploadFiles(any()))
        .thenReturn(
          listRawImpressionUploadFilesResponse {
            rawImpressionUploadFiles += rawImpressionUploadFile {
              name = "$DATA_PROVIDER_NAME/rawImpressionUploads/previous/files/file1"
              blobUri = blob1Uri
              blobGeneration = 10L
            }
            rawImpressionUploadFiles += rawImpressionUploadFile {
              name = "$DATA_PROVIDER_NAME/rawImpressionUploads/previous/files/file2"
              blobUri = blob2Uri
              blobGeneration = 15L
            }
          }
        )

      val generations = mapOf(blob1.blobKey to 10L, blob2.blobKey to 15L, blob3.blobKey to 30L)
      createDispatcher(
          readBlobMetadata = {
            RawImpressionBlobMetadata(
              generations.getValue(it),
              sizeBytes = 100L,
              createTime = RAW_BLOB_CREATE_TIME,
            )
          },
          readDoneBlobMetadata = { RawImpressionBlobMetadata(200L, 0L, DONE_BLOB_CREATE_TIME) },
        )
        .upload(DONE_BLOB_PATH, doneBlobGeneration = 200L)

      val createRequest = argumentCaptor<BatchCreateRawImpressionUploadFilesRequest>()
      verifyBlocking(rawImpressionUploadFileService) {
        batchCreateRawImpressionUploadFiles(createRequest.capture())
      }
      assertThat(createRequest.firstValue.requestsList.map { it.rawImpressionUploadFile.blobUri })
        .containsExactly(blob3Uri)
      assertThat(
          createRequest.firstValue.requestsList.map { it.rawImpressionUploadFile.blobGeneration }
        )
        .containsExactly(30L)

      val listRequest = argumentCaptor<ListRawImpressionUploadFilesRequest>()
      verifyBlocking(rawImpressionUploadFileService) {
        listRawImpressionUploadFiles(listRequest.capture())
      }
      assertThat(listRequest.firstValue.parent)
        .isEqualTo("$DATA_PROVIDER_NAME/rawImpressionUploads/previous")
    }

  @Test
  fun `replacement upload with no new object versions is ignored`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    val blobUri =
      BlobUris.buildUri(SelectedStorageClient.parseBlobUri(DONE_BLOB_PATH), blob.blobKey)
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    whenever(rawImpressionUploadService.listRawImpressionUploads(any()))
      .thenReturn(
        listRawImpressionUploadsResponse {
          rawImpressionUploads +=
            RawImpressionUpload.newBuilder()
              .setName("$DATA_PROVIDER_NAME/rawImpressionUploads/previous")
              .setDoneBlobUri(DONE_BLOB_PATH)
              .setDoneBlobGeneration(100L)
              .setDoneBlobCreateTime(DONE_BLOB_CREATE_TIME.minusSeconds(1).toProtoTime())
              .setState(RawImpressionUpload.State.COMPLETED)
              .setRegistrationComplete(true)
              .build()
        }
      )
    whenever(rawImpressionUploadFileService.listRawImpressionUploadFiles(any()))
      .thenReturn(
        listRawImpressionUploadFilesResponse {
          rawImpressionUploadFiles += rawImpressionUploadFile {
            name = "$DATA_PROVIDER_NAME/rawImpressionUploads/previous/files/file1"
            this.blobUri = blobUri
            blobGeneration = RAW_BLOB_GENERATION
          }
        }
      )

    createDispatcher(
        readDoneBlobMetadata = { RawImpressionBlobMetadata(200L, 0L, DONE_BLOB_CREATE_TIME) }
      )
      .upload(DONE_BLOB_PATH, doneBlobGeneration = 200L)

    verifyBlocking(rawImpressionUploadService, never()) { createRawImpressionUpload(any()) }
    verifyBlocking(rawImpressionUploadModelLineService, never()) {
      batchCreateRawImpressionUploadModelLines(any())
    }
  }

  @Test
  fun `new generation supersedes an incomplete predecessor even when its file was registered`() =
    runBlocking<Unit> {
      val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
      val blobUri =
        BlobUris.buildUri(SelectedStorageClient.parseBlobUri(DONE_BLOB_PATH), blob.blobKey)
      val previous =
        RawImpressionUpload.newBuilder()
          .setName("$DATA_PROVIDER_NAME/rawImpressionUploads/previous")
          .setDoneBlobUri(DONE_BLOB_PATH)
          .setDoneBlobGeneration(900L)
          .setDoneBlobCreateTime(DONE_BLOB_CREATE_TIME.minusSeconds(1).toProtoTime())
          .setState(RawImpressionUpload.State.CREATED)
          .build()
      val current =
        RawImpressionUpload.newBuilder()
          .setName("$DATA_PROVIDER_NAME/rawImpressionUploads/current")
          .setDoneBlobUri(DONE_BLOB_PATH)
          .setDoneBlobGeneration(120L)
          .setDoneBlobCreateTime(DONE_BLOB_CREATE_TIME.toProtoTime())
          .setReplacesRawImpressionUpload(previous.name)
          .setState(RawImpressionUpload.State.CREATED)
          .setEtag(UPLOAD_ETAG)
          .build()
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
      stubRawImpressionUploadCreation()
      stubFullResolutionChain(MODEL_LINE_1)
      whenever(rawImpressionUploadService.createRawImpressionUpload(any())).thenReturn(current)
      whenever(rawImpressionUploadService.listRawImpressionUploads(any()))
        .thenReturn(
          listRawImpressionUploadsResponse { rawImpressionUploads += previous },
          listRawImpressionUploadsResponse {
            rawImpressionUploads +=
              previous.toBuilder().setState(RawImpressionUpload.State.FAILED).build()
            rawImpressionUploads += current
          },
        )
      whenever(rawImpressionUploadFileService.listRawImpressionUploadFiles(any()))
        .thenReturn(
          listRawImpressionUploadFilesResponse {
            rawImpressionUploadFiles += rawImpressionUploadFile {
              name = "${previous.name}/files/file1"
              this.blobUri = blobUri
              blobGeneration = RAW_BLOB_GENERATION
            }
          }
        )

      createDispatcher(
          readDoneBlobMetadata = { RawImpressionBlobMetadata(120L, 0L, DONE_BLOB_CREATE_TIME) }
        )
        .upload(DONE_BLOB_PATH, doneBlobGeneration = 120L)

      verifyBlocking(rawImpressionUploadService) { createRawImpressionUpload(any()) }
      verifyBlocking(rawImpressionUploadFileService) { batchCreateRawImpressionUploadFiles(any()) }
    }

  @Test
  fun `refreshed no-op delta creates no work`() =
    runBlocking<Unit> {
      val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
      val blobUri =
        BlobUris.buildUri(SelectedStorageClient.parseBlobUri(DONE_BLOB_PATH), blob.blobKey)
      val previous =
        RawImpressionUpload.newBuilder()
          .setName("$DATA_PROVIDER_NAME/rawImpressionUploads/previous")
          .setDoneBlobUri(DONE_BLOB_PATH)
          .setDoneBlobGeneration(900L)
          .setDoneBlobCreateTime(DONE_BLOB_CREATE_TIME.minusSeconds(1).toProtoTime())
          .setState(RawImpressionUpload.State.COMPLETED)
          .setRegistrationComplete(true)
          .build()
      val current =
        RawImpressionUpload.newBuilder()
          .setName("$DATA_PROVIDER_NAME/rawImpressionUploads/current")
          .setDoneBlobUri(DONE_BLOB_PATH)
          .setDoneBlobGeneration(120L)
          .setDoneBlobCreateTime(DONE_BLOB_CREATE_TIME.toProtoTime())
          .setReplacesRawImpressionUpload(previous.name)
          .setState(RawImpressionUpload.State.CREATED)
          .setEtag(UPLOAD_ETAG)
          .build()
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
      stubRawImpressionUploadCreation()
      whenever(rawImpressionUploadService.createRawImpressionUpload(any())).thenReturn(current)
      whenever(rawImpressionUploadService.listRawImpressionUploads(any()))
        .thenReturn(
          listRawImpressionUploadsResponse { rawImpressionUploads += previous },
          listRawImpressionUploadsResponse {
            rawImpressionUploads += previous
            rawImpressionUploads += current
          },
        )
      whenever(rawImpressionUploadFileService.listRawImpressionUploadFiles(any()))
        .thenReturn(
          listRawImpressionUploadFilesResponse {
            rawImpressionUploadFiles += rawImpressionUploadFile {
              name = "${previous.name}/files/file1"
              this.blobUri = blobUri
              blobGeneration = RAW_BLOB_GENERATION
            }
          }
        )

      createDispatcher(
          readDoneBlobMetadata = { RawImpressionBlobMetadata(120L, 0L, DONE_BLOB_CREATE_TIME) }
        )
        .upload(DONE_BLOB_PATH, doneBlobGeneration = 120L)

      verifyBlocking(rawImpressionUploadService, never()) { createRawImpressionUpload(any()) }
      verifyBlocking(rawImpressionUploadService, never()) {
        markRawImpressionUploadRegistrationComplete(any())
      }
      verifyBlocking(rawImpressionUploadModelLineService, never()) {
        batchCreateRawImpressionUploadModelLines(any())
      }
    }

  @Test
  fun `redelivery excludes the current upload from registered object versions`() =
    runBlocking<Unit> {
      val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
      val blobUri =
        BlobUris.buildUri(SelectedStorageClient.parseBlobUri(DONE_BLOB_PATH), blob.blobKey)
      val currentUpload =
        RawImpressionUpload.newBuilder()
          .setName("$DATA_PROVIDER_NAME/rawImpressionUploads/current")
          .setDoneBlobUri(DONE_BLOB_PATH)
          .setDoneBlobGeneration(DONE_BLOB_GENERATION)
          .setDoneBlobCreateTime(DONE_BLOB_CREATE_TIME.toProtoTime())
          .setState(RawImpressionUpload.State.CREATED)
          .build()
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
      stubRawImpressionUploadCreation()
      stubFullResolutionChain(MODEL_LINE_1)
      whenever(rawImpressionUploadService.createRawImpressionUpload(any())).thenAnswer {
        throw StatusException(Status.ALREADY_EXISTS.withDescription("upload exists"))
      }
      whenever(rawImpressionUploadService.listRawImpressionUploads(any()))
        .thenReturn(listRawImpressionUploadsResponse { rawImpressionUploads += currentUpload })
      whenever(rawImpressionUploadFileService.listRawImpressionUploadFiles(any()))
        .thenReturn(
          listRawImpressionUploadFilesResponse {
            rawImpressionUploadFiles += rawImpressionUploadFile {
              name = "${currentUpload.name}/files/file1"
              this.blobUri = blobUri
              blobGeneration = RAW_BLOB_GENERATION
            }
          }
        )

      createDispatcher().upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

      verifyBlocking(rawImpressionUploadFileService) { batchCreateRawImpressionUploadFiles(any()) }
      verifyBlocking(rawImpressionUploadModelLineService) {
        batchCreateRawImpressionUploadModelLines(any())
      }
    }

  @Test
  fun `legacy generationless registration is quarantined as edited`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    val blobUri =
      BlobUris.buildUri(SelectedStorageClient.parseBlobUri(DONE_BLOB_PATH), blob.blobKey)
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    whenever(rawImpressionUploadService.listRawImpressionUploads(any()))
      .thenReturn(
        listRawImpressionUploadsResponse {
          rawImpressionUploads +=
            RawImpressionUpload.newBuilder()
              .setName("$DATA_PROVIDER_NAME/rawImpressionUploads/legacy")
              .setDoneBlobUri(DONE_BLOB_PATH)
              .setDoneBlobGeneration(100L)
              .setState(RawImpressionUpload.State.COMPLETED)
              .setRegistrationComplete(true)
              .build()
        }
      )
    stubCorrectionCandidateRegistration()
    whenever(rawImpressionUploadFileService.listRawImpressionUploadFiles(any()))
      .thenReturn(
        listRawImpressionUploadFilesResponse {
          rawImpressionUploadFiles += rawImpressionUploadFile {
            name = "$DATA_PROVIDER_NAME/rawImpressionUploads/legacy/files/file1"
            this.blobUri = blobUri
            blobGeneration = 0L
          }
        }
      )

    createDispatcher(
        readDoneBlobMetadata = { RawImpressionBlobMetadata(200L, 0L, DONE_BLOB_CREATE_TIME) }
      )
      .upload(DONE_BLOB_PATH, doneBlobGeneration = 200L)

    val candidate = argumentCaptor<RegisterDetectedRawImpressionUploadCorrectionCandidateRequest>()
    verifyBlocking(correctionCandidateService) {
      registerDetectedRawImpressionUploadCorrectionCandidate(candidate.capture())
    }
    assertThat(candidate.firstValue.rawImpressionUploadCorrectionCandidate.classification)
      .isEqualTo(InternalCandidate.Classification.CLASSIFICATION_EDITED)
    verifyBlocking(rawImpressionUploadService, never()) { createRawImpressionUpload(any()) }
  }

  @Test
  fun `replacement after failed upload registers the complete current directory`() =
    runBlocking<Unit> {
      val blob1 = createMockBlob("$FOLDER_PREFIX/file1.parquet")
      val blob2 = createMockBlob("$FOLDER_PREFIX/file2.parquet")
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob1, blob2))
      stubRawImpressionUploadCreation()
      stubFullResolutionChain(MODEL_LINE_1)
      whenever(rawImpressionUploadService.listRawImpressionUploads(any())).thenAnswer { invocation
        ->
        val request = invocation.getArgument<ListRawImpressionUploadsRequest>(0)
        if (request.filter.doneBlobUri == DONE_BLOB_PATH) {
          listRawImpressionUploadsResponse {
            rawImpressionUploads +=
              RawImpressionUpload.newBuilder()
                .setName("$DATA_PROVIDER_NAME/rawImpressionUploads/failed")
                .setDoneBlobUri(DONE_BLOB_PATH)
                .setDoneBlobGeneration(100L)
                .setDoneBlobCreateTime(DONE_BLOB_CREATE_TIME.minusSeconds(1).toProtoTime())
                .setState(RawImpressionUpload.State.FAILED)
                .build()
          }
        } else {
          listRawImpressionUploadsResponse {}
        }
      }

      createDispatcher(
          readDoneBlobMetadata = { RawImpressionBlobMetadata(200L, 0L, DONE_BLOB_CREATE_TIME) }
        )
        .upload(DONE_BLOB_PATH, doneBlobGeneration = 200L)

      val createRequest = argumentCaptor<BatchCreateRawImpressionUploadFilesRequest>()
      verifyBlocking(rawImpressionUploadFileService) {
        batchCreateRawImpressionUploadFiles(createRequest.capture())
      }
      assertThat(createRequest.firstValue.requestsList.map { it.rawImpressionUploadFile.blobUri })
        .containsExactly(
          BlobUris.buildUri(SelectedStorageClient.parseBlobUri(DONE_BLOB_PATH), blob1.blobKey),
          BlobUris.buildUri(SelectedStorageClient.parseBlobUri(DONE_BLOB_PATH), blob2.blobKey),
        )
      verifyBlocking(rawImpressionUploadFileService, never()) {
        listRawImpressionUploadFiles(any())
      }
    }

  @Test
  fun `newer registered creation time wins regardless of generation number`() =
    runBlocking<Unit> {
      val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
      whenever(rawImpressionUploadService.listRawImpressionUploads(any()))
        .thenReturn(
          listRawImpressionUploadsResponse {
            rawImpressionUploads +=
              RawImpressionUpload.newBuilder()
                .setName("$DATA_PROVIDER_NAME/rawImpressionUploads/newer")
                .setDoneBlobUri(DONE_BLOB_PATH)
                .setDoneBlobGeneration(120L)
                .setDoneBlobCreateTime(DONE_BLOB_CREATE_TIME.plusSeconds(1).toProtoTime())
                .setState(RawImpressionUpload.State.COMPLETED)
                .build()
          }
        )

      val outcome =
        createDispatcher(
            readDoneBlobMetadata = { RawImpressionBlobMetadata(900L, 0L, DONE_BLOB_CREATE_TIME) }
          )
          .upload(DONE_BLOB_PATH, doneBlobGeneration = 900L)

      assertThat(outcome).isEqualTo(VidLabelingDispatcher.UploadOutcome.SUPERSEDED)
      verifyBlocking(rawImpressionUploadService, never()) { createRawImpressionUpload(any()) }
      verifyBlocking(rawImpressionUploadFileService, never()) {
        batchCreateRawImpressionUploadFiles(any())
      }
    }

  @Test
  fun `upload ignores stale done event before listing files`() =
    runBlocking<Unit> {
      val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
      stubRawImpressionUploadCreation()
      stubFullResolutionChain(MODEL_LINE_1)

      val outcome =
        createDispatcher(
            readDoneBlobMetadata = {
              RawImpressionBlobMetadata(200L, 0L, DONE_BLOB_CREATE_TIME.plusSeconds(1))
            }
          )
          .upload(DONE_BLOB_PATH, doneBlobGeneration = 150L)

      assertThat(outcome).isEqualTo(VidLabelingDispatcher.UploadOutcome.STALE)
      val lifecycleLog =
        logRecords.single { it.message.contains("event=edpa.vid_labeling.upload_registered ") }
      assertThat(lifecycleLog.message).contains("xmm.lifecycle.stage=upload_registration")
      assertThat(lifecycleLog.message).contains("xmm.outcome=stale")
      assertThat(lifecycleLog.message)
        .contains("xmm.gcs.object.path_hash=" + VidLabelingTraceLogging.sha256(DONE_BLOB_PATH))
      assertThat(lifecycleLog.message).doesNotContain(DONE_BLOB_PATH)
      verify(storageClient, never()).listBlobs(any())
      verifyBlocking(rawImpressionUploadService, never()) { createRawImpressionUpload(any()) }
    }

  @Test
  fun `upload rethrows cancellation without recording application failure`() =
    runBlocking<Unit> {
      val dispatcher =
        createDispatcher(readDoneBlobMetadata = { throw CancellationException("cancelled") })

      assertFailsWith<CancellationException> {
        dispatcher.upload(DONE_BLOB_PATH, doneBlobGeneration = DONE_BLOB_GENERATION)
      }

      assertThat(
          logRecords.none {
            it.message.contains("event=edpa.vid_labeling.upload_registered ") &&
              it.message.contains("xmm.outcome=failed")
          }
        )
        .isTrue()
    }

  @Test
  fun `upload ignores done event when done object changes during listing`() =
    runBlocking<Unit> {
      val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
      var metadataReadCount = 0

      val outcome =
        createDispatcher(
            readDoneBlobMetadata = {
              metadataReadCount++
              if (metadataReadCount == 1) {
                RawImpressionBlobMetadata(150L, 0L, DONE_BLOB_CREATE_TIME)
              } else {
                RawImpressionBlobMetadata(200L, 0L, DONE_BLOB_CREATE_TIME.plusSeconds(1))
              }
            }
          )
          .upload(DONE_BLOB_PATH, doneBlobGeneration = 150L)

      assertThat(outcome).isEqualTo(VidLabelingDispatcher.UploadOutcome.STALE)
      verifyBlocking(rawImpressionUploadService, never()) { createRawImpressionUpload(any()) }
    }

  @Test
  fun `upload chunks RawImpressionUploadFiles at batch size 100`() =
    runBlocking<Unit> {
      val blobs = (1..250).map { createMockBlob("$FOLDER_PREFIX/file$it.parquet") }
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(*blobs.toTypedArray()))
      stubRawImpressionUploadCreation()
      stubFullResolutionChain(MODEL_LINE_1)

      val dispatcher = createDispatcher()
      dispatcher.upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

      val requestCaptor = argumentCaptor<BatchCreateRawImpressionUploadFilesRequest>()
      verifyBlocking(rawImpressionUploadFileService, times(3)) {
        batchCreateRawImpressionUploadFiles(requestCaptor.capture())
      }
      assertThat(requestCaptor.allValues[0].requestsList).hasSize(100)
      assertThat(requestCaptor.allValues[1].requestsList).hasSize(100)
      assertThat(requestCaptor.allValues[2].requestsList).hasSize(50)
    }

  @Test
  fun `upload creates RawImpressionUploadModelLine for each resolved model line`() =
    runBlocking<Unit> {
      val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
      stubRawImpressionUploadCreation()
      stubFullResolutionChain(MODEL_LINE_1, MODEL_LINE_2)

      val dispatcher = createDispatcher()
      dispatcher.upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

      val requestCaptor = argumentCaptor<BatchCreateRawImpressionUploadModelLinesRequest>()
      verifyBlocking(rawImpressionUploadModelLineService) {
        batchCreateRawImpressionUploadModelLines(requestCaptor.capture())
      }
      val request = requestCaptor.firstValue
      val modelLineNames =
        request.requestsList.map { it.rawImpressionUploadModelLine.cmmsModelLine }
      assertThat(modelLineNames).containsExactly(MODEL_LINE_1, MODEL_LINE_2)
    }

  @Test
  fun `upload triggers fast-path dispatch after registration`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubRawImpressionUploadCreation()
    stubFullResolutionChain(MODEL_LINE_1)

    val dispatcher = createDispatcher()
    dispatcher.upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

    // The fast path delegates to the shared sequencer, which lists uploads for this DataProvider
    // (ACTIVE then CREATED). Seeing those calls proves dispatch was triggered post-registration.
    verifyBlocking(rawImpressionUploadService, atLeastOnce()) { listRawImpressionUploads(any()) }
  }

  @Test
  fun `fast-path dispatch failure does not fail registration`() =
    runBlocking<Unit> {
      val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
      stubRawImpressionUploadCreation()
      stubFullResolutionChain(MODEL_LINE_1)
      // Dispatch (best-effort) fails, but registration already succeeded, so upload() must not
      // throw.
      whenever(rawImpressionUploadService.listRawImpressionUploads(any())).thenAnswer { invocation
        ->
        val request = invocation.getArgument<ListRawImpressionUploadsRequest>(0)
        if (request.filter.doneBlobUri.isNotEmpty()) {
          listRawImpressionUploadsResponse {}
        } else {
          throw StatusException(Status.UNAVAILABLE.withDescription("metadata store unavailable"))
        }
      }

      val dispatcher = createDispatcher()
      dispatcher.upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

      verifyBlocking(rawImpressionUploadService) { createRawImpressionUpload(any()) }
      verifyBlocking(rawImpressionUploadModelLineService) {
        batchCreateRawImpressionUploadModelLines(any())
      }
    }

  @Test
  fun `upload fetches existing upload and continues on createRawImpressionUpload ALREADY_EXISTS`() =
    runBlocking<Unit> {
      val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
      stubRawImpressionUploadCreation()
      stubFullResolutionChain(MODEL_LINE_1)
      // Redelivery after the idempotency cache expired: create returns the literal error, and the
      // existing upload (whose prior delivery may have died before files/model-lines) is found by
      // done_blob_uri so the idempotent downstream steps still run.
      whenever(rawImpressionUploadService.createRawImpressionUpload(any())).thenAnswer {
        throw StatusException(Status.ALREADY_EXISTS.withDescription("upload exists"))
      }
      whenever(rawImpressionUploadService.listRawImpressionUploads(any()))
        .thenReturn(
          listRawImpressionUploadsResponse {
            rawImpressionUploads +=
              RawImpressionUpload.newBuilder()
                .setName("$DATA_PROVIDER_NAME/rawImpressionUploads/$RAW_IMPRESSION_UPLOAD_ID")
                .setDoneBlobUri(DONE_BLOB_PATH)
                .setDoneBlobGeneration(DONE_BLOB_GENERATION)
                .setState(RawImpressionUpload.State.CREATED)
                .setEtag(UPLOAD_ETAG)
                .build()
          }
        )

      val dispatcher = createDispatcher()
      dispatcher.upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

      // Continued: files + model lines created against the recovered upload.
      val listRequestCaptor = argumentCaptor<ListRawImpressionUploadsRequest>()
      verifyBlocking(rawImpressionUploadService, atLeastOnce()) {
        listRawImpressionUploads(listRequestCaptor.capture())
      }
      assertThat(listRequestCaptor.allValues.map { it.filter.doneBlobUri }).contains(DONE_BLOB_PATH)
      verifyBlocking(rawImpressionUploadFileService) { batchCreateRawImpressionUploadFiles(any()) }
      verifyBlocking(rawImpressionUploadModelLineService) {
        batchCreateRawImpressionUploadModelLines(any())
      }
    }

  @Test
  fun `upload acks when a newer upload wins after the final done generation check`() =
    runBlocking<Unit> {
      val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
      whenever(rawImpressionUploadService.createRawImpressionUpload(any())).thenAnswer {
        throw StatusException(Status.ALREADY_EXISTS.withDescription("newer upload exists"))
      }
      whenever(rawImpressionUploadService.listRawImpressionUploads(any()))
        .thenReturn(
          listRawImpressionUploadsResponse {},
          listRawImpressionUploadsResponse {
            rawImpressionUploads +=
              RawImpressionUpload.newBuilder()
                .setName("$DATA_PROVIDER_NAME/rawImpressionUploads/newer-upload")
                .setDoneBlobUri(DONE_BLOB_PATH)
                .setDoneBlobGeneration(DONE_BLOB_GENERATION + 1L)
                .setDoneBlobCreateTime(DONE_BLOB_CREATE_TIME.plusSeconds(1).toProtoTime())
                .build()
          },
        )
      val metadataRead = RecordingThrottler()

      createDispatcher(
          rpcThrottlers =
            VidLabelingRpcThrottlersTestHelper.alwaysReady().copy(metadataRead = metadataRead)
        )
        .upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

      verifyBlocking(rawImpressionUploadFileService, never()) {
        batchCreateRawImpressionUploadFiles(any())
      }
      // Initial revision discovery and exact/latest ALREADY_EXISTS recovery.
      assertThat(metadataRead.onReadyCalls).isEqualTo(3)
    }

  @Test
  fun `upload acks when batchCreateRawImpressionUploadFiles returns ALREADY_EXISTS`() =
    runBlocking<Unit> {
      val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
      stubRawImpressionUploadCreation()
      stubFullResolutionChain(MODEL_LINE_1)
      whenever(rawImpressionUploadFileService.batchCreateRawImpressionUploadFiles(any()))
        .thenAnswer { throw StatusException(Status.ALREADY_EXISTS.withDescription("files exist")) }

      val dispatcher = createDispatcher()
      // Files already exist → ack and continue with the rest of registration.
      dispatcher.upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

      verifyBlocking(rawImpressionUploadModelLineService) {
        batchCreateRawImpressionUploadModelLines(any())
      }
    }

  @Test
  fun `upload acks when batchCreateRawImpressionUploadModelLines returns ALREADY_EXISTS`() =
    runBlocking<Unit> {
      val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
      stubRawImpressionUploadCreation()
      stubFullResolutionChain(MODEL_LINE_1)
      whenever(rawImpressionUploadModelLineService.batchCreateRawImpressionUploadModelLines(any()))
        .thenAnswer {
          throw StatusException(Status.ALREADY_EXISTS.withDescription("model lines exist"))
        }

      val dispatcher = createDispatcher()
      // Model lines already exist → ack rather than throwing.
      dispatcher.upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)
    }

  @Test
  fun `upload derives file request id from upload context`() =
    runBlocking<Unit> {
      val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
      stubRawImpressionUploadCreation()
      stubFullResolutionChain(MODEL_LINE_1)

      val dispatcher = createDispatcher()
      dispatcher.upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

      val captor = argumentCaptor<BatchCreateRawImpressionUploadFilesRequest>()
      verifyBlocking(rawImpressionUploadFileService) {
        batchCreateRawImpressionUploadFiles(captor.capture())
      }
      val fileRequest = captor.firstValue.requestsList.single()
      // Regression: the file request_id must fold in the parent upload, not just the blob URI.
      assertThat(fileRequest.requestId)
        .isEqualTo(
          RequestIds.forRawImpressionUploadFile(
            captor.firstValue.parent,
            fileRequest.rawImpressionUploadFile.blobUri,
          )
        )
    }

  @Test
  fun `upload emits filesProcessed counter on success`() = runBlocking {
    val blob1 = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    val blob2 = createMockBlob("$FOLDER_PREFIX/file2.parquet")
    val blob3 = createMockBlob("$FOLDER_PREFIX/file3.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob1, blob2, blob3))
    stubRawImpressionUploadCreation()
    stubFullResolutionChain(MODEL_LINE_1)

    val metricsEnv = createMetricsEnvironment()
    try {
      val dispatcher = createDispatcher(metrics = metricsEnv.metrics)
      dispatcher.upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

      metricsEnv.metricReader.forceFlush()
      val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems
      val metricByName = metricData.associateBy { it.name }

      val filesPoint =
        metricByName
          .getValue("edpa.vid_labeling_dispatcher.files_processed")
          .longSumData
          .points
          .single()
      assertThat(filesPoint.value).isEqualTo(3)
      assertThat(filesPoint.attributes.get(DATA_PROVIDER_ATTR)).isEqualTo(DATA_PROVIDER_NAME)
    } finally {
      metricsEnv.close()
    }
  }

  @Test
  fun `upload emits correction candidate classification`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file.parquet")
    val blobUri =
      BlobUris.buildUri(SelectedStorageClient.parseBlobUri(DONE_BLOB_PATH), blob.blobKey)
    val previous = previousUpload()
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubRawImpressionUploadCreation()
    stubCorrectionCandidateRegistration()
    whenever(
        correctionCandidateService.registerDetectedRawImpressionUploadCorrectionCandidate(any())
      )
      .thenReturn(
        registerDetectedRawImpressionUploadCorrectionCandidateResponse { newlyCreated = true },
        registerDetectedRawImpressionUploadCorrectionCandidateResponse { newlyCreated = false },
      )
    whenever(rawImpressionUploadService.listRawImpressionUploads(any()))
      .thenReturn(listRawImpressionUploadsResponse { rawImpressionUploads += previous })
    whenever(rawImpressionUploadFileService.listRawImpressionUploadFiles(any()))
      .thenReturn(
        listRawImpressionUploadFilesResponse {
          rawImpressionUploadFiles += manifestFile(previous.name, blobUri, 10L)
        }
      )
    val metricsEnv = createMetricsEnvironment()
    try {
      val dispatcher =
        createDispatcher(
          readBlobMetadata = { RawImpressionBlobMetadata(20L, 100L, RAW_BLOB_CREATE_TIME) },
          metrics = metricsEnv.metrics,
        )
      dispatcher.upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)
      dispatcher.upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

      metricsEnv.metricReader.forceFlush()
      val point =
        metricsEnv.metricExporter.finishedMetricItems
          .associateBy { it.name }
          .getValue("edpa.vid_labeling_dispatcher.correction_candidates")
          .longSumData
          .points
          .single()
      assertThat(point.value).isEqualTo(1)
      assertThat(point.attributes.get(DATA_PROVIDER_ATTR)).isEqualTo(DATA_PROVIDER_NAME)
      assertThat(point.attributes.get(CORRECTION_CLASSIFICATION_ATTR)).isEqualTo("EDITED")
    } finally {
      metricsEnv.close()
    }
  }

  @Test
  fun `upload records upload duration on failure`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubRawImpressionUploadCreation()
    whenever(modelLinesService.listModelLines(any())).thenAnswer {
      throw StatusException(Status.UNAVAILABLE.withDescription("VID Repo unavailable"))
    }

    val metricsEnv = createMetricsEnvironment()
    try {
      val dispatcher = createDispatcher(metrics = metricsEnv.metrics)
      assertFailsWith<Exception> { dispatcher.upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION) }

      metricsEnv.metricReader.forceFlush()
      val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems
      val metricByName = metricData.associateBy { it.name }

      val durationPoint =
        metricByName
          .getValue("edpa.vid_labeling_dispatcher.dispatch_duration")
          .histogramData
          .points
          .single()
      assertThat(durationPoint.attributes.get(UPLOAD_STATUS_ATTR)).isEqualTo("failed")
      assertThat(durationPoint.attributes.get(DATA_PROVIDER_ATTR)).isEqualTo(DATA_PROVIDER_NAME)
    } finally {
      metricsEnv.close()
    }
  }

  companion object {
    private const val DATA_PROVIDER_NAME = "dataProviders/edp123"
    private const val MODEL_SUITE_NAME = "modelProviders/mp1/modelSuites/ms1"
    private const val MODEL_LINE_1 = "$MODEL_SUITE_NAME/modelLines/ml1"
    private const val MODEL_LINE_2 = "$MODEL_SUITE_NAME/modelLines/ml2"
    private const val MODEL_RELEASE_NAME = "$MODEL_SUITE_NAME/modelReleases/mr1"
    private const val MODEL_BLOB_PATH = "gs://models/vid-model-v1.pb"
    private const val FOLDER_PREFIX = "/test-bucket/edp1/2024-01-15"
    private const val DONE_BLOB_PATH = "file://$FOLDER_PREFIX/done"
    private const val RAW_IMPRESSION_UPLOAD_ID = "upload-abc123"
    private const val UPLOAD_ETAG = "upload-etag"
    private const val RECOVERY_PREDECESSOR_UPLOAD =
      "$DATA_PROVIDER_NAME/rawImpressionUploads/predecessor"
    private const val RECOVERY_PREDECESSOR_REPLACEMENT =
      "$DATA_PROVIDER_NAME/rawImpressionUploads/predecessor-replacement"
    private const val RECOVERY_PREDECESSOR_DONE_BLOB_PATH =
      "file:///test-bucket/edp1/2024-01-14/done"
    private const val EVICTION_OPERATION_ID = "123e4567-e89b-42d3-a456-426614174000"
    private const val DONE_BLOB_GENERATION = 12345L
    private const val RAW_BLOB_GENERATION = 67890L
    private const val NUMBER_OF_SHARDS = 2
    private const val MAX_FILE_BATCH_SIZE_BYTES = 1000L
    private const val QUEUE_NAME = "queues/vid-labeler"
    private const val POOL_ASSIGNER_QUEUE_NAME = "queues/pool-assigner"

    private val FIXED_NOW: Instant = Instant.parse("2026-06-03T12:00:00Z")
    private val DONE_BLOB_CREATE_TIME: Instant = Instant.parse("2026-06-03T11:00:00Z")
    private val RAW_BLOB_CREATE_TIME: Instant = Instant.parse("2026-06-03T10:00:00Z")
    private val EVENT_DATE: LocalDate = LocalDate.parse("2026-06-01")
    private val EVENT_DATE_PROTO = date {
      year = 2026
      month = 6
      day = 1
    }

    private val DATA_PROVIDER_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.vid_labeling_dispatcher.data_provider")
    private val UPLOAD_STATUS_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.vid_labeling_dispatcher.dispatch_status")
    private val CORRECTION_CLASSIFICATION_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.vid_labeling_dispatcher.correction_classification")

    private val DEFAULT_MODEL_LINE_CONFIGS: Map<String, VidLabelerParams.ModelLineConfig> =
      mapOf(
        MODEL_LINE_1 to
          VidLabelerParamsKt.modelLineConfig {
            labelerInputFieldMapping +=
              LabelerInputFieldMapping.newBuilder()
                .setFieldPath("age")
                .setScalar(ScalarColumn.newBuilder().setColumn("user_age"))
                .build()
            labelerInputFieldMapping +=
              LabelerInputFieldMapping.newBuilder()
                .setFieldPath("gender")
                .setScalar(ScalarColumn.newBuilder().setColumn("user_gender"))
                .build()
          },
        MODEL_LINE_2 to
          VidLabelerParamsKt.modelLineConfig {
            labelerInputFieldMapping +=
              LabelerInputFieldMapping.newBuilder()
                .setFieldPath("age")
                .setScalar(ScalarColumn.newBuilder().setColumn("user_age"))
                .build()
          },
      )
  }
}
