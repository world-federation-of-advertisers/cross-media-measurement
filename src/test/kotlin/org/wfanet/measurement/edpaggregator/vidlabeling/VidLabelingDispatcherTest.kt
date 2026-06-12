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
import com.google.protobuf.util.Timestamps
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
import java.time.ZoneId
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.Instrumentation
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
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreatePoolAssignmentJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.CreateRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreatePoolAssignmentJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRawImpressionUploadFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelerParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient

@RunWith(JUnit4::class)
class VidLabelingDispatcherTest {

  private val workItemsService: WorkItemsGrpcKt.WorkItemsCoroutineImplBase = mockService()
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
  private val poolAssignmentJobService:
    PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineImplBase =
    mockService()
  private val storageClient: StorageClient = mock()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(workItemsService)
    addService(modelLinesService)
    addService(modelRolloutsService)
    addService(modelShardsService)
    addService(rawImpressionUploadService)
    addService(rawImpressionUploadFileService)
    addService(poolAssignmentJobService)
  }

  private val workItemsStub by lazy {
    WorkItemsGrpcKt.WorkItemsCoroutineStub(grpcTestServerRule.channel)
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

  private val poolAssignmentJobStub by lazy {
    PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub(
      grpcTestServerRule.channel
    )
  }

  private val vidLabelerParamsTemplate = vidLabelerParams {
    dataProvider = DATA_PROVIDER_NAME
    vidLabeledImpressionsStorageParams =
      VidLabelerParamsKt.storageParams {
        gcsProjectId = "test-project"
        impressionsBlobPrefix = "gs://output-bucket/labeled"
      }
    rawImpressionsStorageParams =
      VidLabelerParamsKt.storageParams {
        gcsProjectId = "test-project"
        impressionsBlobPrefix = "gs://input-bucket/raw"
      }
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

  private fun createDispatcher(
    numberOfShards: Int = DEFAULT_NUMBER_OF_SHARDS,
    overrideModelLines: List<String> = emptyList(),
    modelLineConfigs: Map<String, VidLabelerParams.ModelLineConfig> = DEFAULT_MODEL_LINE_CONFIGS,
    metrics: VidLabelingDispatcherMetrics = VidLabelingDispatcherMetrics(),
  ): VidLabelingDispatcher {
    return VidLabelingDispatcher(
      storageClient = storageClient,
      workItemsStub = workItemsStub,
      rawImpressionUploadStub = rawImpressionUploadStub,
      rawImpressionUploadFilesStub = rawImpressionUploadFilesStub,
      poolAssignmentJobStub = poolAssignmentJobStub,
      modelLinesStub = modelLinesStub,
      modelRolloutsStub = modelRolloutsStub,
      modelShardsStub = modelShardsStub,
      dataProviderName = DATA_PROVIDER_NAME,
      vidLabelerParamsTemplate = vidLabelerParamsTemplate,
      queueName = QUEUE_NAME,
      numberOfShards = numberOfShards,
      modelSuiteName = MODEL_SUITE_NAME,
      overrideModelLines = overrideModelLines,
      modelLineConfigs = modelLineConfigs,
      clock = fixedClock,
      metrics = metrics,
    )
  }

  private fun createMockBlob(key: String): StorageClient.Blob {
    val blob: StorageClient.Blob = mock()
    whenever(blob.blobKey).thenReturn(key)
    return blob
  }

  private suspend fun stubRawImpressionUploadCreation() {
    whenever(rawImpressionUploadService.createRawImpressionUpload(any()))
      .thenReturn(
        RawImpressionUpload.newBuilder()
          .setName("$DATA_PROVIDER_NAME/rawImpressionUploads/$RAW_IMPRESSION_UPLOAD_ID")
          .setDoneBlobUri(DONE_BLOB_PATH)
          .build()
      )
    whenever(rawImpressionUploadFileService.batchCreateRawImpressionUploadFiles(any()))
      .thenReturn(batchCreateRawImpressionUploadFilesResponse {})
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
  fun `upload with empty directory creates no work items`() = runBlocking {
    whenever(storageClient.listBlobs(any())).thenReturn(emptyFlow())

    val dispatcher = createDispatcher()
    dispatcher.upload(DONE_BLOB_PATH)

    verifyBlocking(workItemsService, never()) { createWorkItem(any()) }
    verifyBlocking(modelLinesService, never()) { listModelLines(any()) }
  }

  @Test
  fun `upload creates N work items per active model line`() =
    runBlocking<Unit> {
      val blob1 = createMockBlob("$FOLDER_PREFIX/file1.parquet")
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob1))
      stubRawImpressionUploadCreation()
      stubFullResolutionChain(MODEL_LINE_1, MODEL_LINE_2)
      whenever(workItemsService.createWorkItem(any())).thenReturn(WorkItem.getDefaultInstance())

      val dispatcher = createDispatcher(numberOfShards = 3)
      dispatcher.upload(DONE_BLOB_PATH)

      val requestCaptor = argumentCaptor<CreateWorkItemRequest>()
      verifyBlocking(workItemsService, times(6)) { createWorkItem(requestCaptor.capture()) }

      val workItemIds = requestCaptor.allValues.map { it.workItemId }
      assertThat(workItemIds)
        .containsExactly(
          "vid-labeling-$RAW_IMPRESSION_UPLOAD_ID-ml1-shard-0",
          "vid-labeling-$RAW_IMPRESSION_UPLOAD_ID-ml1-shard-1",
          "vid-labeling-$RAW_IMPRESSION_UPLOAD_ID-ml1-shard-2",
          "vid-labeling-$RAW_IMPRESSION_UPLOAD_ID-ml2-shard-0",
          "vid-labeling-$RAW_IMPRESSION_UPLOAD_ID-ml2-shard-1",
          "vid-labeling-$RAW_IMPRESSION_UPLOAD_ID-ml2-shard-2",
        )
    }

  @Test
  fun `upload creates a RawImpressionUploadFile for each blob`() = runBlocking<Unit> {
    val blob1 = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    val blob2 = createMockBlob("$FOLDER_PREFIX/file2.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob1, blob2))
    stubRawImpressionUploadCreation()
    stubFullResolutionChain(MODEL_LINE_1)
    whenever(workItemsService.createWorkItem(any())).thenReturn(WorkItem.getDefaultInstance())

    val dispatcher = createDispatcher(numberOfShards = 1)
    dispatcher.upload(DONE_BLOB_PATH)

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
  }

  @Test
  fun `upload with override model lines skips ListModelLines API`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubRawImpressionUploadCreation()
    stubOverrideResolutionChain()
    whenever(workItemsService.createWorkItem(any())).thenReturn(WorkItem.getDefaultInstance())

    val dispatcher = createDispatcher(numberOfShards = 2, overrideModelLines = listOf(MODEL_LINE_1))
    dispatcher.upload(DONE_BLOB_PATH)

    verifyBlocking(modelLinesService, never()) { listModelLines(any()) }

    val requestCaptor = argumentCaptor<CreateWorkItemRequest>()
    verifyBlocking(workItemsService, times(2)) { createWorkItem(requestCaptor.capture()) }
  }

  @Test
  fun `upload with no active model lines creates no work items`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubRawImpressionUploadCreation()
    whenever(modelLinesService.listModelLines(any())).thenReturn(listModelLinesResponse {})

    val dispatcher = createDispatcher()
    dispatcher.upload(DONE_BLOB_PATH)

    verifyBlocking(workItemsService, never()) { createWorkItem(any()) }
  }

  @Test
  fun `upload sets shard index and model blob path in work item params`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubRawImpressionUploadCreation()
    stubFullResolutionChain(MODEL_LINE_1)
    whenever(workItemsService.createWorkItem(any())).thenReturn(WorkItem.getDefaultInstance())

    val dispatcher = createDispatcher(numberOfShards = 2)
    dispatcher.upload(DONE_BLOB_PATH)

    val requestCaptor = argumentCaptor<CreateWorkItemRequest>()
    verifyBlocking(workItemsService, times(2)) { createWorkItem(requestCaptor.capture()) }

    val params0 =
      requestCaptor.allValues[0]
        .workItem
        .workItemParams
        .unpack(WorkItemParams::class.java)
        .appParams
        .unpack(VidLabelerParams::class.java)
    assertThat(params0.shardIndex).isEqualTo(0)
    assertThat(params0.totalShards).isEqualTo(2)
    assertThat(params0.modelBlobPathsMap[MODEL_LINE_1]).isEqualTo(MODEL_BLOB_PATH)

    val params1 =
      requestCaptor.allValues[1]
        .workItem
        .workItemParams
        .unpack(WorkItemParams::class.java)
        .appParams
        .unpack(VidLabelerParams::class.java)
    assertThat(params1.shardIndex).isEqualTo(1)
    assertThat(params1.totalShards).isEqualTo(2)
  }

  @Test
  fun `upload excludes done marker from file list`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    val doneBlob = createMockBlob("$FOLDER_PREFIX/done")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob, doneBlob))
    stubRawImpressionUploadCreation()
    stubFullResolutionChain(MODEL_LINE_1)
    whenever(workItemsService.createWorkItem(any())).thenReturn(WorkItem.getDefaultInstance())

    val dispatcher = createDispatcher(numberOfShards = 1)
    dispatcher.upload(DONE_BLOB_PATH)

    val requestCaptor = argumentCaptor<CreateWorkItemRequest>()
    verifyBlocking(workItemsService, times(1)) { createWorkItem(requestCaptor.capture()) }
  }

  @Test
  fun `upload propagates exception on work item creation failure`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubRawImpressionUploadCreation()
    stubFullResolutionChain(MODEL_LINE_1)
    whenever(workItemsService.createWorkItem(any())).thenAnswer {
      throw StatusException(Status.UNAVAILABLE.withDescription("Service unavailable"))
    }

    val dispatcher = createDispatcher(numberOfShards = 1)
    val exception = assertFailsWith<Exception> { dispatcher.upload(DONE_BLOB_PATH) }
    assertThat(exception).hasMessageThat().contains("Error creating WorkItem")
    assertThat(exception).hasCauseThat().isInstanceOf(StatusException::class.java)
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
    val exception = assertFailsWith<Exception> { dispatcher.upload(DONE_BLOB_PATH) }
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

    val dispatcher = createDispatcher(numberOfShards = 1)
    dispatcher.upload(DONE_BLOB_PATH)

    verifyBlocking(workItemsService, never()) { createWorkItem(any()) }
  }

  private suspend fun stubMemoizedResolutionChain(vararg modelLineNames: String) {
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
            memoizedVidAssignmentEnabled = true
          }
        }
      )
  }

  @Test
  fun `upload creates PoolAssignmentJobs for memoized model lines`() =
    runBlocking<Unit> {
      val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
      stubRawImpressionUploadCreation()
      stubMemoizedResolutionChain(MODEL_LINE_1)
      whenever(poolAssignmentJobService.batchCreatePoolAssignmentJobs(any()))
        .thenReturn(batchCreatePoolAssignmentJobsResponse {})

      val dispatcher = createDispatcher(numberOfShards = 3)
      dispatcher.upload(DONE_BLOB_PATH)

      val requestCaptor = argumentCaptor<BatchCreatePoolAssignmentJobsRequest>()
      verifyBlocking(poolAssignmentJobService) {
        batchCreatePoolAssignmentJobs(requestCaptor.capture())
      }
      val request = requestCaptor.firstValue
      assertThat(request.requestsList).hasSize(3)
      assertThat(request.requestsList.map { it.poolAssignmentJob.shardIndex })
        .containsExactly(0, 1, 2)
      assertThat(request.requestsList.map { it.poolAssignmentJob.cmmsModelLine })
        .containsExactly(MODEL_LINE_1, MODEL_LINE_1, MODEL_LINE_1)

      verifyBlocking(workItemsService, never()) { createWorkItem(any()) }
    }

  @Test
  fun `upload chunks RawImpressionUploadFiles at batch size 100`() =
    runBlocking<Unit> {
      val blobs = (1..250).map { createMockBlob("$FOLDER_PREFIX/file$it.parquet") }
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(*blobs.toTypedArray()))
      stubRawImpressionUploadCreation()
      stubFullResolutionChain(MODEL_LINE_1)
      whenever(workItemsService.createWorkItem(any())).thenReturn(WorkItem.getDefaultInstance())

      val dispatcher = createDispatcher(numberOfShards = 1)
      dispatcher.upload(DONE_BLOB_PATH)

      val requestCaptor = argumentCaptor<BatchCreateRawImpressionUploadFilesRequest>()
      verifyBlocking(rawImpressionUploadFileService, times(3)) {
        batchCreateRawImpressionUploadFiles(requestCaptor.capture())
      }
      assertThat(requestCaptor.allValues[0].requestsList).hasSize(100)
      assertThat(requestCaptor.allValues[1].requestsList).hasSize(100)
      assertThat(requestCaptor.allValues[2].requestsList).hasSize(50)
    }

  @Test
  fun `upload with same generation produces same request ID`() =
    runBlocking<Unit> {
      val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
      stubRawImpressionUploadCreation()
      stubFullResolutionChain(MODEL_LINE_1)
      whenever(workItemsService.createWorkItem(any())).thenReturn(WorkItem.getDefaultInstance())

      val dispatcher = createDispatcher(numberOfShards = 1)
      dispatcher.upload(DONE_BLOB_PATH, doneBlobGeneration = 123L)

      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
      dispatcher.upload(DONE_BLOB_PATH, doneBlobGeneration = 123L)

      val requestCaptor = argumentCaptor<CreateRawImpressionUploadRequest>()
      verifyBlocking(rawImpressionUploadService, times(2)) {
        createRawImpressionUpload(requestCaptor.capture())
      }
      assertThat(requestCaptor.allValues[0].requestId)
        .isEqualTo(requestCaptor.allValues[1].requestId)
    }

  @Test
  fun `upload with different generation produces different request ID`() =
    runBlocking<Unit> {
      val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
      stubRawImpressionUploadCreation()
      stubFullResolutionChain(MODEL_LINE_1)
      whenever(workItemsService.createWorkItem(any())).thenReturn(WorkItem.getDefaultInstance())

      val dispatcher = createDispatcher(numberOfShards = 1)
      dispatcher.upload(DONE_BLOB_PATH, doneBlobGeneration = 123L)

      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
      dispatcher.upload(DONE_BLOB_PATH, doneBlobGeneration = 456L)

      val requestCaptor = argumentCaptor<CreateRawImpressionUploadRequest>()
      verifyBlocking(rawImpressionUploadService, times(2)) {
        createRawImpressionUpload(requestCaptor.capture())
      }
      assertThat(requestCaptor.allValues[0].requestId)
        .isNotEqualTo(requestCaptor.allValues[1].requestId)
    }

  @Test
  fun `upload emits filesProcessed counter on success`() = runBlocking {
    val blob1 = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    val blob2 = createMockBlob("$FOLDER_PREFIX/file2.parquet")
    val blob3 = createMockBlob("$FOLDER_PREFIX/file3.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob1, blob2, blob3))
    stubRawImpressionUploadCreation()
    stubFullResolutionChain(MODEL_LINE_1)
    whenever(workItemsService.createWorkItem(any())).thenReturn(WorkItem.getDefaultInstance())

    val metricsEnv = createMetricsEnvironment()
    try {
      val dispatcher = createDispatcher(numberOfShards = 1, metrics = metricsEnv.metrics)
      dispatcher.upload(DONE_BLOB_PATH)

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
  fun `upload emits workItemsCreated counter on success`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubRawImpressionUploadCreation()
    stubFullResolutionChain(MODEL_LINE_1, MODEL_LINE_2)
    whenever(workItemsService.createWorkItem(any())).thenReturn(WorkItem.getDefaultInstance())

    val metricsEnv = createMetricsEnvironment()
    try {
      val dispatcher = createDispatcher(numberOfShards = 2, metrics = metricsEnv.metrics)
      dispatcher.upload(DONE_BLOB_PATH)

      metricsEnv.metricReader.forceFlush()
      val metricData: List<MetricData> = metricsEnv.metricExporter.finishedMetricItems
      val metricByName = metricData.associateBy { it.name }

      val workItemsPoint =
        metricByName
          .getValue("edpa.vid_labeling_dispatcher.work_items_created")
          .longSumData
          .points
          .single()
      assertThat(workItemsPoint.value).isEqualTo(4)
      assertThat(workItemsPoint.attributes.get(DATA_PROVIDER_ATTR)).isEqualTo(DATA_PROVIDER_NAME)
    } finally {
      metricsEnv.close()
    }
  }

  @Test
  fun `upload records upload duration on failure`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubRawImpressionUploadCreation()
    stubFullResolutionChain(MODEL_LINE_1)
    whenever(workItemsService.createWorkItem(any())).thenAnswer {
      throw StatusException(Status.UNAVAILABLE.withDescription("Service unavailable"))
    }

    val metricsEnv = createMetricsEnvironment()
    try {
      val dispatcher = createDispatcher(numberOfShards = 1, metrics = metricsEnv.metrics)
      assertFailsWith<Exception> { dispatcher.upload(DONE_BLOB_PATH) }

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

  @Test
  fun `upload with numberOfShards zero creates no work items`() = runBlocking {
    val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
    whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
    stubRawImpressionUploadCreation()
    stubFullResolutionChain(MODEL_LINE_1)

    val dispatcher = createDispatcher(numberOfShards = 0)
    dispatcher.upload(DONE_BLOB_PATH)

    verifyBlocking(workItemsService, never()) { createWorkItem(any()) }
  }

  companion object {
    private const val DATA_PROVIDER_NAME = "dataProviders/edp123"
    private const val QUEUE_NAME = "queues/vid-labeler-queue"
    private const val MODEL_SUITE_NAME = "modelProviders/mp1/modelSuites/ms1"
    private const val MODEL_LINE_1 = "$MODEL_SUITE_NAME/modelLines/ml1"
    private const val MODEL_LINE_2 = "$MODEL_SUITE_NAME/modelLines/ml2"
    private const val MODEL_RELEASE_NAME = "$MODEL_SUITE_NAME/modelReleases/mr1"
    private const val MODEL_BLOB_PATH = "gs://models/vid-model-v1.pb"
    private const val DEFAULT_NUMBER_OF_SHARDS = 3
    private const val FOLDER_PREFIX = "/test-bucket/edp1/2024-01-15"
    private const val DONE_BLOB_PATH = "file://$FOLDER_PREFIX/done"
    private const val RAW_IMPRESSION_UPLOAD_ID = "upload-abc123"

    private val FIXED_NOW: Instant = Instant.parse("2026-06-03T12:00:00Z")

    private val DATA_PROVIDER_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.vid_labeling_dispatcher.data_provider")
    private val UPLOAD_STATUS_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.vid_labeling_dispatcher.dispatch_status")

    private val DEFAULT_MODEL_LINE_CONFIGS: Map<String, VidLabelerParams.ModelLineConfig> =
      mapOf(
        MODEL_LINE_1 to
          VidLabelerParamsKt.modelLineConfig {
            labelerInputFieldMapping["age"] = "user_age"
            labelerInputFieldMapping["gender"] = "user_gender"
          },
        MODEL_LINE_2 to
          VidLabelerParamsKt.modelLineConfig { labelerInputFieldMapping["age"] = "user_age" },
      )
  }
}
