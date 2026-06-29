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
import org.mockito.kotlin.atLeastOnce
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
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
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.CreateRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.SubpoolAssignerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRawImpressionUploadFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelerParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient

@RunWith(JUnit4::class)
class VidLabelingDispatcherTest {

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
  private val rawImpressionUploadModelLineService:
    RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase =
    mockService()
  private val workItemsService: WorkItemsGrpcKt.WorkItemsCoroutineImplBase = mockService()
  private val poolAssignmentJobService:
    PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineImplBase =
    mockService()
  private val storageClient: StorageClient = mock()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(modelLinesService)
    addService(modelRolloutsService)
    addService(modelShardsService)
    addService(rawImpressionUploadService)
    addService(rawImpressionUploadFileService)
    addService(rawImpressionUploadModelLineService)
    addService(workItemsService)
    addService(poolAssignmentJobService)
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

  private val rawImpressionUploadModelLineStub by lazy {
    RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub(
      grpcTestServerRule.channel
    )
  }

  private val workItemsStub by lazy {
    WorkItemsGrpcKt.WorkItemsCoroutineStub(grpcTestServerRule.channel)
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
    modelLineConfigs: Map<String, VidLabelerParams.ModelLineConfig> = DEFAULT_MODEL_LINE_CONFIGS
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
    )
  }

  private fun createDispatcher(
    overrideModelLines: List<String> = emptyList(),
    modelLineConfigs: Map<String, VidLabelerParams.ModelLineConfig> = DEFAULT_MODEL_LINE_CONFIGS,
    metrics: VidLabelingDispatcherMetrics = VidLabelingDispatcherMetrics(),
  ): VidLabelingDispatcher {
    return VidLabelingDispatcher(
      storageClient = storageClient,
      rawImpressionUploadStub = rawImpressionUploadStub,
      rawImpressionUploadFilesStub = rawImpressionUploadFilesStub,
      rawImpressionUploadModelLineStub = rawImpressionUploadModelLineStub,
      modelLinesStub = modelLinesStub,
      dispatchSequencer = createSequencer(modelLineConfigs),
      dataProviderName = DATA_PROVIDER_NAME,
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
    whenever(rawImpressionUploadModelLineService.batchCreateRawImpressionUploadModelLines(any()))
      .thenReturn(batchCreateRawImpressionUploadModelLinesResponse {})
    // The post-registration fast path lists uploads; default to none so dispatch is a no-op unless
    // a test overrides this.
    whenever(rawImpressionUploadService.listRawImpressionUploads(any()))
      .thenReturn(listRawImpressionUploadsResponse {})
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
  fun `upload with empty directory creates no upload and resolves no model lines`() = runBlocking {
    whenever(storageClient.listBlobs(any())).thenReturn(emptyFlow())

    val dispatcher = createDispatcher()
    dispatcher.upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

    verifyBlocking(rawImpressionUploadService, never()) { createRawImpressionUpload(any()) }
    verifyBlocking(modelLinesService, never()) { listModelLines(any()) }
  }

  @Test
  fun `upload creates a RawImpressionUploadFile for each blob`() =
    runBlocking<Unit> {
      val blob1 = createMockBlob("$FOLDER_PREFIX/file1.parquet")
      val blob2 = createMockBlob("$FOLDER_PREFIX/file2.parquet")
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob1, blob2))
      stubRawImpressionUploadCreation()
      stubFullResolutionChain(MODEL_LINE_1)

      val dispatcher = createDispatcher()
      dispatcher.upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

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
  fun `upload with no active model lines creates upload and files but no model lines`() =
    runBlocking {
      val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
      stubRawImpressionUploadCreation()
      whenever(modelLinesService.listModelLines(any())).thenReturn(listModelLinesResponse {})

      val dispatcher = createDispatcher()
      dispatcher.upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

      verifyBlocking(rawImpressionUploadService) { createRawImpressionUpload(any()) }
      verifyBlocking(rawImpressionUploadFileService) { batchCreateRawImpressionUploadFiles(any()) }
      verifyBlocking(rawImpressionUploadModelLineService, never()) {
        batchCreateRawImpressionUploadModelLines(any())
      }
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
  fun `upload with same generation produces same request ID`() =
    runBlocking<Unit> {
      val blob = createMockBlob("$FOLDER_PREFIX/file1.parquet")
      whenever(storageClient.listBlobs(any())).thenReturn(flowOf(blob))
      stubRawImpressionUploadCreation()
      stubFullResolutionChain(MODEL_LINE_1)

      val dispatcher = createDispatcher()
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

      val dispatcher = createDispatcher()
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
      whenever(rawImpressionUploadService.listRawImpressionUploads(any())).thenAnswer {
        throw StatusException(Status.UNAVAILABLE.withDescription("metadata store unavailable"))
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
                .build()
          }
        )

      val dispatcher = createDispatcher()
      dispatcher.upload(DONE_BLOB_PATH, DONE_BLOB_GENERATION)

      // Continued: files + model lines created against the recovered upload.
      verifyBlocking(rawImpressionUploadFileService) { batchCreateRawImpressionUploadFiles(any()) }
      verifyBlocking(rawImpressionUploadModelLineService) {
        batchCreateRawImpressionUploadModelLines(any())
      }
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
    private const val DONE_BLOB_GENERATION = 12345L
    private const val NUMBER_OF_SHARDS = 2
    private const val QUEUE_NAME = "queues/vid-labeler"
    private const val POOL_ASSIGNER_QUEUE_NAME = "queues/pool-assigner"

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
