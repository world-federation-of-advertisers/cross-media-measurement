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

package org.wfanet.measurement.edpaggregator.vidlabeler

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.aead.AeadConfig
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.stub
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.rawimpressions.RankIndexStore
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.LabelerInputFieldMapping
import org.wfanet.measurement.edpaggregator.v1alpha.MarkVidLabelingJobSucceededRequest
import org.wfanet.measurement.edpaggregator.v1alpha.MarkVidLabelingJobSucceededResponseKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.ScalarColumn
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.listRankIndexBlobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.markVidLabelingJobSucceededResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.rankIndexMap
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelingJob
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.storage.ParquetStorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class VidLabelerAppTest {

  private val workItemsService: WorkItemsGrpcKt.WorkItemsCoroutineImplBase = mockService()
  private val workItemAttemptsService: WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase =
    mockService()
  private val vidLabelingJobsService:
    VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineImplBase =
    mockService()
  private val rawImpressionUploadModelLinesService:
    RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase =
    mockService()
  private val rankIndexBlobsService:
    RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineImplBase =
    mockService()
  private val rawImpressionUploadFilesService:
    RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineImplBase =
    mockService()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(workItemsService)
    addService(workItemAttemptsService)
    addService(vidLabelingJobsService)
    addService(rawImpressionUploadModelLinesService)
    addService(rankIndexBlobsService)
    addService(rawImpressionUploadFilesService)
  }

  @get:Rule val tempFolder = TemporaryFolder()

  private val workItemsStub by lazy {
    WorkItemsGrpcKt.WorkItemsCoroutineStub(grpcTestServerRule.channel)
  }
  private val workItemAttemptsStub by lazy {
    WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub(grpcTestServerRule.channel)
  }
  private val vidLabelingJobsStub by lazy {
    VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub(grpcTestServerRule.channel)
  }
  private val rawImpressionUploadModelLinesStub by lazy {
    RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub(
      grpcTestServerRule.channel
    )
  }
  private val rankIndexBlobsStub by lazy {
    RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub(grpcTestServerRule.channel)
  }
  private val rawImpressionUploadFilesStub by lazy {
    RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub(
      grpcTestServerRule.channel
    )
  }

  // Real KMS + vid-rank-map storage so MemoizedRankIndex.load resolves the output KEK from a seeded
  // RankIndexBlob (the KEK accessor reads encrypted_dek.kek_uri off the loaded blobs).
  private val kekUri = FakeKmsClient.KEY_URI_PREFIX + "key1"
  private lateinit var kmsClient: FakeKmsClient
  private lateinit var vidRankMapStorageClient: InMemoryStorageClient
  private lateinit var rankStore: RankIndexStore
  private lateinit var encryptedDek: EncryptedDek

  private val mockQueueSubscriber: QueueSubscriber = mock()
  private val mockParquetStorageClient: ParquetStorageClient = mock()
  private val mockVidAssigner: VidAssigner = mock()

  @Before
  fun setUp() {
    AeadConfig.register()
    kmsClient =
      FakeKmsClient().apply {
        setAead(
          kekUri,
          KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM")).getPrimitive(Aead::class.java),
        )
      }
    vidRankMapStorageClient = InMemoryStorageClient()
    rankStore = RankIndexStore(vidRankMapStorageClient, kmsClient)
    encryptedDek = rankStore.generateDek(kekUri)
  }

  /** Seeds one SNAPSHOT RankIndexBlob and stubs the service to return its row. */
  private fun seedRankIndexBlob() = runBlocking {
    val checksum =
      rankStore.writeBlob(
        RANK_BLOB_KEY,
        encryptedDek,
        flowOf(
          rankIndexMap {
            poolOffset = 0L
            rankedSize = 1000
            fingerprints = ByteString.copyFrom(ByteArray(12))
            ranks += 0
          }
        ),
      )
    val row = rankIndexBlob {
      poolOffset = 0L
      blobType = RankIndexBlob.BlobType.SNAPSHOT
      cmmsModelLine = MODEL_LINE
      blobUri = RANK_BLOB_KEY
      encryptedDek = this@VidLabelerAppTest.encryptedDek
      blobChecksum = checksum
      createTime = timestamp { seconds = 1L }
    }
    rankIndexBlobsService.stub {
      onBlocking { listRankIndexBlobs(any()) } doReturn
        listRankIndexBlobsResponse { rankIndexBlobs += row }
    }
  }

  private fun createApp(
    kmsClients: Map<String, KmsClient> = mapOf(DATA_PROVIDER_NAME to kmsClient),
    encryptKekUris: Map<String, String> = mapOf(DATA_PROVIDER_NAME to kekUri),
    loadAssigner: suspend (modelStorageConfig: StorageConfig, modelBlobUri: String) -> VidAssigner =
      { _, _ ->
        mockVidAssigner
      },
    metrics: VidLabelerAppMetrics = VidLabelerAppMetrics(),
  ): VidLabelerApp {
    return VidLabelerApp(
      subscriptionId = "test-subscription",
      queueSubscriber = mockQueueSubscriber,
      parser = WorkItem.parser(),
      workItemsClient = workItemsStub,
      workItemAttemptsClient = workItemAttemptsStub,
      kmsClients = kmsClients,
      encryptKekUris = encryptKekUris,
      getStorageConfig = { StorageConfig(rootDirectory = tempFolder.root) },
      vidLabelingJobsStub = vidLabelingJobsStub,
      rawImpressionUploadModelLinesStub = rawImpressionUploadModelLinesStub,
      rankIndexBlobsStub = rankIndexBlobsStub,
      rawImpressionUploadFilesStub = rawImpressionUploadFilesStub,
      buildParquetStorageClient = { _, _ -> mockParquetStorageClient },
      buildVidRankMapStorageClient = { vidRankMapStorageClient },
      // The model is loaded up-front by VidLabeler.label(); the converter never runs because the
      // (mocked) RawImpressionUploadFileService returns no files, so there are no events to label.
      loadAssigner = loadAssigner,
      buildImpressionConverter = { _, _ ->
        ImpressionConverter { _, _, _ -> error("impressionConverter should not be invoked") }
      },
      metrics = metrics,
    )
  }

  private fun memoizedParams(): VidLabelerParams = vidLabelerParams {
    dataProvider = DATA_PROVIDER_NAME
    rawImpressionsStorageParams =
      VidLabelerParamsKt.storageParams {
        gcsProjectId = "test-project"
        impressionsBlobPrefix = "file:///raw-bucket/impressions"
      }
    vidLabeledImpressionsStorageParams =
      VidLabelerParamsKt.storageParams {
        gcsProjectId = "test-project"
        impressionsBlobPrefix = "file:///output-bucket/labeled"
      }
    modelLineConfigs.put(
      MODEL_LINE,
      VidLabelerParamsKt.modelLineConfig {
        labelerInputFieldMapping +=
          LabelerInputFieldMapping.newBuilder()
            .setFieldPath("event_id.id")
            .setScalar(ScalarColumn.newBuilder().setColumn("event_id_column"))
            .build()
      },
    )
    // Shared payload is top-level; MemoizedParams carries only the rank-index storage.
    vidLabelingJob = VID_LABELING_JOB
    modelLines += MODEL_LINE
    modelBlobPaths.put(MODEL_LINE, "file:///models/model.binpb")
    modelStorageParams =
      VidLabelerParamsKt.storageParams {
        gcsProjectId = "test-model-project"
        impressionsBlobPrefix = "file:///models"
      }
    memoizedParams =
      VidLabelerParamsKt.memoizedParams {
        vidRankMapStorageParams =
          VidLabelerParamsKt.storageParams {
            gcsProjectId = "test-project"
            impressionsBlobPrefix = "file:///rank-map/blobs"
          }
      }
  }

  /** Non-memoized (hash-only) params: top-level `vid_labeling_job` + bundled `model_lines`. */
  private fun nonMemoizedParams(): VidLabelerParams = vidLabelerParams {
    dataProvider = DATA_PROVIDER_NAME
    rawImpressionsStorageParams =
      VidLabelerParamsKt.storageParams {
        gcsProjectId = "test-project"
        impressionsBlobPrefix = "file:///raw-bucket/impressions"
      }
    vidLabeledImpressionsStorageParams =
      VidLabelerParamsKt.storageParams {
        gcsProjectId = "test-project"
        impressionsBlobPrefix = "file:///output-bucket/labeled"
      }
    modelLineConfigs.put(
      MODEL_LINE,
      VidLabelerParamsKt.modelLineConfig {
        labelerInputFieldMapping +=
          LabelerInputFieldMapping.newBuilder()
            .setFieldPath("event_id.id")
            .setScalar(ScalarColumn.newBuilder().setColumn("event_id_column"))
            .build()
      },
    )
    modelLines += MODEL_LINE
    modelBlobPaths.put(MODEL_LINE, "file:///models/model.binpb")
    modelStorageParams =
      VidLabelerParamsKt.storageParams {
        gcsProjectId = "test-model-project"
        impressionsBlobPrefix = "file:///models"
      }
    vidLabelingJob = VID_LABELING_JOB
  }

  private fun buildMessage(params: VidLabelerParams): com.google.protobuf.Any {
    return workItemParams { appParams = params.pack() }.pack()
  }

  @Test
  fun `runWork marks job succeeded`() = runBlocking {
    seedRankIndexBlob()
    vidLabelingJobsService.stub {
      onBlocking { getVidLabelingJob(any()) } doReturn
        vidLabelingJob {
          name = VID_LABELING_JOB
          state = VidLabelingJob.State.CREATED
          etag = "etag-1"
        }
      onBlocking { markVidLabelingJobSucceeded(any()) } doReturn
        markVidLabelingJobSucceededResponse {
          vidLabelingJob = vidLabelingJob {
            name = VID_LABELING_JOB
            state = VidLabelingJob.State.SUCCEEDED
          }
        }
    }

    val app = createApp()
    app.runWork(buildMessage(memoizedParams()))

    val captor = argumentCaptor<MarkVidLabelingJobSucceededRequest>()
    verifyBlocking(vidLabelingJobsService) { markVidLabelingJobSucceeded(captor.capture()) }
    assertThat(captor.firstValue.name).isEqualTo(VID_LABELING_JOB)
    assertThat(captor.firstValue.etag).isEqualTo("etag-1")
    assertThat(captor.firstValue.requestId).isNotEmpty()
    // Not last-job-out: no model line completed, so no transition and no done blob.
    verifyBlocking(rawImpressionUploadModelLinesService, never()) {
      markRawImpressionUploadModelLineCompleted(any())
    }
  }

  @Test
  fun `runWork labels the non-memoized path and marks job succeeded`() = runBlocking {
    // No rank index is seeded: the non-memoized (hash-only) path reads no RankIndexBlob and wraps
    // its labeled output with the EDP's KEK from encryptKekUris instead.
    vidLabelingJobsService.stub {
      onBlocking { getVidLabelingJob(any()) } doReturn
        vidLabelingJob {
          name = VID_LABELING_JOB
          state = VidLabelingJob.State.CREATED
          etag = "etag-1"
        }
      onBlocking { markVidLabelingJobSucceeded(any()) } doReturn
        markVidLabelingJobSucceededResponse {
          vidLabelingJob = vidLabelingJob {
            name = VID_LABELING_JOB
            state = VidLabelingJob.State.SUCCEEDED
          }
        }
    }

    val app = createApp()
    app.runWork(buildMessage(nonMemoizedParams()))

    val captor = argumentCaptor<MarkVidLabelingJobSucceededRequest>()
    verifyBlocking(vidLabelingJobsService) { markVidLabelingJobSucceeded(captor.capture()) }
    assertThat(captor.firstValue.name).isEqualTo(VID_LABELING_JOB)
    assertThat(captor.firstValue.etag).isEqualTo("etag-1")
    assertThat(captor.firstValue.requestId).isNotEmpty()
    // Not last-job-out: no model line completed, so no transition.
    verifyBlocking(rawImpressionUploadModelLinesService, never()) {
      markRawImpressionUploadModelLineCompleted(any())
    }
  }

  @Test
  fun `runWork on last-job-out completes model line`() = runBlocking {
    seedRankIndexBlob()
    vidLabelingJobsService.stub {
      onBlocking { getVidLabelingJob(any()) } doReturn
        vidLabelingJob {
          name = VID_LABELING_JOB
          state = VidLabelingJob.State.CREATED
          etag = "etag-1"
        }
      onBlocking { markVidLabelingJobSucceeded(any()) } doReturn
        markVidLabelingJobSucceededResponse {
          vidLabelingJob = vidLabelingJob {
            name = VID_LABELING_JOB
            state = VidLabelingJob.State.SUCCEEDED
          }
          lastVidLabelingJobResult =
            MarkVidLabelingJobSucceededResponseKt.lastVidLabelingJobResult {
              completedModelLines += MODEL_LINE
            }
        }
    }
    stubModelLineList(
      preMark = listOf(MODEL_LINE to RawImpressionUploadModelLine.State.LABELING),
      postMark = listOf(MODEL_LINE to RawImpressionUploadModelLine.State.COMPLETED),
    )

    // FileSystemStorageClient requires the bucket directory to pre-exist (GCS buckets always do).
    tempFolder.root.resolve("output-bucket").mkdirs()

    val app = createApp()
    app.runWork(buildMessage(memoizedParams()))

    verifyBlocking(rawImpressionUploadModelLinesService) {
      markRawImpressionUploadModelLineCompleted(any())
    }
    // The done marker (model-line/ml1/<event_date>/done) is written from the shared footer
    // event_date read in VidLabelerApp.resolveSharedEventDate. This WorkItem carries no input files
    // (the mocked file service returns none), so no footer is read and no marker is written — the
    // done-marker write is not exercised here. See the TODO in resolveSharedEventDate: it can be
    // covered once ParquetStorageClient can write footer key-value metadata.
  }

  @Test
  fun `runWork skips relabel but still marks succeeded when job already SUCCEEDED`() = runBlocking {
    // No seedRankIndexBlob(): the already-SUCCEEDED branch skips label(), so MemoizedRankIndex.load
    // is never invoked. The idempotent mark + last-job-out recovery must still run.
    vidLabelingJobsService.stub {
      onBlocking { getVidLabelingJob(any()) } doReturn
        vidLabelingJob {
          name = VID_LABELING_JOB
          state = VidLabelingJob.State.SUCCEEDED
          etag = "etag-1"
        }
      onBlocking { markVidLabelingJobSucceeded(any()) } doReturn
        markVidLabelingJobSucceededResponse {
          vidLabelingJob = vidLabelingJob {
            name = VID_LABELING_JOB
            state = VidLabelingJob.State.SUCCEEDED
          }
          lastVidLabelingJobResult =
            MarkVidLabelingJobSucceededResponseKt.lastVidLabelingJobResult {
              completedModelLines += MODEL_LINE
            }
        }
    }
    stubModelLineList(
      preMark = listOf(MODEL_LINE to RawImpressionUploadModelLine.State.LABELING),
      postMark = listOf(MODEL_LINE to RawImpressionUploadModelLine.State.COMPLETED),
    )

    tempFolder.root.resolve("output-bucket").mkdirs()

    val app = createApp()
    app.runWork(buildMessage(memoizedParams()))

    // The (mocked) converter would throw if label() ran; reaching here proves relabel was skipped.
    verifyBlocking(vidLabelingJobsService) { markVidLabelingJobSucceeded(any()) }
    // Last-job-out recovery still runs from the already-SUCCEEDED path.
    verifyBlocking(rawImpressionUploadModelLinesService) {
      markRawImpressionUploadModelLineCompleted(any())
    }
  }

  @Test
  fun `runWork warns and continues when no matching RawImpressionUploadModelLine`() = runBlocking {
    seedRankIndexBlob()
    vidLabelingJobsService.stub {
      onBlocking { getVidLabelingJob(any()) } doReturn
        vidLabelingJob {
          name = VID_LABELING_JOB
          state = VidLabelingJob.State.CREATED
          etag = "etag-1"
        }
      onBlocking { markVidLabelingJobSucceeded(any()) } doReturn
        markVidLabelingJobSucceededResponse {
          vidLabelingJob = vidLabelingJob {
            name = VID_LABELING_JOB
            state = VidLabelingJob.State.SUCCEEDED
          }
          lastVidLabelingJobResult =
            MarkVidLabelingJobSucceededResponseKt.lastVidLabelingJobResult {
              completedModelLines += MODEL_LINE
            }
        }
    }
    // getParent finds nothing for the completed model line.
    rawImpressionUploadModelLinesService.stub {
      onBlocking { listRawImpressionUploadModelLines(any()) } doReturn
        listRawImpressionUploadModelLinesResponse {}
    }

    tempFolder.root.resolve("output-bucket").mkdirs()

    val app = createApp()
    // No throw: a missing parent is logged and skipped.
    app.runWork(buildMessage(memoizedParams()))

    // Mark succeeded still happens; no completion transition is attempted for the absent parent.
    verifyBlocking(vidLabelingJobsService) { markVidLabelingJobSucceeded(any()) }
    verifyBlocking(rawImpressionUploadModelLinesService, never()) {
      markRawImpressionUploadModelLineCompleted(any())
    }
  }

  @Test
  fun `runWork swallows FAILED_PRECONDITION from markRawImpressionUploadModelLineCompleted`() =
    runBlocking {
      seedRankIndexBlob()
      vidLabelingJobsService.stub {
        onBlocking { getVidLabelingJob(any()) } doReturn
          vidLabelingJob {
            name = VID_LABELING_JOB
            state = VidLabelingJob.State.CREATED
            etag = "etag-1"
          }
        onBlocking { markVidLabelingJobSucceeded(any()) } doReturn
          markVidLabelingJobSucceededResponse {
            vidLabelingJob = vidLabelingJob {
              name = VID_LABELING_JOB
              state = VidLabelingJob.State.SUCCEEDED
            }
            lastVidLabelingJobResult =
              MarkVidLabelingJobSucceededResponseKt.lastVidLabelingJobResult {
                completedModelLines += MODEL_LINE
              }
          }
      }
      stubModelLineList(
        preMark = listOf(MODEL_LINE to RawImpressionUploadModelLine.State.LABELING),
        postMark = listOf(MODEL_LINE to RawImpressionUploadModelLine.State.COMPLETED),
      )
      // The parent already advanced: the transition is a benign already-advanced race.
      rawImpressionUploadModelLinesService.stub {
        onBlocking { markRawImpressionUploadModelLineCompleted(any()) } doThrow
          StatusRuntimeException(Status.FAILED_PRECONDITION)
      }

      tempFolder.root.resolve("output-bucket").mkdirs()

      val app = createApp()
      // No throw: FAILED_PRECONDITION on the completion transition is treated as already-done.
      app.runWork(buildMessage(memoizedParams()))

      verifyBlocking(rawImpressionUploadModelLinesService) {
        markRawImpressionUploadModelLineCompleted(any())
      }
    }

  @Test
  fun `runWork completes the model line even while a sibling is still labeling`() = runBlocking {
    seedRankIndexBlob()
    vidLabelingJobsService.stub {
      onBlocking { getVidLabelingJob(any()) } doReturn
        vidLabelingJob {
          name = VID_LABELING_JOB
          state = VidLabelingJob.State.CREATED
          etag = "etag-1"
        }
      onBlocking { markVidLabelingJobSucceeded(any()) } doReturn
        markVidLabelingJobSucceededResponse {
          vidLabelingJob = vidLabelingJob {
            name = VID_LABELING_JOB
            state = VidLabelingJob.State.SUCCEEDED
          }
          lastVidLabelingJobResult =
            MarkVidLabelingJobSucceededResponseKt.lastVidLabelingJobResult {
              completedModelLines += MODEL_LINE
            }
        }
    }
    // This WorkItem's model line reached last-job-out (the service returns it in
    // completedModelLines) while a sibling of the same upload is still LABELING. Per-model-line
    // finalization: only the completed line is transitioned to COMPLETED (and, in production, only
    // it gets a done marker); the still-labeling sibling is untouched.
    stubModelLineList(
      preMark =
        listOf(
          MODEL_LINE to RawImpressionUploadModelLine.State.LABELING,
          SIBLING_MODEL_LINE to RawImpressionUploadModelLine.State.LABELING,
        ),
      postMark =
        listOf(
          MODEL_LINE to RawImpressionUploadModelLine.State.COMPLETED,
          SIBLING_MODEL_LINE to RawImpressionUploadModelLine.State.LABELING,
        ),
    )

    tempFolder.root.resolve("output-bucket").mkdirs()

    val app = createApp()
    app.runWork(buildMessage(memoizedParams()))

    // The job's own model line is transitioned to COMPLETED; the still-LABELING sibling is not in
    // completedModelLines, so it is neither transitioned nor given a done marker. (The done
    // marker for the completed line is written from the footer event_date — not exercised here;
    // see the TODO in VidLabelerApp.resolveSharedEventDate.)
    verifyBlocking(rawImpressionUploadModelLinesService) {
      markRawImpressionUploadModelLineCompleted(any())
    }
  }

  /**
   * Stubs [listRawImpressionUploadModelLines] for the unfiltered List call a last-job-out makes in
   * `VidLabelerApp.markSucceededAndTransition` to resolve the parent rows to transition: the first
   * call returns [preMark]; any subsequent call returns [postMark]. Each entry is a `(model line,
   * state)` pair.
   */
  private fun stubModelLineList(
    preMark: List<Pair<String, RawImpressionUploadModelLine.State>>,
    postMark: List<Pair<String, RawImpressionUploadModelLine.State>>,
  ) {
    val callCount = AtomicInteger(0)
    rawImpressionUploadModelLinesService.stub {
      onBlocking { listRawImpressionUploadModelLines(any()) } doAnswer
        {
          val rows = if (callCount.getAndIncrement() == 0) preMark else postMark
          listRawImpressionUploadModelLinesResponse {
            for ((modelLine, modelLineState) in rows) {
              rawImpressionUploadModelLines += rawImpressionUploadModelLine {
                name = PARENT_NAME
                cmmsModelLine = modelLine
                state = modelLineState
                etag = "parent-etag"
              }
            }
          }
        }
    }
  }

  @Test
  fun `runWork propagates failure without marking the job FAILED`() = runBlocking {
    seedRankIndexBlob()
    vidLabelingJobsService.stub {
      onBlocking { getVidLabelingJob(any()) } doReturn
        vidLabelingJob {
          name = VID_LABELING_JOB
          state = VidLabelingJob.State.CREATED
          etag = "etag-1"
        }
      // The work fails: mark-succeeded throws a non-cancellation StatusRuntimeException. Use
      // StatusRuntimeException (not a checked StatusException) because Mockito rejects a checked
      // exception on the suspend stub.
      onBlocking { markVidLabelingJobSucceeded(any()) } doThrow
        StatusRuntimeException(Status.FAILED_PRECONDITION)
    }

    val app = createApp()
    // The failure propagates so the framework nacks and Pub/Sub retries. The worker never marks the
    // job FAILED itself — the DLQ listener owns the terminal FAILED transition on retry exhaustion.
    assertFailsWith<StatusException> { app.runWork(buildMessage(memoizedParams())) }

    verifyBlocking(vidLabelingJobsService, never()) { markVidLabelingJobFailed(any()) }
  }

  @Test
  fun `runWork throws when raw_impressions_storage_params is not set`() = runBlocking {
    val app = createApp()
    val params = vidLabelerParams {
      dataProvider = DATA_PROVIDER_NAME
      vidLabeledImpressionsStorageParams =
        VidLabelerParamsKt.storageParams {
          gcsProjectId = "test-project"
          impressionsBlobPrefix = "file:///output-bucket/labeled"
        }
    }

    val exception = assertFailsWith<IllegalArgumentException> { app.runWork(buildMessage(params)) }
    assertThat(exception).hasMessageThat().contains("raw_impressions_storage_params must be set")
  }

  @Test
  fun `runWork throws when vid_labeled_impressions_storage_params is not set`() = runBlocking {
    val app = createApp()
    val params = vidLabelerParams {
      dataProvider = DATA_PROVIDER_NAME
      rawImpressionsStorageParams =
        VidLabelerParamsKt.storageParams {
          gcsProjectId = "test-project"
          impressionsBlobPrefix = "file:///raw-bucket/impressions"
        }
    }

    val exception = assertFailsWith<IllegalArgumentException> { app.runWork(buildMessage(params)) }
    assertThat(exception)
      .hasMessageThat()
      .contains("vid_labeled_impressions_storage_params must be set")
  }

  @Test
  fun `runWork throws when neither memoized_params nor vid_labeling_job is set`() = runBlocking {
    // No memoized_params routes to the non-memoized path, which requires a top-level
    // vid_labeling_job.
    val app = createApp()
    val params = vidLabelerParams {
      dataProvider = DATA_PROVIDER_NAME
      rawImpressionsStorageParams =
        VidLabelerParamsKt.storageParams {
          gcsProjectId = "test-project"
          impressionsBlobPrefix = "file:///raw-bucket/impressions"
        }
      vidLabeledImpressionsStorageParams =
        VidLabelerParamsKt.storageParams {
          gcsProjectId = "test-project"
          impressionsBlobPrefix = "file:///output-bucket/labeled"
        }
    }

    val exception = assertFailsWith<IllegalArgumentException> { app.runWork(buildMessage(params)) }
    assertThat(exception).hasMessageThat().contains("vid_labeling_job must be set")
  }

  @Test
  fun `runWork throws when KMS client not found for data provider`() = runBlocking {
    val app = createApp(kmsClients = emptyMap())
    val exception =
      assertFailsWith<IllegalArgumentException> { app.runWork(buildMessage(memoizedParams())) }
    assertThat(exception).hasMessageThat().contains("KMS client not found")
    assertThat(exception).hasMessageThat().contains(DATA_PROVIDER_NAME)
  }

  @Test
  fun `runWork throws when data_provider is empty`() = runBlocking {
    val app = createApp()
    val params = vidLabelerParams {
      rawImpressionsStorageParams =
        VidLabelerParamsKt.storageParams {
          gcsProjectId = "test-project"
          impressionsBlobPrefix = "file:///raw-bucket/impressions"
        }
      vidLabeledImpressionsStorageParams =
        VidLabelerParamsKt.storageParams {
          gcsProjectId = "test-project"
          impressionsBlobPrefix = "file:///output-bucket/labeled"
        }
    }

    val exception = assertFailsWith<IllegalArgumentException> { app.runWork(buildMessage(params)) }
    assertThat(exception).hasMessageThat().contains("data_provider must not be empty")
  }

  @Test
  fun `runWork loads the compiled model once across WorkItems`() = runBlocking {
    seedRankIndexBlob()
    vidLabelingJobsService.stub {
      onBlocking { getVidLabelingJob(any()) } doReturn
        vidLabelingJob {
          name = VID_LABELING_JOB
          state = VidLabelingJob.State.CREATED
          etag = "etag-1"
        }
      onBlocking { markVidLabelingJobSucceeded(any()) } doReturn
        markVidLabelingJobSucceededResponse {
          vidLabelingJob = vidLabelingJob {
            name = VID_LABELING_JOB
            state = VidLabelingJob.State.SUCCEEDED
          }
        }
    }

    val loadCount = AtomicInteger(0)
    val app =
      createApp(
        loadAssigner = { _, _ ->
          loadCount.incrementAndGet()
          mockVidAssigner
        }
      )

    // Two WorkItems for the same model blob: the shared VidModelLoader caches the compiled
    // model, so it is loaded from storage exactly once across both calls.
    app.runWork(buildMessage(memoizedParams()))
    app.runWork(buildMessage(memoizedParams()))

    assertThat(loadCount.get()).isEqualTo(1)
  }

  @Test
  fun `runWork records work-item and duration metrics on success`() = runBlocking {
    seedRankIndexBlob()
    vidLabelingJobsService.stub {
      onBlocking { getVidLabelingJob(any()) } doReturn
        vidLabelingJob {
          name = VID_LABELING_JOB
          state = VidLabelingJob.State.CREATED
          etag = "etag-1"
        }
      onBlocking { markVidLabelingJobSucceeded(any()) } doReturn
        markVidLabelingJobSucceededResponse {
          vidLabelingJob = vidLabelingJob {
            name = VID_LABELING_JOB
            state = VidLabelingJob.State.SUCCEEDED
          }
          lastVidLabelingJobResult =
            MarkVidLabelingJobSucceededResponseKt.lastVidLabelingJobResult {
              completedModelLines += MODEL_LINE
            }
        }
    }
    stubModelLineList(
      preMark = listOf(MODEL_LINE to RawImpressionUploadModelLine.State.LABELING),
      postMark = listOf(MODEL_LINE to RawImpressionUploadModelLine.State.COMPLETED),
    )
    tempFolder.root.resolve("output-bucket").mkdirs()

    val reader = InMemoryMetricReader.create()
    val meter = SdkMeterProvider.builder().registerMetricReader(reader).build().get("test")
    val app = createApp(metrics = VidLabelerAppMetrics(meter))

    app.runWork(buildMessage(memoizedParams()))

    val collected = reader.collectAllMetrics().associateBy { it.name }
    assertThat(
        collected.getValue("edpa.vid_labeler_app.work_items_processed").longSumData.points.map {
          it.value
        }
      )
      .containsExactly(1L)
    // The done_blobs_written counter is incremented in VidLabelerApp.writeDoneBlob, which needs the
    // footer event_date; this WorkItem carries no input files so no marker (and no increment) is
    // produced. See the TODO in VidLabelerApp.resolveSharedEventDate.
    assertThat(
        collected.getValue("edpa.vid_labeler_app.work_item_duration").histogramData.points.sumOf {
          it.count
        }
      )
      .isEqualTo(1L)
  }

  @Test
  fun `runWork records mark-succeeded failure metric and does not count the work item`() =
    runBlocking {
      seedRankIndexBlob()
      vidLabelingJobsService.stub {
        onBlocking { getVidLabelingJob(any()) } doReturn
          vidLabelingJob {
            name = VID_LABELING_JOB
            state = VidLabelingJob.State.CREATED
            etag = "etag-1"
          }
        onBlocking { markVidLabelingJobSucceeded(any()) } doThrow
          StatusRuntimeException(Status.INTERNAL)
      }

      val reader = InMemoryMetricReader.create()
      val meter = SdkMeterProvider.builder().registerMetricReader(reader).build().get("test")
      val app = createApp(metrics = VidLabelerAppMetrics(meter))

      assertFailsWith<StatusException> { app.runWork(buildMessage(memoizedParams())) }

      val collected = reader.collectAllMetrics().associateBy { it.name }
      assertThat(
          collected
            .getValue("edpa.vid_labeler_app.mark_succeeded_failures")
            .longSumData
            .points
            .map { it.value }
        )
        .containsExactly(1L)
      // The failed WorkItem is not counted as processed, but its duration is still recorded.
      assertThat(
          collected["edpa.vid_labeler_app.work_items_processed"]?.longSumData?.points?.sumOf {
            it.value
          } ?: 0L
        )
        .isEqualTo(0L)
      assertThat(
          collected.getValue("edpa.vid_labeler_app.work_item_duration").histogramData.points.sumOf {
            it.count
          }
        )
        .isEqualTo(1L)
    }

  @Test
  fun `runWork records mark-completed failure metric on a non-benign error`() = runBlocking {
    seedRankIndexBlob()
    vidLabelingJobsService.stub {
      onBlocking { getVidLabelingJob(any()) } doReturn
        vidLabelingJob {
          name = VID_LABELING_JOB
          state = VidLabelingJob.State.CREATED
          etag = "etag-1"
        }
      onBlocking { markVidLabelingJobSucceeded(any()) } doReturn
        markVidLabelingJobSucceededResponse {
          vidLabelingJob = vidLabelingJob {
            name = VID_LABELING_JOB
            state = VidLabelingJob.State.SUCCEEDED
          }
          lastVidLabelingJobResult =
            MarkVidLabelingJobSucceededResponseKt.lastVidLabelingJobResult {
              completedModelLines += MODEL_LINE
            }
        }
    }
    stubModelLineList(
      preMark = listOf(MODEL_LINE to RawImpressionUploadModelLine.State.LABELING),
      postMark = listOf(MODEL_LINE to RawImpressionUploadModelLine.State.COMPLETED),
    )
    // A non-benign error (not FAILED_PRECONDITION/ABORTED) propagates and is counted.
    rawImpressionUploadModelLinesService.stub {
      onBlocking { markRawImpressionUploadModelLineCompleted(any()) } doThrow
        StatusRuntimeException(Status.INTERNAL)
    }

    val reader = InMemoryMetricReader.create()
    val meter = SdkMeterProvider.builder().registerMetricReader(reader).build().get("test")
    val app = createApp(metrics = VidLabelerAppMetrics(meter))

    assertFailsWith<StatusException> { app.runWork(buildMessage(memoizedParams())) }

    val collected = reader.collectAllMetrics().associateBy { it.name }
    assertThat(
        collected
          .getValue("edpa.vid_labeler_app.mark_completed_failures")
          .longSumData
          .points
          .single()
          .value
      )
      .isEqualTo(1L)
  }

  companion object {
    private const val DATA_PROVIDER_NAME = "dataProviders/edp123"
    private const val UPLOAD = "dataProviders/edp123/rawImpressionUploads/up1"
    private const val VID_LABELING_JOB =
      "dataProviders/edp123/rawImpressionUploads/up1/vidLabelingJobs/vlj-0"
    private const val MODEL_LINE = "modelProviders/mp/modelSuites/ms/modelLines/ml1"
    private const val SIBLING_MODEL_LINE = "modelProviders/mp/modelSuites/ms/modelLines/ml2"
    private const val PARENT_NAME =
      "dataProviders/edp123/rawImpressionUploads/up1/rawImpressionUploadModelLines/rl1"
    private const val RANK_BLOB_KEY = "rank-index/snapshot/pool0"
  }
}
