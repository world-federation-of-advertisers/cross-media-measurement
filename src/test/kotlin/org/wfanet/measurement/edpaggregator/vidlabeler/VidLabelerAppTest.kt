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
import com.google.crypto.tink.KmsClient
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.stub
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.MarkVidLabelingJobSucceededRequest
import org.wfanet.measurement.edpaggregator.v1alpha.MarkVidLabelingJobSucceededResponseKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.markVidLabelingJobSucceededResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelingJob
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.storage.ParquetStorageClient
import org.wfanet.measurement.storage.StorageClient

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

  private val mockDecryptKmsClient: KmsClient = mock()
  private val mockEncryptKmsClient: KmsClient = mock()
  private val mockQueueSubscriber: QueueSubscriber = mock()
  private val mockParquetStorageClient: ParquetStorageClient = mock()
  private val mockVidRankMapStorageClient: StorageClient = mock()
  private val mockVidAssigner: VidAssigner = mock()

  private fun createApp(
    rawImpressionsKmsClient: Map<String, KmsClient> =
      mapOf(DATA_PROVIDER_NAME to mockDecryptKmsClient),
    vidLabeledImpressionsKmsClient: Map<String, KmsClient> =
      mapOf(DATA_PROVIDER_NAME to mockEncryptKmsClient),
  ): VidLabelerApp {
    return VidLabelerApp(
      subscriptionId = "test-subscription",
      queueSubscriber = mockQueueSubscriber,
      parser = WorkItem.parser(),
      workItemsClient = workItemsStub,
      workItemAttemptsClient = workItemAttemptsStub,
      rawImpressionsKmsClient = rawImpressionsKmsClient,
      vidLabeledImpressionsKmsClient = vidLabeledImpressionsKmsClient,
      getStorageConfig = { StorageConfig(rootDirectory = tempFolder.root) },
      vidLabelingJobsStub = vidLabelingJobsStub,
      rawImpressionUploadModelLinesStub = rawImpressionUploadModelLinesStub,
      rankIndexBlobsStub = rankIndexBlobsStub,
      rawImpressionUploadFilesStub = rawImpressionUploadFilesStub,
      buildParquetStorageClient = { mockParquetStorageClient },
      buildVidRankMapStorageClient = { mockVidRankMapStorageClient },
      // The model is loaded up-front by VidLabeler.label(); the converter never runs because the
      // (mocked) RawImpressionUploadFileService returns no files, so there are no events to label.
      loadAssigner = { mockVidAssigner },
      impressionConverter =
        ImpressionConverter { _, _ -> error("impressionConverter should not be invoked") },
      getVidLabeledImpressionsKekUri = { "fake-kek-uri" },
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
        labelerInputFieldMapping.put("event_id.id", "event_id_column")
      },
    )
    memoizedParams =
      VidLabelerParamsKt.memoizedParams {
        vidLabelingJob = VID_LABELING_JOB
        rawImpressionUploadFiles += "$UPLOAD/rawImpressionUploadFiles/f1"
        modelLine = MODEL_LINE
        modelBlobPath = "file:///models/model.binpb"
        vidRankMapStorageParams =
          VidLabelerParamsKt.storageParams {
            gcsProjectId = "test-project"
            impressionsBlobPrefix = "file:///rank-map/blobs"
          }
      }
  }

  private fun buildMessage(params: VidLabelerParams): com.google.protobuf.Any {
    return workItemParams { appParams = params.pack() }.pack()
  }

  @Test
  fun `runWork marks job succeeded`() = runBlocking {
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
  fun `runWork on last-job-out completes model line and writes done blob`() = runBlocking {
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
    rawImpressionUploadModelLinesService.stub {
      onBlocking { listRawImpressionUploadModelLines(any()) } doReturn
        listRawImpressionUploadModelLinesResponse {
          rawImpressionUploadModelLines += rawImpressionUploadModelLine {
            name = PARENT_NAME
            cmmsModelLine = MODEL_LINE
            state = RawImpressionUploadModelLine.State.LABELING
            etag = "parent-etag"
          }
        }
    }

    // FileSystemStorageClient requires the bucket directory to pre-exist (GCS buckets always do).
    tempFolder.root.resolve("output-bucket").mkdirs()

    val app = createApp()
    app.runWork(buildMessage(memoizedParams()))

    verifyBlocking(rawImpressionUploadModelLinesService) {
      markRawImpressionUploadModelLineCompleted(any())
    }
    // file:///output-bucket/labeled -> bucket "output-bucket", key "labeled/...". The done blob is
    // written under {rootDir}/{bucket}/{key}.
    val doneFile =
      tempFolder.root.toPath().resolve("output-bucket/labeled/labeled-impressions/done").toFile()
    assertThat(doneFile.exists()).isTrue()
  }

  @Test
  fun `runWork throws when memoized_params is not set`() = runBlocking {
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
    assertThat(exception).hasMessageThat().contains("memoized_params must be set")
  }

  @Test
  fun `runWork throws when encrypt KMS client not found for data provider`() = runBlocking {
    val app = createApp(vidLabeledImpressionsKmsClient = emptyMap())
    val exception =
      assertFailsWith<IllegalArgumentException> { app.runWork(buildMessage(memoizedParams())) }
    assertThat(exception).hasMessageThat().contains("Encrypt KMS client not found")
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

  companion object {
    private const val DATA_PROVIDER_NAME = "dataProviders/edp123"
    private const val UPLOAD = "dataProviders/edp123/rawImpressionUploads/up1"
    private const val VID_LABELING_JOB =
      "dataProviders/edp123/rawImpressionUploads/up1/vidLabelingJobs/vlj-0"
    private const val MODEL_LINE = "modelProviders/mp/modelSuites/ms/modelLines/ml1"
    private const val PARENT_NAME =
      "dataProviders/edp123/rawImpressionUploads/up1/rawImpressionUploadModelLines/rl1"
  }
}
