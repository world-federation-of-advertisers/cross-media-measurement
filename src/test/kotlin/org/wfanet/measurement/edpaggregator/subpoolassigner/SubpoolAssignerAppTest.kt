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

package org.wfanet.measurement.edpaggregator.subpoolassigner

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.RegistryConfiguration
import com.google.crypto.tink.aead.AeadConfig
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.verifyNoInteractions
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.testing.VidLabelingRpcThrottlersTestHelper
import org.wfanet.measurement.edpaggregator.v1alpha.LabelerInputFieldMapping
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJob
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ScalarColumn
import org.wfanet.measurement.edpaggregator.v1alpha.SubpoolAssignerParams
import org.wfanet.measurement.edpaggregator.v1alpha.SubpoolAssignerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.poolAssignmentJob
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.subpoolAssignerParams
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.storage.BlobChangedException
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.ParquetStorageClient
import org.wfanet.measurement.storage.ParquetValue
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.parquetValue
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.virtualpeople.common.LabelerInput

private const val DATA_PROVIDER = "dataProviders/dp"
private const val UPLOAD = "dataProviders/dp/rawImpressionUploads/up1"
private const val ALLOWED_PRIOR_UPLOAD = "dataProviders/dp/rawImpressionUploads/up0"
private const val MODEL_LINE = "modelProviders/mp/modelSuites/ms/modelLines/ml1"

/** Tests Phase-0 model validation and the Phase-1 params template. */
@RunWith(JUnit4::class)
class SubpoolAssignerAppTest {

  private fun storageParams(prefix: String) =
    SubpoolAssignerParamsKt.storageParams {
      gcsProjectId = "test-project"
      blobPrefix = prefix
    }

  private fun validParams(): SubpoolAssignerParams = subpoolAssignerParams {
    dataProvider = DATA_PROVIDER
    rawImpressionStorageParams = storageParams("raw-impressions")
    vidLabeledImpressionsStorageParams = storageParams("vid-labeled")
    subpoolMapStorageParams = storageParams("subpool-map")
    modelStorageParams = storageParams("model")
    vidRankMapStorageParams = storageParams("vid-rank-map")
    rawImpressionUpload = UPLOAD
    modelLine = MODEL_LINE
    modelBlobPath = "model/blob"
    labelerInputFieldMapping +=
      LabelerInputFieldMapping.newBuilder()
        .setFieldPath("event_id.id")
        .setScalar(ScalarColumn.newBuilder().setColumn("raw_event_id"))
        .build()
    eventTemplateFieldMapping.put("banner_ad.viewable", "raw_viewable")
    eventTemplateDescriptorBlobUri = "descriptor/event-template.pb"
    eventTemplateType = "wfa.measurement.api.v2alpha.event_templates.testing.TestEvent"
    populationSpecBlobUri = "gs://configs/population-spec.textproto"
    eventTemplateVersion = 1
    requiredEntityKeyFieldMapping.put("household", "hh_col")
    optionalEntityKeyFieldMapping.put("creative", "cr_col")
    allowedPriorSnapshotRawImpressionUploads += ALLOWED_PRIOR_UPLOAD
    totalShards = 4
    maxFileBatchSizeBytes = 777
    activeStartTime = timestamp { seconds = 1000 }
    activeEndTime = timestamp { seconds = 2000 }
  }

  @Test
  fun `runWork returns for a completed storage loser and throws for unfinished work`() =
    runBlocking<Unit> {
      AeadConfig.register()
      val kekUri = FakeKmsClient.KEY_URI_PREFIX + "app"
      val kms =
        FakeKmsClient().apply {
          setAead(
            kekUri,
            KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
              .getPrimitive(RegistryConfiguration.get(), Aead::class.java),
          )
        }
      val row =
        mapOf(
          "raw_event_id" to parquetValue { stringValue = "event-1" },
          "raw_time" to parquetValue { int64Value = 1_000_000_000L },
        )
      val parquetBlob =
        mock<ParquetStorageClient.ParquetBlob> {
          on { readRows() } doReturn flowOf(row)
          onBlocking { readSchema() } doReturn
            mapOf(
              "raw_event_id" to ParquetValue.KindCase.STRING_VALUE,
              "raw_time" to ParquetValue.KindCase.INT64_VALUE,
            )
          onBlocking { readKeyValueMetadata() } doReturn emptyMap()
        }
      val parquet =
        mock<ParquetStorageClient> { onBlocking { getBlob(any()) } doReturn parquetBlob }
      val files =
        mock<RawImpressionUploadFileServiceCoroutineStub> {
          onBlocking { listRawImpressionUploadFiles(any(), any()) } doReturn
            listRawImpressionUploadFilesResponse {
              rawImpressionUploadFiles += rawImpressionUploadFile {
                name = "$UPLOAD/files/file1"
                blobUri = "gs://raw/input.parquet"
                blobGeneration = 42L
              }
            }
        }
      val emit =
        object : PoolEmitLabeler {
          override fun emit(input: LabelerInput, onPoolOffset: (Long) -> Unit): Int {
            onPoolOffset(7L)
            return 1
          }

          override fun rankedSize(poolOffset: Long): Int = 100
        }
      for (completed in listOf(true, false)) {
        var reads = 0
        val jobs =
          mock<PoolAssignmentJobServiceCoroutineStub> {
            onBlocking { getPoolAssignmentJob(any(), any()) } doAnswer
              {
                poolAssignmentJob {
                  name = "$UPLOAD/poolAssignmentJobs/job"
                  etag = "etag"
                  state =
                    if (reads++ == 0 || !completed) PoolAssignmentJob.State.CREATED
                    else PoolAssignmentJob.State.SUCCEEDED
                }
              }
          }
        var writes = 0
        val output =
          object : ConditionalOperationStorageClient by InMemoryStorageClient() {
            override suspend fun writeBlobIfNotFound(
              blobKey: String,
              content: Flow<ByteString>,
            ): StorageClient.Blob {
              writes++
              throw BlobChangedException("concurrent output")
            }
          }
        val lines =
          mock<RawImpressionUploadModelLineServiceCoroutineStub> {
            onBlocking { listRawImpressionUploadModelLines(any(), any()) } doReturn
              listRawImpressionUploadModelLinesResponse {
                rawImpressionUploadModelLines += rawImpressionUploadModelLine {
                  name = "$UPLOAD/rawImpressionUploadModelLines/line"
                  cmmsModelLine = MODEL_LINE
                  state = RawImpressionUploadModelLine.State.RANKING
                  poolOffsets += 7L
                }
              }
          }
        val app =
          SubpoolAssignerApp(
            subscriptionId = "test",
            queueSubscriber = mock(),
            parser = WorkItem.parser(),
            workItemsClient = mock<WorkItemsCoroutineStub>(),
            workItemAttemptsClient = mock<WorkItemAttemptsCoroutineStub>(),
            vidRankBuilderQueue = "queues/ranker",
            kmsClients = mapOf(DATA_PROVIDER to kms),
            getSubpoolMapStorageConfig = { StorageConfig(blobPrefix = it.blobPrefix) },
            getRawImpressionsStorageConfig = { StorageConfig(blobPrefix = it.blobPrefix) },
            getModelStorageConfig = { StorageConfig(blobPrefix = it.blobPrefix) },
            rawImpressionUploadsStub = mock(),
            rawImpressionUploadModelLinesStub = lines,
            rankerJobsStub = mock(),
            poolAssignmentJobsStub = jobs,
            rawImpressionUploadFilesStub = files,
            buildParquetStorageClient = { _, _ -> parquet },
            buildSubpoolMapStorageClient = { output },
            loadPoolEmitLabeler = { _, _ -> emit },
            getSubpoolMapKekUri = { kekUri },
            rpcThrottlers = VidLabelingRpcThrottlersTestHelper.alwaysReady(),
          )
        val params =
          validParams().copy {
            totalShards = 1
            labelerInputFieldMapping +=
              LabelerInputFieldMapping.newBuilder()
                .setFieldPath("timestamp_usec")
                .setScalar(ScalarColumn.newBuilder().setColumn("raw_time"))
                .build()
            activeStartTime = timestamp {}
            poolAssignmentJob = "$UPLOAD/poolAssignmentJobs/job"
          }
        val message = workItemParams { appParams = params.pack() }.pack()
        if (completed) app.runWork(message)
        else assertFailsWith<BlobChangedException> { app.runWork(message) }
        assertThat(writes).isEqualTo(1)
        assertThat(reads).isEqualTo(2)
        verifyBlocking(jobs, never()) { markPoolAssignmentJobSucceeded(any(), any()) }
        verifyBlocking(lines, never()) { markRawImpressionUploadModelLineRanking(any(), any()) }
      }
    }

  @Test
  fun `runWork rejects an invalid model before reading raw impressions`() =
    runBlocking<Unit> {
      val modelFailure = IllegalArgumentException("Model has no ranked pools")
      val filesStub =
        mock<RawImpressionUploadFileServiceCoroutineStub> {
          onBlocking { listRawImpressionUploadFiles(any(), any()) } doAnswer
            {
              error("Raw impression files must not be read for an invalid model")
            }
        }
      val parquetStorageClient = mock<ParquetStorageClient>()
      val app =
        SubpoolAssignerApp(
          subscriptionId = "subpool-assigner-subscription",
          queueSubscriber = mock<QueueSubscriber>(),
          parser = WorkItem.parser(),
          workItemsClient = mock<WorkItemsCoroutineStub>(),
          workItemAttemptsClient = mock<WorkItemAttemptsCoroutineStub>(),
          vidRankBuilderQueue = "queues/vid-rank-builder",
          kmsClients = mapOf(DATA_PROVIDER to mock<KmsClient>()),
          getSubpoolMapStorageConfig = { StorageConfig(blobPrefix = it.blobPrefix) },
          getRawImpressionsStorageConfig = { StorageConfig(blobPrefix = it.blobPrefix) },
          getModelStorageConfig = { StorageConfig(blobPrefix = it.blobPrefix) },
          rawImpressionUploadsStub = mock<RawImpressionUploadServiceCoroutineStub>(),
          rawImpressionUploadModelLinesStub =
            mock<RawImpressionUploadModelLineServiceCoroutineStub>(),
          rankerJobsStub = mock<RankerJobServiceCoroutineStub>(),
          poolAssignmentJobsStub = mock<PoolAssignmentJobServiceCoroutineStub>(),
          rawImpressionUploadFilesStub = filesStub,
          buildParquetStorageClient = { _, _ -> parquetStorageClient },
          buildSubpoolMapStorageClient = { mock<ConditionalOperationStorageClient>() },
          loadPoolEmitLabeler = { _, _ -> throw modelFailure },
          getSubpoolMapKekUri = { "kms://test-key" },
          rpcThrottlers = VidLabelingRpcThrottlersTestHelper.alwaysReady(),
        )
      val message = workItemParams { appParams = validParams().pack() }.pack()

      val exception = assertFailsWith<IllegalArgumentException> { app.runWork(message) }

      assertThat(exception).isSameInstanceAs(modelFailure)
      verifyNoInteractions(filesStub, parquetStorageClient)
    }

  @Test
  fun `forwards max_file_batch_size_bytes verbatim into the template`() {
    val template = SubpoolAssignerApp.buildVidRankBuilderParamsTemplate(validParams())

    assertThat(template.maxFileBatchSizeBytes).isEqualTo(777)
  }

  @Test
  fun `requires max_file_batch_size_bytes to be set`() {
    // REQUIRED contract: the dispatcher must set it; an unset (0) value fails at this boundary
    // rather than being forwarded for Phase-1 to silently default.
    val params = validParams().toBuilder().clearMaxFileBatchSizeBytes().build()

    assertFailsWith<IllegalArgumentException> {
      SubpoolAssignerApp.buildVidRankBuilderParamsTemplate(params)
    }
  }

  @Test
  fun `maps the static pass-through fields`() {
    val template = SubpoolAssignerApp.buildVidRankBuilderParamsTemplate(validParams())

    assertThat(template.dataProvider).isEqualTo(DATA_PROVIDER)
    assertThat(template.rawImpressionUpload).isEqualTo(UPLOAD)
    assertThat(template.modelLine).isEqualTo(MODEL_LINE)
    assertThat(template.modelBlobPath).isEqualTo("model/blob")
    assertThat(template.totalShards).isEqualTo(4)
    assertThat(template.labelerInputFieldMappingList)
      .containsExactly(
        LabelerInputFieldMapping.newBuilder()
          .setFieldPath("event_id.id")
          .setScalar(ScalarColumn.newBuilder().setColumn("raw_event_id"))
          .build()
      )
    assertThat(template.eventTemplateFieldMappingMap)
      .containsExactly("banner_ad.viewable", "raw_viewable")
    // Event-template descriptor forwarded so the Phase-1 last-out can stamp it on Phase-2, which
    // requires it to build the labeled output.
    assertThat(template.eventTemplateDescriptorBlobUri).isEqualTo("descriptor/event-template.pb")
    assertThat(template.eventTemplateType)
      .isEqualTo("wfa.measurement.api.v2alpha.event_templates.testing.TestEvent")
    assertThat(template.populationSpecBlobUri).isEqualTo("gs://configs/population-spec.textproto")
    assertThat(template.eventTemplateVersion).isEqualTo(1)
    // Per-impression entity-key columns forwarded for the Phase-2 fan-out.
    assertThat(template.requiredEntityKeyFieldMappingMap).containsExactly("household", "hh_col")
    assertThat(template.optionalEntityKeyFieldMappingMap).containsExactly("creative", "cr_col")
    assertThat(template.allowedPriorSnapshotRawImpressionUploadsList)
      .containsExactly(ALLOWED_PRIOR_UPLOAD)
    // Active-window timestamps forwarded verbatim from Phase-0 for the Phase-2 fan-out.
    assertThat(template.activeStartTime).isEqualTo(timestamp { seconds = 1000 })
    assertThat(template.activeEndTime).isEqualTo(timestamp { seconds = 2000 })
    // Storage params copy the {gcs_project_id, blob_prefix} shape across the two protos.
    assertThat(template.rawImpressionStorageParams.gcsProjectId).isEqualTo("test-project")
    assertThat(template.rawImpressionStorageParams.blobPrefix).isEqualTo("raw-impressions")
    assertThat(template.vidLabeledImpressionsStorageParams.blobPrefix).isEqualTo("vid-labeled")
    assertThat(template.subpoolMapStorageParams.blobPrefix).isEqualTo("subpool-map")
    assertThat(template.vidRankMapStorageParams.blobPrefix).isEqualTo("vid-rank-map")
  }

  @Test
  fun `requires vid_labeled_impressions_storage_params`() {
    val params = validParams().toBuilder().clearVidLabeledImpressionsStorageParams().build()

    assertFailsWith<IllegalArgumentException> {
      SubpoolAssignerApp.buildVidRankBuilderParamsTemplate(params)
    }
  }

  @Test
  fun `requires vid_rank_map_storage_params`() {
    val params = validParams().toBuilder().clearVidRankMapStorageParams().build()

    assertFailsWith<IllegalArgumentException> {
      SubpoolAssignerApp.buildVidRankBuilderParamsTemplate(params)
    }
  }
}
