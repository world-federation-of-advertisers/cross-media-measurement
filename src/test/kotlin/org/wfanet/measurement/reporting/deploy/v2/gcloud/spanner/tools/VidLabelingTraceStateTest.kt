// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator.tools

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Timestamp
import com.google.type.Interval
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceAttributes
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.ListPoolAssignmentJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankIndexBlobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankerJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.ListVidLabelingJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJob
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.vidlabeling.RequestIds
import org.wfanet.measurement.edpaggregator.vidlabeling.WorkItemIds
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.ListWorkItemAttemptsResponse
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.ListWorkItemsResponse
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttempt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub

class VidLabelingTraceStateTest {
  @Test
  fun `final state exposes an entirely missing completed branch`() = runBlocking {
    val metadataStub = mock<ImpressionMetadataServiceCoroutineStub>()
    val dataProvidersStub = mock<DataProvidersCoroutineStub>()
    whenever(metadataStub.listImpressionMetadata(any(), any()))
      .thenReturn(ListImpressionMetadataResponse.getDefaultInstance())
    whenever(dataProvidersStub.getDataProvider(any(), any()))
      .thenReturn(DataProvider.newBuilder().setName("dataProviders/123").build())
    val resolver = GcsKingdomFinalStateResolver(metadataStub, dataProvidersStub) { null }
    val upload = RawImpressionUpload.newBuilder().setName(UPLOAD).setDoneBlobGeneration(7).build()

    val nodes = resolver.resolve(upload, listOf(modelLine("direct", DIRECT_MODEL_LINE)), emptySet())

    assertThat(nodes.filter { it.authoritativeState == "MISSING" }.map { it.stage })
      .containsAtLeast("data_watcher", "data_availability_metadata", "data_availability_publish")
    Unit
  }

  @Test
  fun `final state resolves sidecar done object and Kingdom publication`() = runBlocking {
    val metadataStub = mock<ImpressionMetadataServiceCoroutineStub>()
    val dataProvidersStub = mock<DataProvidersCoroutineStub>()
    val row =
      ImpressionMetadata.newBuilder()
        .setName("dataProviders/123/impressionMetadata/metadata-1")
        .setModelLine(DIRECT_MODEL_LINE)
        .setBlobUri("gs://bucket/model-line/direct/2026-09-01/output.metadata.binpb")
        .setState(ImpressionMetadata.State.ACTIVE)
        .build()
    whenever(metadataStub.listImpressionMetadata(any(), any()))
      .thenReturn(ListImpressionMetadataResponse.newBuilder().addImpressionMetadata(row).build())
    whenever(dataProvidersStub.getDataProvider(any(), any()))
      .thenReturn(
        DataProvider.newBuilder()
          .setName("dataProviders/123")
          .addDataAvailabilityIntervals(
            DataProvider.DataAvailabilityMapEntry.newBuilder()
              .setKey(DIRECT_MODEL_LINE)
              .setValue(
                Interval.newBuilder()
                  .setStartTime(Timestamp.newBuilder().setSeconds(1))
                  .setEndTime(Timestamp.newBuilder().setSeconds(2))
              )
          )
          .build()
      )
    val doneUri = "gs://bucket/model-line/direct/2026-09-01/done"
    val resolver =
      GcsKingdomFinalStateResolver(metadataStub, dataProvidersStub) { uri ->
        when (uri) {
          "gs://raw/input/done" -> StoredObjectMetadata(7)
          doneUri -> StoredObjectMetadata(9)
          else -> StoredObjectMetadata(1)
        }
      }
    val upload =
      RawImpressionUpload.newBuilder()
        .setName(UPLOAD)
        .setDoneBlobUri("gs://raw/input/done")
        .setDoneBlobGeneration(7)
        .build()
    val modelLine = modelLine("direct", DIRECT_MODEL_LINE)

    val nodes =
      resolver.resolve(
        upload,
        listOf(modelLine),
        setOf(VidLabelingTraceAttributes.gcsObjectIdentity(doneUri, 9)),
      )

    assertThat(nodes.map { it.stage })
      .containsAtLeast(
        "label",
        "label_finalize",
        "data_watcher",
        "data_availability_metadata",
        "data_availability_publish",
      )
    assertThat(nodes.none { it.authoritativeState == "MISSING" }).isTrue()
  }

  @Test
  fun `resolve starts at upload and builds both route graphs`() = runBlocking {
    val uploads = mock<RawImpressionUploadServiceCoroutineStub>()
    val rawFiles = mock<RawImpressionUploadFileServiceCoroutineStub>()
    val modelLines = mock<RawImpressionUploadModelLineServiceCoroutineStub>()
    val poolJobs = mock<PoolAssignmentJobServiceCoroutineStub>()
    val rankerJobs = mock<RankerJobServiceCoroutineStub>()
    val labelingJobs = mock<VidLabelingJobServiceCoroutineStub>()
    val rankBlobs = mock<RankIndexBlobServiceCoroutineStub>()
    val workItems = mock<WorkItemsCoroutineStub>()
    val attempts = mock<WorkItemAttemptsCoroutineStub>()
    val metadataStub = mock<ImpressionMetadataServiceCoroutineStub>()
    val dataProvidersStub = mock<DataProvidersCoroutineStub>()
    val upload =
      RawImpressionUpload.newBuilder()
        .setName(UPLOAD)
        .setDoneBlobUri("gs://raw/input/done")
        .setState(RawImpressionUpload.State.COMPLETED)
        .setDoneBlobGeneration(7)
        .build()
    val memoized = modelLine("memo", MEMOIZED_MODEL_LINE)
    val direct = modelLine("direct", DIRECT_MODEL_LINE)
    whenever(uploads.getRawImpressionUpload(any(), any())).thenReturn(upload)
    whenever(rawFiles.listRawImpressionUploadFiles(any(), any()))
      .thenReturn(
        ListRawImpressionUploadFilesResponse.newBuilder()
          .addRawImpressionUploadFiles(
            RawImpressionUploadFile.newBuilder()
              .setName(UPLOAD + "/files/file-1")
              .setBlobUri("gs://raw/input/file-1")
              .setBlobGeneration(3)
          )
          .build()
      )
    whenever(modelLines.listRawImpressionUploadModelLines(any(), any()))
      .thenReturn(
        ListRawImpressionUploadModelLinesResponse.newBuilder()
          .addAllRawImpressionUploadModelLines(listOf(memoized, direct))
          .build()
      )
    whenever(poolJobs.listPoolAssignmentJobs(any(), any())).thenAnswer { invocation ->
      val request =
        invocation.arguments[0]
          as org.wfanet.measurement.edpaggregator.v1alpha.ListPoolAssignmentJobsRequest
      val response = ListPoolAssignmentJobsResponse.newBuilder()
      if (request.filter.cmmsModelLine == MEMOIZED_MODEL_LINE) {
        response.addPoolAssignmentJobs(
          PoolAssignmentJob.newBuilder()
            .setName(UPLOAD + "/poolAssignmentJobs/pool-1")
            .setCmmsModelLine(MEMOIZED_MODEL_LINE)
            .setShardIndex(0)
            .setState(PoolAssignmentJob.State.SUCCEEDED)
        )
      }
      response.build()
    }
    whenever(rankerJobs.listRankerJobs(any(), any()))
      .thenReturn(ListRankerJobsResponse.getDefaultInstance())
    whenever(labelingJobs.listVidLabelingJobs(any(), any())).thenAnswer { invocation ->
      val request =
        invocation.arguments[0]
          as org.wfanet.measurement.edpaggregator.v1alpha.ListVidLabelingJobsRequest
      ListVidLabelingJobsResponse.newBuilder()
        .addVidLabelingJobs(
          VidLabelingJob.newBuilder()
            .setName(
              UPLOAD + "/vidLabelingJobs/" + request.filter.cmmsModelLine.substringAfterLast('/')
            )
            .addCmmsModelLines(request.filter.cmmsModelLine)
            .setState(VidLabelingJob.State.SUCCEEDED)
        )
        .build()
    }
    whenever(rankBlobs.listRankIndexBlobs(any(), any()))
      .thenReturn(ListRankIndexBlobsResponse.getDefaultInstance())
    val directJobName = UPLOAD + "/vidLabelingJobs/direct"
    val originalWorkItemId = WorkItemIds.forVidLabeler(directJobName)
    val originalWorkItemName = "workItems/" + originalWorkItemId
    val retryWorkItemName =
      "workItems/" + RequestIds.forRetriedWorkItem(originalWorkItemId, "failure-1")
    whenever(workItems.listWorkItems(any(), any()))
      .thenReturn(
        ListWorkItemsResponse.newBuilder()
          .addWorkItems(
            WorkItem.newBuilder()
              .setName(originalWorkItemName)
              .setState(WorkItem.State.SUCCEEDED)
              .setGeneration(2)
          )
          .addWorkItems(
            WorkItem.newBuilder()
              .setName(retryWorkItemName)
              .setState(WorkItem.State.RUNNING)
              .setGeneration(1)
          )
          .build()
      )
    whenever(attempts.listWorkItemAttempts(any(), any())).thenAnswer { invocation ->
      val request =
        invocation.arguments[0]
          as
          org.wfanet.measurement.securecomputation.controlplane.v1alpha.ListWorkItemAttemptsRequest
      ListWorkItemAttemptsResponse.newBuilder()
        .addWorkItemAttempts(
          WorkItemAttempt.newBuilder()
            .setName(request.parent + "/workItemAttempts/attempt-1")
            .setState(WorkItemAttempt.State.SUCCEEDED)
            .setAttemptNumber(1)
        )
        .build()
    }
    whenever(metadataStub.listImpressionMetadata(any(), any())).thenAnswer { invocation ->
      val request =
        invocation.arguments[0]
          as org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
      val modelLineName = request.filter.modelLine
      val id = modelLineName.substringAfterLast('/')
      ListImpressionMetadataResponse.newBuilder()
        .addImpressionMetadata(
          ImpressionMetadata.newBuilder()
            .setName("dataProviders/123/impressionMetadata/" + id)
            .setModelLine(modelLineName)
            .setBlobUri("gs://bucket/model-line/" + id + "/2026-09-01/output.metadata.binpb")
            .setState(ImpressionMetadata.State.ACTIVE)
        )
        .build()
    }
    whenever(dataProvidersStub.getDataProvider(any(), any()))
      .thenReturn(
        DataProvider.newBuilder()
          .setName("dataProviders/123")
          .addDataAvailabilityIntervals(availability(MEMOIZED_MODEL_LINE))
          .addDataAvailabilityIntervals(availability(DIRECT_MODEL_LINE))
          .build()
      )
    val finalStateResolver =
      GcsKingdomFinalStateResolver(metadataStub, dataProvidersStub) { uri ->
        when {
          uri == "gs://raw/input/done" -> StoredObjectMetadata(7)
          uri.endsWith("/done") -> StoredObjectMetadata(9)
          else -> StoredObjectMetadata(1)
        }
      }
    val resolver =
      GrpcVidLabelingStateResolver(
        uploads,
        rawFiles,
        modelLines,
        poolJobs,
        rankerJobs,
        labelingJobs,
        rankBlobs,
        workItems,
        attempts,
      )

    val initialGraph = resolver.resolve(UPLOAD)
    val boundaryIdentities =
      setOf(
        VidLabelingTraceAttributes.gcsObjectIdentity(
          "gs://bucket/model-line/memoized/2026-09-01/done",
          9,
        ),
        VidLabelingTraceAttributes.gcsObjectIdentity(
          "gs://bucket/model-line/direct/2026-09-01/done",
          9,
        ),
      )
    val graph =
      initialGraph.copy(
        nodes =
          initialGraph.nodes +
            finalStateResolver.resolve(
              initialGraph.upload,
              initialGraph.modelLines,
              boundaryIdentities,
            )
      )

    assertThat(graph.modelLines.map { it.cmmsModelLine })
      .containsExactly(MEMOIZED_MODEL_LINE, DIRECT_MODEL_LINE)
    assertThat(graph.upload.doneBlobUri).isEqualTo("gs://raw/input/done")
    assertThat(graph.nodes.any { it.identifiers["xmm.edpa.pool_assignment_job.name"] != null })
      .isTrue()
    assertThat(graph.nodes.any { it.id == UPLOAD + "/files/file-1" }).isTrue()
    assertThat(graph.nodes.filter { it.stage == "label" }.mapNotNull { it.modelLine })
      .containsAtLeast(MEMOIZED_MODEL_LINE, DIRECT_MODEL_LINE)
    assertThat(graph.nodes.flatMap { it.identifiers.values })
      .containsAtLeast(originalWorkItemName, retryWorkItemName)
    assertThat(graph.nodes.flatMap { it.identifiers.values }.any { "/workItemAttempts/" in it })
      .isTrue()
    assertThat(graph.nodes.filter { it.stage == "data_availability_publish" }).hasSize(2)
    Unit
  }

  private fun modelLine(id: String, cmmsModelLine: String): RawImpressionUploadModelLine =
    RawImpressionUploadModelLine.newBuilder()
      .setName(UPLOAD + "/rawImpressionUploadModelLines/" + id)
      .setCmmsModelLine(cmmsModelLine)
      .setState(RawImpressionUploadModelLine.State.COMPLETED)
      .setFailureAttemptId("failure-1")
      .build()

  private fun availability(modelLine: String): DataProvider.DataAvailabilityMapEntry =
    DataProvider.DataAvailabilityMapEntry.newBuilder()
      .setKey(modelLine)
      .setValue(
        Interval.newBuilder()
          .setStartTime(Timestamp.newBuilder().setSeconds(1))
          .setEndTime(Timestamp.newBuilder().setSeconds(2))
      )
      .build()

  companion object {
    private const val UPLOAD = "dataProviders/123/rawImpressionUploads/upload-1"
    private const val MEMOIZED_MODEL_LINE =
      "modelProviders/456/modelSuites/suite/modelLines/memoized"
    private const val DIRECT_MODEL_LINE = "modelProviders/456/modelSuites/suite/modelLines/direct"
  }
}
