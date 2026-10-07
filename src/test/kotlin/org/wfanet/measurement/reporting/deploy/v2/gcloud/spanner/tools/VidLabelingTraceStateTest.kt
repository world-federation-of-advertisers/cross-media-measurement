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
import com.google.protobuf.Any
import com.google.protobuf.Timestamp
import com.google.type.Interval
import io.grpc.Status
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelShardKt.modelBlob
import org.wfanet.measurement.api.v2alpha.ModelShardsGrpcKt.ModelShardsCoroutineStub
import org.wfanet.measurement.api.v2alpha.listModelRolloutsResponse
import org.wfanet.measurement.api.v2alpha.listModelShardsResponse
import org.wfanet.measurement.api.v2alpha.modelLine as kingdomModelLine
import org.wfanet.measurement.api.v2alpha.modelRollout
import org.wfanet.measurement.api.v2alpha.modelShard
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceLogging
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncParams
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
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.ListWorkItemsRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.ListWorkItemsResponse
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttempt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub

class VidLabelingTraceStateTest {
  @Test
  fun `route resolver reads memoization from the matching model shard`() = runBlocking {
    val modelLines = mock<ModelLinesCoroutineStub>()
    val modelRollouts = mock<ModelRolloutsCoroutineStub>()
    val modelShards = mock<ModelShardsCoroutineStub>()
    whenever(modelLines.getModelLine(any(), any()))
      .thenReturn(kingdomModelLine { name = MEMOIZED_MODEL_LINE })
    whenever(modelRollouts.listModelRollouts(any(), any()))
      .thenReturn(
        listModelRolloutsResponse {
          this.modelRollouts += modelRollout { modelRelease = MODEL_RELEASE }
        }
      )
    whenever(modelShards.listModelShards(any(), any()))
      .thenReturn(
        listModelShardsResponse {
          this.modelShards += modelShard {
            modelRelease = MODEL_RELEASE
            modelBlob = modelBlob { modelBlobPath = "gs://models/model.bin" }
            memoizedVidAssignmentEnabled = true
          }
        }
      )
    val resolver = KingdomVidLabelingRouteResolver(modelLines, modelRollouts, modelShards)

    assertThat(resolver.isMemoized("dataProviders/123", MEMOIZED_MODEL_LINE)).isTrue()
  }

  @Test
  fun `final state exposes an entirely missing completed branch`() = runBlocking {
    val metadataStub = mock<ImpressionMetadataServiceCoroutineStub>()
    val dataProvidersStub = mock<DataProvidersCoroutineStub>()
    whenever(metadataStub.listImpressionMetadata(any(), any()))
      .thenReturn(ListImpressionMetadataResponse.getDefaultInstance())
    whenever(dataProvidersStub.getDataProvider(any(), any()))
      .thenReturn(DataProvider.newBuilder().setName("dataProviders/123").build())
    val resolver = GcsKingdomFinalStateResolver(metadataStub, dataProvidersStub) { _, _ -> null }
    val upload = RawImpressionUpload.newBuilder().setName(UPLOAD).setDoneBlobGeneration(7).build()

    val nodes =
      resolver.resolve(upload, listOf(modelLine("direct", DIRECT_MODEL_LINE)), emptyList())

    assertThat(nodes.filter { it.authoritativeState == "MISSING" }.map { it.stage })
      .containsAtLeast("data_availability_metadata", "data_availability_publish")
    Unit
  }

  @Test
  fun `failed availability WorkItem makes unavailable downstream stages not applicable`() =
    runBlocking {
      val metadataStub = mock<ImpressionMetadataServiceCoroutineStub>()
      val dataProvidersStub = mock<DataProvidersCoroutineStub>()
      whenever(metadataStub.listImpressionMetadata(any(), any()))
        .thenReturn(ListImpressionMetadataResponse.getDefaultInstance())
      whenever(dataProvidersStub.getDataProvider(any(), any()))
        .thenReturn(DataProvider.newBuilder().setName("dataProviders/123").build())
      val workItem =
        availabilityWorkItem(
          "FAILED",
          listOf(
            VidLabelingAvailabilityAttempt(
              AVAILABILITY_WORK_ITEM + "/workItemAttempts/attempt-3",
              "FAILED",
              3,
              "METADATA_PERSISTENCE",
              "SpannerException",
            )
          ),
        )
      val resolver =
        GcsKingdomFinalStateResolver(metadataStub, dataProvidersStub) { _, generation ->
          StoredObjectMetadata(checkNotNull(generation))
        }
      val upload = RawImpressionUpload.newBuilder().setName(UPLOAD).setDoneBlobGeneration(7).build()

      val nodes =
        resolver.resolve(upload, listOf(modelLine("direct", DIRECT_MODEL_LINE)), listOf(workItem))

      assertThat(nodes.single { it.id == workItem.name + ":done_object" }.authoritativeState)
        .isEqualTo("PUBLISHED")
      assertThat(
          nodes
            .filter { it.stage in setOf("data_availability_metadata", "data_availability_publish") }
            .all { it.disposition == ExpectedNodeDisposition.NOT_APPLICABLE }
        )
        .isTrue()
    }

  @Test
  fun `final state reads exact raw done generation and resolves publication`() = runBlocking {
    val metadataStub = mock<ImpressionMetadataServiceCoroutineStub>()
    val dataProvidersStub = mock<DataProvidersCoroutineStub>()
    val row =
      ImpressionMetadata.newBuilder()
        .setName("dataProviders/123/impressionMetadata/metadata-1")
        .setModelLine(DIRECT_MODEL_LINE)
        .setBlobUri("gs://bucket/model-line/direct/2026-09-01/output.metadata.binpb")
        .setRawImpressionUpload(UPLOAD)
        .setOutputDoneBlobGeneration(9)
        .setInterval(
          Interval.newBuilder()
            .setStartTime(Timestamp.newBuilder().setSeconds(1))
            .setEndTime(Timestamp.newBuilder().setSeconds(2))
        )
        .setState(ImpressionMetadata.State.ACTIVE)
        .build()
    whenever(metadataStub.listImpressionMetadata(any(), any())).thenAnswer { invocation ->
      val request =
        invocation.arguments[0]
          as org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
      assertThat(request.filter.rawImpressionUpload).isEqualTo(UPLOAD)
      ListImpressionMetadataResponse.newBuilder().addImpressionMetadata(row).build()
    }
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
    val objectReads = mutableListOf<Pair<String, Long?>>()
    val resolver =
      GcsKingdomFinalStateResolver(metadataStub, dataProvidersStub) { uri, generation ->
        objectReads += uri to generation
        when (uri) {
          "gs://raw/input/done" ->
            if (generation == 7L) StoredObjectMetadata(7) else StoredObjectMetadata(8)
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
    val workItem = availabilityWorkItem("SUCCEEDED", doneBlobUri = doneUri)

    val nodes = resolver.resolve(upload, listOf(modelLine), listOf(workItem))

    assertThat(nodes.map { it.stage })
      .containsAtLeast("label", "data_availability_metadata", "data_availability_publish")
    assertThat(nodes.count { it.stage == VidLabelingTraceLogging.LABEL_OUTPUT_STAGE }).isEqualTo(2)
    assertThat(nodes.none { it.authoritativeState == "MISSING" }).isTrue()
    assertThat(objectReads).contains("gs://raw/input/done" to 7L)
    assertThat(objectReads).doesNotContain("gs://raw/input/done" to null)
    assertThat(objectReads).contains(doneUri to 9L)
  }

  @Test
  fun `final state requires Kingdom availability to cover this upload interval`() = runBlocking {
    val metadataStub = mock<ImpressionMetadataServiceCoroutineStub>()
    val dataProvidersStub = mock<DataProvidersCoroutineStub>()
    whenever(metadataStub.listImpressionMetadata(any(), any()))
      .thenReturn(
        ListImpressionMetadataResponse.newBuilder()
          .addImpressionMetadata(
            ImpressionMetadata.newBuilder()
              .setName("dataProviders/123/impressionMetadata/metadata-new")
              .setModelLine(DIRECT_MODEL_LINE)
              .setRawImpressionUpload(UPLOAD)
              .setBlobUri("gs://bucket/model-line/direct/2026-09-02/output.metadata.binpb")
              .setInterval(
                Interval.newBuilder()
                  .setStartTime(Timestamp.newBuilder().setSeconds(10))
                  .setEndTime(Timestamp.newBuilder().setSeconds(20))
              )
              .setState(ImpressionMetadata.State.ACTIVE)
          )
          .build()
      )
    whenever(dataProvidersStub.getDataProvider(any(), any()))
      .thenReturn(
        DataProvider.newBuilder()
          .setName("dataProviders/123")
          .addDataAvailabilityIntervals(availability(DIRECT_MODEL_LINE))
          .build()
      )
    val resolver =
      GcsKingdomFinalStateResolver(metadataStub, dataProvidersStub) { _, generation ->
        StoredObjectMetadata(generation ?: 9L)
      }
    val upload =
      RawImpressionUpload.newBuilder()
        .setName(UPLOAD)
        .setDoneBlobUri("gs://raw/input/done")
        .setDoneBlobGeneration(7)
        .build()

    val nodes =
      resolver.resolve(upload, listOf(modelLine("direct", DIRECT_MODEL_LINE)), emptyList())

    assertThat(nodes.single { it.stage == "data_availability_publish" }.authoritativeState)
      .isEqualTo("MISSING")
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
    whenever(workItems.getWorkItem(any(), any())).thenAnswer { invocation ->
      val request =
        invocation.arguments[0]
          as org.wfanet.measurement.securecomputation.controlplane.v1alpha.GetWorkItemRequest
      when (request.name) {
        originalWorkItemName ->
          WorkItem.newBuilder()
            .setName(originalWorkItemName)
            .setState(WorkItem.State.SUCCEEDED)
            .setGeneration(2)
            .build()
        retryWorkItemName ->
          WorkItem.newBuilder()
            .setName(retryWorkItemName)
            .setState(WorkItem.State.RUNNING)
            .setGeneration(1)
            .build()
        AVAILABILITY_WORK_ITEM -> availabilityWorkItemProto(AVAILABILITY_WORK_ITEM, "SUCCEEDED")
        else -> throw Status.NOT_FOUND.asException()
      }
    }
    whenever(workItems.listWorkItems(any(), any())).thenAnswer { invocation ->
      val request = invocation.arguments[0] as ListWorkItemsRequest
      if (request.pageToken.isEmpty()) {
        ListWorkItemsResponse.newBuilder()
          .addWorkItems(availabilityWorkItemProto(AVAILABILITY_WORK_ITEM, "SUCCEEDED"))
          .setNextPageToken("next-page")
          .build()
      } else {
        ListWorkItemsResponse.newBuilder()
          .addWorkItems(
            availabilityWorkItemProto(
              "workItems/other-upload",
              "FAILED",
              "dataProviders/123/rawImpressionUploads/other",
            )
          )
          .build()
      }
    }
    whenever(attempts.listWorkItemAttempts(any(), any())).thenAnswer { invocation ->
      val request =
        invocation.arguments[0]
          as
          org.wfanet.measurement.securecomputation.controlplane.v1alpha.ListWorkItemAttemptsRequest
      val response = ListWorkItemAttemptsResponse.newBuilder()
      if (request.parent == AVAILABILITY_WORK_ITEM) {
        response
          .addWorkItemAttempts(
            WorkItemAttempt.newBuilder()
              .setName(request.parent + "/workItemAttempts/attempt-1")
              .setState(WorkItemAttempt.State.FAILED)
              .setAttemptNumber(1)
              .setErrorMessage("SYNCHRONIZATION:UnavailableException")
          )
          .addWorkItemAttempts(
            WorkItemAttempt.newBuilder()
              .setName(request.parent + "/workItemAttempts/attempt-2")
              .setState(WorkItemAttempt.State.SUCCEEDED)
              .setAttemptNumber(2)
          )
      } else {
        response.addWorkItemAttempts(
          WorkItemAttempt.newBuilder()
            .setName(request.parent + "/workItemAttempts/attempt-1")
            .setState(WorkItemAttempt.State.SUCCEEDED)
            .setAttemptNumber(1)
        )
      }
      response.build()
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
            .setRawImpressionUpload(UPLOAD)
            .setOutputDoneBlobGeneration(9)
            .setInterval(
              Interval.newBuilder()
                .setStartTime(Timestamp.newBuilder().setSeconds(1))
                .setEndTime(Timestamp.newBuilder().setSeconds(2))
            )
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
      GcsKingdomFinalStateResolver(metadataStub, dataProvidersStub) { uri, generation ->
        when {
          uri == "gs://raw/input/done" && generation == 7L -> StoredObjectMetadata(7)
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
        VidLabelingRouteResolver { _, modelLineName -> modelLineName == MEMOIZED_MODEL_LINE },
      )

    val initialGraph = resolver.resolve(UPLOAD)
    val availabilityWorkItems = resolver.resolveAvailabilityWorkItems(UPLOAD)
    val graphWithAvailability = initialGraph.withAvailabilityWorkItems(availabilityWorkItems)
    val graph =
      graphWithAvailability.copy(
        nodes =
          graphWithAvailability.nodes +
            finalStateResolver.resolve(
              graphWithAvailability.upload,
              graphWithAvailability.modelLines,
              availabilityWorkItems,
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
    assertThat(graph.availabilityWorkItems).hasSize(1)
    assertThat(graph.availabilityWorkItems.single().attemptCount).isEqualTo(2)
    assertThat(graph.availabilityWorkItems.single().failureStage).isEqualTo("SYNCHRONIZATION")
    assertThat(graph.availabilityWorkItems.single().rawImpressionUploadModelLine)
      .isEqualTo(UPLOAD + "/rawImpressionUploadModelLines/direct")
    assertThat(
        graph.nodes
          .filter { it.stage.startsWith("availability_work_item") }
          .map { it.authoritativeState }
      )
      .containsAtLeast("CREATED", "SUCCEEDED")
    verifyBlocking(workItems, times(2)) { listWorkItems(any(), any()) }
    Unit
  }

  @Test
  fun `resolve keeps memoized route before phase zero creates children`() = runBlocking {
    val uploads = mock<RawImpressionUploadServiceCoroutineStub>()
    val rawFiles = mock<RawImpressionUploadFileServiceCoroutineStub>()
    val uploadModelLines = mock<RawImpressionUploadModelLineServiceCoroutineStub>()
    val poolJobs = mock<PoolAssignmentJobServiceCoroutineStub>()
    val rankerJobs = mock<RankerJobServiceCoroutineStub>()
    val labelingJobs = mock<VidLabelingJobServiceCoroutineStub>()
    val rankBlobs = mock<RankIndexBlobServiceCoroutineStub>()
    val workItems = mock<WorkItemsCoroutineStub>()
    val attempts = mock<WorkItemAttemptsCoroutineStub>()
    whenever(uploads.getRawImpressionUpload(any(), any()))
      .thenReturn(RawImpressionUpload.newBuilder().setName(UPLOAD).build())
    whenever(rawFiles.listRawImpressionUploadFiles(any(), any()))
      .thenReturn(ListRawImpressionUploadFilesResponse.getDefaultInstance())
    whenever(uploadModelLines.listRawImpressionUploadModelLines(any(), any()))
      .thenReturn(
        ListRawImpressionUploadModelLinesResponse.newBuilder()
          .addRawImpressionUploadModelLines(
            RawImpressionUploadModelLine.newBuilder()
              .setName(UPLOAD + "/rawImpressionUploadModelLines/memo")
              .setCmmsModelLine(MEMOIZED_MODEL_LINE)
              .setState(RawImpressionUploadModelLine.State.CREATED)
          )
          .build()
      )
    whenever(poolJobs.listPoolAssignmentJobs(any(), any()))
      .thenReturn(ListPoolAssignmentJobsResponse.getDefaultInstance())
    whenever(rankerJobs.listRankerJobs(any(), any()))
      .thenReturn(ListRankerJobsResponse.getDefaultInstance())
    whenever(labelingJobs.listVidLabelingJobs(any(), any()))
      .thenReturn(ListVidLabelingJobsResponse.getDefaultInstance())
    whenever(rankBlobs.listRankIndexBlobs(any(), any()))
      .thenReturn(ListRankIndexBlobsResponse.getDefaultInstance())
    val resolver =
      GrpcVidLabelingStateResolver(
        uploads,
        rawFiles,
        uploadModelLines,
        poolJobs,
        rankerJobs,
        labelingJobs,
        rankBlobs,
        workItems,
        attempts,
        VidLabelingRouteResolver { _, _ -> true },
      )

    val graph = resolver.resolve(UPLOAD)

    assertThat(
        graph.nodes
          .single { it.id.endsWith("/rawImpressionUploadModelLines/memo") }
          .identifiers["xmm.edpa.label.route"]
      )
      .isEqualTo("memoized")
  }

  private fun availabilityWorkItem(
    state: String,
    attempts: List<VidLabelingAvailabilityAttempt> = emptyList(),
    doneBlobUri: String = "gs://bucket/model-line/direct/2026-09-01/done",
  ): VidLabelingAvailabilityWorkItem =
    VidLabelingAvailabilityWorkItem(
      AVAILABILITY_WORK_ITEM,
      DIRECT_MODEL_LINE,
      state,
      1,
      attempts,
      "2026-09-01",
      doneBlobUri,
      "done-path-hash",
      9,
    )

  private fun availabilityWorkItemProto(
    name: String,
    state: String,
    rawImpressionUpload: String = UPLOAD,
  ): WorkItem {
    val appParams =
      DataAvailabilitySyncParams.newBuilder()
        .setDataProvider("dataProviders/123")
        .setTriggeringRawImpressionUpload(rawImpressionUpload)
        .setRawImpressionUploadModelLine(UPLOAD + "/rawImpressionUploadModelLines/direct")
        .setModelLine(DIRECT_MODEL_LINE)
        .setEventDate(com.google.type.Date.newBuilder().setYear(2026).setMonth(9).setDay(1))
        .build()
    val dataPath =
      WorkItem.WorkItemParams.DataPathParams.newBuilder()
        .setDataPath("gs://bucket/model-line/direct/2026-09-01/done")
        .setGeneration(9)
        .setEventType(WorkItem.WorkItemParams.DataPathParams.StorageEventType.FINALIZED)
        .build()
    val params =
      WorkItem.WorkItemParams.newBuilder()
        .setAppParams(Any.pack(appParams))
        .setDataPathParams(dataPath)
        .build()
    return WorkItem.newBuilder()
      .setName(name)
      .setQueue("data-availability-sync-queue")
      .setWorkItemParams(Any.pack(params))
      .setState(WorkItem.State.valueOf(state))
      .setGeneration(1)
      .build()
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
    private const val MODEL_RELEASE = "modelProviders/456/modelSuites/suite/modelReleases/release-1"
    private const val AVAILABILITY_WORK_ITEM = "workItems/das-01234567"
  }
}
