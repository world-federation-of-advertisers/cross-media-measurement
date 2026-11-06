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
import com.google.protobuf.util.Timestamps
import com.google.type.Interval
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
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

    val resolution = resolver.resolve("dataProviders/123", listOf(MEMOIZED_MODEL_LINE))

    assertThat(resolution.routes).containsEntry(MEMOIZED_MODEL_LINE, true)
    assertThat(resolution.counts["model_shard_pages"]).isEqualTo(1)
  }

  @OptIn(ExperimentalCoroutinesApi::class)
  @Test
  fun `route resolver bounds parallel model line reads`() = runTest {
    val modelLineNames =
      List(8) { index -> "modelProviders/456/modelSuites/suite/modelLines/line-$index" }
    val releases =
      modelLineNames.associateWith { modelLineName ->
        "modelProviders/456/modelSuites/suite/modelReleases/" +
          modelLineName.substringAfterLast('/')
      }
    val release = CompletableDeferred<Unit>()
    val inFlight = AtomicInteger()
    val peakInFlight = AtomicInteger()
    val resolver =
      KingdomVidLabelingRouteResolver.fromFunctions(
        maxConcurrency = 4,
        getModelLine = { request ->
          val current = inFlight.incrementAndGet()
          peakInFlight.updateAndGet { previous -> maxOf(previous, current) }
          try {
            release.await()
            kingdomModelLine { name = request.name }
          } finally {
            inFlight.decrementAndGet()
          }
        },
        listModelRollouts = { request ->
          listModelRolloutsResponse {
            modelRollouts += modelRollout { modelRelease = releases.getValue(request.parent) }
          }
        },
        listModelShards = {
          listModelShardsResponse {
            modelShards +=
              releases.values.map { modelReleaseName ->
                modelShard {
                  modelRelease = modelReleaseName
                  modelBlob = modelBlob { modelBlobPath = "gs://models/model.bin" }
                  memoizedVidAssignmentEnabled = true
                }
              }
          }
        },
      )

    val result = async { resolver.resolve("dataProviders/123", modelLineNames.reversed()) }
    runCurrent()

    assertThat(peakInFlight.get()).isEqualTo(4)
    assertThat(result.isCompleted).isFalse()
    release.complete(Unit)
    assertThat(result.await().routes.keys.toList()).isEqualTo(modelLineNames.sorted())
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
      resolver.resolve(upload, listOf(modelLine("direct", DIRECT_MODEL_LINE)), emptyList()).nodes

    assertThat(nodes.filter { it.authoritativeState == "MISSING" }.map { it.stage })
      .containsAtLeast("upload_registration", "data_availability_publish")
    Unit
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

    val resolution = resolver.resolve(upload, listOf(modelLine), emptyList())
    val nodes = resolution.nodes

    assertThat(nodes.map { it.stage })
      .containsAtLeast("label", "data_availability_metadata", "data_availability_publish")
    assertThat(nodes.count { it.stage == VidLabelingTraceLogging.LABEL_OUTPUT_STAGE }).isEqualTo(2)
    assertThat(nodes.none { it.authoritativeState == "MISSING" }).isTrue()
    assertThat(resolution.counts["gcs_reads"]).isEqualTo(3)
    assertThat(objectReads).contains("gs://raw/input/done" to 7L)
    assertThat(objectReads).doesNotContain("gs://raw/input/done" to null)
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
      resolver.resolve(upload, listOf(modelLine("direct", DIRECT_MODEL_LINE)), emptyList()).nodes

    val availabilityNode = nodes.single { it.stage == "data_availability_publish" }
    assertThat(availabilityNode.authoritativeState).isEqualTo("MISSING")
    assertThat(availabilityNode.identifiers["xmm.edpa.availability.interval_start"])
      .isEqualTo(Timestamps.toString(Timestamp.newBuilder().setSeconds(10).build()))
    assertThat(availabilityNode.identifiers["xmm.edpa.availability.interval_end"])
      .isEqualTo(Timestamps.toString(Timestamp.newBuilder().setSeconds(20).build()))
  }

  @Test
  fun `final state correlates covered rows with the broad Kingdom interval`() = runBlocking {
    val metadataStub = mock<ImpressionMetadataServiceCoroutineStub>()
    val dataProvidersStub = mock<DataProvidersCoroutineStub>()
    val firstRow =
      ImpressionMetadata.newBuilder()
        .setName("dataProviders/123/impressionMetadata/healed-replacement")
        .setModelLine(DIRECT_MODEL_LINE)
        .setRawImpressionUpload(UPLOAD)
        .setBlobUri("gs://bucket/model-line/direct/2026-09-02/output.metadata.binpb")
        .setInterval(
          Interval.newBuilder()
            .setStartTime(Timestamp.newBuilder().setSeconds(10))
            .setEndTime(Timestamp.newBuilder().setSeconds(20))
        )
        .setState(ImpressionMetadata.State.ACTIVE)
        .build()
    val secondRow =
      firstRow
        .toBuilder()
        .setName("dataProviders/123/impressionMetadata/deferred-upload")
        .setBlobUri("gs://bucket/model-line/direct/2026-09-03/output.metadata.binpb")
        .setInterval(
          Interval.newBuilder()
            .setStartTime(Timestamp.newBuilder().setSeconds(20))
            .setEndTime(Timestamp.newBuilder().setSeconds(30))
        )
        .build()
    whenever(metadataStub.listImpressionMetadata(any(), any()))
      .thenReturn(
        ListImpressionMetadataResponse.newBuilder()
          .addAllImpressionMetadata(listOf(firstRow, secondRow))
          .build()
      )
    val broadInterval =
      Interval.newBuilder()
        .setStartTime(Timestamp.newBuilder().setSeconds(1))
        .setEndTime(Timestamp.newBuilder().setSeconds(40))
        .build()
    whenever(dataProvidersStub.getDataProvider(any(), any()))
      .thenReturn(
        DataProvider.newBuilder()
          .setName("dataProviders/123")
          .addDataAvailabilityIntervals(
            DataProvider.DataAvailabilityMapEntry.newBuilder()
              .setKey(DIRECT_MODEL_LINE)
              .setValue(broadInterval)
          )
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
      resolver.resolve(upload, listOf(modelLine("direct", DIRECT_MODEL_LINE)), emptyList()).nodes
    val availabilityNodes = nodes.filter { it.stage == "data_availability_publish" }

    assertThat(availabilityNodes).hasSize(2)
    assertThat(availabilityNodes.map { it.authoritativeState })
      .containsExactly("PUBLISHED", "PUBLISHED")
    assertThat(
        availabilityNodes.map { it.identifiers["xmm.edpa.availability.interval_start"] }.toSet()
      )
      .containsExactly(Timestamps.toString(broadInterval.startTime))
    assertThat(
        availabilityNodes.map { it.identifiers["xmm.edpa.availability.interval_end"] }.toSet()
      )
      .containsExactly(Timestamps.toString(broadInterval.endTime))
    Unit
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
      )
    val workItemResolver = GrpcVidLabelingWorkItemResolver(workItems, attempts)
    val routeResolver = VidLabelingRouteResolver { _, modelLineNames ->
      VidLabelingRouteResolution(
        modelLineNames.associateWith { it == MEMOIZED_MODEL_LINE },
        emptyMap(),
      )
    }

    val initialGraph = resolver.resolve(UPLOAD)
    val workItemResolution =
      workItemResolver.resolve(UPLOAD, initialGraph.expectedWorkItems, pageLimit = 10)
    val routeResolution =
      routeResolver.resolve("dataProviders/123", initialGraph.modelLines.map { it.cmmsModelLine })
    val finalState =
      finalStateResolver.resolve(initialGraph.upload, initialGraph.modelLines, emptyList())
    val graph =
      initialGraph
        .copy(nodes = initialGraph.nodes + workItemResolution.nodes + finalState.nodes)
        .withRoutes(routeResolution)

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
    val workItemNodes = graph.nodes.filter { "xmm.work_item.name" in it.identifiers }
    assertThat(workItemNodes).isNotEmpty()
    assertThat(workItemNodes.all { "xmm.model_line.name" !in it.identifiers }).isTrue()
    assertThat(graph.nodes.filter { it.stage == "data_availability_publish" }).hasSize(2)
    assertThat(workItemResolution.counts["pages_fetched"]).isEqualTo(1)
    assertThat(workItemResolution.counts["work_items_matched"]).isEqualTo(2)
    verifyBlocking(workItems, times(1)) { listWorkItems(any(), any()) }
    verifyBlocking(workItems, never()) { getWorkItem(any(), any()) }
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
      )

    val graph =
      resolver
        .resolve(UPLOAD)
        .withRoutes(VidLabelingRouteResolution(mapOf(MEMOIZED_MODEL_LINE to true), emptyMap()))

    assertThat(
        graph.nodes
          .single { it.id.endsWith("/rawImpressionUploadModelLines/memo") }
          .identifiers["xmm.edpa.label.route"]
      )
      .isEqualTo("memoized")
  }

  @Test
  fun `WorkItem resolver scans once for 32 pool 3 ranker and 2 labeling jobs`() = runBlocking {
    val workItems = mock<WorkItemsCoroutineStub>()
    val attempts = mock<WorkItemAttemptsCoroutineStub>()
    val expectedGroups = buildList {
      repeat(32) { add(expectedWorkItemGroup("pool-$it", "pool_assignment")) }
      repeat(3) { add(expectedWorkItemGroup("rank-$it", "rank")) }
      repeat(2) { add(expectedWorkItemGroup("label-$it", "label")) }
    }
    whenever(workItems.listWorkItems(any(), any()))
      .thenReturn(
        ListWorkItemsResponse.newBuilder()
          .addAllWorkItems(
            expectedGroups.reversed().map { group ->
              WorkItem.newBuilder()
                .setName(group.originalName)
                .setState(WorkItem.State.SUCCEEDED)
                .setGeneration(2)
                .build()
            }
          )
          .addWorkItems(WorkItem.newBuilder().setName("workItems/unrelated"))
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
            .setName(request.parent + "/workItemAttempts/2")
            .setAttemptNumber(2)
            .setState(WorkItemAttempt.State.SUCCEEDED)
            .setWorkItemGeneration(2)
        )
        .addWorkItemAttempts(
          WorkItemAttempt.newBuilder()
            .setName(request.parent + "/workItemAttempts/1")
            .setAttemptNumber(1)
            .setState(WorkItemAttempt.State.FAILED)
            .setWorkItemGeneration(1)
        )
        .build()
    }
    val resolver = GrpcVidLabelingWorkItemResolver(workItems, attempts)

    val result = resolver.resolve(UPLOAD, expectedGroups, pageLimit = 10)

    assertThat(result.truncated).isFalse()
    assertThat(result.counts["pages_fetched"]).isEqualTo(1)
    assertThat(result.counts["rows_retained"]).isEqualTo(37)
    assertThat(result.counts["expected_work_item_names"]).isEqualTo(148)
    assertThat(result.counts["work_items_matched"]).isEqualTo(37)
    assertThat(result.nodes.count { it.stage == "pool_assignment" }).isEqualTo(32 * 3)
    assertThat(result.nodes.count { it.stage == "rank" }).isEqualTo(3 * 3)
    assertThat(result.nodes.count { it.stage == "label" }).isEqualTo(2 * 3)
    assertThat(result.workItems.map { it.workItem.name })
      .isEqualTo(result.workItems.map { it.workItem.name }.sorted())
    assertThat(result.workItems.flatMap { it.attempts }.map { it.attemptNumber }.toSet())
      .containsExactly(1, 2)
    val attemptNodes = result.nodes.filter { "xmm.work_item_attempt.name" in it.identifiers }
    assertThat(
        attemptNodes
          .filter { it.id.endsWith("/workItemAttempts/1") }
          .all { it.identifiers["xmm.work_item.generation"] == "1" }
      )
      .isTrue()
    assertThat(
        attemptNodes
          .filter { it.id.endsWith("/workItemAttempts/2") }
          .all { it.identifiers["xmm.work_item.generation"] == "2" }
      )
      .isTrue()
    verifyBlocking(workItems, times(1)) { listWorkItems(any(), any()) }
    verifyBlocking(workItems, never()) { getWorkItem(any(), any()) }
    verifyBlocking(attempts, times(37)) { listWorkItemAttempts(any(), any()) }
  }

  @Test
  fun `WorkItem resolver retains availability tasks in the shared inventory scan`() = runBlocking {
    val workItems = mock<WorkItemsCoroutineStub>()
    val attempts = mock<WorkItemAttemptsCoroutineStub>()
    val availabilityWorkItems =
      List(3) { index -> availabilityWorkItemProto("workItems/availability-$index") }
    whenever(workItems.listWorkItems(any(), any()))
      .thenReturn(
        ListWorkItemsResponse.newBuilder()
          .addAllWorkItems(
            availabilityWorkItems + WorkItem.newBuilder().setName("workItems/other").build()
          )
          .build()
      )
    whenever(attempts.listWorkItemAttempts(any(), any()))
      .thenReturn(ListWorkItemAttemptsResponse.getDefaultInstance())
    val resolver =
      GrpcVidLabelingWorkItemResolver(
        workItems,
        attempts,
        RelevantWorkItemSelector { rawImpressionUpload, workItem ->
          workItem.isAvailabilityWorkItemFor(rawImpressionUpload)
        },
        availabilityLookupEnabled = true,
      )

    val result = resolver.resolve(UPLOAD, emptyList(), pageLimit = 10)
    val parsed = result.workItems.mapNotNull { it.toAvailabilityWorkItem(UPLOAD) }

    assertThat(result.availabilityLookupEnabled).isTrue()
    assertThat(result.counts["pages_fetched"]).isEqualTo(1)
    assertThat(result.counts["rows_retained"]).isEqualTo(3)
    assertThat(parsed).hasSize(3)
    assertThat(parsed.map { it.doneBlobGeneration }.distinct()).containsExactly(9L)
    verifyBlocking(workItems, times(1)) { listWorkItems(any(), any()) }
    verifyBlocking(attempts, times(3)) { listWorkItemAttempts(any(), any()) }
  }

  @OptIn(ExperimentalCoroutinesApi::class)
  @Test
  fun `WorkItem resolver bounds parallel attempt reads`() = runTest {
    val expectedGroups = List(16) { expectedWorkItemGroup("work-$it", "label") }
    val inventory =
      ListWorkItemsResponse.newBuilder()
        .addAllWorkItems(
          expectedGroups.reversed().map { group ->
            WorkItem.newBuilder()
              .setName(group.originalName)
              .setState(WorkItem.State.SUCCEEDED)
              .setGeneration(1)
              .build()
          }
        )
        .build()
    val release = CompletableDeferred<Unit>()
    val inFlight = AtomicInteger()
    val peakInFlight = AtomicInteger()
    val resolver =
      GrpcVidLabelingWorkItemResolver.fromFunctions(
        maxAttemptConcurrency = 8,
        listWorkItems = { inventory },
        listWorkItemAttempts = {
          val current = inFlight.incrementAndGet()
          peakInFlight.updateAndGet { previous -> maxOf(previous, current) }
          try {
            release.await()
            ListWorkItemAttemptsResponse.getDefaultInstance()
          } finally {
            inFlight.decrementAndGet()
          }
        },
      )

    val result = async { resolver.resolve(UPLOAD, expectedGroups, pageLimit = 1) }
    runCurrent()

    assertThat(peakInFlight.get()).isEqualTo(8)
    assertThat(result.isCompleted).isFalse()
    release.complete(Unit)
    assertThat(result.await().workItems.map { it.workItem.name })
      .isEqualTo(expectedGroups.map { it.originalName }.sorted())
  }

  @OptIn(ExperimentalCoroutinesApi::class)
  @Test
  fun `final state resolver bounds parallel GCS reads`() = runTest {
    val metadata = mock<ImpressionMetadataServiceCoroutineStub>()
    val dataProviders = mock<DataProvidersCoroutineStub>()
    whenever(metadata.listImpressionMetadata(any(), any())).thenAnswer { invocation ->
      val request =
        invocation.arguments[0]
          as org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
      val id = request.filter.modelLine.substringAfterLast('/')
      ListImpressionMetadataResponse.newBuilder()
        .addImpressionMetadata(
          ImpressionMetadata.newBuilder()
            .setName("dataProviders/123/impressionMetadata/$id")
            .setModelLine(request.filter.modelLine)
            .setRawImpressionUpload(UPLOAD)
            .setBlobUri("gs://bucket/model-line/$id/date/output.metadata.binpb")
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
    whenever(dataProviders.getDataProvider(any(), any()))
      .thenReturn(DataProvider.newBuilder().setName("dataProviders/123").build())
    val release = CompletableDeferred<Unit>()
    val inFlight = AtomicInteger()
    val peakInFlight = AtomicInteger()
    val resolver =
      GcsKingdomFinalStateResolver(
        metadata,
        dataProviders,
        modelLineConcurrency = 4,
        gcsConcurrency = 8,
      ) { _, generation ->
        val current = inFlight.incrementAndGet()
        peakInFlight.updateAndGet { previous -> maxOf(previous, current) }
        try {
          release.await()
          StoredObjectMetadata(generation ?: 1)
        } finally {
          inFlight.decrementAndGet()
        }
      }
    val upload =
      RawImpressionUpload.newBuilder()
        .setName(UPLOAD)
        .setDoneBlobUri("gs://raw/done")
        .setDoneBlobGeneration(7)
        .build()
    val modelLines =
      List(4) { index ->
        modelLine("line-$index", "modelProviders/456/modelSuites/suite/modelLines/line-$index")
      }

    val result = async { resolver.resolve(upload, modelLines.reversed(), emptyList()) }
    runCurrent()

    assertThat(peakInFlight.get()).isEqualTo(8)
    assertThat(result.isCompleted).isFalse()
    release.complete(Unit)
    val resolution = result.await()
    assertThat(resolution.counts["gcs_reads"]).isEqualTo(9)
    assertThat(resolution.nodes.mapNotNull { it.modelLine }.distinct())
      .isEqualTo(modelLines.map { it.cmmsModelLine }.sorted())
  }

  @Test
  fun `WorkItem resolver reports unseen names unresolved when page limit is reached`() =
    runBlocking {
      val workItems = mock<WorkItemsCoroutineStub>()
      val attempts = mock<WorkItemAttemptsCoroutineStub>()
      whenever(workItems.listWorkItems(any(), any()))
        .thenReturn(
          ListWorkItemsResponse.newBuilder()
            .addWorkItems(WorkItem.newBuilder().setName("workItems/unrelated"))
            .setNextPageToken("next")
            .build()
        )
      val expected = expectedWorkItemGroup("expected", "label")
      val resolver = GrpcVidLabelingWorkItemResolver(workItems, attempts)

      val result = resolver.resolve(UPLOAD, listOf(expected), pageLimit = 1)

      assertThat(result.truncated).isTrue()
      assertThat(result.nodes.single().authoritativeState).isEqualTo("UNRESOLVED")
      assertThat(result.nodes.single().disposition).isEqualTo(ExpectedNodeDisposition.UNRESOLVED)
      verifyBlocking(workItems, times(1)) { listWorkItems(any(), any()) }
      verifyBlocking(attempts, never()) { listWorkItemAttempts(any(), any()) }
    }

  @Test
  fun `WorkItem resolver fails immediately when a page token repeats`() = runBlocking {
    val requests = AtomicInteger()
    val resolver =
      GrpcVidLabelingWorkItemResolver.fromFunctions(
        listWorkItems = {
          requests.incrementAndGet()
          ListWorkItemsResponse.newBuilder().setNextPageToken("repeated-token").build()
        },
        listWorkItemAttempts = { error("attempts must not be listed") },
      )

    val exception =
      assertFailsWith<IllegalStateException> {
        resolver.resolve(UPLOAD, emptyList(), pageLimit = 100)
      }

    assertThat(exception).hasMessageThat().contains("repeated page token")
    assertThat(requests.get()).isEqualTo(2)
  }

  private fun expectedWorkItemGroup(id: String, stage: String): ExpectedWorkItemGroup {
    val originalName = "workItems/$id"
    return ExpectedWorkItemGroup(
      originalName,
      MEMOIZED_MODEL_LINE,
      stage,
      listOf(
        originalName,
        "workItems/$id-recovery-1",
        "workItems/$id-recovery-2",
        "workItems/$id-recovery-3",
      ),
    )
  }

  private fun availabilityWorkItemProto(name: String): WorkItem {
    val appParams =
      DataAvailabilitySyncParams.newBuilder()
        .setDataProvider("dataProviders/123")
        .setTriggeringRawImpressionUpload(UPLOAD)
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
      .setState(WorkItem.State.SUCCEEDED)
      .setGeneration(1)
      .setCreateTime(Timestamp.newBuilder().setSeconds(123))
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
  }
}
