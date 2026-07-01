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

package org.wfanet.measurement.edpaggregator.vidrankbuilder

import com.google.common.truth.Truth.assertThat
import io.grpc.Status
import io.grpc.StatusException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.doReturnConsecutively
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateVidLabelingJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankerJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateVidLabelingJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.listRankerJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listVidLabelingJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.markRankerJobSucceededResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelingJob
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem

private const val UPLOAD = "dataProviders/dp/rawImpressionUploads/up1"
private const val MODEL_LINE = "modelProviders/mp/modelSuites/ms/modelLines/ml1"
private const val RANKER_JOB = "dataProviders/dp/rawImpressionUploads/up1/rankerJobs/rj7"
private const val PARENT_NAME =
  "dataProviders/dp/rawImpressionUploads/up1/rawImpressionUploadModelLines/rl1"
private const val QUEUE = "queues/vid-labeler"

private val VID_LABELER_TEMPLATE = vidLabelerParams {
  dataProvider = "dataProviders/dp"
  modelLines += MODEL_LINE
  modelBlobPaths.put(MODEL_LINE, "model/blob")
  memoizedParams = VidLabelerParamsKt.memoizedParams {}
}

@RunWith(JUnit4::class)
class VidRankBuilderTest {
  private val subpoolMapBlobUris = mapOf(7L to "merged/subpool-7")
  private val subpoolRankedSizes = mapOf(7L to 100)

  private fun rankerMock(): SubpoolRanker = mock {
    onBlocking { rank(any(), any(), any()) } doReturn
      SubpoolRanker.Result(
        poolOffset = 7L,
        allocated = 1,
        renewed = 0,
        overflow = 0,
        freed = 0,
        cumulativeSize = 1,
      )
  }

  private fun rankerJobsMock(
    state: RankerJob.State = RankerJob.State.CREATED,
    isLastJob: Boolean = false,
  ): RankerJobServiceCoroutineStub = mock {
    onBlocking { getRankerJob(any(), any()) } doReturn
      rankerJob {
        name = RANKER_JOB
        this.state = state
        etag = "etag-1"
      }
    onBlocking { markRankerJobSucceeded(any(), any()) } doReturn
      markRankerJobSucceededResponse {
        rankerJob = rankerJob { name = RANKER_JOB }
        this.isLastJob = isLastJob
      }
    onBlocking { listRankerJobs(any(), any()) } doAnswer
      { invocation ->
        // total_size respects the request's state_in filter; this (upload, model line) has one
        // SUCCEEDED job.
        val request = invocation.getArgument<ListRankerJobsRequest>(0)
        val states = listOf(RankerJob.State.SUCCEEDED)
        val filter = request.filter.stateInList
        listRankerJobsResponse {
          totalSize = (if (filter.isEmpty()) states else states.filter { it in filter }).size
        }
      }
  }

  private fun modelLinesMock(
    state: RawImpressionUploadModelLine.State = RawImpressionUploadModelLine.State.RANKING
  ): RawImpressionUploadModelLineServiceCoroutineStub = mock {
    onBlocking { listRawImpressionUploadModelLines(any(), any()) } doReturn
      listRawImpressionUploadModelLinesResponse {
        rawImpressionUploadModelLines += rawImpressionUploadModelLine {
          name = PARENT_NAME
          cmmsModelLine = MODEL_LINE
          this.state = state
        }
      }
    onBlocking { markRawImpressionUploadModelLineLabeling(any(), any()) } doReturn
      rawImpressionUploadModelLine { name = PARENT_NAME }
    onBlocking { markRawImpressionUploadModelLineCompleted(any(), any()) } doReturn
      rawImpressionUploadModelLine { name = PARENT_NAME }
  }

  /** Echoes each created `VidLabelingJob` back with a deterministic name (mirrors the service). */
  private fun vidLabelingJobsMock(
    existing: List<VidLabelingJob> = emptyList()
  ): VidLabelingJobServiceCoroutineStub = mock {
    onBlocking { batchCreateVidLabelingJobs(any(), any()) } doAnswer
      { invocation ->
        val request = invocation.getArgument<BatchCreateVidLabelingJobsRequest>(0)
        batchCreateVidLabelingJobsResponse {
          request.requestsList.forEachIndexed { i, createRequest ->
            vidLabelingJobs +=
              createRequest.vidLabelingJob.copy { name = "${request.parent}/vidLabelingJobs/job$i" }
          }
        }
      }
    onBlocking { listVidLabelingJobs(any(), any()) } doReturn
      listVidLabelingJobsResponse { vidLabelingJobs += existing }
  }

  private fun filesMock(): RawImpressionUploadFileServiceCoroutineStub = mock {
    onBlocking { listRawImpressionUploadFiles(any(), any()) } doReturn
      listRawImpressionUploadFilesResponse {
        rawImpressionUploadFiles += rawImpressionUploadFile {
          name = "$UPLOAD/files/0"
          sizeBytes = 100
        }
        rawImpressionUploadFiles += rawImpressionUploadFile {
          name = "$UPLOAD/files/1"
          sizeBytes = 100
        }
        rawImpressionUploadFiles += rawImpressionUploadFile {
          name = "$UPLOAD/files/2"
          sizeBytes = 100
        }
      }
  }

  /** A `WorkItems` stub that records every `CreateWorkItem` request into [into]. */
  private fun recordingWorkItems(into: MutableList<CreateWorkItemRequest>): WorkItemsCoroutineStub =
    mock {
      onBlocking { createWorkItem(any(), any()) } doAnswer
        {
          into.add(it.getArgument(0))
          workItem {}
        }
    }

  /** Unpacks the memoized [VidLabelerParams] carried by a published WorkItem. */
  private fun publishedParams(request: CreateWorkItemRequest): VidLabelerParams =
    request.workItem.workItemParams
      .unpack(WorkItemParams::class.java)
      .appParams
      .unpack(VidLabelerParams::class.java)

  /**
   * A `VidLabelingJobService` stub that records every `BatchCreateVidLabelingJobs` request into
   * [into], otherwise behaving like [vidLabelingJobsMock].
   */
  private fun recordingVidLabelingJobs(
    into: MutableList<BatchCreateVidLabelingJobsRequest>,
    existing: List<VidLabelingJob> = emptyList(),
  ): VidLabelingJobServiceCoroutineStub = mock {
    onBlocking { batchCreateVidLabelingJobs(any(), any()) } doAnswer
      { invocation ->
        val request = invocation.getArgument<BatchCreateVidLabelingJobsRequest>(0)
        into.add(request)
        batchCreateVidLabelingJobsResponse {
          request.requestsList.forEachIndexed { i, createRequest ->
            vidLabelingJobs +=
              createRequest.vidLabelingJob.copy { name = "${request.parent}/vidLabelingJobs/job$i" }
          }
        }
      }
    onBlocking { listVidLabelingJobs(any(), any()) } doReturn
      listVidLabelingJobsResponse { vidLabelingJobs += existing }
  }

  /** The `raw_impression_upload_files` of every `VidLabelingJob` created across [requests]. */
  private fun createdFileBatches(
    requests: List<BatchCreateVidLabelingJobsRequest>
  ): List<List<String>> =
    requests.flatMap { request ->
      request.requestsList.map { it.vidLabelingJob.rawImpressionUploadFilesList }
    }

  private fun builder(
    subpoolRanker: SubpoolRanker,
    rankerJobsStub: RankerJobServiceCoroutineStub,
    modelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub = modelLinesMock(),
    vidLabelingJobsStub: VidLabelingJobServiceCoroutineStub = vidLabelingJobsMock(),
    filesStub: RawImpressionUploadFileServiceCoroutineStub = filesMock(),
    workItemsStub: WorkItemsCoroutineStub = mock(),
    maxFileBatchSizeBytes: Long = 1_000_000_000,
    vidLabelerQueue: String = QUEUE,
  ) =
    VidRankBuilder(
      subpoolRanker = subpoolRanker,
      rankerJobsStub = rankerJobsStub,
      rawImpressionUploadModelLinesStub = modelLinesStub,
      vidLabelingJobsStub = vidLabelingJobsStub,
      rawImpressionUploadFilesStub = filesStub,
      workItemsStub = workItemsStub,
      rawImpressionUpload = UPLOAD,
      modelLine = MODEL_LINE,
      rankerJob = RANKER_JOB,
      subpoolMapBlobUris = subpoolMapBlobUris,
      subpoolRankedSizes = subpoolRankedSizes,
      vidLabelerParamsTemplate = VID_LABELER_TEMPLATE,
      vidLabelerQueue = vidLabelerQueue,
      maxFileBatchSizeBytes = maxFileBatchSizeBytes,
    )

  @Test
  fun `non-last job ranks its subpools and marks succeeded without fan-out`() = runBlocking {
    val ranker = rankerMock()
    val rankerJobs = rankerJobsMock(isLastJob = false)
    val vidLabelingJobs = vidLabelingJobsMock()
    val modelLines = modelLinesMock()
    val published = mutableListOf<CreateWorkItemRequest>()

    val result =
      builder(
          ranker,
          rankerJobs,
          modelLines,
          vidLabelingJobs,
          workItemsStub = recordingWorkItems(published),
        )
        .run()

    assertThat(result.lastJobOut).isFalse()
    assertThat(result.subpoolsRanked).isEqualTo(1)
    verifyBlocking(ranker) { rank(7L, "merged/subpool-7", 100) }
    verifyBlocking(rankerJobs) { markRankerJobSucceeded(any(), any()) }
    verifyBlocking(vidLabelingJobs, never()) { batchCreateVidLabelingJobs(any(), any()) }
    assertThat(published).isEmpty()
    verifyBlocking(modelLines, never()) { markRawImpressionUploadModelLineLabeling(any(), any()) }
  }

  @Test
  fun `last job out creates VidLabelingJobs, publishes WorkItems, and flips parent to LABELING`() =
    runBlocking {
      val ranker = rankerMock()
      val rankerJobs = rankerJobsMock(isLastJob = true)
      val jobRequests = mutableListOf<BatchCreateVidLabelingJobsRequest>()
      val vidLabelingJobs = recordingVidLabelingJobs(jobRequests)
      val modelLines = modelLinesMock(RawImpressionUploadModelLine.State.RANKING)
      val published = mutableListOf<CreateWorkItemRequest>()

      val result =
        builder(
            ranker,
            rankerJobs,
            modelLines,
            vidLabelingJobs,
            workItemsStub = recordingWorkItems(published),
          )
          .run()

      assertThat(result.lastJobOut).isTrue()
      // 3 files, default bin-packing -> one VidLabelingJob covering all three.
      verifyBlocking(vidLabelingJobs) { batchCreateVidLabelingJobs(any(), any()) }
      assertThat(published).hasSize(1)
      val params = publishedParams(published.single())
      assertThat(params.vidLabelingJob).isNotEmpty()
      assertThat(createdFileBatches(jobRequests).single())
        .containsExactly("$UPLOAD/files/0", "$UPLOAD/files/1", "$UPLOAD/files/2")
      // Template fields carry through.
      assertThat(params.modelBlobPathsMap.getValue(MODEL_LINE)).isEqualTo("model/blob")
      verifyBlocking(modelLines) { markRawImpressionUploadModelLineLabeling(any(), any()) }
    }

  @Test
  fun `last job out bin-packs files by size across multiple VidLabelingJobs`() =
    runBlocking<Unit> {
      val published = mutableListOf<CreateWorkItemRequest>()
      val jobRequests = mutableListOf<BatchCreateVidLabelingJobsRequest>()

      // Each file is 100 bytes; a 250-byte limit packs files 0+1 together, then file 2.
      builder(
          rankerMock(),
          rankerJobsMock(isLastJob = true),
          vidLabelingJobsStub = recordingVidLabelingJobs(jobRequests),
          workItemsStub = recordingWorkItems(published),
          maxFileBatchSizeBytes = 250,
        )
        .run()

      assertThat(published).hasSize(2)
      val fileSets = createdFileBatches(jobRequests).map { it.toSet() }
      assertThat(fileSets)
        .containsExactly(setOf("$UPLOAD/files/0", "$UPLOAD/files/1"), setOf("$UPLOAD/files/2"))
    }

  @Test
  fun `a file larger than the batch limit gets its own VidLabelingJob`() =
    runBlocking<Unit> {
      val published = mutableListOf<CreateWorkItemRequest>()
      // file 1 (500 bytes) exceeds the 250-byte cap, so FFD isolates it; the two 100-byte files
      // pack together into the remaining batch.
      val files =
        mock<RawImpressionUploadFileServiceCoroutineStub> {
          onBlocking { listRawImpressionUploadFiles(any(), any()) } doReturn
            listRawImpressionUploadFilesResponse {
              rawImpressionUploadFiles += rawImpressionUploadFile {
                name = "$UPLOAD/files/0"
                sizeBytes = 100
              }
              rawImpressionUploadFiles += rawImpressionUploadFile {
                name = "$UPLOAD/files/1"
                sizeBytes = 500
              }
              rawImpressionUploadFiles += rawImpressionUploadFile {
                name = "$UPLOAD/files/2"
                sizeBytes = 100
              }
            }
        }

      val jobRequests = mutableListOf<BatchCreateVidLabelingJobsRequest>()
      builder(
          rankerMock(),
          rankerJobsMock(isLastJob = true),
          filesStub = files,
          vidLabelingJobsStub = recordingVidLabelingJobs(jobRequests),
          workItemsStub = recordingWorkItems(published),
          maxFileBatchSizeBytes = 250,
        )
        .run()

      val fileSets = createdFileBatches(jobRequests).map { it.toSet() }
      assertThat(fileSets)
        .containsExactly(setOf("$UPLOAD/files/1"), setOf("$UPLOAD/files/0", "$UPLOAD/files/2"))
    }

  @Test
  fun `bin-packs First-Fit-Decreasing to minimize the number of batches`() =
    runBlocking<Unit> {
      val published = mutableListOf<CreateWorkItemRequest>()
      // Sizes 4,5,5,6 with a cap of 10: a name-order next-fit needs three batches
      // ({4,5},{5},{6}), but FFD (6,5,5,4) fills exactly two: {6,4} and {5,5}.
      val files =
        mock<RawImpressionUploadFileServiceCoroutineStub> {
          onBlocking { listRawImpressionUploadFiles(any(), any()) } doReturn
            listRawImpressionUploadFilesResponse {
              rawImpressionUploadFiles += rawImpressionUploadFile {
                name = "$UPLOAD/files/0"
                sizeBytes = 4
              }
              rawImpressionUploadFiles += rawImpressionUploadFile {
                name = "$UPLOAD/files/1"
                sizeBytes = 5
              }
              rawImpressionUploadFiles += rawImpressionUploadFile {
                name = "$UPLOAD/files/2"
                sizeBytes = 5
              }
              rawImpressionUploadFiles += rawImpressionUploadFile {
                name = "$UPLOAD/files/3"
                sizeBytes = 6
              }
            }
        }

      val jobRequests = mutableListOf<BatchCreateVidLabelingJobsRequest>()
      builder(
          rankerMock(),
          rankerJobsMock(isLastJob = true),
          filesStub = files,
          vidLabelingJobsStub = recordingVidLabelingJobs(jobRequests),
          workItemsStub = recordingWorkItems(published),
          maxFileBatchSizeBytes = 10,
        )
        .run()

      assertThat(published).hasSize(2)
      val fileSets = createdFileBatches(jobRequests).map { it.toSet() }
      assertThat(fileSets)
        .containsExactly(
          setOf("$UPLOAD/files/3", "$UPLOAD/files/0"),
          setOf("$UPLOAD/files/1", "$UPLOAD/files/2"),
        )
    }

  @Test
  fun `last job out with no files completes the parent directly`() = runBlocking {
    val vidLabelingJobs = vidLabelingJobsMock()
    val modelLines = modelLinesMock(RawImpressionUploadModelLine.State.RANKING)
    val published = mutableListOf<CreateWorkItemRequest>()
    val emptyFiles =
      mock<RawImpressionUploadFileServiceCoroutineStub> {
        onBlocking { listRawImpressionUploadFiles(any(), any()) } doReturn
          listRawImpressionUploadFilesResponse {}
      }

    builder(
        rankerMock(),
        rankerJobsMock(isLastJob = true),
        modelLines,
        vidLabelingJobs,
        emptyFiles,
        recordingWorkItems(published),
      )
      .run()

    verifyBlocking(vidLabelingJobs, never()) { batchCreateVidLabelingJobs(any(), any()) }
    assertThat(published).isEmpty()
    // No VidLabelingJob exists, so no Phase-2 last-job-out would ever complete the model line: the
    // ranker completes it directly (LABELING -> COMPLETED) instead of stranding it in LABELING.
    verifyBlocking(modelLines) { markRawImpressionUploadModelLineLabeling(any(), any()) }
    verifyBlocking(modelLines) { markRawImpressionUploadModelLineCompleted(any(), any()) }
  }

  @Test
  fun `publish tolerates an already-existing WorkItem`() = runBlocking {
    val workItems =
      mock<WorkItemsCoroutineStub> {
        onBlocking { createWorkItem(any(), any()) } doAnswer
          {
            throw StatusException(Status.ALREADY_EXISTS)
          }
      }

    val result =
      builder(rankerMock(), rankerJobsMock(isLastJob = true), workItemsStub = workItems).run()

    assertThat(result.lastJobOut).isTrue()
  }

  @Test
  fun `fan out fails when the vid labeler queue is unconfigured`() = runBlocking {
    assertFailsWith<IllegalArgumentException> {
      builder(rankerMock(), rankerJobsMock(isLastJob = true), vidLabelerQueue = "").run()
    }
    Unit
  }

  @Test
  fun `redelivery of an already-succeeded last job re-publishes existing jobs without re-ranking`() =
    runBlocking {
      val ranker = rankerMock()
      // Job already SUCCEEDED; all jobs succeeded; parent still RANKING -> recover last-job-out.
      val rankerJobs = rankerJobsMock(state = RankerJob.State.SUCCEEDED)
      // Two VidLabelingJobs already exist from the original last-job-out.
      val vidLabelingJobs =
        vidLabelingJobsMock(
          existing =
            listOf(
              vidLabelingJob {
                name = "$UPLOAD/vidLabelingJobs/job0"
                cmmsModelLines += MODEL_LINE
                rawImpressionUploadFiles += "$UPLOAD/files/0"
              },
              vidLabelingJob {
                name = "$UPLOAD/vidLabelingJobs/job1"
                cmmsModelLines += MODEL_LINE
                rawImpressionUploadFiles += "$UPLOAD/files/1"
              },
            )
        )
      val modelLines = modelLinesMock(RawImpressionUploadModelLine.State.RANKING)
      val files = filesMock()
      val published = mutableListOf<CreateWorkItemRequest>()

      val result =
        builder(
            ranker,
            rankerJobs,
            modelLines,
            vidLabelingJobs,
            files,
            recordingWorkItems(published),
          )
          .run()

      assertThat(result.lastJobOut).isTrue()
      verifyBlocking(ranker, never()) { rank(any(), any(), any()) }
      verifyBlocking(rankerJobs, never()) { markRankerJobSucceeded(any(), any()) }
      // Recovery re-publishes from the existing jobs; it neither re-lists files nor re-creates
      // jobs.
      verifyBlocking(vidLabelingJobs) { listVidLabelingJobs(any(), any()) }
      verifyBlocking(vidLabelingJobs, never()) { batchCreateVidLabelingJobs(any(), any()) }
      verifyBlocking(files, never()) { listRawImpressionUploadFiles(any(), any()) }
      assertThat(published).hasSize(2)
      // Files live on the existing VidLabelingJob (the TEE gets it); the WorkItem carries only the
      // job name, so assert recovery republished a WorkItem per existing job.
      assertThat(published.map { publishedParams(it).vidLabelingJob })
        .containsExactly("$UPLOAD/vidLabelingJobs/job0", "$UPLOAD/vidLabelingJobs/job1")
      verifyBlocking(modelLines) { markRawImpressionUploadModelLineLabeling(any(), any()) }
    }

  @Test
  fun `duplicate parent rows for the model line fail loud`() = runBlocking {
    val rankerJobs = rankerJobsMock(isLastJob = true)
    val modelLines =
      mock<RawImpressionUploadModelLineServiceCoroutineStub> {
        onBlocking { listRawImpressionUploadModelLines(any(), any()) } doReturn
          listRawImpressionUploadModelLinesResponse {
            rawImpressionUploadModelLines += rawImpressionUploadModelLine {
              name = PARENT_NAME
              cmmsModelLine = MODEL_LINE
              state = RawImpressionUploadModelLine.State.RANKING
            }
            rawImpressionUploadModelLines += rawImpressionUploadModelLine {
              name = "$UPLOAD/rawImpressionUploadModelLines/rl2"
              cmmsModelLine = MODEL_LINE
              state = RawImpressionUploadModelLine.State.RANKING
            }
          }
      }

    assertFailsWith<IllegalStateException> { builder(rankerMock(), rankerJobs, modelLines).run() }
    Unit
  }

  @Test
  fun `a short BatchCreateVidLabelingJobs response fails loud`() = runBlocking {
    val rankerJobs = rankerJobsMock(isLastJob = true)
    // 3 files -> one batch / one create request, but the service returns zero jobs.
    val vidLabelingJobs =
      mock<VidLabelingJobServiceCoroutineStub> {
        onBlocking { batchCreateVidLabelingJobs(any(), any()) } doReturn
          batchCreateVidLabelingJobsResponse {}
      }

    assertFailsWith<IllegalStateException> {
      builder(rankerMock(), rankerJobs, vidLabelingJobsStub = vidLabelingJobs).run()
    }
    Unit
  }

  @Test
  fun `redelivery recovery with a missing parent fails`() = runBlocking {
    val ranker = rankerMock()
    // Job already SUCCEEDED -> recovery path, but the parent row is absent (impossible state):
    // it must crash loud rather than be swallowed as a benign "parent advanced" no-op.
    val rankerJobs = rankerJobsMock(state = RankerJob.State.SUCCEEDED)
    val modelLines =
      mock<RawImpressionUploadModelLineServiceCoroutineStub> {
        onBlocking { listRawImpressionUploadModelLines(any(), any()) } doReturn
          listRawImpressionUploadModelLinesResponse {}
      }

    assertFailsWith<IllegalArgumentException> { builder(ranker, rankerJobs, modelLines).run() }
    Unit
  }

  @Test
  fun `a failing subpool propagates without marking the ranker job FAILED`() = runBlocking {
    val ranker =
      mock<SubpoolRanker> {
        onBlocking { rank(any(), any(), any()) } doAnswer { throw IllegalStateException("boom") }
      }
    val rankerJobs = rankerJobsMock(state = RankerJob.State.CREATED)

    assertFailsWith<IllegalStateException> { builder(ranker, rankerJobs).run() }

    // The DLQ listener owns the terminal FAILED transition on retry exhaustion; this worker never
    // marks the job FAILED itself.
    verifyBlocking(rankerJobs, never()) { markRankerJobFailed(any(), any()) }
  }

  @Test
  fun `lost etag race on mark succeeded acks when the job is already succeeded`() = runBlocking {
    val ranker = rankerMock()
    val rankerJobs =
      mock<RankerJobServiceCoroutineStub> {
        onBlocking { getRankerJob(any(), any()) } doReturnConsecutively
          listOf(
            rankerJob {
              name = RANKER_JOB
              state = RankerJob.State.CREATED
              etag = "etag-1"
            },
            rankerJob {
              name = RANKER_JOB
              state = RankerJob.State.SUCCEEDED
            },
          )
        onBlocking { markRankerJobSucceeded(any(), any()) } doAnswer
          {
            throw StatusException(Status.ABORTED)
          }
      }

    val result = builder(ranker, rankerJobs).run()

    assertThat(result.lastJobOut).isFalse()
    assertThat(result.subpoolsRanked).isEqualTo(1)
    verifyBlocking(rankerJobs, never()) { markRankerJobFailed(any(), any()) }
  }

  @Test
  fun `a real conflict on mark succeeded fails the job`() = runBlocking {
    val ranker = rankerMock()
    val rankerJobs =
      mock<RankerJobServiceCoroutineStub> {
        onBlocking { getRankerJob(any(), any()) } doReturn
          rankerJob {
            name = RANKER_JOB
            state = RankerJob.State.CREATED
            etag = "etag-1"
          }
        onBlocking { markRankerJobSucceeded(any(), any()) } doAnswer
          {
            throw StatusException(Status.ABORTED)
          }
      }

    assertFailsWith<StatusException> { builder(ranker, rankerJobs).run() }
    Unit
  }

  @Test
  fun `missing subpool ranked size fails the job`() = runBlocking {
    val ranker = rankerMock()
    val rankerJobs = rankerJobsMock(state = RankerJob.State.CREATED)
    val subject =
      VidRankBuilder(
        subpoolRanker = ranker,
        rankerJobsStub = rankerJobs,
        rawImpressionUploadModelLinesStub = modelLinesMock(),
        vidLabelingJobsStub = vidLabelingJobsMock(),
        rawImpressionUploadFilesStub = filesMock(),
        workItemsStub = mock(),
        rawImpressionUpload = UPLOAD,
        modelLine = MODEL_LINE,
        rankerJob = RANKER_JOB,
        subpoolMapBlobUris = mapOf(7L to "merged/subpool-7"),
        subpoolRankedSizes = emptyMap(),
        vidLabelerParamsTemplate = VID_LABELER_TEMPLATE,
        vidLabelerQueue = QUEUE,
        maxFileBatchSizeBytes = 1_000_000_000,
      )

    assertFailsWith<IllegalArgumentException> { subject.run() }
    Unit
  }

  @Test
  fun `last job out with a missing parent fails`() = runBlocking {
    val ranker = rankerMock()
    val rankerJobs = rankerJobsMock(isLastJob = true)
    val modelLines =
      mock<RawImpressionUploadModelLineServiceCoroutineStub> {
        onBlocking { listRawImpressionUploadModelLines(any(), any()) } doReturn
          listRawImpressionUploadModelLinesResponse {}
      }

    assertFailsWith<IllegalArgumentException> { builder(ranker, rankerJobs, modelLines).run() }
    Unit
  }

  @Test
  fun `empty (zero-byte) files all pack into a single VidLabelingJob`() =
    runBlocking<Unit> {
      val published = mutableListOf<CreateWorkItemRequest>()
      // size_bytes = 0 means a genuinely empty file (not "unknown"); it contributes nothing to the
      // running total, so no batch ever overflows.
      val files =
        mock<RawImpressionUploadFileServiceCoroutineStub> {
          onBlocking { listRawImpressionUploadFiles(any(), any()) } doReturn
            listRawImpressionUploadFilesResponse {
              rawImpressionUploadFiles += rawImpressionUploadFile {
                name = "$UPLOAD/files/0"
                sizeBytes = 0
              }
              rawImpressionUploadFiles += rawImpressionUploadFile {
                name = "$UPLOAD/files/1"
                sizeBytes = 0
              }
              rawImpressionUploadFiles += rawImpressionUploadFile {
                name = "$UPLOAD/files/2"
                sizeBytes = 0
              }
            }
        }

      val jobRequests = mutableListOf<BatchCreateVidLabelingJobsRequest>()
      builder(
          rankerMock(),
          rankerJobsMock(isLastJob = true),
          filesStub = files,
          vidLabelingJobsStub = recordingVidLabelingJobs(jobRequests),
          workItemsStub = recordingWorkItems(published),
          maxFileBatchSizeBytes = 250,
        )
        .run()

      assertThat(published).hasSize(1)
      assertThat(createdFileBatches(jobRequests).single())
        .containsExactly("$UPLOAD/files/0", "$UPLOAD/files/1", "$UPLOAD/files/2")
    }

  @Test
  fun `last job out tolerates a benign race flipping the parent to LABELING`() = runBlocking {
    // Another runner (or the Monitor) already advanced the parent: the flip comes back ABORTED.
    // The last-job-out must treat it as done, not fail.
    val modelLines =
      mock<RawImpressionUploadModelLineServiceCoroutineStub> {
        onBlocking { listRawImpressionUploadModelLines(any(), any()) } doReturn
          listRawImpressionUploadModelLinesResponse {
            rawImpressionUploadModelLines += rawImpressionUploadModelLine {
              name = PARENT_NAME
              cmmsModelLine = MODEL_LINE
              state = RawImpressionUploadModelLine.State.RANKING
            }
          }
        onBlocking { markRawImpressionUploadModelLineLabeling(any(), any()) } doAnswer
          {
            throw StatusException(Status.ABORTED)
          }
      }

    val result = builder(rankerMock(), rankerJobsMock(isLastJob = true), modelLines).run()

    assertThat(result.lastJobOut).isTrue()
  }

  @Test
  fun `BatchCreateVidLabelingJobs is chunked to the per-batch limit`() = runBlocking {
    val rankerJobs = rankerJobsMock(isLastJob = true)
    val vidLabelingJobs = vidLabelingJobsMock()
    // Three 100-byte files with a 100-byte cap -> three single-file batches; a per-call limit of 1
    // forces one BatchCreateVidLabelingJobs request per batch.
    val subject =
      VidRankBuilder(
        subpoolRanker = rankerMock(),
        rankerJobsStub = rankerJobs,
        rawImpressionUploadModelLinesStub = modelLinesMock(),
        vidLabelingJobsStub = vidLabelingJobs,
        rawImpressionUploadFilesStub = filesMock(),
        workItemsStub = mock(),
        rawImpressionUpload = UPLOAD,
        modelLine = MODEL_LINE,
        rankerJob = RANKER_JOB,
        subpoolMapBlobUris = subpoolMapBlobUris,
        subpoolRankedSizes = subpoolRankedSizes,
        vidLabelerParamsTemplate = VID_LABELER_TEMPLATE,
        vidLabelerQueue = QUEUE,
        maxFileBatchSizeBytes = 100,
        maxJobsPerBatchCreate = 1,
      )

    val result = subject.run()

    assertThat(result.lastJobOut).isTrue()
    verifyBlocking(vidLabelingJobs, times(3)) { batchCreateVidLabelingJobs(any(), any()) }
  }

  @Test
  fun `redelivery recovery is a no-op once the parent has advanced past RANKING`() = runBlocking {
    val ranker = rankerMock()
    val rankerJobs = rankerJobsMock(state = RankerJob.State.SUCCEEDED)
    // Parent already LABELING: the original last-job-out completed; nothing left to recover.
    val vidLabelingJobs = vidLabelingJobsMock()
    val modelLines = modelLinesMock(RawImpressionUploadModelLine.State.LABELING)
    val published = mutableListOf<CreateWorkItemRequest>()

    val result =
      builder(
          ranker,
          rankerJobs,
          modelLines,
          vidLabelingJobs,
          workItemsStub = recordingWorkItems(published),
        )
        .run()

    assertThat(result.lastJobOut).isFalse()
    verifyBlocking(ranker, never()) { rank(any(), any(), any()) }
    verifyBlocking(vidLabelingJobs, never()) { listVidLabelingJobs(any(), any()) }
    assertThat(published).isEmpty()
    verifyBlocking(modelLines, never()) { markRawImpressionUploadModelLineLabeling(any(), any()) }
  }

  @Test
  fun `redelivery with other jobs still pending does not fan out`() = runBlocking {
    val ranker = rankerMock()
    val rankerJobs =
      mock<RankerJobServiceCoroutineStub> {
        onBlocking { getRankerJob(any(), any()) } doReturn
          rankerJob {
            name = RANKER_JOB
            state = RankerJob.State.SUCCEEDED
          }
        onBlocking { listRankerJobs(any(), any()) } doAnswer
          { invocation ->
            // One SUCCEEDED + one still-CREATED job; total_size respects the state_in filter.
            val request = invocation.getArgument<ListRankerJobsRequest>(0)
            val states = listOf(RankerJob.State.SUCCEEDED, RankerJob.State.CREATED)
            val filter = request.filter.stateInList
            listRankerJobsResponse {
              totalSize = (if (filter.isEmpty()) states else states.filter { it in filter }).size
            }
          }
      }
    val modelLines = modelLinesMock(RawImpressionUploadModelLine.State.RANKING)

    val result = builder(ranker, rankerJobs, modelLines).run()

    assertThat(result.lastJobOut).isFalse()
    verifyBlocking(modelLines, never()) { markRawImpressionUploadModelLineLabeling(any(), any()) }
  }

  @Test
  fun `a non-ALREADY_EXISTS publish failure is rethrown`() = runBlocking {
    val workItems =
      mock<WorkItemsCoroutineStub> {
        onBlocking { createWorkItem(any(), any()) } doAnswer
          {
            throw StatusException(Status.INTERNAL)
          }
      }

    assertFailsWith<StatusException> {
      builder(rankerMock(), rankerJobsMock(isLastJob = true), workItemsStub = workItems).run()
    }
    Unit
  }

  @Test
  fun `a non-benign failure marking succeeded is rethrown`() = runBlocking {
    val rankerJobs =
      mock<RankerJobServiceCoroutineStub> {
        onBlocking { getRankerJob(any(), any()) } doReturn
          rankerJob {
            name = RANKER_JOB
            state = RankerJob.State.CREATED
            etag = "etag-1"
          }
        onBlocking { markRankerJobSucceeded(any(), any()) } doAnswer
          {
            throw StatusException(Status.INTERNAL)
          }
      }

    assertFailsWith<StatusException> { builder(rankerMock(), rankerJobs).run() }
    Unit
  }

  @Test
  fun `a non-benign failure flipping the parent is rethrown`() = runBlocking {
    val modelLines =
      mock<RawImpressionUploadModelLineServiceCoroutineStub> {
        onBlocking { listRawImpressionUploadModelLines(any(), any()) } doReturn
          listRawImpressionUploadModelLinesResponse {
            rawImpressionUploadModelLines += rawImpressionUploadModelLine {
              name = PARENT_NAME
              cmmsModelLine = MODEL_LINE
              state = RawImpressionUploadModelLine.State.RANKING
            }
          }
        onBlocking { markRawImpressionUploadModelLineLabeling(any(), any()) } doAnswer
          {
            throw StatusException(Status.INTERNAL)
          }
      }

    assertFailsWith<StatusException> {
      builder(rankerMock(), rankerJobsMock(isLastJob = true), modelLines).run()
    }
    Unit
  }

  @Test
  fun `redelivery recovery is a no-op when no RankerJobs are listed`() = runBlocking {
    val ranker = rankerMock()
    // Already SUCCEEDED and parent still RANKING, but listing returns nothing (sawAny = false): the
    // last-job-out condition is not satisfied, so recovery must not fan out.
    val rankerJobs =
      mock<RankerJobServiceCoroutineStub> {
        onBlocking { getRankerJob(any(), any()) } doReturn
          rankerJob {
            name = RANKER_JOB
            state = RankerJob.State.SUCCEEDED
          }
        onBlocking { listRankerJobs(any(), any()) } doReturn listRankerJobsResponse {}
      }
    val modelLines = modelLinesMock(RawImpressionUploadModelLine.State.RANKING)

    val result = builder(ranker, rankerJobs, modelLines).run()

    assertThat(result.lastJobOut).isFalse()
    verifyBlocking(modelLines, never()) { markRawImpressionUploadModelLineLabeling(any(), any()) }
  }
}
