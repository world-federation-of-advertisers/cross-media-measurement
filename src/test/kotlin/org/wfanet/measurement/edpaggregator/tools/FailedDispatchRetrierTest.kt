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

package org.wfanet.measurement.edpaggregator.tools

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.timestamp
import io.grpc.Status
import io.grpc.StatusException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.edpaggregator.v1alpha.MarkRawImpressionUploadModelLineLabelingRequest
import org.wfanet.measurement.edpaggregator.v1alpha.MarkRawImpressionUploadModelLinePoolAssigningRequest
import org.wfanet.measurement.edpaggregator.v1alpha.MarkRawImpressionUploadModelLineRankingRequest
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.listPoolAssignmentJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRankerJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listVidLabelingJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.poolAssignmentJob
import org.wfanet.measurement.edpaggregator.v1alpha.rankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelingJob
import org.wfanet.measurement.edpaggregator.vidlabeling.RequestIds
import org.wfanet.measurement.edpaggregator.vidlabeling.WorkItemIds
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.GetWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem

@RunWith(JUnit4::class)
class FailedDispatchRetrierTest {
  private val modelLineService:
    RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase =
    mockService()
  private val poolAssignmentJobService:
    PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineImplBase =
    mockService()
  private val rankerJobService: RankerJobServiceGrpcKt.RankerJobServiceCoroutineImplBase =
    mockService()
  private val vidLabelingJobService:
    VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineImplBase =
    mockService()
  private val workItemsService: WorkItemsGrpcKt.WorkItemsCoroutineImplBase = mockService()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(modelLineService)
    addService(poolAssignmentJobService)
    addService(rankerJobService)
    addService(vidLabelingJobService)
    addService(workItemsService)
  }

  private val retrier: FailedDispatchRetrier by lazy {
    val channel = grpcTestServerRule.channel
    FailedDispatchRetrier(
      RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub(
        channel
      ),
      PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub(channel),
      RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub(channel),
      VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub(channel),
      WorkItemsGrpcKt.WorkItemsCoroutineStub(channel),
    )
  }

  private fun failedModelLine() = rawImpressionUploadModelLine {
    name = MODEL_LINE_NAME
    cmmsModelLine = MODEL_LINE
    state = RawImpressionUploadModelLine.State.FAILED
    etag = ETAG
    failureAttemptId = FAILURE_ATTEMPT_ID
    failureReason = RawImpressionUploadModelLine.FailureReason.PROCESSING_FAILURE
  }

  private suspend fun stubFailedModelLine() {
    whenever(modelLineService.listRawImpressionUploadModelLines(any()))
      .thenReturn(
        listRawImpressionUploadModelLinesResponse {
          rawImpressionUploadModelLines += failedModelLine()
        }
      )
  }

  @Test
  fun `retryFailed re-triggers Phase 2 when VidLabelingJobs exist`() {
    val events = mutableListOf<String>()
    val result = runBlocking {
      stubFailedModelLine()
      whenever(vidLabelingJobService.listVidLabelingJobs(any()))
        .thenReturn(
          listVidLabelingJobsResponse { vidLabelingJobs += vidLabelingJob { name = VID_JOB_NAME } }
        )
      whenever(workItemsService.getWorkItem(any())).thenAnswer {
        events += "get-original"
        workItem {
          queue = "q"
          state = WorkItem.State.QUEUED
        }
      }
      whenever(workItemsService.createWorkItem(any())).thenAnswer {
        events += "create"
        workItem {}
      }
      whenever(modelLineService.markRawImpressionUploadModelLineLabeling(any())).thenAnswer {
        events += "transition"
        failedModelLine().copy { state = RawImpressionUploadModelLine.State.LABELING }
      }

      retrier.retryFailed(UPLOAD_NAME, MODEL_LINE)
    }

    assertThat(result.newState).isEqualTo(RawImpressionUploadModelLine.State.LABELING)
    assertThat(result.workItemsRepublished).isEqualTo(1)
    assertThat(result.wasAlreadyStarted).isFalse()
    assertThat(events).containsExactly("get-original", "transition", "create").inOrder()
    val requestCaptor = argumentCaptor<MarkRawImpressionUploadModelLineLabelingRequest>()
    verifyBlocking(modelLineService) {
      markRawImpressionUploadModelLineLabeling(requestCaptor.capture())
    }
    assertThat(requestCaptor.firstValue.requestId).isNotEmpty()
  }

  @Test
  fun `retryFailed re-triggers Phase 1 when only RankerJobs exist`() {
    val result = runBlocking {
      stubFailedModelLine()
      whenever(vidLabelingJobService.listVidLabelingJobs(any()))
        .thenReturn(listVidLabelingJobsResponse {})
      whenever(rankerJobService.listRankerJobs(any()))
        .thenReturn(listRankerJobsResponse { rankerJobs += rankerJob { name = RANKER_JOB_NAME } })
      whenever(workItemsService.getWorkItem(any()))
        .thenReturn(
          workItem {
            queue = "q"
            state = WorkItem.State.QUEUED
          }
        )
      whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})
      whenever(modelLineService.markRawImpressionUploadModelLineRanking(any()))
        .thenReturn(failedModelLine().copy { state = RawImpressionUploadModelLine.State.RANKING })

      retrier.retryFailed(UPLOAD_NAME, MODEL_LINE)
    }

    assertThat(result.newState).isEqualTo(RawImpressionUploadModelLine.State.RANKING)
    assertThat(result.workItemsRepublished).isEqualTo(1)
    val requestCaptor = argumentCaptor<MarkRawImpressionUploadModelLineRankingRequest>()
    verifyBlocking(modelLineService) {
      markRawImpressionUploadModelLineRanking(requestCaptor.capture())
    }
    assertThat(requestCaptor.firstValue.requestId).isNotEmpty()
  }

  @Test
  fun `retryFailed re-triggers Phase 0 when only PoolAssignmentJobs exist`() {
    val result = runBlocking {
      stubFailedModelLine()
      whenever(vidLabelingJobService.listVidLabelingJobs(any()))
        .thenReturn(listVidLabelingJobsResponse {})
      whenever(rankerJobService.listRankerJobs(any())).thenReturn(listRankerJobsResponse {})
      whenever(poolAssignmentJobService.listPoolAssignmentJobs(any()))
        .thenReturn(
          listPoolAssignmentJobsResponse {
            poolAssignmentJobs += poolAssignmentJob { shardIndex = 0 }
          }
        )
      whenever(workItemsService.getWorkItem(any())).thenReturn(workItem { queue = "q" })
      whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})
      whenever(modelLineService.markRawImpressionUploadModelLinePoolAssigning(any()))
        .thenReturn(
          failedModelLine().copy { state = RawImpressionUploadModelLine.State.POOL_ASSIGNING }
        )

      retrier.retryFailed(UPLOAD_NAME, MODEL_LINE)
    }

    assertThat(result.newState).isEqualTo(RawImpressionUploadModelLine.State.POOL_ASSIGNING)
    assertThat(result.workItemsRepublished).isEqualTo(1)
    val requestCaptor = argumentCaptor<MarkRawImpressionUploadModelLinePoolAssigningRequest>()
    verifyBlocking(modelLineService) {
      markRawImpressionUploadModelLinePoolAssigning(requestCaptor.capture())
    }
    assertThat(requestCaptor.firstValue.requestId).isNotEmpty()
  }

  @Test
  fun `retryFailed rejects evicted output`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        runBlocking {
          whenever(modelLineService.listRawImpressionUploadModelLines(any()))
            .thenReturn(
              listRawImpressionUploadModelLinesResponse {
                rawImpressionUploadModelLines +=
                  failedModelLine().copy {
                    failureReason = RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT
                  }
              }
            )

          retrier.retryFailed(UPLOAD_NAME, MODEL_LINE)
        }
      }

    assertThat(error).hasMessageThat().contains("only processing failures can be retried")
    verifyBlocking(workItemsService, never()) { createWorkItem(any()) }
    verifyBlocking(modelLineService, never()) { markRawImpressionUploadModelLineLabeling(any()) }
    verifyBlocking(modelLineService, never()) { markRawImpressionUploadModelLineRanking(any()) }
    verifyBlocking(modelLineService, never()) {
      markRawImpressionUploadModelLinePoolAssigning(any())
    }
  }

  @Test
  fun `retryFailed retries legacy failed rows without a failure reason`() {
    val result = runBlocking {
      whenever(modelLineService.listRawImpressionUploadModelLines(any()))
        .thenReturn(
          listRawImpressionUploadModelLinesResponse {
            rawImpressionUploadModelLines += failedModelLine().copy { clearFailureReason() }
          }
        )
      whenever(vidLabelingJobService.listVidLabelingJobs(any()))
        .thenReturn(
          listVidLabelingJobsResponse { vidLabelingJobs += vidLabelingJob { name = VID_JOB_NAME } }
        )
      whenever(modelLineService.markRawImpressionUploadModelLineLabeling(any()))
        .thenReturn(failedModelLine().copy { state = RawImpressionUploadModelLine.State.LABELING })
      whenever(workItemsService.getWorkItem(any())).thenReturn(workItem { queue = "q" })
      whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})

      retrier.retryFailed(UPLOAD_NAME, MODEL_LINE)
    }

    assertThat(result.newState).isEqualTo(RawImpressionUploadModelLine.State.LABELING)
    assertThat(result.workItemsRepublished).isEqualTo(1)
  }

  @Test
  fun `retryFailed throws when failure identity is missing`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        runBlocking {
          whenever(modelLineService.listRawImpressionUploadModelLines(any()))
            .thenReturn(
              listRawImpressionUploadModelLinesResponse {
                rawImpressionUploadModelLines +=
                  failedModelLine().copy {
                    state = RawImpressionUploadModelLine.State.RANKING
                    clearFailureAttemptId()
                  }
              }
            )
          retrier.retryFailed(UPLOAD_NAME, MODEL_LINE)
        }
      }
    assertThat(error).hasMessageThat().contains("has no failure_attempt_id")
  }

  @Test
  fun `retryFailed returns existing retry when model line already left FAILED`() {
    val result = runBlocking {
      whenever(modelLineService.listRawImpressionUploadModelLines(any()))
        .thenReturn(
          listRawImpressionUploadModelLinesResponse {
            rawImpressionUploadModelLines +=
              failedModelLine().copy { state = RawImpressionUploadModelLine.State.LABELING }
          }
        )
      whenever(vidLabelingJobService.listVidLabelingJobs(any()))
        .thenReturn(
          listVidLabelingJobsResponse { vidLabelingJobs += vidLabelingJob { name = VID_JOB_NAME } }
        )
      whenever(workItemsService.getWorkItem(any()))
        .thenReturn(workItem { state = WorkItem.State.RUNNING })

      retrier.retryFailed(UPLOAD_NAME, MODEL_LINE)
    }

    assertThat(result.newState).isEqualTo(RawImpressionUploadModelLine.State.LABELING)
    assertThat(result.workItemsRepublished).isEqualTo(0)
    assertThat(result.wasAlreadyStarted).isTrue()
    val requestCaptor = argumentCaptor<GetWorkItemRequest>()
    verifyBlocking(workItemsService) { getWorkItem(requestCaptor.capture()) }
    assertThat(requestCaptor.firstValue.name)
      .isEqualTo(
        "workItems/${RequestIds.forRetriedWorkItem(WorkItemIds.forVidLabeler(VID_JOB_NAME), FAILURE_ATTEMPT_ID)}"
      )
    verifyBlocking(workItemsService, never()) { createWorkItem(any()) }
    verifyBlocking(modelLineService, never()) { markRawImpressionUploadModelLineLabeling(any()) }
  }

  @Test
  fun `retryFailed follows a failed retry WorkItem to its active successor`() {
    val result = runBlocking {
      whenever(modelLineService.listRawImpressionUploadModelLines(any()))
        .thenReturn(
          listRawImpressionUploadModelLinesResponse {
            rawImpressionUploadModelLines +=
              failedModelLine().copy { state = RawImpressionUploadModelLine.State.LABELING }
          }
        )
      whenever(vidLabelingJobService.listVidLabelingJobs(any()))
        .thenReturn(
          listVidLabelingJobsResponse { vidLabelingJobs += vidLabelingJob { name = VID_JOB_NAME } }
        )
      whenever(workItemsService.getWorkItem(any()))
        .thenReturn(
          workItem {
            state = WorkItem.State.FAILED
            updateTime = timestamp {
              seconds = 123
              nanos = 456
            }
          },
          workItem { state = WorkItem.State.QUEUED },
        )

      retrier.retryFailed(UPLOAD_NAME, MODEL_LINE)
    }

    assertThat(result.wasAlreadyStarted).isTrue()
    val requestCaptor = argumentCaptor<GetWorkItemRequest>()
    verifyBlocking(workItemsService, times(2)) { getWorkItem(requestCaptor.capture()) }
    val firstRetryId =
      RequestIds.forRetriedWorkItem(WorkItemIds.forVidLabeler(VID_JOB_NAME), FAILURE_ATTEMPT_ID)
    assertThat(requestCaptor.allValues[0].name).isEqualTo("workItems/$firstRetryId")
    assertThat(requestCaptor.allValues[1].name)
      .isEqualTo("workItems/${RequestIds.forRetriedWorkItem(firstRetryId, "123:456")}")
  }

  @Test
  fun `retryFailed finishes publication when a prior invocation committed only the claim`() {
    val result = runBlocking {
      whenever(modelLineService.listRawImpressionUploadModelLines(any()))
        .thenReturn(
          listRawImpressionUploadModelLinesResponse {
            rawImpressionUploadModelLines +=
              failedModelLine().copy { state = RawImpressionUploadModelLine.State.LABELING }
          }
        )
      whenever(vidLabelingJobService.listVidLabelingJobs(any()))
        .thenReturn(
          listVidLabelingJobsResponse { vidLabelingJobs += vidLabelingJob { name = VID_JOB_NAME } }
        )
      whenever(workItemsService.getWorkItem(any())).thenAnswer { invocation ->
        val request = invocation.getArgument<GetWorkItemRequest>(0)
        if (request.name == "workItems/${WorkItemIds.forVidLabeler(VID_JOB_NAME)}") {
          workItem { queue = "q" }
        } else {
          throw Status.NOT_FOUND.asRuntimeException()
        }
      }
      whenever(modelLineService.markRawImpressionUploadModelLineLabeling(any()))
        .thenReturn(failedModelLine().copy { state = RawImpressionUploadModelLine.State.LABELING })
      whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})

      retrier.retryFailed(UPLOAD_NAME, MODEL_LINE, RawImpressionUploadModelLine.State.LABELING)
    }

    assertThat(result.workItemsRepublished).isEqualTo(1)
    assertThat(result.wasAlreadyStarted).isTrue()
    verifyBlocking(modelLineService) { markRawImpressionUploadModelLineLabeling(any()) }
    verifyBlocking(workItemsService) { createWorkItem(any()) }
  }

  @Test
  fun `retryFailed throws when the original work item is gone`() {
    val error =
      assertFailsWith<IllegalStateException> {
        runBlocking {
          stubFailedModelLine()
          whenever(vidLabelingJobService.listVidLabelingJobs(any()))
            .thenReturn(
              listVidLabelingJobsResponse {
                vidLabelingJobs += vidLabelingJob { name = VID_JOB_NAME }
              }
            )
          whenever(workItemsService.getWorkItem(any())).thenAnswer {
            throw Status.NOT_FOUND.asRuntimeException()
          }
          whenever(modelLineService.markRawImpressionUploadModelLineLabeling(any()))
            .thenReturn(
              failedModelLine().copy { state = RawImpressionUploadModelLine.State.LABELING }
            )
          retrier.retryFailed(UPLOAD_NAME, MODEL_LINE)
        }
      }
    assertThat(error).hasMessageThat().contains("cannot be re-published")
    verifyBlocking(modelLineService, never()) { markRawImpressionUploadModelLineLabeling(any()) }
    verifyBlocking(workItemsService, never()) { createWorkItem(any()) }
  }

  @Test
  fun `retryFailed validates every original work item before claiming`() {
    val error =
      assertFailsWith<IllegalStateException> {
        runBlocking {
          stubFailedModelLine()
          whenever(vidLabelingJobService.listVidLabelingJobs(any()))
            .thenReturn(
              listVidLabelingJobsResponse {
                vidLabelingJobs += vidLabelingJob { name = VID_JOB_NAME }
                vidLabelingJobs += vidLabelingJob { name = SECOND_VID_JOB_NAME }
              }
            )
          whenever(workItemsService.getWorkItem(any())).thenAnswer { invocation ->
            val request = invocation.getArgument<GetWorkItemRequest>(0)
            if (request.name == "workItems/${WorkItemIds.forVidLabeler(VID_JOB_NAME)}") {
              workItem { queue = "q" }
            } else {
              throw Status.NOT_FOUND.asRuntimeException()
            }
          }

          retrier.retryFailed(UPLOAD_NAME, MODEL_LINE)
        }
      }

    assertThat(error).hasMessageThat().contains("cannot be re-published")
    verifyBlocking(workItemsService, times(2)) { getWorkItem(any()) }
    verifyBlocking(modelLineService, never()) { markRawImpressionUploadModelLineLabeling(any()) }
    verifyBlocking(workItemsService, never()) { createWorkItem(any()) }
  }

  @Test
  fun `retryFailed does not publish when the atomic claim is rejected`() {
    val error =
      assertFailsWith<StatusException> {
        runBlocking {
          stubFailedModelLine()
          whenever(vidLabelingJobService.listVidLabelingJobs(any()))
            .thenReturn(
              listVidLabelingJobsResponse {
                vidLabelingJobs += vidLabelingJob { name = VID_JOB_NAME }
              }
            )
          whenever(workItemsService.getWorkItem(any())).thenReturn(workItem { queue = "q" })
          whenever(modelLineService.markRawImpressionUploadModelLineLabeling(any())).thenAnswer {
            throw Status.FAILED_PRECONDITION.asRuntimeException()
          }

          retrier.retryFailed(UPLOAD_NAME, MODEL_LINE)
        }
      }

    assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    verifyBlocking(workItemsService, never()) { createWorkItem(any()) }
  }

  @Test
  fun `retryFailed does not publish when a claim replay returns an evicted row`() {
    val error =
      assertFailsWith<IllegalStateException> {
        runBlocking {
          stubFailedModelLine()
          whenever(vidLabelingJobService.listVidLabelingJobs(any()))
            .thenReturn(
              listVidLabelingJobsResponse {
                vidLabelingJobs += vidLabelingJob { name = VID_JOB_NAME }
              }
            )
          whenever(workItemsService.getWorkItem(any())).thenReturn(workItem { queue = "q" })
          whenever(modelLineService.markRawImpressionUploadModelLineLabeling(any()))
            .thenReturn(
              failedModelLine().copy {
                failureReason = RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT
              }
            )

          retrier.retryFailed(UPLOAD_NAME, MODEL_LINE)
        }
      }

    assertThat(error).hasMessageThat().contains("no WorkItems were published")
    verifyBlocking(workItemsService, never()) { createWorkItem(any()) }
  }

  @Test
  fun `retryFailed advances the model line when retry WorkItems already exist`() {
    val result = runBlocking {
      stubFailedModelLine()
      whenever(vidLabelingJobService.listVidLabelingJobs(any()))
        .thenReturn(
          listVidLabelingJobsResponse { vidLabelingJobs += vidLabelingJob { name = VID_JOB_NAME } }
        )
      whenever(workItemsService.getWorkItem(any()))
        .thenReturn(
          workItem {
            queue = "q"
            state = WorkItem.State.QUEUED
          }
        )
      // Re-retry: the deterministic rt-<hash> WorkItem already exists from a prior retry.
      whenever(workItemsService.createWorkItem(any())).thenAnswer {
        throw Status.ALREADY_EXISTS.asRuntimeException()
      }
      whenever(modelLineService.markRawImpressionUploadModelLineLabeling(any()))
        .thenReturn(failedModelLine().copy { state = RawImpressionUploadModelLine.State.LABELING })

      retrier.retryFailed(UPLOAD_NAME, MODEL_LINE)
    }

    assertThat(result.workItemsRepublished).isEqualTo(0)
    assertThat(result.newState).isEqualTo(RawImpressionUploadModelLine.State.LABELING)
    verifyBlocking(modelLineService) { markRawImpressionUploadModelLineLabeling(any()) }
  }

  @Test
  fun `retryFailed creates a successor when the previous retry WorkItem failed`() {
    val result = runBlocking {
      stubFailedModelLine()
      whenever(vidLabelingJobService.listVidLabelingJobs(any()))
        .thenReturn(
          listVidLabelingJobsResponse { vidLabelingJobs += vidLabelingJob { name = VID_JOB_NAME } }
        )
      whenever(workItemsService.getWorkItem(any()))
        .thenReturn(
          workItem { queue = "q" },
          workItem {
            queue = "q"
            state = WorkItem.State.FAILED
            updateTime = timestamp {
              seconds = 123
              nanos = 456
            }
          },
        )
      whenever(workItemsService.createWorkItem(any()))
        .thenAnswer { throw Status.ALREADY_EXISTS.asRuntimeException() }
        .thenReturn(workItem {})
      whenever(modelLineService.markRawImpressionUploadModelLineLabeling(any()))
        .thenReturn(failedModelLine().copy { state = RawImpressionUploadModelLine.State.LABELING })

      retrier.retryFailed(UPLOAD_NAME, MODEL_LINE)
    }

    assertThat(result.workItemsRepublished).isEqualTo(1)
    assertThat(result.newState).isEqualTo(RawImpressionUploadModelLine.State.LABELING)
    val requestCaptor = argumentCaptor<CreateWorkItemRequest>()
    verifyBlocking(workItemsService, times(2)) { createWorkItem(requestCaptor.capture()) }
    assertThat(requestCaptor.allValues[1].workItemId)
      .isNotEqualTo(requestCaptor.allValues[0].workItemId)
  }

  @Test
  fun `retryFailed honors a --from-phase override`() {
    val result = runBlocking {
      stubFailedModelLine()
      // VidLabelingJobs would auto-detect LABELING, but --from-phase forces RANKING.
      whenever(rankerJobService.listRankerJobs(any()))
        .thenReturn(listRankerJobsResponse { rankerJobs += rankerJob { name = RANKER_JOB_NAME } })
      whenever(workItemsService.getWorkItem(any())).thenReturn(workItem { queue = "q" })
      whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})
      whenever(modelLineService.markRawImpressionUploadModelLineRanking(any()))
        .thenReturn(failedModelLine().copy { state = RawImpressionUploadModelLine.State.RANKING })

      retrier.retryFailed(UPLOAD_NAME, MODEL_LINE, RawImpressionUploadModelLine.State.RANKING)
    }

    assertThat(result.newState).isEqualTo(RawImpressionUploadModelLine.State.RANKING)
    assertThat(result.workItemsRepublished).isEqualTo(1)
    verifyBlocking(modelLineService, never()) { markRawImpressionUploadModelLineLabeling(any()) }
  }

  @Test
  fun `retryFailed rejects a non-phase --from-phase`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        runBlocking {
          stubFailedModelLine()
          retrier.retryFailed(UPLOAD_NAME, MODEL_LINE, RawImpressionUploadModelLine.State.COMPLETED)
        }
      }
    assertThat(error).hasMessageThat().contains("--from-phase")
  }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/dp1"
    private const val UPLOAD_ID = "upload1"
    private const val UPLOAD_NAME = "$DATA_PROVIDER/rawImpressionUploads/$UPLOAD_ID"
    private const val MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/ml1"
    private const val MODEL_LINE_NAME = "$UPLOAD_NAME/rawImpressionUploadModelLines/rml1"
    private const val VID_JOB_NAME = "$UPLOAD_NAME/vidLabelingJobs/vlj1"
    private const val SECOND_VID_JOB_NAME = "$UPLOAD_NAME/vidLabelingJobs/vlj2"
    private const val RANKER_JOB_NAME = "$UPLOAD_NAME/rankerJobs/rj1"
    private const val ETAG = "etag-1"
    private const val FAILURE_ATTEMPT_ID = "failure-attempt-1"
  }
}
