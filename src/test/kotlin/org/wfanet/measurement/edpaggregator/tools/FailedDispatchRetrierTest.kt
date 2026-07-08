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
import io.grpc.Status
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argThat
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.listRankerJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
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
  }

  @Test
  fun `retryFailed republishes ranking work items and transitions to RANKING`() {
    val result = runBlocking {
      whenever(modelLineService.listRawImpressionUploadModelLines(any()))
        .thenReturn(
          listRawImpressionUploadModelLinesResponse {
            rawImpressionUploadModelLines += failedModelLine()
          }
        )
      whenever(rankerJobService.listRankerJobs(any()))
        .thenReturn(listRankerJobsResponse { rankerJobs += rankerJob { name = RANKER_JOB_NAME } })
      whenever(workItemsService.getWorkItem(any())).thenReturn(workItem { queue = RANKER_QUEUE })
      whenever(workItemsService.createWorkItem(any())).thenReturn(workItem {})
      whenever(modelLineService.markRawImpressionUploadModelLineRanking(any()))
        .thenReturn(failedModelLine().copy { state = RawImpressionUploadModelLine.State.RANKING })

      retrier.retryFailed(UPLOAD_NAME, MODEL_LINE, RawImpressionUploadModelLine.State.RANKING)
    }

    assertThat(result.workItemsRepublished).isEqualTo(1)
    assertThat(result.newState).isEqualTo(RawImpressionUploadModelLine.State.RANKING)
    verifyBlocking(workItemsService) {
      createWorkItem(argThat { workItemId.startsWith("rt-") && workItem.queue == RANKER_QUEUE })
    }
  }

  @Test
  fun `retryFailed throws when the model line is not FAILED`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        runBlocking {
          whenever(modelLineService.listRawImpressionUploadModelLines(any()))
            .thenReturn(
              listRawImpressionUploadModelLinesResponse {
                rawImpressionUploadModelLines +=
                  failedModelLine().copy { state = RawImpressionUploadModelLine.State.RANKING }
              }
            )
          retrier.retryFailed(UPLOAD_NAME, MODEL_LINE, RawImpressionUploadModelLine.State.RANKING)
        }
      }
    assertThat(error).hasMessageThat().contains("expected FAILED")
  }

  @Test
  fun `retryFailed throws when the original work item is gone`() {
    val error =
      assertFailsWith<IllegalStateException> {
        runBlocking {
          whenever(modelLineService.listRawImpressionUploadModelLines(any()))
            .thenReturn(
              listRawImpressionUploadModelLinesResponse {
                rawImpressionUploadModelLines += failedModelLine()
              }
            )
          whenever(rankerJobService.listRankerJobs(any()))
            .thenReturn(
              listRankerJobsResponse { rankerJobs += rankerJob { name = RANKER_JOB_NAME } }
            )
          whenever(workItemsService.getWorkItem(any())).thenAnswer {
            throw Status.NOT_FOUND.asRuntimeException()
          }
          retrier.retryFailed(UPLOAD_NAME, MODEL_LINE, RawImpressionUploadModelLine.State.RANKING)
        }
      }
    assertThat(error).hasMessageThat().contains("cannot be re-published")
  }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/dp1"
    private const val UPLOAD_ID = "upload1"
    private const val UPLOAD_NAME = "$DATA_PROVIDER/rawImpressionUploads/$UPLOAD_ID"
    private const val MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/ml1"
    private const val MODEL_LINE_NAME = "$UPLOAD_NAME/rawImpressionUploadModelLines/rml1"
    private const val RANKER_JOB_NAME =
      "$DATA_PROVIDER/rawImpressionUploads/$UPLOAD_ID/rankerJobs/rj1"
    private const val ETAG = "etag-1"
    private const val RANKER_QUEUE = "vid-rank-builder-queue"
  }
}
