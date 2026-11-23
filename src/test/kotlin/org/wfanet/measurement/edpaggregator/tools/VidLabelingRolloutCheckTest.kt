// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator.tools

import com.google.common.truth.Truth.assertThat
import io.grpc.Status
import io.grpc.StatusException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.edpaggregator.v1alpha.ListPoolAssignmentJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankerJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListVidLabelingJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJob
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperationServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.listPoolAssignmentJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRankerJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listUploadHealingOperationsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listVidLabelingJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.poolAssignmentJob
import org.wfanet.measurement.edpaggregator.v1alpha.rankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelingJob
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.ListWorkItemsRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.listWorkItemsResponse
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem

class VidLabelingRolloutCheckTest {
  private var uploadRows =
    listOf(
      rawImpressionUpload {
        name = UPLOAD
        state = RawImpressionUpload.State.COMPLETED
        registrationComplete = true
        doneBlobUri = DONE
        doneBlobGeneration = 12
      }
    )
  private var items = emptyList<WorkItem>()
  private var operations = emptyList<UploadHealingOperation>()
  private var queryFails = false
  private var repeatedToken = false
  private var uploadReads = 0
  private var shardRows =
    (0..1).map { index ->
      poolAssignmentJob {
        shardIndex = index
        state = PoolAssignmentJob.State.SUCCEEDED
      }
    }
  private var labelFiles = listOf("$UPLOAD/files/a", "$UPLOAD/files/b")
  private var ranked = false
  private var modelState = RawImpressionUploadModelLine.State.COMPLETED
  @get:Rule
  val server = GrpcTestServerRule {
    addService(
      object : RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineImplBase() {
        override suspend fun listRawImpressionUploads(request: ListRawImpressionUploadsRequest) =
          listRawImpressionUploadsResponse {
            uploadReads++
            if (queryFails) throw Status.UNAVAILABLE.asException()
            val offset = request.pageToken.toIntOrNull() ?: 0
            rawImpressionUploads += uploadRows.drop(offset).take(1)
            if (offset + 1 < uploadRows.size || repeatedToken) nextPageToken = "1"
          }
      }
    )
    addService(
      object : WorkItemsGrpcKt.WorkItemsCoroutineImplBase() {
        override suspend fun listWorkItems(request: ListWorkItemsRequest) = listWorkItemsResponse {
          workItems += items
        }
      }
    )
    addService(
      object :
        UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineImplBase() {
        override suspend fun listUploadHealingOperations(
          request: ListUploadHealingOperationsRequest
        ) = listUploadHealingOperationsResponse { uploadHealingOperations += operations }
      }
    )
    addService(
      object :
        RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase() {
        override suspend fun listRawImpressionUploadModelLines(
          request: ListRawImpressionUploadModelLinesRequest
        ) = listRawImpressionUploadModelLinesResponse {
          rawImpressionUploadModelLines += rawImpressionUploadModelLine {
            cmmsModelLine = MODEL
            state = modelState
            poolAssignmentComplete = true
          }
        }
      }
    )
    addService(
      object : PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineImplBase() {
        override suspend fun listPoolAssignmentJobs(request: ListPoolAssignmentJobsRequest) =
          listPoolAssignmentJobsResponse {
            poolAssignmentJobs += shardRows
          }
      }
    )
    addService(
      object : RankerJobServiceGrpcKt.RankerJobServiceCoroutineImplBase() {
        override suspend fun listRankerJobs(request: ListRankerJobsRequest) =
          listRankerJobsResponse {
            if (ranked) rankerJobs += rankerJob {}
          }
      }
    )
    addService(
      object : VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineImplBase() {
        override suspend fun listVidLabelingJobs(request: ListVidLabelingJobsRequest) =
          listVidLabelingJobsResponse {
            vidLabelingJobs += vidLabelingJob {
              state = VidLabelingJob.State.SUCCEEDED
              rawImpressionUploadFiles += labelFiles
            }
          }
      }
    )
    addService(
      object :
        RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineImplBase() {
        override suspend fun listRawImpressionUploadFiles(
          request: ListRawImpressionUploadFilesRequest
        ) = listRawImpressionUploadFilesResponse {
          rawImpressionUploadFiles += rawImpressionUploadFile { name = "$UPLOAD/files/a" }
          rawImpressionUploadFiles += rawImpressionUploadFile { name = "$UPLOAD/files/b" }
        }
      }
    )
  }

  private fun checker() =
    VidLabelingRolloutChecker(
      RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub(server.channel),
      RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub(
        server.channel
      ),
      PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub(server.channel),
      RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub(server.channel),
      VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub(server.channel),
      RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub(
        server.channel
      ),
      UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineStub(
        server.channel
      ),
      WorkItemsGrpcKt.WorkItemsCoroutineStub(server.channel),
    )

  private suspend fun drained() = checker().requireDrained(setOf("dataProviders/dp"), setOf(QUEUE))

  private suspend fun smoke() =
    checker().requireZeroMembership("dataProviders/dp", DONE, 12, MODEL, 2)

  @Test
  fun `drained evidence is read completely without mutation`() =
    runBlocking<Unit> {
      uploadRows = uploadRows + uploadRows.single().copy { name = "$UPLOAD-other" }
      drained()
      assertThat(uploadReads).isEqualTo(2)
    }

  @Test
  fun `active queues block rollout`() =
    runBlocking<Unit> {
      items =
        listOf(
          workItem {
            name = "workItems/active"
            queue = QUEUE
            state = WorkItem.State.RUNNING
          }
        )
      assertFailsWith<IllegalStateException> { drained() }
    }

  @Test
  fun `unknown queue state blocks rollout`() =
    runBlocking<Unit> {
      items =
        listOf(
          WorkItem.newBuilder()
            .setName("workItems/unknown")
            .setQueue(QUEUE)
            .setStateValue(999)
            .build()
        )
      assertFailsWith<IllegalStateException> { drained() }
    }

  @Test
  fun `nonterminal upload on a later page blocks rollout`() =
    runBlocking<Unit> {
      uploadRows =
        uploadRows +
          uploadRows.single().copy {
            name = "$UPLOAD-next"
            state = RawImpressionUpload.State.ACTIVE
          }
      assertFailsWith<IllegalStateException> { drained() }
      assertThat(uploadReads).isEqualTo(2)
    }

  @Test
  fun `active sibling under terminal upload blocks rollout`() =
    runBlocking<Unit> {
      modelState = RawImpressionUploadModelLine.State.LABELING
      assertFailsWith<IllegalStateException> { drained() }
    }

  @Test
  fun `unfinished healing blocks rollout`() =
    runBlocking<Unit> {
      operations =
        listOf(
          uploadHealingOperation {
            name = "operations/pending"
            state = UploadHealingOperation.State.APPROVAL_REQUIRED
          }
        )
      assertFailsWith<IllegalStateException> { drained() }
    }

  @Test
  fun `query failure does not report drained`() =
    runBlocking<Unit> {
      queryFails = true
      assertFailsWith<StatusException> { drained() }
    }

  @Test
  fun `repeated page tokens fail closed`() =
    runBlocking<Unit> {
      repeatedToken = true
      assertFailsWith<IllegalStateException> { drained() }
    }

  @Test
  fun `one exact generation zero-membership smoke verifies successful coverage`() =
    runBlocking<Unit> { smoke() }

  @Test
  fun `wrong smoke generation fails`() =
    runBlocking<Unit> {
      assertFailsWith<IllegalStateException> {
        checker().requireZeroMembership("dataProviders/dp", DONE, 13, MODEL, 2)
      }
    }

  @Test
  fun `missing smoke shard fails`() =
    runBlocking<Unit> {
      shardRows = shardRows.take(1)
      assertFailsWith<IllegalStateException> { smoke() }
    }

  @Test
  fun `ranking in zero smoke fails`() =
    runBlocking<Unit> {
      ranked = true
      assertFailsWith<IllegalStateException> { smoke() }
    }

  @Test
  fun `duplicated smoke file coverage fails`() =
    runBlocking<Unit> {
      labelFiles = labelFiles + labelFiles.first()
      assertFailsWith<IllegalStateException> { smoke() }
    }

  companion object {
    private const val UPLOAD = "dataProviders/dp/rawImpressionUploads/upload"
    private const val DONE = "gs://test/vid-labeling-rollout-smoke/run/done"
    private const val MODEL = "modelProviders/mp/modelSuites/ms/modelLines/ml"
    private const val QUEUE = "queues/vid-labeler-queue"
  }
}
