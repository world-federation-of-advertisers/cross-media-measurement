// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator.vidlabeling

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import io.grpc.Status
import io.grpc.StatusException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.edpaggregator.testing.VidLabelingRpcThrottlersTestHelper
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateVidLabelingJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateVidLabelingJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelerParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.copy

class MemoizedVidLabelingJobDispatcherTest {
  private val filesByUpload = mutableMapOf<String, List<RawImpressionUploadFile>>()
  private val jobsByRequest = linkedMapOf<String, VidLabelingJob>()
  private val workItems = linkedMapOf<String, WorkItem>()
  private val outcomes = mutableListOf<String>()
  private val pages = mutableListOf<String>()
  private var batchCalls = 0
  private var failBatch = -1
  private var shortResponse = false
  private var losePublicationResponse = false
  private val template = vidLabelerParams {
    dataProvider = "dataProviders/dp"
    modelLines += MODEL
    modelBlobPaths[MODEL] = "gs://models/model"
  }

  @get:Rule
  val server = GrpcTestServerRule {
    addService(
      object :
        RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineImplBase() {
        override suspend fun listRawImpressionUploadFiles(
          request: ListRawImpressionUploadFilesRequest
        ) = listRawImpressionUploadFilesResponse {
          pages += request.pageToken
          val offset = request.pageToken.toIntOrNull() ?: 0
          val files = filesByUpload[request.parent].orEmpty()
          rawImpressionUploadFiles += files.drop(offset).take(2)
          if (offset + 2 < files.size) nextPageToken = (offset + 2).toString()
        }
      }
    )
    addService(
      object : VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineImplBase() {
        override suspend fun batchCreateVidLabelingJobs(
          request: BatchCreateVidLabelingJobsRequest
        ) = batchCreateVidLabelingJobsResponse {
          batchCalls++
          if (batchCalls == failBatch) throw Status.UNAVAILABLE.asException()
          if (!shortResponse) {
            for (create in request.requestsList) {
              vidLabelingJobs +=
                jobsByRequest.getOrPut(create.requestId) {
                  create.vidLabelingJob.copy {
                    name = "${request.parent}/vidLabelingJobs/job-${jobsByRequest.size}"
                  }
                }
            }
          }
        }
      }
    )
    addService(
      object : WorkItemsGrpcKt.WorkItemsCoroutineImplBase() {
        override suspend fun createWorkItem(request: CreateWorkItemRequest): WorkItem {
          if (workItems.containsKey(request.workItemId)) throw Status.ALREADY_EXISTS.asException()
          val created =
            request.workItem.copy {
              name = "workItems/${request.workItemId}"
              state = WorkItem.State.QUEUED
            }
          workItems[request.workItemId] = created
          if (losePublicationResponse) {
            losePublicationResponse = false
            throw Status.UNAVAILABLE.asException()
          }
          return created
        }
      }
    )
  }

  private fun seedFiles(upload: String = UPLOAD) {
    filesByUpload[upload] =
      (0..4).map { index ->
        rawImpressionUploadFile {
          name = "$upload/files/f$index"
          blobUri = "gs://input/${upload.substringAfterLast('/')}/$index"
          sizeBytes = 100
        }
      }
  }

  private fun dispatcher(upload: String = UPLOAD, model: String = MODEL) =
    MemoizedVidLabelingJobDispatcher(
      rawImpressionUploadFilesStub =
        RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub(
          server.channel
        ),
      vidLabelingJobsStub =
        VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub(server.channel),
      workItemsStub = WorkItemsGrpcKt.WorkItemsCoroutineStub(server.channel),
      rawImpressionUpload = upload,
      modelLine = model,
      vidLabelerParamsTemplate =
        template.copy {
          modelLines.clear()
          modelLines += model
          modelBlobPaths.clear()
          modelBlobPaths[model] = "gs://models/model"
        },
      vidLabelerQueue = "queues/labeling",
      maxFileBatchSizeBytes = 100,
      rpcThrottlers = VidLabelingRpcThrottlersTestHelper.alwaysReady(),
      traceContextProvider = { mapOf("traceparent" to "test-trace") },
      maxJobsPerBatchCreate = 2,
      onWorkItemPublished = { _, outcome -> outcomes += outcome },
    )

  @Test
  fun `dispatch packs every paginated file and preserves labeling payload and trace`() =
    runBlocking<Unit> {
      seedFiles()
      assertThat(dispatcher().dispatch()).isEqualTo(5)
      assertThat(batchCalls).isEqualTo(3)
      assertThat(pages).containsExactly("", "2", "4").inOrder()
      assertThat(jobsByRequest.values.flatMap { it.rawImpressionUploadFilesList })
        .containsExactlyElementsIn(filesByUpload.getValue(UPLOAD).map { it.name })
      for (job in jobsByRequest.values) {
        assertThat(job.cmmsModelLinesList).containsExactly(MODEL)
        val item = workItems.getValue(WorkItemIds.forVidLabeler(job.name))
        assertThat(item.queue).isEqualTo("queues/labeling")
        val params = item.workItemParams.unpack(WorkItem.WorkItemParams::class.java)
        assertThat(params.traceContextMap).containsEntry("traceparent", "test-trace")
        assertThat(params.appParams.unpack(VidLabelerParams::class.java))
          .isEqualTo(template.copy { vidLabelingJob = job.name })
      }
    }

  @Test
  fun `dispatch recovers omitted batches after interrupted job creation`() =
    runBlocking<Unit> {
      seedFiles()
      failBatch = 2
      assertFailsWith<StatusException> { dispatcher().dispatch() }
      assertThat(jobsByRequest).hasSize(2)
      assertThat(workItems).isEmpty()
      val original = jobsByRequest.toMap()
      failBatch = -1
      assertThat(dispatcher().dispatch()).isEqualTo(5)
      assertThat(jobsByRequest).hasSize(5)
      assertThat(workItems).hasSize(5)
      for ((id, job) in original) assertThat(jobsByRequest[id]).isEqualTo(job)
    }

  @Test
  fun `dispatch recovers lost publication responses without duplicate work`() =
    runBlocking<Unit> {
      seedFiles()
      losePublicationResponse = true
      assertFailsWith<StatusException> { dispatcher().dispatch() }
      assertThat(workItems).hasSize(1)
      assertThat(dispatcher().dispatch()).isEqualTo(5)
      assertThat(workItems).hasSize(5)
      assertThat(outcomes.count { it == "already_exists" }).isEqualTo(1)
      val original = workItems.toMap()
      assertThat(dispatcher().dispatch()).isEqualTo(5)
      assertThat(workItems).containsExactlyEntriesIn(original)
    }

  @Test
  fun `dispatch keeps request identities separate across uploads and model lines`() =
    runBlocking<Unit> {
      seedFiles()
      dispatcher().dispatch()
      val other = "$UPLOAD-other"
      seedFiles(other)
      dispatcher(upload = other).dispatch()
      dispatcher(model = "$MODEL-other").dispatch()
      assertThat(jobsByRequest).hasSize(15)
      assertThat(workItems).hasSize(15)
    }

  @Test
  fun `dispatch rejects short job creation responses before publishing`() =
    runBlocking<Unit> {
      seedFiles()
      shortResponse = true
      val error = assertFailsWith<IllegalStateException> { dispatcher().dispatch() }
      assertThat(error).hasMessageThat().contains("returned 0 jobs")
      assertThat(workItems).isEmpty()
    }

  @Test
  fun `dispatch returns no jobs for an empty registered file list`() =
    runBlocking<Unit> {
      assertThat(dispatcher().dispatch()).isEqualTo(0)
      assertThat(batchCalls).isEqualTo(0)
      assertThat(workItems).isEmpty()
    }

  companion object {
    private const val UPLOAD = "dataProviders/dp/rawImpressionUploads/u"
    private const val MODEL = "modelProviders/mp/modelSuites/ms/modelLines/ml"
  }
}
