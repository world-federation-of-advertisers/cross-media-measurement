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

package org.wfanet.measurement.edpaggregator.vidlabeling

import io.grpc.Status
import io.grpc.StatusException
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.UUID
import kotlinx.coroutines.flow.collect
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.edpaggregator.VidLabelingRpcThrottlers
import org.wfanet.measurement.edpaggregator.rawimpressions.RawImpressionFileBinPacker
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidRankBuilderParams
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateVidLabelingJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.createVidLabelingJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelingJob
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.createWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem

/** Creates and publishes deterministic file batches for memoized VID labeling. */
class MemoizedVidLabelingJobDispatcher(
  private val rawImpressionUploadsStub: RawImpressionUploadServiceCoroutineStub,
  private val rawImpressionUploadFilesStub: RawImpressionUploadFileServiceCoroutineStub,
  private val vidLabelingJobsStub: VidLabelingJobServiceCoroutineStub,
  private val workItemsStub: WorkItemsCoroutineStub,
  private val rawImpressionUpload: String,
  private val modelLine: String,
  private val vidLabelerParamsTemplate: VidLabelerParams,
  private val vidLabelerQueue: String,
  private val maxFileBatchSizeBytes: Long,
  private val rpcThrottlers: VidLabelingRpcThrottlers,
  private val traceContextProvider: () -> Map<String, String>,
  private val maxJobsPerBatchCreate: Int = MAX_JOBS_PER_REQUEST,
  private val onJobResolved: (VidLabelingJob) -> Unit = {},
  private val onWorkItemPublished: (VidLabelingJob, String) -> Unit = { _, _ -> },
) {
  init {
    requireBoundedResourceName(rawImpressionUpload, "upload resource name")
    requireBoundedResourceName(modelLine, "model-line resource name")
    require(maxFileBatchSizeBytes > 0) { "maxFileBatchSizeBytes must be > 0" }
    require(maxJobsPerBatchCreate > 0) { "maxJobsPerBatchCreate must be > 0" }
    require(vidLabelerQueue.isNotEmpty()) { "vidLabelerQueue must be configured" }
  }

  /** Returns the number of labeling jobs whose WorkItems were resolved. */
  suspend fun dispatch(): Int {
    val upload =
      rpcThrottlers.metadataRead.onReady {
        rawImpressionUploadsStub.getRawImpressionUpload(
          getRawImpressionUploadRequest { name = rawImpressionUpload }
        )
      }
    check(upload.registrationComplete) {
      "RawImpressionUpload $rawImpressionUpload registration is incomplete"
    }
    val files = listUploadFiles()
    if (files.isEmpty()) return 0
    files.forEach { requireBoundedResourceName(it.name, "file resource name") }
    val batches =
      RawImpressionFileBinPacker.pack(
        files,
        maxFileBatchSizeBytes,
        maxFilesPerBatch = MAX_FILES_PER_JOB,
      )
    val jobs = createVidLabelingJobs(batches)
    jobs.forEach { publishVidLabelerWorkItem(it) }
    return jobs.size
  }

  private fun requireBoundedResourceName(name: String, field: String) {
    require(name.isNotEmpty() && name.length <= MAX_RESOURCE_NAME_LENGTH) {
      "$field must contain between 1 and $MAX_RESOURCE_NAME_LENGTH ASCII characters"
    }
    require(name.all { it.code <= 0x7F }) { "$field must contain only ASCII characters" }
  }

  /** Lists every (non-deleted) `RawImpressionUploadFile` under this upload. */
  private suspend fun listUploadFiles(): List<RawImpressionUploadFile> {
    val files = mutableListOf<RawImpressionUploadFile>()
    rawImpressionUploadFilesStub
      .listResources { pageToken: String ->
        val response =
          rpcThrottlers.metadataRead.onReady {
            rawImpressionUploadFilesStub.listRawImpressionUploadFiles(
              listRawImpressionUploadFilesRequest {
                parent = rawImpressionUpload
                this.pageToken = pageToken
              }
            )
          }
        ResourceList(response.rawImpressionUploadFilesList, response.nextPageToken)
      }
      .collect { page -> files.addAll(page) }
    return files
  }

  /**
   * Creates deterministic batch-slot jobs against the sealed upload manifest. Equal membership
   * replays existing jobs; the metadata service rejects changed file/model-line membership. Packing
   * limits must remain identical across retries. Bounded resource names and conservative file/job
   * counts keep requests and original Create responses below the metadata receiver limit.
   */
  private suspend fun createVidLabelingJobs(batches: List<List<String>>): List<VidLabelingJob> {
    val created = mutableListOf<VidLabelingJob>()
    var pending = batchCreateVidLabelingJobsRequest { parent = rawImpressionUpload }

    suspend fun flush() {
      if (pending.requestsCount == 0) return
      val request = pending
      val response =
        rpcThrottlers.metadataWrite.onReady {
          vidLabelingJobsStub.batchCreateVidLabelingJobs(request)
        }
      check(response.vidLabelingJobsCount == request.requestsCount) {
        "BatchCreateVidLabelingJobs returned ${response.vidLabelingJobsCount} jobs for " +
          "${request.requestsCount} requests"
      }
      created.addAll(response.vidLabelingJobsList)
      response.vidLabelingJobsList.forEach(onJobResolved)
      pending = batchCreateVidLabelingJobsRequest { parent = rawImpressionUpload }
    }

    for ((batchIndex, batch) in batches.withIndex()) {
      val create = createVidLabelingJobRequest {
        parent = rawImpressionUpload
        vidLabelingJob = vidLabelingJob {
          cmmsModelLines += modelLine
          rawImpressionUploadFiles += batch
        }
        requestId = labelingJobRequestId(batchIndex)
      }
      if (pending.requestsCount == minOf(maxJobsPerBatchCreate, MAX_JOBS_PER_REQUEST)) {
        flush()
      }
      pending = pending.copy { requests += create }
    }
    flush()
    return created
  }

  /**
   * Publishes one Phase-2 WorkItem for [job], copying [vidLabelerParamsTemplate] with the job's
   * name. The TEE resolves the job's `raw_impression_upload_files` by getting the `VidLabelingJob`,
   * so they are not duplicated onto the WorkItem params. Tolerates `ALREADY_EXISTS` so a
   * redelivered last-job-out is a no-op.
   */
  private suspend fun publishVidLabelerWorkItem(job: VidLabelingJob) {
    val params = vidLabelerParamsTemplate.copy { vidLabelingJob = job.name }
    val workItemId = WorkItemIds.forVidLabeler(job.name)
    var outcome = "created"
    try {
      rpcThrottlers.controlPlane.onReady {
        workItemsStub.createWorkItem(
          createWorkItemRequest {
            this.workItemId = workItemId
            workItem = workItem {
              queue = vidLabelerQueue
              workItemParams =
                workItemParams {
                    appParams = params.pack()
                    traceContext.putAll(traceContextProvider())
                  }
                  .pack()
            }
          }
        )
      }
    } catch (e: StatusException) {
      if (e.status.code != Status.Code.ALREADY_EXISTS) throw e
      outcome = "already_exists"
    }
    onWorkItemPublished(job, outcome)
  }

  /**
   * Request ID for an immutable batch slot, keyed by upload, model line and index. Registration
   * must be complete and all packing limits unchanged before redispatching the same slots. Changed
   * membership is a conflict, never an idempotent replay.
   */
  private fun labelingJobRequestId(batchIndex: Int): String =
    deterministicUuid("$rawImpressionUpload|$modelLine|labelingJob|$batchIndex")

  private fun deterministicUuid(seed: String): String {
    val bytes = MessageDigest.getInstance("MD5").digest(seed.toByteArray(Charsets.UTF_8))
    bytes[6] = ((bytes[6].toInt() and 0x0f) or 0x40).toByte() // version 4
    bytes[8] = ((bytes[8].toInt() and 0x3f) or 0x80).toByte() // variant 10xx
    val buffer = ByteBuffer.wrap(bytes)
    return UUID(buffer.long, buffer.long).toString()
  }

  companion object {
    private const val MAX_RESOURCE_NAME_LENGTH = 512
    // Each bounded ASCII file name costs at most 515 wire bytes (tag + length + name).
    // 1,000 names plus 4 KiB for the model line, returned UUID name, timestamps and etag fit
    // within 512 KiB. Five jobs plus 1 KiB of request/response framing per job fit within 3 MiB,
    // leaving at least 1 MiB below the 4 MiB receiver limit. Serialization tests verify these
    // conservative estimates; payload sizing never runs during dispatch.
    private const val MAX_FILES_PER_JOB = 1_000
    private const val MAX_JOBS_PER_REQUEST = 5

    fun buildParamsTemplate(params: VidRankBuilderParams): VidLabelerParams {
      require(params.modelBlobPath.isNotEmpty()) { "model_blob_path must be set" }
      require(params.hasRawImpressionStorageParams()) {
        "raw_impression_storage_params must be set"
      }
      require(params.hasVidLabeledImpressionsStorageParams()) {
        "vid_labeled_impressions_storage_params must be set"
      }
      require(params.hasModelStorageParams()) { "model_storage_params must be set" }
      // Fail here rather than in the Phase-2 TEE: an unset value means a producer in the Phase-0 ->
      // Phase-1 chain dropped the pass-through, which would otherwise surface only as a
      // crash-looping VidLabeler nacking every fan-out WorkItem.
      require(params.populationSpecBlobUri.isNotEmpty()) { "population_spec_blob_uri must be set" }
      require(params.eventTemplateVersion > 0) { "event_template_version must be set" }
      return vidLabelerParams {
        dataProvider = params.dataProvider
        rawImpressionsStorageParams =
          VidLabelerParamsKt.storageParams {
            gcsProjectId = params.rawImpressionStorageParams.gcsProjectId
            impressionsBlobPrefix = params.rawImpressionStorageParams.blobPrefix
          }
        vidLabeledImpressionsStorageParams =
          VidLabelerParamsKt.storageParams {
            gcsProjectId = params.vidLabeledImpressionsStorageParams.gcsProjectId
            impressionsBlobPrefix = params.vidLabeledImpressionsStorageParams.blobPrefix
          }
        modelLineConfigs.put(
          params.modelLine,
          VidLabelerParamsKt.modelLineConfig {
            labelerInputFieldMapping.addAll(params.labelerInputFieldMappingList)
            eventTemplateFieldMapping.putAll(params.eventTemplateFieldMappingMap)
            // Phase-2 requires the event-template descriptor to build the labeled output; stamp it
            // (and its type) onto the memoized VidLabeler ModelLineConfig.
            eventTemplateDescriptorBlobUri = params.eventTemplateDescriptorBlobUri
            eventTemplateType = params.eventTemplateType
            // Phase-2 resolves each assigned VID's population attributes from this spec and rejects
            // a WorkItem without it; stamp it alongside the descriptor.
            populationSpecBlobUri = params.populationSpecBlobUri
            eventTemplateVersion = params.eventTemplateVersion
            // Per-impression entity-key columns for Phase-2 (read per row from these columns).
            requiredEntityKeyFieldMapping.putAll(params.requiredEntityKeyFieldMappingMap)
            optionalEntityKeyFieldMapping.putAll(params.optionalEntityKeyFieldMappingMap)
            // Active-window pass-through (OPTIONAL): Phase-2 drops impressions outside this window.
            if (params.hasActiveStartTime()) {
              activeStartTime = params.activeStartTime
            }
            if (params.hasActiveEndTime()) {
              activeEndTime = params.activeEndTime
            }
          },
        )
        // Shared payload at the top level (both Phase-2 paths); the memoized WorkItem covers
        // exactly
        // one model line. `vid_labeling_job` is stamped per fan-out WorkItem by the ranker.
        modelLines += params.modelLine
        modelBlobPaths.put(params.modelLine, params.modelBlobPath)
        modelStorageParams =
          VidLabelerParamsKt.storageParams {
            gcsProjectId = params.modelStorageParams.gcsProjectId
            impressionsBlobPrefix = params.modelStorageParams.blobPrefix
          }
        // MemoizedParams carries only the memoized-specific rank-index storage; its presence
        // selects
        // the memoized path.
        memoizedParams =
          VidLabelerParamsKt.memoizedParams {
            vidRankMapStorageParams =
              VidLabelerParamsKt.storageParams {
                gcsProjectId = params.vidRankMapStorageParams.gcsProjectId
                impressionsBlobPrefix = params.vidRankMapStorageParams.blobPrefix
              }
          }
      }
    }
  }
}
