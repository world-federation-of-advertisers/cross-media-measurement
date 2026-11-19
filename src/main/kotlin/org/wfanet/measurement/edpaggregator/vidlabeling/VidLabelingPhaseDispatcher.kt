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

import io.grpc.Status
import io.grpc.StatusException
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span
import java.security.MessageDigest
import java.util.UUID
import java.util.logging.Logger
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.telemetry.W3CTraceContext
import org.wfanet.measurement.common.telemetry.XmmTraceAttributes
import org.wfanet.measurement.edpaggregator.VidLabelingRpcThrottlers
import org.wfanet.measurement.edpaggregator.rawimpressions.RawImpressionFileBinPacker
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceAttributes
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceLogging
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidRankBuilderParams
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateVidLabelingJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.createVidLabelingJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelingJob
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.createWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem

/** Publishes complete file-batched labeling work before advancing a parent phase. */
class VidLabelingPhaseDispatcher(
  private val upload: String,
  private val modelLine: String,
  private val queue: String,
  private val template: VidLabelerParams,
  private val maxFileBatchSizeBytes: Long,
  private val files: RawImpressionUploadFileServiceCoroutineStub,
  private val jobs: VidLabelingJobServiceCoroutineStub,
  private val workItems: WorkItemsCoroutineStub,
  private val throttlers: VidLabelingRpcThrottlers,
  private val traceContext: () -> Map<String, String> = W3CTraceContext::inject,
  private val maxJobsPerBatchCreate: Int = 50,
  private val publicationLogger: Logger =
    Logger.getLogger(VidLabelingPhaseDispatcher::class.java.name),
  private val rankerJobName: String? = null,
) {
  init {
    require(maxFileBatchSizeBytes > 0L)
    require(maxJobsPerBatchCreate > 0)
    require(queue.isNotEmpty()) { "VID labeler queue must be configured" }
  }

  suspend fun dispatch(): Int {
    val inputs = mutableListOf<RawImpressionUploadFile>()
    var token = ""
    do {
      val page =
        throttlers.metadataRead.onReady {
          try {
            files.listRawImpressionUploadFiles(
              listRawImpressionUploadFilesRequest {
                parent = upload
                pageToken = token
              }
            )
          } catch (e: StatusException) {
            throw e
          }
        }
      inputs += page.rawImpressionUploadFilesList
      token = page.nextPageToken
    } while (token.isNotEmpty())
    val batches = RawImpressionFileBinPacker.pack(inputs, maxFileBatchSizeBytes)
    val selected = mutableListOf<org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob>()
    for (group in batches.withIndex().chunked(maxJobsPerBatchCreate)) {
      val response =
        throttlers.metadataWrite.onReady {
          try {
            jobs.batchCreateVidLabelingJobs(
              batchCreateVidLabelingJobsRequest {
                parent = upload
                for ((index, batch) in group) requests += createVidLabelingJobRequest {
                  parent = upload
                  requestId = stableUuid("$upload|$modelLine|labelingJob|$index")
                  vidLabelingJob = vidLabelingJob {
                    cmmsModelLines += modelLine
                    rawImpressionUploadFiles += batch
                  }
                }
              }
            )
          } catch (e: StatusException) {
            throw e
          }
        }
      check(response.vidLabelingJobsCount == group.size)
      selected += response.vidLabelingJobsList
      for (job in response.vidLabelingJobsList) {
        logPublication("edpa.vid_labeling.rank.vid_labeling_job", job.name, "resolved")
      }
    }
    for (job in selected) {
      var outcome = "created"
      try {
        throttlers.controlPlane.onReady {
          workItems.createWorkItem(
            createWorkItemRequest {
              workItemId = WorkItemIds.forVidLabeler(job.name)
              workItem = workItem {
                this.queue = this@VidLabelingPhaseDispatcher.queue
                workItemParams =
                  workItemParams {
                      appParams = template.copy { vidLabelingJob = job.name }.pack()
                      this.traceContext.putAll(this@VidLabelingPhaseDispatcher.traceContext())
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
      logPublication(
        "edpa.vid_labeling.rank.work_item",
        job.name,
        outcome,
        "workItems/" + WorkItemIds.forVidLabeler(job.name),
      )
    }
    return selected.size
  }

  private fun logPublication(
    event: String,
    job: String,
    outcome: String,
    workItem: String? = null,
  ) {
    val attributes =
      Attributes.builder()
        .put(VidLabelingTraceAttributes.VID_LABELING_JOB_NAME, job)
        .put(XmmTraceAttributes.OUTCOME, outcome)
    if (workItem != null) attributes.put(XmmTraceAttributes.WORK_ITEM_NAME, workItem)
    Span.current().addEvent(event, attributes.build())
    VidLabelingTraceLogging.log(
      publicationLogger,
      event,
      VidLabelingTraceAttributes.RAW_IMPRESSION_UPLOAD_NAME_STRING to upload,
      VidLabelingTraceAttributes.MODEL_LINE_NAME_STRING to modelLine,
      VidLabelingTraceAttributes.RANKER_JOB_NAME_STRING to rankerJobName,
      VidLabelingTraceAttributes.VID_LABELING_JOB_NAME_STRING to job,
      XmmTraceAttributes.WORK_ITEM_NAME_STRING to workItem,
      XmmTraceAttributes.LIFECYCLE_STAGE_STRING to "rank_finalize",
      XmmTraceAttributes.OUTCOME_STRING to outcome,
    )
  }

  private fun stableUuid(seed: String): String {
    val bytes = MessageDigest.getInstance("MD5").digest(seed.toByteArray())
    bytes[6] = ((bytes[6].toInt() and 0x0f) or 0x40).toByte()
    bytes[8] = ((bytes[8].toInt() and 0x3f) or 0x80).toByte()
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    return UUID(buffer.long, buffer.long).toString()
  }

  companion object {
    fun templateFromRankBuilder(params: VidRankBuilderParams): VidLabelerParams {
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
