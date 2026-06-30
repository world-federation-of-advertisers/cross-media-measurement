/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.securecomputation.deploy.gcloud.deadletter

import com.google.protobuf.Parser
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.UUID
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.collect
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJob
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.SubpoolAssignerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidRankBuilderParams
import org.wfanet.measurement.edpaggregator.v1alpha.getPoolAssignmentJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getRankerJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getVidLabelingJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markPoolAssignmentJobFailedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRankerJobFailedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineFailedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markVidLabelingJobFailedRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemsGrpcKt
import org.wfanet.measurement.internal.securecomputation.controlplane.failWorkItemRequest
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.service.Errors

/**
 * Service that listens to a dead letter queue and marks failed work items as FAILED in the
 * database.
 *
 * This service subscribes to a Google PubSub dead letter queue where messages are sent after a TEE
 * application fails to process them after multiple attempts. It processes each message by
 * extracting the work item ID and calling the WorkItems API to mark the item as failed.
 *
 * After failing the WorkItem it also marks the EDP-Aggregator resource(s) that the WorkItem's phase
 * params reference (the per-phase job and the parent `RawImpressionUploadModelLine`) FAILED, so a
 * TEE that exhausts its retries surfaces as a terminal failure on the EDPA side too. This EDPA
 * marking is best-effort; the worker apps still self-mark FAILED on a non-retry-exhausting failure
 * today, but that self-marking should be removed once this DLQ path lands (cleanup tracked
 * separately).
 *
 * @param subscriptionId The subscription ID for the dead letter queue.
 * @param queueSubscriber A client that manages connections and interactions with the queue.
 * @param parser Parser used to parse serialized queue messages into WorkItem instances.
 * @param workItemsStub gRPC stub for calling the WorkItems service to fail work items.
 * @param poolAssignmentJobsStub EDPA stub used to mark Phase-0 `PoolAssignmentJob`s FAILED.
 * @param rankerJobsStub EDPA stub used to mark Phase-1 `RankerJob`s FAILED.
 * @param vidLabelingJobsStub EDPA stub used to mark Phase-2 `VidLabelingJob`s FAILED.
 * @param rawImpressionUploadModelLinesStub EDPA stub used to resolve and mark the parent
 *   `RawImpressionUploadModelLine` FAILED.
 */
class DeadLetterQueueListener(
  private val subscriptionId: String,
  private val queueSubscriber: QueueSubscriber,
  private val parser: Parser<WorkItem>,
  private val workItemsStub: WorkItemsGrpcKt.WorkItemsCoroutineStub,
  private val poolAssignmentJobsStub: PoolAssignmentJobServiceCoroutineStub,
  private val rankerJobsStub: RankerJobServiceCoroutineStub,
  private val vidLabelingJobsStub: VidLabelingJobServiceCoroutineStub,
  private val rawImpressionUploadModelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub,
) : AutoCloseable {

  /** Starts the listener by subscribing to the dead letter queue. */
  suspend fun run() {
    logger.info("Starting DeadLetterQueueListener for subscription: $subscriptionId")
    receiveAndProcessMessages()
  }

  /**
   * Begins listening for messages on the dead letter queue. Each message is processed as it
   * arrives. If an error occurs during processing, it is logged and handling continues.
   */
  private suspend fun receiveAndProcessMessages() {
    val messageChannel: ReceiveChannel<QueueSubscriber.QueueMessage<WorkItem>> =
      queueSubscriber.subscribe(subscriptionId, parser)

    logger.info("Successfully subscribed to dead letter queue: $subscriptionId")

    for (message: QueueSubscriber.QueueMessage<WorkItem> in messageChannel) {
      try {
        processMessage(message)
      } catch (e: Exception) {
        logger.log(Level.SEVERE, "Unexpected error processing dead letter queue message", e)
        // Continue processing other messages even if one fails
      }
    }
  }

  /**
   * Processes a message from the dead letter queue by calling the WorkItems API to mark it as
   * failed, then (best-effort) marking the EDP-Aggregator resources the WorkItem references FAILED.
   *
   * Messages with empty work item names are acknowledged and skipped. If the work item is already
   * in a FAILED state or not found, the message is acknowledged. Other errors result in the message
   * being nacked for retry. The EDPA marking is best-effort and never changes the ack/nack decision
   * (a dead-lettered message is already terminal).
   *
   * @param queueMessage The message received from the dead letter queue.
   */
  private suspend fun processMessage(queueMessage: QueueSubscriber.QueueMessage<WorkItem>) {
    val workItem = queueMessage.body

    if (workItem.name.isEmpty()) {
      logger.warning("Received message with empty WorkItem name. Acknowledging and skipping.")
      queueMessage.ack()
      return
    }

    logger.fine("Processing dead letter message for work item: ${workItem.name}")

    try {
      workItemsStub.failWorkItem(failWorkItemRequest { workItemResourceId = workItem.name })
      logger.fine("Successfully marked work item as failed: ${workItem.name}")
      // Mark the EDPA resource(s) referenced by this WorkItem FAILED. Best-effort: any failure is
      // logged and swallowed so the already-terminal dead-letter message is still acked below.
      markEdpaResourcesFailed(workItem)
      queueMessage.ack()
    } catch (e: Exception) {
      when (e) {
        is StatusRuntimeException -> {
          if (e.status.code == Status.Code.NOT_FOUND) {
            logger.warning("Work item not found: ${workItem.name}. Acknowledging message.")
            queueMessage.ack()
          } else if (isAlreadyFailedError(e)) {
            logger.info(
              "Work item ${workItem.name} is already in FAILED state. Acknowledging message."
            )
            queueMessage.ack()
          } else {
            logger.log(Level.SEVERE, "Error calling WorkItems API", e)
            queueMessage.nack()
          }
        }
        else -> {
          logger.log(Level.SEVERE, "Unexpected error processing message", e)
          queueMessage.nack()
        }
      }
    }
  }

  /**
   * Marks the EDP-Aggregator resource(s) referenced by [workItem]'s phase params FAILED,
   * dispatching on the `app_params` type:
   * - [SubpoolAssignerParams] (Phase 0) -> `MarkPoolAssignmentJobFailed` + parent model line,
   * - [VidRankBuilderParams] (Phase 1) -> `MarkRankerJobFailed` + parent model line,
   * - [VidLabelerParams] (Phase 2) -> when memoized, `MarkVidLabelingJobFailed` + parent model
   *   line; otherwise (non-memoized) the parent model line only,
   * - anything else (e.g. `ResultsFulfiller` params) -> no EDPA marking.
   *
   * Every call is best-effort and never throws: a failure to mark an EDPA resource must not re-nack
   * an already-terminal dead-letter message.
   */
  private suspend fun markEdpaResourcesFailed(workItem: WorkItem) {
    val appParams =
      try {
        workItem.workItemParams.unpack(WorkItemParams::class.java).appParams
      } catch (e: Exception) {
        logger.log(
          Level.WARNING,
          "Could not unpack WorkItemParams for ${workItem.name}; skipping EDPA marking",
          e,
        )
        return
      }

    val errorMessage = "WorkItem ${workItem.name} dead-lettered (retries exhausted)"
    when (appParams.typeUrl.substringAfterLast('/')) {
      SUBPOOL_ASSIGNER_PARAMS_TYPE -> {
        val params = appParams.unpack(SubpoolAssignerParams::class.java)
        markPoolAssignmentJobFailedBestEffort(params.poolAssignmentJob, errorMessage)
        markModelLineFailedBestEffort(params.rawImpressionUpload, params.modelLine, errorMessage)
      }
      VID_RANK_BUILDER_PARAMS_TYPE -> {
        val params = appParams.unpack(VidRankBuilderParams::class.java)
        markRankerJobFailedBestEffort(params.rankerJob, errorMessage)
        markModelLineFailedBestEffort(params.rawImpressionUpload, params.modelLine, errorMessage)
      }
      VID_LABELER_PARAMS_TYPE -> {
        val params = appParams.unpack(VidLabelerParams::class.java)
        if (params.hasMemoizedParams()) {
          // Memoized Phase-2 path: the WorkItem references its `VidLabelingJob` directly. Mark the
          // job FAILED, and mark the parent `RawImpressionUploadModelLine` resolved from the upload
          // (the job's parent resource) plus the memoized model line.
          val memoizedParams = params.memoizedParams
          markVidLabelingJobFailedBestEffort(memoizedParams.vidLabelingJob, errorMessage)
          val rawImpressionUpload =
            memoizedParams.vidLabelingJob.substringBeforeLast("/vidLabelingJobs/", "")
          markModelLineFailedBestEffort(rawImpressionUpload, memoizedParams.modelLine, errorMessage)
        } else {
          // Non-memoized Phase-2 path: the WorkItem references the upload + a single override model
          // line (no `VidLabelingJob`), so only the parent `RawImpressionUploadModelLine` is
          // marked.
          val modelLine = params.overrideModelLinesList.singleOrNull()
          if (modelLine == null) {
            logger.warning(
              "VidLabelerParams for ${workItem.name} has ${params.overrideModelLinesCount} model " +
                "lines (expected exactly one); skipping model-line marking"
            )
          } else {
            markModelLineFailedBestEffort(params.rawImpressionUpload, modelLine, errorMessage)
          }
        }
      }
      else -> {
        logger.fine(
          "WorkItem ${workItem.name} has app_params type ${appParams.typeUrl}; no EDPA marking"
        )
      }
    }
  }

  /**
   * Best-effort `MarkPoolAssignmentJobFailed`; logs and swallows any failure. `Get`s the job first
   * to obtain its current `etag` (REQUIRED on `MarkPoolAssignmentJobFailedRequest`) and to skip the
   * mark when the job is already in a terminal state.
   */
  private suspend fun markPoolAssignmentJobFailedBestEffort(name: String, errorMessage: String) {
    if (name.isEmpty()) return
    try {
      val job =
        poolAssignmentJobsStub.getPoolAssignmentJob(
          getPoolAssignmentJobRequest { this.name = name }
        )
      if (
        job.state == PoolAssignmentJob.State.SUCCEEDED ||
          job.state == PoolAssignmentJob.State.FAILED
      ) {
        logger.info("PoolAssignmentJob $name already terminal (${job.state}); skipping FAILED mark")
        return
      }
      poolAssignmentJobsStub.markPoolAssignmentJobFailed(
        markPoolAssignmentJobFailedRequest {
          this.name = name
          this.etag = job.etag
          this.errorMessage = errorMessage.take(MAX_ERROR_MESSAGE)
          // TODO(#4078): MarkPoolAssignmentJobFailedRequest has no `request_id` field on the
          // current base; #4078 adds it (flipping it OPTIONAL -> REQUIRED). Once it lands, set
          // requestId = deterministicUuid("MarkPoolAssignmentJobFailed:$name") for AIP-155
          // idempotency on Pub/Sub redelivery, mirroring MarkVidLabelingJobFailed.
        }
      )
      logger.info("Marked PoolAssignmentJob $name FAILED from dead-letter queue")
    } catch (e: Exception) {
      logger.log(Level.WARNING, "Best-effort MarkPoolAssignmentJobFailed($name) failed", e)
    }
  }

  /**
   * Best-effort `MarkRankerJobFailed`; logs and swallows any failure. `Get`s the job first to
   * obtain its current `etag` (REQUIRED on `MarkRankerJobFailedRequest`) and to skip the mark when
   * the job is already in a terminal state.
   */
  private suspend fun markRankerJobFailedBestEffort(name: String, errorMessage: String) {
    if (name.isEmpty()) return
    try {
      val job = rankerJobsStub.getRankerJob(getRankerJobRequest { this.name = name })
      if (job.state == RankerJob.State.SUCCEEDED || job.state == RankerJob.State.FAILED) {
        logger.info("RankerJob $name already terminal (${job.state}); skipping FAILED mark")
        return
      }
      rankerJobsStub.markRankerJobFailed(
        markRankerJobFailedRequest {
          this.name = name
          this.etag = job.etag
          this.errorMessage = errorMessage.take(MAX_ERROR_MESSAGE)
          // AIP-155 idempotency on Pub/Sub redelivery: derived deterministically from the resource
          // + operation so every attempt for the same mark shares one idempotent result (mirrors
          // MarkVidLabelingJobFailed).
          requestId = deterministicUuid("MarkRankerJobFailed:$name")
        }
      )
      logger.info("Marked RankerJob $name FAILED from dead-letter queue")
    } catch (e: Exception) {
      logger.log(Level.WARNING, "Best-effort MarkRankerJobFailed($name) failed", e)
    }
  }

  /**
   * Best-effort `MarkVidLabelingJobFailed`; logs and swallows any failure. `Get`s the job first to
   * obtain its current `etag` (REQUIRED on `MarkVidLabelingJobFailedRequest`) and to skip the mark
   * when the job is already in a terminal state. Unlike the sibling Mark RPCs this one carries a
   * `request_id`, set to a deterministic UUID so a redelivery is idempotent.
   */
  private suspend fun markVidLabelingJobFailedBestEffort(name: String, errorMessage: String) {
    if (name.isEmpty()) return
    try {
      val job = vidLabelingJobsStub.getVidLabelingJob(getVidLabelingJobRequest { this.name = name })
      if (job.state == VidLabelingJob.State.SUCCEEDED || job.state == VidLabelingJob.State.FAILED) {
        logger.info("VidLabelingJob $name already terminal (${job.state}); skipping FAILED mark")
        return
      }
      vidLabelingJobsStub.markVidLabelingJobFailed(
        markVidLabelingJobFailedRequest {
          this.name = name
          this.etag = job.etag
          this.errorMessage = errorMessage.take(MAX_ERROR_MESSAGE)
          requestId = deterministicUuid("MarkVidLabelingJobFailed:$name")
        }
      )
      logger.info("Marked VidLabelingJob $name FAILED from dead-letter queue")
    } catch (e: Exception) {
      logger.log(Level.WARNING, "Best-effort MarkVidLabelingJobFailed($name) failed", e)
    }
  }

  /**
   * Best-effort marking of the parent `RawImpressionUploadModelLine` FAILED. Resolves the parent
   * resource name from ([rawImpressionUpload], [cmmsModelLine]) by listing the upload's model lines
   * filtered by CMMS model line (mirrors `SubpoolAssigner.getParent`), then marks it FAILED. Logs
   * and swallows any failure.
   */
  private suspend fun markModelLineFailedBestEffort(
    rawImpressionUpload: String,
    cmmsModelLine: String,
    errorMessage: String,
  ) {
    if (rawImpressionUpload.isEmpty() || cmmsModelLine.isEmpty()) return
    try {
      val parent = getParentModelLine(rawImpressionUpload, cmmsModelLine)
      if (parent == null) {
        logger.warning(
          "No RawImpressionUploadModelLine found for $cmmsModelLine under $rawImpressionUpload; " +
            "skipping model-line marking"
        )
        return
      }
      if (
        parent.state == RawImpressionUploadModelLine.State.COMPLETED ||
          parent.state == RawImpressionUploadModelLine.State.FAILED
      ) {
        logger.info(
          "RawImpressionUploadModelLine ${parent.name} already terminal (${parent.state}); " +
            "skipping FAILED mark"
        )
        return
      }
      rawImpressionUploadModelLinesStub.markRawImpressionUploadModelLineFailed(
        markRawImpressionUploadModelLineFailedRequest {
          name = parent.name
          // `etag` is REQUIRED. Reuse the one already returned by getParentModelLine's List (the
          // RawImpressionUploadModelLine resource carries it) instead of an extra Get.
          etag = parent.etag
          this.errorMessage = errorMessage.take(MAX_ERROR_MESSAGE)
          // TODO(#4074): MarkRawImpressionUploadModelLineFailedRequest has no `request_id` field
          // yet; #4074 adds it across the 5 RawImpressionUploadModelLine Mark RPCs. Once it lands,
          // set requestId = deterministicUuid("MarkModelLineFailed:${parent.name}") for AIP-155
          // idempotency on Pub/Sub redelivery.
        }
      )
      logger.info(
        "Marked RawImpressionUploadModelLine ${parent.name} FAILED from dead-letter queue"
      )
    } catch (e: Exception) {
      logger.log(
        Level.WARNING,
        "Best-effort MarkRawImpressionUploadModelLineFailed for $cmmsModelLine under " +
          "$rawImpressionUpload failed",
        e,
      )
    }
  }

  /**
   * Returns the parent `RawImpressionUploadModelLine` for ([rawImpressionUpload], [cmmsModelLine]),
   * located via `ListRawImpressionUploadModelLines` filtered by CMMS model line (paged), or `null`
   * if absent. Mirrors `SubpoolAssigner.getParent`.
   */
  private suspend fun getParentModelLine(
    rawImpressionUpload: String,
    cmmsModelLine: String,
  ): RawImpressionUploadModelLine? {
    var parentRow: RawImpressionUploadModelLine? = null
    rawImpressionUploadModelLinesStub
      .listResources { pageToken: String ->
        val response =
          listRawImpressionUploadModelLines(
            listRawImpressionUploadModelLinesRequest {
              parent = rawImpressionUpload
              filter =
                ListRawImpressionUploadModelLinesRequestKt.filter {
                  this.cmmsModelLine = cmmsModelLine
                }
              this.pageToken = pageToken
            }
          )
        ResourceList(response.rawImpressionUploadModelLinesList, response.nextPageToken)
      }
      .collect { page ->
        page.forEach { line -> if (line.cmmsModelLine == cmmsModelLine) parentRow = line }
      }
    return parentRow
  }

  override fun close() {
    queueSubscriber.close()
  }

  companion object {
    private val logger = Logger.getLogger(DeadLetterQueueListener::class.java.name)

    private const val MAX_ERROR_MESSAGE = 1024

    private val SUBPOOL_ASSIGNER_PARAMS_TYPE = SubpoolAssignerParams.getDescriptor().fullName
    private val VID_RANK_BUILDER_PARAMS_TYPE = VidRankBuilderParams.getDescriptor().fullName
    private val VID_LABELER_PARAMS_TYPE = VidLabelerParams.getDescriptor().fullName

    /**
     * Derives a deterministic UUID4 from [seed], stable across redeliveries, for use as an AIP-155
     * `request_id`. Computed from an MD5 digest of the seed with the RFC-4122 version (4) and
     * variant bits forced, so it satisfies a field's `format = UUID4`. Copied from
     * `SubpoolAssigner.deterministicUuid`. Used for the Mark RPC request_ids that carry one
     * (`MarkVidLabelingJobFailed`, `MarkRankerJobFailed`) so a redelivery is idempotent.
     */
    fun deterministicUuid(seed: String): String {
      val bytes = MessageDigest.getInstance("MD5").digest(seed.toByteArray(Charsets.UTF_8))
      bytes[6] = ((bytes[6].toInt() and 0x0f) or 0x40).toByte() // version 4
      bytes[8] = ((bytes[8].toInt() and 0x3f) or 0x80).toByte() // variant 10xx
      val buffer = ByteBuffer.wrap(bytes)
      return UUID(buffer.long, buffer.long).toString()
    }

    /**
     * Checks if a StatusRuntimeException indicates that the work item is already in a FAILED state.
     */
    fun isAlreadyFailedError(e: StatusRuntimeException): Boolean {
      // Check if this is a failed precondition error due to the item already being in FAILED state
      return e.status.code == Status.Code.FAILED_PRECONDITION &&
        e.errorInfo?.reason == Errors.Reason.INVALID_WORK_ITEM_STATE.name &&
        e.errorInfo?.metadataMap?.get(Errors.Metadata.WORK_ITEM_STATE.key) ==
          WorkItem.State.FAILED.name
    }
  }
}
