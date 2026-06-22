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

import com.google.protobuf.util.Timestamps
import io.grpc.Status
import io.grpc.StatusException
import java.util.logging.Logger
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelShardsGrpcKt
import org.wfanet.measurement.api.v2alpha.listModelRolloutsRequest
import org.wfanet.measurement.api.v2alpha.listModelShardsRequest
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreatePoolAssignmentJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createPoolAssignmentJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineLabelingRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLinePoolAssigningRequest
import org.wfanet.measurement.edpaggregator.v1alpha.poolAssignmentJob
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelerParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.createWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem

/**
 * Sequences VID labeling dispatch for one `DataProvider`.
 *
 * This is the single shared component that both [VidLabelingDispatcher] (the upload-triggered "fast
 * path") and `VidLabelingMonitor` (the periodic backstop) call to start pipeline work. Keeping the
 * logic in one place means the per-`DataProvider` sequencing rule, the model-shard resolution, and
 * the work-creation steps are defined exactly once.
 *
 * [dispatchNext] enforces the core invariant: **at most one upload per `DataProvider` is `ACTIVE`
 * at a time.** If any upload is already `ACTIVE`, it does nothing; otherwise it activates the
 * oldest `CREATED` upload — for each of that upload's `CREATED` model lines it creates the Phase-0
 * (memoized) or Phase-2 (non-memoized) work and transitions the model line out of `CREATED`. This
 * prevents two uploads for the same `(DataProvider, ModelLine)` from corrupting the cumulative rank
 * index by running Phase 1 concurrently.
 *
 * Because the fast path and the monitor can run concurrently, two callers can momentarily both
 * observe "no `ACTIVE` upload" and both pick the same oldest `CREATED` upload (selection is
 * deterministic). Each model-line transition is therefore guarded by an etag compare-and-swap: the
 * first caller to call `Mark*` with the model line's etag wins, and the loser observes `ABORTED`
 * (or `FAILED_PRECONDITION` if the line already advanced) and no-ops. Work creation is idempotent
 * (`PoolAssignmentJob`s via deterministic request IDs, `WorkItem`s via deterministic IDs), so the
 * loser's redundant create calls are harmless.
 *
 * @param rawImpressionUploadStub stub for `RawImpressionUploadService`.
 * @param rawImpressionUploadModelLineStub stub for `RawImpressionUploadModelLineService`.
 * @param poolAssignmentJobStub stub for `PoolAssignmentJobService`.
 * @param workItemsStub stub for creating WorkItems via the Secure Computation API.
 * @param modelRolloutsStub VID Repository ModelRollouts API.
 * @param modelShardsStub VID Repository ModelShards API.
 * @param dataProviderName resource name of the `DataProvider` this sequencer dispatches for.
 * @param vidLabelerParamsTemplate template [VidLabelerParams] carrying storage + connection fields.
 * @param queueName resource name of the Secure Computation queue.
 * @param numberOfShards static number of shards per model line.
 * @param modelLineConfigs field-mapping configuration keyed by model line resource name.
 */
class VidLabelingDispatchSequencer(
  private val rawImpressionUploadStub:
    RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub,
  private val rawImpressionUploadModelLineStub:
    RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub,
  private val poolAssignmentJobStub:
    PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub,
  private val workItemsStub: WorkItemsGrpcKt.WorkItemsCoroutineStub,
  private val modelRolloutsStub: ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub,
  private val modelShardsStub: ModelShardsGrpcKt.ModelShardsCoroutineStub,
  private val dataProviderName: String,
  private val vidLabelerParamsTemplate: VidLabelerParams,
  private val queueName: String,
  private val numberOfShards: Int,
  private val modelLineConfigs: Map<String, VidLabelerParams.ModelLineConfig>,
) {

  /** Outcome of one [dispatchNext] call. */
  data class DispatchResult(
    /** Resource name of the upload dispatched this call, or null if none was dispatched. */
    val dispatchedUpload: String?,
    /** Number of `CREATED` uploads held behind an in-progress upload. */
    val queuedUploads: Int,
  )

  /** Resolved model shard info for a model line. */
  data class ResolvedShardInfo(val modelBlobPath: String, val memoizationEnabled: Boolean)

  /**
   * Dispatches the oldest `CREATED` upload for this `DataProvider`, iff none is `ACTIVE`.
   *
   * Safe to call concurrently with another invocation (e.g. the fast path racing the monitor): the
   * per-model-line etag CAS ensures each model line is claimed at most once.
   */
  suspend fun dispatchNext(): DispatchResult {
    val activeUploads: List<RawImpressionUpload> = listUploads(RawImpressionUpload.State.ACTIVE)
    if (activeUploads.isNotEmpty()) {
      return DispatchResult(dispatchedUpload = null, queuedUploads = countQueued())
    }

    val nextUpload: RawImpressionUpload =
      listUploads(RawImpressionUpload.State.CREATED).minByOrNull {
        Timestamps.toNanos(it.createTime)
      } ?: return DispatchResult(dispatchedUpload = null, queuedUploads = 0)

    activateUpload(nextUpload)
    logger.info("Dispatched upload ${nextUpload.name}")
    return DispatchResult(dispatchedUpload = nextUpload.name, queuedUploads = 0)
  }

  /**
   * Resolves model shard info for a model line via the ModelRollout -> ModelShard chain.
   *
   * Exposed so the upload-triggered dispatcher can reuse the exact resolution logic to decide which
   * model lines to register, rather than maintaining its own copy.
   *
   * @return resolved shard info, or null if no active rollout or shard is found.
   */
  suspend fun resolveShardInfo(modelLineName: String): ResolvedShardInfo? {
    val modelReleaseName: String = resolveActiveModelRelease(modelLineName) ?: return null
    return resolveShardInfoFromShards(modelReleaseName)
  }

  /**
   * Starts the pipeline for each `CREATED` model line of [upload]: creates the Phase-0/Phase-2 work
   * and transitions the model line out of `CREATED` (which rolls the upload up to `ACTIVE`).
   */
  private suspend fun activateUpload(upload: RawImpressionUpload) {
    val uploadId: String = upload.name.substringAfterLast("/")
    val modelLines: List<RawImpressionUploadModelLine> = listUploadModelLines(upload.name)

    for (modelLine in modelLines) {
      if (modelLine.state != RawImpressionUploadModelLine.State.CREATED) continue

      val shardInfo: ResolvedShardInfo? = resolveShardInfo(modelLine.cmmsModelLine)
      if (shardInfo == null) {
        logger.warning(
          "Could not resolve model shard for ${modelLine.cmmsModelLine}; skipping dispatch"
        )
        continue
      }

      if (shardInfo.memoizationEnabled) {
        createPoolAssignmentJobs(upload.name, modelLine.cmmsModelLine)
        markPoolAssigning(modelLine.name, modelLine.etag)
      } else {
        for (shardIndex in 0 until numberOfShards) {
          createWorkItem(
            upload.name,
            modelLine.cmmsModelLine,
            shardInfo.modelBlobPath,
            shardIndex,
            uploadId,
          )
        }
        markLabeling(modelLine.name, modelLine.etag)
      }
    }
  }

  private suspend fun countQueued(): Int = listUploads(RawImpressionUpload.State.CREATED).size

  /** Lists this DataProvider's uploads in [state]. */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun listUploads(state: RawImpressionUpload.State): List<RawImpressionUpload> =
    rawImpressionUploadStub
      .listResources { pageToken: String ->
        val response =
          try {
            rawImpressionUploadStub.listRawImpressionUploads(
              listRawImpressionUploadsRequest {
                parent = dataProviderName
                filter = ListRawImpressionUploadsRequestKt.filter { stateIn += state }
                if (pageToken.isNotEmpty()) {
                  this.pageToken = pageToken
                }
              }
            )
          } catch (e: StatusException) {
            throw Exception("Error listing RawImpressionUploads for $dataProviderName", e)
          }
        ResourceList(response.rawImpressionUploadsList, response.nextPageToken)
      }
      .flattenConcat()
      .toList()

  /** Lists the `RawImpressionUploadModelLine` children of [uploadName]. */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun listUploadModelLines(uploadName: String): List<RawImpressionUploadModelLine> =
    rawImpressionUploadModelLineStub
      .listResources { pageToken: String ->
        val response =
          try {
            rawImpressionUploadModelLineStub.listRawImpressionUploadModelLines(
              listRawImpressionUploadModelLinesRequest {
                parent = uploadName
                if (pageToken.isNotEmpty()) {
                  this.pageToken = pageToken
                }
              }
            )
          } catch (e: StatusException) {
            throw Exception("Error listing RawImpressionUploadModelLines for $uploadName", e)
          }
        ResourceList(response.rawImpressionUploadModelLinesList, response.nextPageToken)
      }
      .flattenConcat()
      .toList()

  /** Finds the active `ModelRelease` for a model line via ListModelRollouts. */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun resolveActiveModelRelease(modelLineName: String): String? {
    val modelRelease: String? =
      modelRolloutsStub
        .listResources { pageToken: String ->
          val response =
            try {
              modelRolloutsStub.listModelRollouts(
                listModelRolloutsRequest {
                  parent = modelLineName
                  if (pageToken.isNotEmpty()) {
                    this.pageToken = pageToken
                  }
                }
              )
            } catch (e: StatusException) {
              throw Exception("Error listing model rollouts for $modelLineName", e)
            }
          ResourceList(response.modelRolloutsList, response.nextPageToken)
        }
        .flattenConcat()
        .firstOrNull { it.modelRelease.isNotEmpty() }
        ?.modelRelease

    if (modelRelease == null) {
      logger.warning("No model rollout found for model line $modelLineName")
    }
    return modelRelease
  }

  /** Resolves model shard info from `ModelShard` resources for this DataProvider + ModelRelease. */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun resolveShardInfoFromShards(modelReleaseName: String): ResolvedShardInfo? {
    val shard =
      modelShardsStub
        .listResources { pageToken: String ->
          val response =
            try {
              modelShardsStub.listModelShards(
                listModelShardsRequest {
                  parent = dataProviderName
                  if (pageToken.isNotEmpty()) {
                    this.pageToken = pageToken
                  }
                }
              )
            } catch (e: StatusException) {
              throw Exception(
                "Error listing model shards for $dataProviderName release $modelReleaseName",
                e,
              )
            }
          ResourceList(response.modelShardsList, response.nextPageToken)
        }
        .flattenConcat()
        .firstOrNull { it.modelRelease == modelReleaseName && it.hasModelBlob() }

    if (shard == null) {
      logger.warning("No model shard found for release $modelReleaseName on $dataProviderName")
      return null
    }

    return ResolvedShardInfo(
      modelBlobPath = shard.modelBlob.modelBlobPath,
      memoizationEnabled = shard.memoizedVidAssignmentEnabled,
    )
  }

  /** Creates one `PoolAssignmentJob` per shard for a memoized model line (Phase 0). */
  private suspend fun createPoolAssignmentJobs(uploadName: String, modelLineName: String) {
    for (shardChunk in (0 until numberOfShards).chunked(POOL_ASSIGNMENT_JOB_BATCH_SIZE)) {
      val request = batchCreatePoolAssignmentJobsRequest {
        parent = uploadName
        for (shardIndex in shardChunk) {
          requests += createPoolAssignmentJobRequest {
            parent = uploadName
            poolAssignmentJob = poolAssignmentJob {
              cmmsModelLine = modelLineName
              this.shardIndex = shardIndex
            }
            requestId = RequestIds.forPoolAssignmentJob(uploadName, modelLineName, shardIndex)
          }
        }
      }
      try {
        poolAssignmentJobStub.batchCreatePoolAssignmentJobs(request)
      } catch (e: StatusException) {
        if (e.status.code == Status.Code.ALREADY_EXISTS) {
          // Idempotent redelivery / concurrent dispatch: these jobs already exist. Ack and
          // continue.
          logger.info("PoolAssignmentJobs for $modelLineName already exist; skipping")
          continue
        }
        throw e
      }
    }
    logger.info("Created $numberOfShards PoolAssignmentJobs for memoized model line $modelLineName")
  }

  /**
   * Creates a WorkItem in the Secure Computation control plane for one model line shard.
   *
   * Idempotency here uses resource-name uniqueness (a deterministic [workItemId]), not an AIP-155
   * `request_id` — `CreateWorkItemRequest` has no `request_id` field. A retry therefore returns
   * `ALREADY_EXISTS` (handled below) rather than the cached response.
   */
  private suspend fun createWorkItem(
    uploadName: String,
    modelLineName: String,
    modelBlobPath: String,
    shardIndex: Int,
    uploadId: String,
  ) {
    val modelLineConfig =
      requireNotNull(modelLineConfigs[modelLineName]) {
        "No ModelLineConfig found for model line: $modelLineName"
      }

    val params = vidLabelerParams {
      dataProvider = dataProviderName
      vidLabeledImpressionsStorageParams =
        vidLabelerParamsTemplate.vidLabeledImpressionsStorageParams
      rawImpressionsStorageParams = vidLabelerParamsTemplate.rawImpressionsStorageParams
      vidRepoConnection = vidLabelerParamsTemplate.vidRepoConnection
      modelLineConfigs[modelLineName] =
        VidLabelerParamsKt.modelLineConfig {
          labelerInputFieldMapping.putAll(modelLineConfig.labelerInputFieldMappingMap)
          eventTemplateFieldMapping.putAll(modelLineConfig.eventTemplateFieldMappingMap)
        }
      overrideModelLines += listOf(modelLineName)
      rawImpressionUpload = uploadName
      this.shardIndex = shardIndex
      totalShards = numberOfShards
      modelBlobPaths[modelLineName] = modelBlobPath
    }

    val workItemId =
      "vid-labeling-$uploadId-${modelLineName.substringAfterLast("/")}-shard-$shardIndex"
    val request = createWorkItemRequest {
      this.workItemId = workItemId
      workItem = workItem {
        queue = queueName
        workItemParams = workItemParams { appParams = params.pack() }.pack()
      }
    }
    try {
      workItemsStub.createWorkItem(request)
    } catch (e: StatusException) {
      if (e.status.code == Status.Code.ALREADY_EXISTS) {
        // A concurrent dispatch already created this WorkItem; the deterministic ID makes this a
        // no-op. Safe to ignore.
        logger.info("WorkItem $workItemId already exists; skipping (concurrent dispatch)")
        return
      }
      throw Exception("Error creating WorkItem $workItemId", e)
    }
    logger.info("Created WorkItem $workItemId for model line $modelLineName shard $shardIndex")
  }

  private suspend fun markPoolAssigning(modelLineName: String, etag: String) {
    try {
      rawImpressionUploadModelLineStub.markRawImpressionUploadModelLinePoolAssigning(
        markRawImpressionUploadModelLinePoolAssigningRequest {
          name = modelLineName
          this.etag = etag
        }
      )
    } catch (e: StatusException) {
      if (isConcurrentClaimLoss(e)) {
        logger.info(
          "Skipping POOL_ASSIGNING for $modelLineName: ${e.status.code} (claimed by a " +
            "concurrent dispatch)"
        )
        return
      }
      throw Exception("Error marking $modelLineName POOL_ASSIGNING", e)
    }
  }

  private suspend fun markLabeling(modelLineName: String, etag: String) {
    try {
      rawImpressionUploadModelLineStub.markRawImpressionUploadModelLineLabeling(
        markRawImpressionUploadModelLineLabelingRequest {
          name = modelLineName
          this.etag = etag
        }
      )
    } catch (e: StatusException) {
      if (isConcurrentClaimLoss(e)) {
        logger.info(
          "Skipping LABELING for $modelLineName: ${e.status.code} (claimed by a concurrent " +
            "dispatch)"
        )
        return
      }
      throw Exception("Error marking $modelLineName LABELING", e)
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private const val POOL_ASSIGNMENT_JOB_BATCH_SIZE = 100

    /**
     * Whether [e] indicates this caller lost a concurrent dispatch race for a model line.
     *
     * `ABORTED` is the etag CAS failure (another caller claimed the line first);
     * `FAILED_PRECONDITION` means the line already advanced out of the expected state. Both mean
     * "someone else already did this transition," so the caller should skip rather than fail.
     */
    private fun isConcurrentClaimLoss(e: StatusException): Boolean {
      val code: Status.Code = e.status.code
      return code == Status.Code.ABORTED || code == Status.Code.FAILED_PRECONDITION
    }
  }
}
