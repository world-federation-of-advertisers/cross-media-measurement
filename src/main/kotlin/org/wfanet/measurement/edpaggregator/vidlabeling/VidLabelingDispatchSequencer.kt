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
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelShardsGrpcKt
import org.wfanet.measurement.api.v2alpha.getModelLineRequest
import org.wfanet.measurement.api.v2alpha.listModelRolloutsRequest
import org.wfanet.measurement.api.v2alpha.listModelShardsRequest
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.SubpoolAssignerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreatePoolAssignmentJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createPoolAssignmentJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineLabelingRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLinePoolAssigningRequest
import org.wfanet.measurement.edpaggregator.v1alpha.poolAssignmentJob
import org.wfanet.measurement.edpaggregator.v1alpha.subpoolAssignerParams
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
 * logic in one place means the per-`(DataProvider, ModelLine)` sequencing rule, the model-shard
 * resolution, and the work-creation steps are defined exactly once.
 *
 * [dispatchNext] enforces the core invariant: **at most one upload per `(DataProvider, ModelLine)`
 * runs at a time.** A model line is dispatched only if no upload currently has that same
 * `cmmsModelLine` running, which protects the cumulative rank index from concurrent Phase-1 runs;
 * different model lines proceed in parallel.
 *
 * Both paths are handled: a **memoized** model line dispatches Phase-0 (pre-creating a
 * `PoolAssignmentJob` per shard, publishing one SubpoolAssigner `WorkItem` per shard on the
 * [poolAssignerQueueName] queue, then transitioning to `POOL_ASSIGNING`); a **non-memoized** model
 * line dispatches Phase-2 directly (one VidLabeler `WorkItem` per shard on [queueName], then
 * transitioning to `LABELING`).
 *
 * Because the fast path and the monitor can run concurrently, two callers can momentarily both pick
 * the same model line. Each transition is therefore guarded by an etag compare-and-swap: the first
 * caller to call `Mark*` with the model line's etag wins, and the loser observes `ABORTED` (or
 * `FAILED_PRECONDITION` if the line already advanced) and no-ops. Every create is idempotent
 * (deterministic `WorkItem` IDs; deterministic `PoolAssignmentJob` `request_id`s), so the loser's
 * redundant create calls are harmless.
 *
 * @param rawImpressionUploadStub stub for `RawImpressionUploadService`.
 * @param rawImpressionUploadModelLineStub stub for `RawImpressionUploadModelLineService`.
 * @param workItemsStub stub for creating WorkItems via the Secure Computation API.
 * @param poolAssignmentJobStub stub for `PoolAssignmentJobService` (memoized Phase-0).
 * @param modelRolloutsStub VID Repository ModelRollouts API.
 * @param modelShardsStub VID Repository ModelShards API.
 * @param modelLinesStub VID Repository ModelLines API; used to resolve the memoized active window.
 * @param dataProviderName resource name of the `DataProvider` this sequencer dispatches for.
 * @param vidLabelerParamsTemplate template [VidLabelerParams] carrying storage + connection fields.
 * @param subpoolAssignerParamsTemplate template [SubpoolAssignerParams] carrying the storage +
 *   connection fields shared by every memoized Phase-0 WorkItem.
 * @param queueName resource name of the Secure Computation queue for Phase-2 VidLabeler WorkItems.
 * @param poolAssignerQueueName resource name of the queue for Phase-0 SubpoolAssigner WorkItems.
 * @param numberOfShards static number of shards per model line.
 * @param modelLineConfigs field-mapping configuration keyed by model line resource name.
 */
class VidLabelingDispatchSequencer(
  private val rawImpressionUploadStub:
    RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub,
  private val rawImpressionUploadModelLineStub:
    RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub,
  private val workItemsStub: WorkItemsGrpcKt.WorkItemsCoroutineStub,
  private val poolAssignmentJobStub:
    PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub,
  private val modelRolloutsStub: ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub,
  private val modelShardsStub: ModelShardsGrpcKt.ModelShardsCoroutineStub,
  private val modelLinesStub: ModelLinesGrpcKt.ModelLinesCoroutineStub,
  private val dataProviderName: String,
  private val vidLabelerParamsTemplate: VidLabelerParams,
  private val subpoolAssignerParamsTemplate: SubpoolAssignerParams,
  private val queueName: String,
  private val poolAssignerQueueName: String,
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
   * Dispatches each `CREATED` model line whose `(DataProvider, ModelLine)` is not already running.
   *
   * Serialization is per `(DataProvider, ModelLine)`, not per `DataProvider`: a model line is
   * dispatched only if no upload currently has that same `cmmsModelLine` in a running state
   * ([IN_PROGRESS_STATES]) — that is what protects the cumulative rank index from concurrent
   * Phase-1 runs. Different model lines, whether on the same or different uploads, run in parallel.
   * Within a single model line, uploads are dispatched oldest-first (FIFO).
   *
   * Safe to call concurrently with another invocation (e.g. the fast path racing the monitor): the
   * per-model-line etag CAS ensures each model line is claimed at most once.
   */
  suspend fun dispatchNext(): DispatchResult {
    val uploads: List<RawImpressionUpload> =
      (listUploads(RawImpressionUpload.State.CREATED) +
          listUploads(RawImpressionUpload.State.ACTIVE))
        .sortedBy { Timestamps.toNanos(it.createTime) }
    val modelLinesByUpload: Map<String, List<RawImpressionUploadModelLine>> =
      uploads.associate { it.name to listUploadModelLines(it.name) }

    // Model lines already running anywhere for this DataProvider; never start a second upload for
    // one of them.
    val busyModelLines: MutableSet<String> =
      modelLinesByUpload.values
        .flatten()
        .filter { it.state in IN_PROGRESS_STATES }
        .map { it.cmmsModelLine }
        .toMutableSet()

    var dispatchedUpload: String? = null
    var queuedModelLines = 0
    for (upload in uploads) {
      for (modelLine in modelLinesByUpload.getValue(upload.name)) {
        if (modelLine.state != RawImpressionUploadModelLine.State.CREATED) continue
        if (modelLine.cmmsModelLine in busyModelLines) {
          queuedModelLines++
          continue
        }
        if (activateModelLine(upload.name, modelLine)) {
          busyModelLines += modelLine.cmmsModelLine
          if (dispatchedUpload == null) dispatchedUpload = upload.name
        }
      }
    }

    if (dispatchedUpload != null) {
      logger.info("Dispatched model line(s) for $dataProviderName starting with $dispatchedUpload")
    }
    return DispatchResult(dispatchedUpload = dispatchedUpload, queuedUploads = queuedModelLines)
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
   * Starts the pipeline for one `CREATED` [modelLine] of the upload named [uploadName]: resolves
   * its model shard, creates the Phase-0 (memoized) or Phase-2 (non-memoized) work, and transitions
   * the model line out of `CREATED` (which rolls the upload up to `ACTIVE`).
   *
   * @return true if the model line was activated (shard resolved and work attempted); false if it
   *   was skipped (model shard unresolved), in which case it is left `CREATED` for a later attempt.
   */
  private suspend fun activateModelLine(
    uploadName: String,
    modelLine: RawImpressionUploadModelLine,
  ): Boolean {
    val shardInfo: ResolvedShardInfo =
      resolveShardInfo(modelLine.cmmsModelLine)
        ?: run {
          logger.warning(
            "Could not resolve model shard for ${modelLine.cmmsModelLine}; skipping dispatch"
          )
          return false
        }

    if (shardInfo.memoizationEnabled) {
      dispatchMemoized(uploadName, modelLine, shardInfo)
    } else {
      dispatchNonMemoized(uploadName, modelLine, shardInfo)
    }
    return true
  }

  /**
   * Non-memoized (Phase-2) dispatch: one VidLabeler `WorkItem` per shard on [queueName], then
   * transition the model line to `LABELING`.
   */
  private suspend fun dispatchNonMemoized(
    uploadName: String,
    modelLine: RawImpressionUploadModelLine,
    shardInfo: ResolvedShardInfo,
  ) {
    val uploadId: String =
      requireNotNull(RawImpressionUploadKey.fromName(uploadName)) {
          "Invalid RawImpressionUpload name: $uploadName"
        }
        .rawImpressionUploadId
    for (shardIndex in 0 until numberOfShards) {
      createWorkItem(
        uploadName,
        modelLine.cmmsModelLine,
        shardInfo.modelBlobPath,
        shardIndex,
        uploadId,
      )
    }
    markLabeling(modelLine.name, modelLine.etag)
  }

  /**
   * Memoized (Phase-0) dispatch: pre-create a `PoolAssignmentJob` per shard, publish one
   * SubpoolAssigner `WorkItem` per shard on [poolAssignerQueueName] (each carrying the resource
   * name of its pre-created job), then transition the model line to `POOL_ASSIGNING`.
   *
   * Mirrors [dispatchNonMemoized]'s create-then-mark order. Every create is idempotent
   * (deterministic `request_id` for the jobs, deterministic `workItemId` for the WorkItems), so a
   * caller that loses the per-model-line etag CAS at [markPoolAssigning] has only repeated harmless
   * creates.
   */
  private suspend fun dispatchMemoized(
    uploadName: String,
    modelLine: RawImpressionUploadModelLine,
    shardInfo: ResolvedShardInfo,
  ) {
    val modelLineConfig =
      requireNotNull(modelLineConfigs[modelLine.cmmsModelLine]) {
        "No ModelLineConfig found for model line: ${modelLine.cmmsModelLine}"
      }
    // The active window is read from the ModelLine so the TEE can drop out-of-window impressions.
    val resolvedModelLine: ModelLine = getModelLine(modelLine.cmmsModelLine)

    // Pre-create the shard rows first; the server assigns each a name we thread into its WorkItem.
    val poolAssignmentJobsByShard: Map<Int, String> =
      createPoolAssignmentJobs(uploadName, modelLine.cmmsModelLine)

    val uploadId: String = uploadName.substringAfterLast("/")
    for (shardIndex in 0 until numberOfShards) {
      val poolAssignmentJob: String =
        requireNotNull(poolAssignmentJobsByShard[shardIndex]) {
          "BatchCreatePoolAssignmentJobs returned no job for shard $shardIndex of " +
            modelLine.cmmsModelLine
        }
      createSubpoolAssignerWorkItem(
        uploadName = uploadName,
        modelLineName = modelLine.cmmsModelLine,
        modelBlobPath = shardInfo.modelBlobPath,
        resolvedModelLine = resolvedModelLine,
        modelLineConfig = modelLineConfig,
        poolAssignmentJob = poolAssignmentJob,
        shardIndex = shardIndex,
        uploadId = uploadId,
      )
    }
    markPoolAssigning(modelLine.name, modelLine.etag)
  }

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

    val modelLineId =
      requireNotNull(ModelLineKey.fromName(modelLineName)) {
          "Invalid ModelLine name: $modelLineName"
        }
        .modelLineId
    val workItemId = "vid-labeling-$uploadId-$modelLineId-shard-$shardIndex"
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
          "Skipping POOL_ASSIGNING for $modelLineName: ${e.status.code} (claimed by a concurrent " +
            "dispatch)"
        )
        return
      }
      throw Exception("Error marking $modelLineName POOL_ASSIGNING", e)
    }
  }

  /** Fetches the `ModelLine` to read its active window (`active_start_time`/`active_end_time`). */
  private suspend fun getModelLine(modelLineName: String): ModelLine =
    try {
      modelLinesStub.getModelLine(getModelLineRequest { name = modelLineName })
    } catch (e: StatusException) {
      throw Exception("Error getting ModelLine $modelLineName", e)
    }

  /**
   * Pre-creates one `PoolAssignmentJob` per shard for ([uploadName], [modelLineName]) via
   * `BatchCreatePoolAssignmentJobs`, chunked to [POOL_ASSIGNMENT_JOB_BATCH_SIZE] (the server's
   * batch limit).
   *
   * Idempotency is by AIP-155 `request_id` (deterministic per shard), so a redelivered batch
   * returns the existing rows for shards already created and creates the rest — no `ALREADY_EXISTS`
   * to ack.
   *
   * @return a map from shard index to the server-assigned `PoolAssignmentJob` resource name, used
   *   to stamp `SubpoolAssignerParams.pool_assignment_job` on each shard's WorkItem.
   */
  private suspend fun createPoolAssignmentJobs(
    uploadName: String,
    modelLineName: String,
  ): Map<Int, String> {
    val jobsByShard = mutableMapOf<Int, String>()
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
      val response =
        try {
          poolAssignmentJobStub.batchCreatePoolAssignmentJobs(request)
        } catch (e: StatusException) {
          throw Exception(
            "Error creating PoolAssignmentJobs for $modelLineName under $uploadName",
            e,
          )
        }
      for (job in response.poolAssignmentJobsList) {
        jobsByShard[job.shardIndex] = job.name
      }
    }
    return jobsByShard
  }

  /**
   * Creates one Phase-0 SubpoolAssigner `WorkItem` on the [poolAssignerQueueName] queue for a
   * single shard, packing a [SubpoolAssignerParams] that references the shard's pre-created
   * [poolAssignmentJob].
   *
   * Idempotent on a deterministic [workItemId]: a retry returns `ALREADY_EXISTS` (handled below)
   * rather than publishing a duplicate, exactly like [createWorkItem].
   */
  private suspend fun createSubpoolAssignerWorkItem(
    uploadName: String,
    modelLineName: String,
    modelBlobPath: String,
    resolvedModelLine: ModelLine,
    modelLineConfig: VidLabelerParams.ModelLineConfig,
    poolAssignmentJob: String,
    shardIndex: Int,
    uploadId: String,
  ) {
    val params = subpoolAssignerParams {
      dataProvider = dataProviderName
      rawImpressionStorageParams = subpoolAssignerParamsTemplate.rawImpressionStorageParams
      vidLabeledImpressionsStorageParams =
        subpoolAssignerParamsTemplate.vidLabeledImpressionsStorageParams
      vidRankMapStorageParams = subpoolAssignerParamsTemplate.vidRankMapStorageParams
      subpoolMapStorageParams = subpoolAssignerParamsTemplate.subpoolMapStorageParams
      rawImpressionMetadataStorageConnection =
        subpoolAssignerParamsTemplate.rawImpressionMetadataStorageConnection
      rawImpressionUpload = uploadName
      modelLine = modelLineName
      this.modelBlobPath = modelBlobPath
      activeStartTime = resolvedModelLine.activeStartTime
      if (resolvedModelLine.hasActiveEndTime()) {
        activeEndTime = resolvedModelLine.activeEndTime
      }
      this.shardIndex = shardIndex
      totalShards = numberOfShards
      labelerInputFieldMapping.putAll(modelLineConfig.labelerInputFieldMappingMap)
      eventTemplateFieldMapping.putAll(modelLineConfig.eventTemplateFieldMappingMap)
      this.poolAssignmentJob = poolAssignmentJob
    }

    val workItemId =
      "subpool-assigner-$uploadId-${modelLineName.substringAfterLast("/")}-shard-$shardIndex"
    val request = createWorkItemRequest {
      this.workItemId = workItemId
      workItem = workItem {
        queue = poolAssignerQueueName
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
    logger.info(
      "Created SubpoolAssigner WorkItem $workItemId for model line $modelLineName shard $shardIndex"
    )
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    /** Maximum `CreatePoolAssignmentJobRequest`s per `BatchCreatePoolAssignmentJobs` call. */
    private const val POOL_ASSIGNMENT_JOB_BATCH_SIZE = 50

    /**
     * Model-line states that count as "running" for `(DataProvider, ModelLine)` serialization: a
     * model line in any of these is in flight and must not be started in a second upload.
     */
    private val IN_PROGRESS_STATES: Set<RawImpressionUploadModelLine.State> =
      setOf(
        RawImpressionUploadModelLine.State.POOL_ASSIGNING,
        RawImpressionUploadModelLine.State.RANKING,
        RawImpressionUploadModelLine.State.LABELING,
      )

    /**
     * Whether [e] indicates this caller lost a concurrent dispatch race for a model line.
     *
     * `ABORTED` is the etag CAS failure (another caller claimed the line first);
     * `FAILED_PRECONDITION` means the line already advanced out of the expected state. Both mean
     * "someone else already did this transition," so the caller should skip rather than fail.
     *
     * TODO(world-federation-of-advertisers/cross-media-measurement#4018): Once #4018 adds
     *   `MarkRequestId` to `RawImpressionUploadModelLine`, switch these transitions to AIP-155
     *   request-id idempotency so a redelivered `Mark*` returns the prior response instead of
     *   relying on this etag-CAS swallow.
     */
    private fun isConcurrentClaimLoss(e: StatusException): Boolean {
      val code: Status.Code = e.status.code
      return code == Status.Code.ABORTED || code == Status.Code.FAILED_PRECONDITION
    }
  }
}
