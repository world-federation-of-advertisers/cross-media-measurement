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
import org.wfanet.measurement.edpaggregator.rawimpressions.RawImpressionFileBinPacker
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.SubpoolAssignerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreatePoolAssignmentJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateVidLabelingJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.createPoolAssignmentJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createVidLabelingJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineLabelingRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLinePoolAssigningRequest
import org.wfanet.measurement.edpaggregator.v1alpha.poolAssignmentJob
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelingJob
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
 * [poolAssignerQueueName] queue, then transitioning to `POOL_ASSIGNING`); the **non-memoized**
 * model lines of an upload are **bundled together** and dispatched to Phase-2 directly — the
 * upload's `RawImpressionUploadFile`s are bin-packed by size into one `VidLabelingJob` per batch
 * (each job covering every bundled model line), one VidLabeler `WorkItem` is published per job on
 * [queueName], then each bundled model line transitions to `LABELING`.
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
 * @param numberOfShards static number of shards per memoized model line (Phase-0 fan-out).
 * @param modelLineConfigs field-mapping configuration keyed by model line resource name.
 * @param rawImpressionUploadFileStub stub for listing an upload's files to bin-pack (non-memoized).
 * @param vidLabelingJobStub stub for creating Phase-2 `VidLabelingJob`s (non-memoized).
 * @param maxFileBatchSizeBytes bin-packing threshold for non-memoized `VidLabelingJob`s.
 * @param maxJobsPerBatchCreate chunk size for `BatchCreateVidLabelingJobs`.
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
  private val rawImpressionUploadFileStub:
    RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub,
  private val vidLabelingJobStub: VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub,
  private val maxFileBatchSizeBytes: Long,
  private val maxJobsPerBatchCreate: Int = DEFAULT_MAX_JOBS_PER_BATCH_CREATE,
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

  /** A non-memoized model line bundled into an upload's shared Phase-2 dispatch. */
  private data class BundledModelLine(
    val modelLine: RawImpressionUploadModelLine,
    val shardInfo: ResolvedShardInfo,
  )

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
      // Memoized model lines are dispatched individually (Phase-0 fan-out); non-memoized lines of
      // this upload are collected and dispatched together as one bundled Phase-2 fan-out.
      val nonMemoized = mutableListOf<BundledModelLine>()
      for (modelLine in modelLinesByUpload.getValue(upload.name)) {
        if (modelLine.state != RawImpressionUploadModelLine.State.CREATED) continue
        if (modelLine.cmmsModelLine in busyModelLines) {
          queuedModelLines++
          continue
        }
        val shardInfo: ResolvedShardInfo? = resolveShardInfo(modelLine.cmmsModelLine)
        if (shardInfo == null) {
          logger.warning(
            "Could not resolve model shard for ${modelLine.cmmsModelLine}; skipping dispatch"
          )
          continue
        }
        if (shardInfo.memoizationEnabled) {
          dispatchMemoized(upload.name, modelLine, shardInfo)
          busyModelLines += modelLine.cmmsModelLine
          if (dispatchedUpload == null) dispatchedUpload = upload.name
        } else {
          nonMemoized += BundledModelLine(modelLine, shardInfo)
        }
      }
      if (nonMemoized.isNotEmpty()) {
        dispatchNonMemoizedBundle(upload.name, nonMemoized)
        for (bundled in nonMemoized) busyModelLines += bundled.modelLine.cmmsModelLine
        if (dispatchedUpload == null) dispatchedUpload = upload.name
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
   * Non-memoized (Phase-2) dispatch for all bundled non-memoized model lines of [uploadName]:
   * bin-pack the upload's `RawImpressionUploadFile`s by size into batches, create one
   * `VidLabelingJob` per batch (each job covering every bundled model line), publish one VidLabeler
   * `WorkItem` per job on [queueName], then transition every bundled model line to `LABELING`.
   *
   * Mirrors the Phase-1 `VidRankBuilder` fan-out (shared [RawImpressionFileBinPacker]). The
   * last-out `MarkVidLabelingJobSucceeded` flips each covered model line to `COMPLETED`. Create
   * order is jobs -> WorkItems -> mark, so all rows exist before the line is advanced; every create
   * is idempotent (deterministic `request_id`s / `workItemId`s), so a caller that loses the
   * per-model-line etag CAS at [markLabeling] has only repeated harmless creates.
   *
   * Bundle-set stability under partial failure: the job `request_id` is keyed by the *sorted bundle
   * set* ([RequestIds.forVidLabelingJob]), so if [markLabeling] succeeds for some lines but throws
   * for others mid-tick, the next tick re-bundles only the still-`CREATED` lines and creates a
   * *second* set of jobs over the same files — those lines get labeled twice. This is **not** a
   * double-count: the labeled output blob key is deterministic per (input file, model line)
   * ([VidLabelingSink] writes `model-line/<id>/<date>/<sha(inputBlobUri|modelLine)>`), so the
   * second pass overwrites the same blob + `.metadata` sidecar — `DataAvailabilitySync` sees one
   * metadata blob per (file, model line), so `ImpressionMetadata` (and Halo counts) are not
   * duplicated. The only cost is redundant labeling work + extra `VidLabelingJob` rows; acceptable
   * at the current scale (few model lines, low churn). If that cost matters, re-fetch each line's
   * state and abort the whole bundle when any is no longer `CREATED` before creating jobs.
   */
  private suspend fun dispatchNonMemoizedBundle(
    uploadName: String,
    bundle: List<BundledModelLine>,
  ) {
    require(maxFileBatchSizeBytes > 0) {
      "max_file_batch_size_bytes missing for non-memoized model lines under $uploadName; " +
        "set it on VidLabelingConfig for this DataProvider"
    }
    val modelLineNames: List<String> = bundle.map { it.modelLine.cmmsModelLine }
    // The active window is read per ModelLine so the TEE can drop out-of-window impressions; the
    // model blob path comes from each line's resolved ModelShard.
    val resolvedModelLines: Map<String, ModelLine> =
      modelLineNames.associateWith { getModelLine(it) }
    val modelBlobPathByLine: Map<String, String> =
      bundle.associate { it.modelLine.cmmsModelLine to it.shardInfo.modelBlobPath }

    val files: List<RawImpressionUploadFile> = listUploadFiles(uploadName)
    if (files.isEmpty()) {
      // Do NOT advance the model lines to LABELING: with no files there are no VidLabelingJobs, so
      // the last-job-out MarkVidLabelingJobSucceeded would never fire and the lines would strand in
      // LABELING with no completion path. Leave them CREATED so a later dispatch retries (an empty
      // listing is anomalous/transient because files are registered before dispatch); the Monitor
      // surfaces uploads that stay CREATED.
      logger.warning(
        "No RawImpressionUploadFiles under $uploadName; nothing to label for non-memoized model " +
          "lines $modelLineNames; leaving them CREATED for retry"
      )
      return
    }

    val batches: List<List<String>> = RawImpressionFileBinPacker.pack(files, maxFileBatchSizeBytes)
    val labelingJobs: List<VidLabelingJob> =
      createVidLabelingJobs(uploadName, modelLineNames, batches)
    for (job in labelingJobs) {
      createWorkItem(uploadName, modelLineNames, modelBlobPathByLine, resolvedModelLines, job.name)
    }
    for (bundled in bundle) markLabeling(bundled.modelLine.name, bundled.modelLine.etag)
  }

  /** Lists the `RawImpressionUploadFile` children of [uploadName]. */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun listUploadFiles(uploadName: String): List<RawImpressionUploadFile> =
    rawImpressionUploadFileStub
      .listResources { pageToken: String ->
        val response =
          rawImpressionUploadFileStub.listRawImpressionUploadFiles(
            listRawImpressionUploadFilesRequest {
              parent = uploadName
              if (pageToken.isNotEmpty()) {
                this.pageToken = pageToken
              }
            }
          )
        ResourceList(response.rawImpressionUploadFilesList, response.nextPageToken)
      }
      .flattenConcat()
      .toList()

  /**
   * Creates one `VidLabelingJob` per [batches] entry via `BatchCreateVidLabelingJobs` (chunked to
   * [maxJobsPerBatchCreate]). Each job covers every [modelLineNames] (the bundled non-memoized
   * lines) and carries that batch's `RawImpressionUploadFile`s. The `request_id` is keyed by the
   * sorted model-line set + batch index so a redelivered bundle reuses the rows while a different
   * bundle set creates its own. Each chunk is checked to return exactly as many jobs as requested
   * so a partial response can't silently drop a batch. Returns each created job.
   */
  private suspend fun createVidLabelingJobs(
    uploadName: String,
    modelLineNames: List<String>,
    batches: List<List<String>>,
  ): List<VidLabelingJob> {
    val created = mutableListOf<VidLabelingJob>()
    for (group in batches.withIndex().chunked(maxJobsPerBatchCreate)) {
      val response =
        vidLabelingJobStub.batchCreateVidLabelingJobs(
          batchCreateVidLabelingJobsRequest {
            parent = uploadName
            for ((batchIndex, batch) in group) {
              requests += createVidLabelingJobRequest {
                parent = uploadName
                vidLabelingJob = vidLabelingJob {
                  cmmsModelLines += modelLineNames
                  rawImpressionUploadFiles += batch
                }
                requestId = RequestIds.forVidLabelingJob(uploadName, modelLineNames, batchIndex)
              }
            }
          }
        )
      check(response.vidLabelingJobsList.size == group.size) {
        "BatchCreateVidLabelingJobs returned ${response.vidLabelingJobsList.size} jobs for " +
          "${group.size} requests"
      }
      created.addAll(response.vidLabelingJobsList)
    }
    return created
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
    // `vid_rank_map_storage_params`, `subpool_map_storage_params`, and `model_storage_params` are
    // REQUIRED on `SubpoolAssignerParams` but OPTIONAL on `VidLabelingConfig` (only required for
    // EDPs with at least one memoized model line). Enforce that intent here: fail fast at the first
    // memoized dispatch rather than publishing a WorkItem with REQUIRED fields missing.
    require(subpoolAssignerParamsTemplate.hasVidRankMapStorageParams()) {
      "vid_rank_map_storage_params missing for memoized model line ${modelLine.cmmsModelLine}; " +
        "set it on VidLabelingConfig for this DataProvider"
    }
    require(subpoolAssignerParamsTemplate.hasSubpoolMapStorageParams()) {
      "subpool_map_storage_params missing for memoized model line ${modelLine.cmmsModelLine}; " +
        "set it on VidLabelingConfig for this DataProvider"
    }
    require(subpoolAssignerParamsTemplate.hasModelStorageParams()) {
      "model_storage_params missing for memoized model line ${modelLine.cmmsModelLine}; " +
        "set it on VidLabelingConfig for this DataProvider"
    }
    val modelLineConfig =
      requireNotNull(modelLineConfigs[modelLine.cmmsModelLine]) {
        "No ModelLineConfig found for model line: ${modelLine.cmmsModelLine}"
      }
    // The active window is read from the ModelLine so the TEE can drop out-of-window impressions.
    val resolvedModelLine: ModelLine = getModelLine(modelLine.cmmsModelLine)

    // Pre-create the shard rows first; the server assigns each a name we thread into its WorkItem.
    val poolAssignmentJobsByShard: Map<Int, String> =
      createPoolAssignmentJobs(uploadName, modelLine.cmmsModelLine)

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
          rawImpressionUploadStub.listRawImpressionUploads(
            listRawImpressionUploadsRequest {
              parent = dataProviderName
              filter = ListRawImpressionUploadsRequestKt.filter { stateIn += state }
              if (pageToken.isNotEmpty()) {
                this.pageToken = pageToken
              }
            }
          )
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
          rawImpressionUploadModelLineStub.listRawImpressionUploadModelLines(
            listRawImpressionUploadModelLinesRequest {
              parent = uploadName
              if (pageToken.isNotEmpty()) {
                this.pageToken = pageToken
              }
            }
          )
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
            modelRolloutsStub.listModelRollouts(
              listModelRolloutsRequest {
                parent = modelLineName
                if (pageToken.isNotEmpty()) {
                  this.pageToken = pageToken
                }
              }
            )
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
            modelShardsStub.listModelShards(
              listModelShardsRequest {
                parent = dataProviderName
                if (pageToken.isNotEmpty()) {
                  this.pageToken = pageToken
                }
              }
            )
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
   * Creates one Phase-2 VidLabeler WorkItem for [vidLabelingJobName], covering all bundled
   * [modelLineNames]. The TEE resolves the job's `RawImpressionUploadFile`s (the bin-packed batch)
   * from the metadata API, so they are not duplicated onto the params.
   *
   * Idempotency uses resource-name uniqueness (a deterministic [workItemId] derived from the job
   * name), not an AIP-155 `request_id` — `CreateWorkItemRequest` has no `request_id` field. A retry
   * therefore returns `ALREADY_EXISTS` (handled below) rather than the cached response.
   */
  private suspend fun createWorkItem(
    uploadName: String,
    modelLineNames: List<String>,
    modelBlobPathByLine: Map<String, String>,
    resolvedModelLines: Map<String, ModelLine>,
    vidLabelingJobName: String,
  ) {
    // Resolve per-line config OUTSIDE the proto builder: inside `vidLabelerParams { }` the receiver
    // shadows `modelLineConfigs` / `modelBlobPaths` with its own (empty) builder maps.
    val lineConfigs: Map<String, VidLabelerParams.ModelLineConfig> =
      modelLineNames.associateWith { modelLineName ->
        val modelLineConfig =
          requireNotNull(modelLineConfigs[modelLineName]) {
            "No ModelLineConfig found for model line: $modelLineName"
          }
        val resolvedModelLine =
          requireNotNull(resolvedModelLines[modelLineName]) {
            "No resolved ModelLine for model line: $modelLineName"
          }
        VidLabelerParamsKt.modelLineConfig {
          labelerInputFieldMapping.addAll(modelLineConfig.labelerInputFieldMappingList)
          eventTemplateFieldMapping.putAll(modelLineConfig.eventTemplateFieldMappingMap)
          // The active window lets the TEE drop out-of-window impressions before labeling.
          activeStartTime = resolvedModelLine.activeStartTime
          if (resolvedModelLine.hasActiveEndTime()) {
            activeEndTime = resolvedModelLine.activeEndTime
          }
        }
      }

    // Start from the shared template (data provider, storage params, vid-repo connection, and the
    // model-storage project) via `copy { }` so any field later added to the template automatically
    // flows onto non-memoized WorkItems too; only the per-WorkItem fields are set below.
    val params =
      vidLabelerParamsTemplate.copy {
        modelLineConfigs.putAll(lineConfigs)
        for (modelLineName in modelLineNames) {
          modelBlobPaths[modelLineName] =
            requireNotNull(modelBlobPathByLine[modelLineName]) {
              "No model blob path for model line: $modelLineName"
            }
        }
        // The bundled model lines this WorkItem labels. `override_model_lines` is reserved for the
        // operator-header override and is left unset here.
        modelLines += modelLineNames
        rawImpressionUpload = uploadName
        vidLabelingJob = vidLabelingJobName
      }

    val workItemId = WorkItemIds.forVidLabeler(vidLabelingJobName)
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
      throw e
    }
    logger.info("Created WorkItem $workItemId for job $vidLabelingJobName")
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
      throw e
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
      throw e
    }
  }

  /** Fetches the `ModelLine` to read its active window (`active_start_time`/`active_end_time`). */
  private suspend fun getModelLine(modelLineName: String): ModelLine =
    modelLinesStub.getModelLine(getModelLineRequest { name = modelLineName })

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
      val response = poolAssignmentJobStub.batchCreatePoolAssignmentJobs(request)
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
  ) {
    // Start from the shared template (data provider, storage params, TLS connection) and fill in
    // the per-shard fields. `copy` carries every template field, so a field added to the template
    // later is propagated automatically.
    val params =
      subpoolAssignerParamsTemplate.copy {
        rawImpressionUpload = uploadName
        modelLine = modelLineName
        this.modelBlobPath = modelBlobPath
        activeStartTime = resolvedModelLine.activeStartTime
        if (resolvedModelLine.hasActiveEndTime()) {
          activeEndTime = resolvedModelLine.activeEndTime
        }
        this.shardIndex = shardIndex
        totalShards = numberOfShards
        labelerInputFieldMapping.addAll(modelLineConfig.labelerInputFieldMappingList)
        eventTemplateFieldMapping.putAll(modelLineConfig.eventTemplateFieldMappingMap)
        this.poolAssignmentJob = poolAssignmentJob
      }

    val workItemId = WorkItemIds.forSubpoolAssigner(uploadName, modelLineName, shardIndex)
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
      throw e
    }
    logger.info(
      "Created SubpoolAssigner WorkItem $workItemId for model line $modelLineName shard $shardIndex"
    )
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    /** Maximum `CreatePoolAssignmentJobRequest`s per `BatchCreatePoolAssignmentJobs` call. */
    private const val POOL_ASSIGNMENT_JOB_BATCH_SIZE = 50

    /** Maximum `CreateVidLabelingJobRequest`s per `BatchCreateVidLabelingJobs` call. */
    private const val DEFAULT_MAX_JOBS_PER_BATCH_CREATE = 50

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
