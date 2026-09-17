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

import com.google.protobuf.kotlin.unpack
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
import org.wfanet.measurement.edpaggregator.VidLabelingRpcThrottlers
import org.wfanet.measurement.edpaggregator.rawimpressions.RawImpressionFileBinPacker
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJob
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineKt.phaseZeroDispatch
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.SubpoolAssignerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobKt.workItemDispatch
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateVidLabelingJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.createVidLabelingJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getRawImpressionUploadModelLineRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineLabelingRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLinePoolAssigningRequest
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelingJob
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.createWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.getWorkItemRequest
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
 * `POOL_ASSIGNING` claim with an immutable dispatch snapshot, creating one `PoolAssignmentJob` per
 * shard, then publishing one SubpoolAssigner `WorkItem` per shard on the [poolAssignerQueueName]
 * queue); the **non-memoized** model lines of an upload are **bundled together** and dispatched to
 * Phase-2 directly — the upload's `RawImpressionUploadFile`s are bin-packed by size into one
 * `VidLabelingJob` per batch (each job covering every bundled model line), one VidLabeler
 * `WorkItem` is published per job on [queueName], then each bundled model line transitions to
 * `LABELING`.
 *
 * Because the fast path and the monitor can run concurrently, two callers can momentarily both pick
 * the same model line. Each transition is therefore guarded by an etag compare-and-swap: the first
 * caller to call `Mark*` with the model line's etag wins. A losing caller re-reads the model line
 * and no-ops only when the requested transition was actually completed; otherwise it propagates the
 * conflict so the dispatch is retried. Every create is idempotent (deterministic `WorkItem` IDs;
 * deterministic `PoolAssignmentJob` `request_id`s), so the loser's redundant create calls are
 * harmless.
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
 * @param rpcThrottlers process-scoped rate limiters shared with the caller.
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
  private val rpcThrottlers: VidLabelingRpcThrottlers,
  private val maxJobsPerBatchCreate: Int = DEFAULT_MAX_JOBS_PER_BATCH_CREATE,
) {

  private val phaseZeroDispatchReconciler =
    PhaseZeroDispatchReconciler(poolAssignmentJobStub, workItemsStub, rpcThrottlers)

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
        .filter { it.registrationComplete }
        .filter { !it.processingDeferred }
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
          if (dispatchMemoized(upload.name, modelLine, shardInfo)) {
            busyModelLines += modelLine.cmmsModelLine
            if (dispatchedUpload == null) dispatchedUpload = upload.name
          }
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
   * Replays the idempotent Phase-0 publication for a model line whose dispatch was interrupted.
   *
   * The job request IDs and WorkItem IDs are deterministic, so this recreates only publications
   * that are missing. Returns false when the model line no longer resolves to a memoized shard.
   */
  suspend fun resumeMemoizedDispatch(
    uploadName: String,
    modelLine: RawImpressionUploadModelLine,
    poolAssignmentJobs: List<PoolAssignmentJob>,
  ): Boolean {
    val persistedDispatch =
      if (modelLine.hasPhaseZeroDispatch()) {
        decodePhaseZeroDispatch(modelLine.phaseZeroDispatch)
      } else {
        findPersistedPhaseZeroDispatch(uploadName, modelLine.cmmsModelLine, poolAssignmentJobs)
      }
    if (persistedDispatch != null) {
      return resumePersistedMemoizedDispatch(
        uploadName,
        modelLine,
        poolAssignmentJobs,
        persistedDispatch,
      )
    }
    val shardInfo = resolveShardInfo(modelLine.cmmsModelLine) ?: return false
    if (!shardInfo.memoizationEnabled) return false
    return dispatchMemoized(uploadName, modelLine, shardInfo, poolAssignmentJobs)
  }

  /** The original queue and parameters copied from an already-published Phase-0 WorkItem. */
  private data class PersistedPhaseZeroDispatch(
    val proto: RawImpressionUploadModelLine.PhaseZeroDispatch,
    val params: SubpoolAssignerParams,
  ) {
    val queue: String
      get() = proto.workItemQueue
  }

  private fun decodePhaseZeroDispatch(
    dispatch: RawImpressionUploadModelLine.PhaseZeroDispatch
  ): PersistedPhaseZeroDispatch {
    check(dispatch.workItemQueue.isNotEmpty()) { "Persisted Phase-0 WorkItem queue is empty" }
    check(!dispatch.workItemParams.isEmpty) { "Persisted Phase-0 WorkItem parameters are empty" }
    val params =
      WorkItemParams.parseFrom(dispatch.workItemParams)
        .appParams
        .unpack(SubpoolAssignerParams::class.java)
    return PersistedPhaseZeroDispatch(dispatch, params)
  }

  /**
   * Returns the first original Phase-0 publication for this dispatch. A partial publication has at
   * least one such WorkItem, whose immutable parameters are the authoritative dispatch snapshot.
   */
  private suspend fun findPersistedPhaseZeroDispatch(
    uploadName: String,
    modelLineName: String,
    poolAssignmentJobs: List<PoolAssignmentJob>,
  ): PersistedPhaseZeroDispatch? {
    for (job in poolAssignmentJobs.sortedBy { it.shardIndex }) {
      val shardIndex = job.shardIndex
      val workItemId = WorkItemIds.forSubpoolAssigner(uploadName, modelLineName, shardIndex)
      val existing =
        try {
          rpcThrottlers.controlPlane.onReady {
            workItemsStub.getWorkItem(getWorkItemRequest { name = "workItems/$workItemId" })
          }
        } catch (e: StatusException) {
          if (e.status.code == Status.Code.NOT_FOUND) continue
          throw e
        }
      val outerParams = existing.workItemParams.unpack(WorkItemParams::class.java)
      return decodePhaseZeroDispatch(
        phaseZeroDispatch {
          workItemQueue = existing.queue
          workItemParams = outerParams.toByteString()
        }
      )
    }
    return null
  }

  /** Re-publishes missing shards from the parameters captured by an earlier Phase-0 publication. */
  private suspend fun resumePersistedMemoizedDispatch(
    uploadName: String,
    modelLine: RawImpressionUploadModelLine,
    poolAssignmentJobs: List<PoolAssignmentJob>,
    persistedDispatch: PersistedPhaseZeroDispatch,
  ): Boolean {
    val persistedParams = persistedDispatch.params
    check(persistedParams.rawImpressionUpload == uploadName) {
      "Persisted Phase-0 parameters belong to ${persistedParams.rawImpressionUpload}, not " +
        uploadName
    }
    check(persistedParams.modelLine == modelLine.cmmsModelLine) {
      "Persisted Phase-0 parameters belong to ${persistedParams.modelLine}, not " +
        modelLine.cmmsModelLine
    }
    val claimed =
      markPoolAssigning(modelLine.name, modelLine.etag, persistedDispatch.proto) ?: return false
    val authoritativeDispatch =
      if (claimed.hasPhaseZeroDispatch()) {
        decodePhaseZeroDispatch(claimed.phaseZeroDispatch)
      } else {
        persistedDispatch
      }
    val authoritativeParams = authoritativeDispatch.params
    check(
      authoritativeParams.rawImpressionUpload == uploadName &&
        authoritativeParams.modelLine == modelLine.cmmsModelLine &&
        authoritativeParams.totalShards > 0
    ) {
      "Persisted Phase-0 parameters do not match ${modelLine.name}"
    }
    phaseZeroDispatchReconciler.publish(
      phaseZeroDispatchReconciler.prepare(
        uploadName,
        modelLine.cmmsModelLine,
        authoritativeDispatch.proto,
        poolAssignmentJobs,
      )
    )
    return true
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

    val dispatchParams =
      buildVidLabelerParams(uploadName, modelLineNames, modelBlobPathByLine, resolvedModelLines)
    val dispatch = workItemDispatch {
      workItemQueue = queueName
      workItemParams = workItemParams { appParams = dispatchParams.pack() }.toByteString()
    }
    val batches: List<List<String>> = RawImpressionFileBinPacker.pack(files, maxFileBatchSizeBytes)
    val labelingJobs: List<VidLabelingJob> =
      createVidLabelingJobs(uploadName, modelLineNames, batches, dispatch)
    for (job in labelingJobs) {
      createWorkItem(uploadName, job, dispatch)
    }
    for (bundled in bundle) markLabeling(bundled.modelLine.name, bundled.modelLine.etag)
  }

  /** Lists the `RawImpressionUploadFile` children of [uploadName]. */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun listUploadFiles(uploadName: String): List<RawImpressionUploadFile> =
    rawImpressionUploadFileStub
      .listResources { pageToken: String ->
        val response =
          rpcThrottlers.metadataRead.onReady {
            rawImpressionUploadFileStub.listRawImpressionUploadFiles(
              listRawImpressionUploadFilesRequest {
                parent = uploadName
                if (pageToken.isNotEmpty()) {
                  this.pageToken = pageToken
                }
              }
            )
          }
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
    dispatch: VidLabelingJob.WorkItemDispatch,
  ): List<VidLabelingJob> {
    val created = mutableListOf<VidLabelingJob>()
    for (group in batches.withIndex().chunked(maxJobsPerBatchCreate)) {
      val response =
        rpcThrottlers.metadataWrite.onReady {
          vidLabelingJobStub.batchCreateVidLabelingJobs(
            batchCreateVidLabelingJobsRequest {
              parent = uploadName
              for ((batchIndex, batch) in group) {
                requests += createVidLabelingJobRequest {
                  parent = uploadName
                  vidLabelingJob = vidLabelingJob {
                    cmmsModelLines += modelLineNames
                    rawImpressionUploadFiles += batch
                    workItemDispatch = dispatch
                  }
                  requestId = RequestIds.forVidLabelingJob(uploadName, modelLineNames, batchIndex)
                }
              }
            }
          )
        }
      check(response.vidLabelingJobsList.size == group.size) {
        "BatchCreateVidLabelingJobs returned ${response.vidLabelingJobsList.size} jobs for " +
          "${group.size} requests"
      }
      created.addAll(response.vidLabelingJobsList)
    }
    return created
  }

  /**
   * Memoized (Phase-0) dispatch: persist the `POOL_ASSIGNING` claim and dispatch snapshot, create a
   * `PoolAssignmentJob` per shard, then publish one SubpoolAssigner `WorkItem` per shard on
   * [poolAssignerQueueName].
   *
   * The state claim precedes WorkItem publication so a worker never observes a `CREATED` parent.
   * Every create is idempotent (deterministic `request_id` for the jobs, deterministic `workItemId`
   * for the WorkItems), and the monitor resumes any publication interrupted after the claim.
   */
  private suspend fun dispatchMemoized(
    uploadName: String,
    modelLine: RawImpressionUploadModelLine,
    shardInfo: ResolvedShardInfo,
    precreatedJobs: List<PoolAssignmentJob>? = null,
  ): Boolean {
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
    // The bin-packing cap is REQUIRED on `SubpoolAssignerParams`, so an unset one fails here
    // rather than inside the TEE at the Phase-0 last-shard-out.
    require(subpoolAssignerParamsTemplate.maxFileBatchSizeBytes > 0) {
      "max_file_batch_size_bytes missing for memoized model line ${modelLine.cmmsModelLine}; " +
        "set it on VidLabelingConfig for this DataProvider"
    }
    val modelLineConfig =
      requireNotNull(modelLineConfigs[modelLine.cmmsModelLine]) {
        "No ModelLineConfig found for model line: ${modelLine.cmmsModelLine}"
      }
    // The active window is read from the ModelLine so the TEE can drop out-of-window impressions.
    val resolvedModelLine: ModelLine = getModelLine(modelLine.cmmsModelLine)

    val dispatchParams =
      buildSubpoolAssignerParams(
        uploadName,
        modelLine.cmmsModelLine,
        shardInfo.modelBlobPath,
        resolvedModelLine,
        modelLineConfig,
        poolAssignmentJob = "",
        shardIndex = 0,
        totalShards = numberOfShards,
      )

    // Claim the parent and persist the immutable dispatch snapshot before creating jobs or worker
    // messages. Every concurrent dispatcher must use the snapshot returned by this transition.
    val requestedDispatch = phaseZeroDispatch {
      workItemQueue = poolAssignerQueueName
      this.workItemParams = workItemParams { appParams = dispatchParams.pack() }.toByteString()
    }
    val claimed =
      markPoolAssigning(modelLine.name, modelLine.etag, requestedDispatch) ?: return false
    val authoritativeDispatch =
      if (claimed.hasPhaseZeroDispatch()) {
        decodePhaseZeroDispatch(claimed.phaseZeroDispatch)
      } else {
        decodePhaseZeroDispatch(requestedDispatch)
      }
    check(
      authoritativeDispatch.params.rawImpressionUpload == uploadName &&
        authoritativeDispatch.params.modelLine == modelLine.cmmsModelLine &&
        authoritativeDispatch.params.totalShards > 0
    ) {
      "Persisted Phase-0 parameters do not match ${modelLine.name}"
    }
    phaseZeroDispatchReconciler.publish(
      phaseZeroDispatchReconciler.prepare(
        uploadName,
        modelLine.cmmsModelLine,
        authoritativeDispatch.proto,
        precreatedJobs.orEmpty(),
      )
    )
    return true
  }

  /** Lists this DataProvider's uploads in [state]. */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun listUploads(state: RawImpressionUpload.State): List<RawImpressionUpload> =
    rawImpressionUploadStub
      .listResources { pageToken: String ->
        val response =
          rpcThrottlers.metadataRead.onReady {
            rawImpressionUploadStub.listRawImpressionUploads(
              listRawImpressionUploadsRequest {
                parent = dataProviderName
                filter = ListRawImpressionUploadsRequestKt.filter { stateIn += state }
                if (pageToken.isNotEmpty()) {
                  this.pageToken = pageToken
                }
              }
            )
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
          rpcThrottlers.metadataRead.onReady {
            rawImpressionUploadModelLineStub.listRawImpressionUploadModelLines(
              listRawImpressionUploadModelLinesRequest {
                parent = uploadName
                if (pageToken.isNotEmpty()) {
                  this.pageToken = pageToken
                }
              }
            )
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
            rpcThrottlers.kingdom.onReady {
              modelRolloutsStub.listModelRollouts(
                listModelRolloutsRequest {
                  parent = modelLineName
                  if (pageToken.isNotEmpty()) {
                    this.pageToken = pageToken
                  }
                }
              )
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
            rpcThrottlers.kingdom.onReady {
              modelShardsStub.listModelShards(
                listModelShardsRequest {
                  parent = dataProviderName
                  if (pageToken.isNotEmpty()) {
                    this.pageToken = pageToken
                  }
                }
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
   * Creates one Phase-2 VidLabeler WorkItem for [vidLabelingJobName], covering all bundled
   * [modelLineNames]. The TEE resolves the job's `RawImpressionUploadFile`s (the bin-packed batch)
   * from the metadata API, so they are not duplicated onto the params.
   *
   * Idempotency uses resource-name uniqueness (a deterministic [workItemId] derived from the job
   * name), not an AIP-155 `request_id` — `CreateWorkItemRequest` has no `request_id` field. A retry
   * therefore returns `ALREADY_EXISTS` (handled below) rather than the cached response.
   */
  private fun buildVidLabelerParams(
    uploadName: String,
    modelLineNames: List<String>,
    modelBlobPathByLine: Map<String, String>,
    resolvedModelLines: Map<String, ModelLine>,
  ): VidLabelerParams {
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
          // Per-impression entity-key columns (Phase-2 reads them per row). Not copied by
          // convertModelLineConfigs' output alone — this rebuild would drop them otherwise.
          requiredEntityKeyFieldMapping.putAll(modelLineConfig.requiredEntityKeyFieldMappingMap)
          optionalEntityKeyFieldMapping.putAll(modelLineConfig.optionalEntityKeyFieldMappingMap)
          // Phase-2 requires the event-template descriptor to build the labeled output; carry it
          // (and its type) from the config onto every non-memoized WorkItem's ModelLineConfig.
          eventTemplateDescriptorBlobUri = modelLineConfig.eventTemplateDescriptorBlobUri
          eventTemplateType = modelLineConfig.eventTemplateType
          // Phase-2 resolves each assigned VID's population attributes from this spec, and rejects
          // a WorkItem without it. Like the fields above, it is dropped unless copied explicitly.
          populationSpecBlobUri = modelLineConfig.populationSpecBlobUri
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
    return vidLabelerParamsTemplate.copy {
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
    }
  }

  /** Publishes [job] from its persisted dispatch snapshot. */
  private suspend fun createWorkItem(
    uploadName: String,
    job: VidLabelingJob,
    fallbackDispatch: VidLabelingJob.WorkItemDispatch,
  ) {
    val dispatch = if (job.hasWorkItemDispatch()) job.workItemDispatch else fallbackDispatch
    val persistedWorkItemParams = WorkItemParams.parseFrom(dispatch.workItemParams)
    check(persistedWorkItemParams.appParams.`is`(VidLabelerParams::class.java)) {
      "Persisted WorkItem parameters for ${job.name} are not VidLabelerParams"
    }
    val persistedParams = persistedWorkItemParams.appParams.unpack(VidLabelerParams::class.java)
    check(
      persistedParams.rawImpressionUpload == uploadName &&
        persistedParams.modelLinesList.toSet() == job.cmmsModelLinesList.toSet()
    ) {
      "Persisted WorkItem parameters do not belong to ${job.name}"
    }
    val params = persistedParams.copy { vidLabelingJob = job.name }

    val workItemId = WorkItemIds.forVidLabeler(job.name)
    val request = createWorkItemRequest {
      this.workItemId = workItemId
      workItem = workItem {
        queue = dispatch.workItemQueue
        workItemParams =
          persistedWorkItemParams.toBuilder().setAppParams(params.pack()).build().pack()
      }
    }
    try {
      rpcThrottlers.controlPlane.onReady { workItemsStub.createWorkItem(request) }
    } catch (e: StatusException) {
      if (e.status.code == Status.Code.ALREADY_EXISTS) {
        // A concurrent dispatch already created this WorkItem; the deterministic ID makes this a
        // no-op. Safe to ignore.
        logger.info("WorkItem $workItemId already exists; skipping (concurrent dispatch)")
        return
      }
      throw e
    }
    logger.info("Created WorkItem $workItemId for job ${job.name}")
  }

  private suspend fun markLabeling(modelLineName: String, etag: String) {
    transitionWithFreshEtag(
      modelLineName,
      etag,
      RawImpressionUploadModelLine.State.CREATED,
      setOf(
        RawImpressionUploadModelLine.State.LABELING,
        RawImpressionUploadModelLine.State.COMPLETED,
      ),
      "LABELING",
    ) { currentEtag ->
      rpcThrottlers.metadataWrite.onReady {
        rawImpressionUploadModelLineStub.markRawImpressionUploadModelLineLabeling(
          markRawImpressionUploadModelLineLabelingRequest {
            name = modelLineName
            this.etag = currentEtag
            requestId = RequestIds.forMarkRawImpressionUploadModelLineLabeling(modelLineName)
          }
        )
      }
    }
  }

  private suspend fun markPoolAssigning(
    modelLineName: String,
    etag: String,
    dispatch: RawImpressionUploadModelLine.PhaseZeroDispatch,
  ): RawImpressionUploadModelLine? =
    transitionWithFreshEtag(
      modelLineName,
      etag,
      RawImpressionUploadModelLine.State.CREATED,
      setOf(
        RawImpressionUploadModelLine.State.POOL_ASSIGNING,
        RawImpressionUploadModelLine.State.RANKING,
        RawImpressionUploadModelLine.State.LABELING,
        RawImpressionUploadModelLine.State.COMPLETED,
      ),
      "POOL_ASSIGNING",
    ) { currentEtag ->
      rpcThrottlers.metadataWrite.onReady {
        rawImpressionUploadModelLineStub.markRawImpressionUploadModelLinePoolAssigning(
          markRawImpressionUploadModelLinePoolAssigningRequest {
            name = modelLineName
            this.etag = currentEtag
            requestId = RequestIds.forMarkRawImpressionUploadModelLinePoolAssigning(modelLineName)
            phaseZeroDispatch = dispatch
          }
        )
      }
    }

  /** Retries an optimistic transition while concurrent child updates only rotate the etag. */
  private suspend fun transitionWithFreshEtag(
    modelLineName: String,
    initialEtag: String,
    sourceState: RawImpressionUploadModelLine.State,
    completedStates: Set<RawImpressionUploadModelLine.State>,
    targetState: String,
    transition: suspend (etag: String) -> RawImpressionUploadModelLine,
  ): RawImpressionUploadModelLine? {
    var etag = initialEtag
    while (true) {
      try {
        return transition(etag)
      } catch (e: StatusException) {
        if (!isConcurrentClaimLoss(e)) throw e

        val current =
          rpcThrottlers.metadataRead.onReady {
            rawImpressionUploadModelLineStub.getRawImpressionUploadModelLine(
              getRawImpressionUploadModelLineRequest { name = modelLineName }
            )
          }
        if (current.state in completedStates) {
          logger.info(
            "Skipping $targetState for $modelLineName: a concurrent dispatch advanced it to " +
              current.state
          )
          return current
        }
        if (current.state == RawImpressionUploadModelLine.State.FAILED) {
          logger.info("Skipping $targetState for $modelLineName: the model line is FAILED")
          return null
        }
        if (current.state != sourceState) throw e

        logger.info(
          "Retrying $targetState for $modelLineName with its refreshed etag after a concurrent " +
            "child update"
        )
        etag = current.etag
      }
    }
  }

  /** Fetches the `ModelLine` to read its active window (`active_start_time`/`active_end_time`). */
  private suspend fun getModelLine(modelLineName: String): ModelLine =
    rpcThrottlers.kingdom.onReady {
      modelLinesStub.getModelLine(getModelLineRequest { name = modelLineName })
    }

  /** Builds the immutable Phase-0 parameters for one shard. */
  private fun buildSubpoolAssignerParams(
    uploadName: String,
    modelLineName: String,
    modelBlobPath: String,
    resolvedModelLine: ModelLine,
    modelLineConfig: VidLabelerParams.ModelLineConfig,
    poolAssignmentJob: String,
    shardIndex: Int,
    totalShards: Int,
  ): SubpoolAssignerParams {
    // Start from the shared template (data provider, storage params, TLS connection) and fill in
    // the per-shard fields. `copy` carries every template field, so a field added to the template
    // later is propagated automatically.
    return subpoolAssignerParamsTemplate.copy {
      rawImpressionUpload = uploadName
      modelLine = modelLineName
      this.modelBlobPath = modelBlobPath
      activeStartTime = resolvedModelLine.activeStartTime
      if (resolvedModelLine.hasActiveEndTime()) {
        activeEndTime = resolvedModelLine.activeEndTime
      }
      this.shardIndex = shardIndex
      this.totalShards = totalShards
      labelerInputFieldMapping.addAll(modelLineConfig.labelerInputFieldMappingList)
      eventTemplateFieldMapping.putAll(modelLineConfig.eventTemplateFieldMappingMap)
      // Pass-through so the Phase-1 last-out can stamp the event-template descriptor (which
      // Phase-2 requires) onto the memoized VidLabeler ModelLineConfig.
      eventTemplateDescriptorBlobUri = modelLineConfig.eventTemplateDescriptorBlobUri
      eventTemplateType = modelLineConfig.eventTemplateType
      // Pass-through for the same reason: Phase-2 resolves each assigned VID's population
      // attributes from this spec and rejects a WorkItem without it.
      populationSpecBlobUri = modelLineConfig.populationSpecBlobUri
      // Pass-through so the Phase-1 last-out can stamp the per-impression entity-key columns on
      // the memoized VidLabeler ModelLineConfig.
      requiredEntityKeyFieldMapping.putAll(modelLineConfig.requiredEntityKeyFieldMappingMap)
      optionalEntityKeyFieldMapping.putAll(modelLineConfig.optionalEntityKeyFieldMappingMap)
      this.poolAssignmentJob = poolAssignmentJob
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

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

    /** Whether [e] may be a concurrent transition and therefore requires a state re-read. */
    private fun isConcurrentClaimLoss(e: StatusException): Boolean {
      val code: Status.Code = e.status.code
      return code == Status.Code.ABORTED || code == Status.Code.FAILED_PRECONDITION
    }
  }
}
