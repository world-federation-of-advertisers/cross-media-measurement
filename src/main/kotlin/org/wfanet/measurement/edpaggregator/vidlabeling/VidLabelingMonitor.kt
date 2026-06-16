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
import io.grpc.StatusException
import io.opentelemetry.api.common.Attributes
import java.time.Clock
import java.time.Duration
import java.util.UUID
import java.util.logging.Level
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
 * Monitors the VID labeling pipeline for one `DataProvider` and drives dispatch sequencing.
 *
 * Cloud Scheduler invokes [VidLabelingMonitorFunction] periodically; per DataProvider it builds one
 * [VidLabelingMonitor] and calls [run]. This first iteration (#3958) implements:
 * - **Dispatch sequencing:** at most one upload per DataProvider runs at a time. If any upload is
 *   `ACTIVE`, do nothing; otherwise dispatch the oldest `CREATED` upload — for each of its model
 *   lines, create the Phase-0/Phase-2 work and transition the model line out of `CREATED`. This
 *   prevents two uploads for the same `(DataProvider, ModelLine)` from corrupting the cumulative
 *   rank index by running Phase 1 concurrently.
 * - **Failure + staleness monitoring:** uploads stuck in a non-terminal state past
 *   [stalenessThreshold], and model lines in `FAILED`, are logged at `SEVERE` for Cloud Monitoring
 *   alerting.
 *
 * Phase-transition advancement (`POOL_ASSIGNING → RANKING → LABELING → COMPLETED`) and data-quality
 * checks are added in follow-up PRs; the scan is structured so each is an additive step.
 *
 * The dispatch logic (model-shard resolution, WorkItem + PoolAssignmentJob creation) mirrors what
 * the `VidLabelingDispatcher` did before it was made registration-only.
 *
 * @param rawImpressionUploadStub stub for `RawImpressionUploadService`.
 * @param rawImpressionUploadModelLineStub stub for `RawImpressionUploadModelLineService`.
 * @param poolAssignmentJobStub stub for `PoolAssignmentJobService`.
 * @param workItemsStub stub for creating WorkItems via the Secure Computation API.
 * @param modelRolloutsStub VID Repository ModelRollouts API.
 * @param modelShardsStub VID Repository ModelShards API.
 * @param dataProviderName resource name of the `DataProvider` this monitor scans.
 * @param vidLabelerParamsTemplate template [VidLabelerParams] carrying storage + connection fields.
 * @param queueName resource name of the Secure Computation queue.
 * @param numberOfShards static number of shards per model line.
 * @param modelLineConfigs field-mapping configuration keyed by model line resource name.
 * @param stalenessThreshold non-terminal uploads older than this are flagged as stuck.
 * @param clock clock used for staleness evaluation.
 * @param metrics OpenTelemetry recorder.
 */
class VidLabelingMonitor(
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
  private val stalenessThreshold: Duration,
  private val clock: Clock = Clock.systemUTC(),
) {

  /** Outcome of one monitor run for a DataProvider. */
  data class MonitorResult(
    /** Resource name of the upload dispatched this run, or null if none. */
    val dispatchedUpload: String?,
    /** Number of `CREATED` uploads held behind an in-progress upload. */
    val queuedUploads: Int,
    /** Resource names of uploads stuck in a non-terminal state past the SLA. */
    val stuckUploads: List<String>,
    /** Resource names of model lines in `FAILED`. */
    val failedModelLines: List<String>,
  ) {
    val hasIssues: Boolean
      get() = stuckUploads.isNotEmpty() || failedModelLines.isNotEmpty()
  }

  /** Resolved model shard info for a model line. */
  private data class ResolvedShardInfo(
    val modelBlobPath: String,
    val memoizationEnabled: Boolean,
  )

  /** Scans this DataProvider's uploads, dispatches at most one, and reports health issues. */
  suspend fun run(): MonitorResult {
    val dispatched: String? = checkDispatchSequencing()
    val (stuckUploads, failedModelLines) = checkFailuresAndStaleness()
    return MonitorResult(
      dispatchedUpload = dispatched,
      queuedUploads = if (dispatched == null) countQueued() else 0,
      stuckUploads = stuckUploads,
      failedModelLines = failedModelLines,
    )
  }

  /**
   * Dispatches the oldest `CREATED` upload iff no upload for this DataProvider is `ACTIVE`.
   *
   * @return the resource name of the dispatched upload, or null if none was dispatched.
   */
  private suspend fun checkDispatchSequencing(): String? {
    val activeUploads = listUploads(RawImpressionUpload.State.ACTIVE)
    if (activeUploads.isNotEmpty()) {
      val queued = countQueued()
      if (queued > 0) {
        VidLabelingMonitorMetrics.uploadsQueuedCounter.add(queued.toLong(), dataProviderAttributes())
        logger.info(
          "$queued upload(s) queued behind ${activeUploads.size} active upload(s) for " +
            dataProviderName
        )
      }
      return null
    }

    val nextUpload: RawImpressionUpload =
      listUploads(RawImpressionUpload.State.CREATED).minByOrNull {
        Timestamps.toNanos(it.createTime)
      } ?: return null

    activateUpload(nextUpload)
    VidLabelingMonitorMetrics.uploadsDispatchedCounter.add(1, dataProviderAttributes())
    logger.info("Dispatched upload ${nextUpload.name}")
    return nextUpload.name
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
        markPoolAssigning(modelLine.name)
      } else {
        for (shardIndex in 0 until numberOfShards) {
          createWorkItem(modelLine.cmmsModelLine, shardInfo.modelBlobPath, shardIndex, uploadId)
        }
        markLabeling(modelLine.name)
      }
    }
  }

  /**
   * Logs uploads stuck in a non-terminal state past [stalenessThreshold] and model lines in
   * `FAILED` at `SEVERE` for alerting.
   *
   * @return stuck upload names and failed model line names.
   */
  private suspend fun checkFailuresAndStaleness(): Pair<List<String>, List<String>> {
    val activeUploads: List<RawImpressionUpload> = listUploads(RawImpressionUpload.State.ACTIVE)
    val nowNanos: Long = Timestamps.toNanos(Timestamps.fromMillis(clock.millis()))
    val thresholdNanos: Long = stalenessThreshold.toNanos()

    val stuckUploads: List<String> =
      activeUploads
        .filter { nowNanos - Timestamps.toNanos(it.createTime) > thresholdNanos }
        .map { it.name }
    if (stuckUploads.isNotEmpty()) {
      VidLabelingMonitorMetrics.uploadsStuckCounter.add(
        stuckUploads.size.toLong(),
        dataProviderAttributes(),
      )
      for (name in stuckUploads) {
        logger.log(
          Level.SEVERE,
          "ALERT: upload $name has been non-terminal for longer than $stalenessThreshold",
        )
      }
    }

    val failedModelLines: List<String> =
      activeUploads.flatMap { upload ->
        listUploadModelLines(upload.name)
          .filter { it.state == RawImpressionUploadModelLine.State.FAILED }
          .map { it.name }
      }
    if (failedModelLines.isNotEmpty()) {
      VidLabelingMonitorMetrics.failedUploadsCounter.add(
        failedModelLines.size.toLong(),
        dataProviderAttributes(),
      )
      for (name in failedModelLines) {
        logger.log(Level.SEVERE, "ALERT: model line $name is FAILED")
      }
    }

    return stuckUploads to failedModelLines
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
                filter = ListRawImpressionUploadsRequestKt.filter { this.state = state }
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
  private suspend fun listUploadModelLines(
    uploadName: String
  ): List<RawImpressionUploadModelLine> =
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

  /** Resolves model shard info for a model line via the ModelRollout -> ModelShard chain. */
  private suspend fun resolveShardInfo(modelLineName: String): ResolvedShardInfo? {
    val modelReleaseName: String = resolveActiveModelRelease(modelLineName) ?: return null
    return resolveShardInfoFromShards(modelReleaseName)
  }

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
            requestId = UUID.nameUUIDFromBytes("$uploadName:$modelLineName:$shardIndex".toByteArray()).toString()
          }
        }
      }
      try {
        poolAssignmentJobStub.batchCreatePoolAssignmentJobs(request)
      } catch (e: StatusException) {
        throw Exception("Error creating PoolAssignmentJobs for $modelLineName", e)
      }
    }
    logger.info("Created $numberOfShards PoolAssignmentJobs for memoized model line $modelLineName")
  }

  /** Creates a WorkItem in the Secure Computation control plane for one model line shard. */
  private suspend fun createWorkItem(
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
      vidLabeledImpressionsStorageParams = vidLabelerParamsTemplate.vidLabeledImpressionsStorageParams
      rawImpressionsStorageParams = vidLabelerParamsTemplate.rawImpressionsStorageParams
      vidRepoConnection = vidLabelerParamsTemplate.vidRepoConnection
      modelLineConfigs[modelLineName] =
        VidLabelerParamsKt.modelLineConfig {
          labelerInputFieldMapping.putAll(modelLineConfig.labelerInputFieldMappingMap)
          eventTemplateFieldMapping.putAll(modelLineConfig.eventTemplateFieldMappingMap)
        }
      overrideModelLines += listOf(modelLineName)
      this.shardIndex = shardIndex
      totalShards = numberOfShards
      modelBlobPaths[modelLineName] = modelBlobPath
    }

    val workItemId = "vid-labeling-$uploadId-${modelLineName.substringAfterLast("/")}-shard-$shardIndex"
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
      throw Exception("Error creating WorkItem $workItemId", e)
    }
    logger.info("Created WorkItem $workItemId for model line $modelLineName shard $shardIndex")
  }

  private suspend fun markPoolAssigning(modelLineResourceName: String) {
    try {
      rawImpressionUploadModelLineStub.markRawImpressionUploadModelLinePoolAssigning(
        markRawImpressionUploadModelLinePoolAssigningRequest { name = modelLineResourceName }
      )
    } catch (e: StatusException) {
      throw Exception("Error marking $modelLineResourceName POOL_ASSIGNING", e)
    }
  }

  private suspend fun markLabeling(modelLineResourceName: String) {
    try {
      rawImpressionUploadModelLineStub.markRawImpressionUploadModelLineLabeling(
        markRawImpressionUploadModelLineLabelingRequest { name = modelLineResourceName }
      )
    } catch (e: StatusException) {
      throw Exception("Error marking $modelLineResourceName LABELING", e)
    }
  }

  private fun dataProviderAttributes(): Attributes =
    Attributes.of(VidLabelingMonitorMetrics.DATA_PROVIDER_ATTR, dataProviderName)

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private const val POOL_ASSIGNMENT_JOB_BATCH_SIZE = 100
  }
}
