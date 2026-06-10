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

import com.google.protobuf.Timestamp
import com.google.protobuf.util.Timestamps
import io.grpc.StatusException
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import java.time.Clock
import java.util.UUID
import java.util.logging.Logger
import kotlin.time.TimeSource
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelShardsGrpcKt
import org.wfanet.measurement.api.v2alpha.listModelLinesRequest
import org.wfanet.measurement.api.v2alpha.listModelRolloutsRequest
import org.wfanet.measurement.api.v2alpha.listModelShardsRequest
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.createRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelerParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.createWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem
import org.wfanet.measurement.storage.BlobUri
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient

/**
 * Uploads VID labeling work items to the Secure Computation control plane.
 *
 * Processes "done" blob events by crawling directories for raw impression files, resolving active
 * model lines via the VID Repository API (ListModelLines -> ListModelRollouts -> ListModelShards),
 * and creating one WorkItem per shard per model line for TEE processing.
 *
 * @param storageClient client for crawling raw impressions directory.
 * @param workItemsStub gRPC stub for creating WorkItems via Secure Computation API.
 * @param rawImpressionUploadStub gRPC stub for the `RawImpressionUploadService`.
 * @param modelLinesStub gRPC stub for the VID Repository ModelLines API.
 * @param modelRolloutsStub gRPC stub for the VID Repository ModelRollouts API.
 * @param modelShardsStub gRPC stub for the VID Repository ModelShards API.
 * @param dataProviderName resource name of the `DataProvider`.
 * @param vidLabelerParamsTemplate template [VidLabelerParams] with storage and connection fields.
 * @param queueName resource name of the Secure Computation queue.
 * @param numberOfShards static number of shards per model line.
 * @param modelSuiteName resource name of the model suite for ListModelLines.
 * @param overrideModelLines if non-empty, use these model lines instead of querying the API.
 *   Overrides bypass active window checks to support backfilling past data.
 * @param modelLineConfigs field mapping configuration keyed by model line resource name.
 * @param clock clock for determining active model line windows.
 * @param metrics OpenTelemetry metrics recorder.
 */
class VidLabelingDispatcher(
  private val storageClient: StorageClient,
  private val workItemsStub: WorkItemsGrpcKt.WorkItemsCoroutineStub,
  private val rawImpressionUploadStub:
    RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub,
  private val modelLinesStub: ModelLinesGrpcKt.ModelLinesCoroutineStub,
  private val modelRolloutsStub: ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub,
  private val modelShardsStub: ModelShardsGrpcKt.ModelShardsCoroutineStub,
  private val dataProviderName: String,
  private val vidLabelerParamsTemplate: VidLabelerParams,
  private val queueName: String,
  private val numberOfShards: Int,
  private val modelSuiteName: String,
  private val overrideModelLines: List<String>,
  private val modelLineConfigs: Map<String, VidLabelerParams.ModelLineConfig>,
  private val clock: Clock = Clock.systemUTC(),
  private val metrics: VidLabelingDispatcherMetrics = VidLabelingDispatcherMetrics(),
) {

  /**
   * Resolved model line info including the model blob path from the VID Repository.
   *
   * @param modelLineName resource name of the model line.
   * @param modelBlobPath path to the compiled model blob.
   */
  private data class ResolvedModelLine(
    val modelLineName: String,
    val modelBlobPath: String,
  )

  /**
   * Uploads VID labeling work for raw impression files in the directory containing the done
   * blob.
   *
   * @param doneBlobPath the full storage URI of the "done" blob that triggered this upload.
   * @throws IllegalArgumentException if [doneBlobPath] uses an unsupported URI scheme.
   * @throws Exception if a WorkItem creation fails via the Secure Computation API.
   */
  suspend fun upload(doneBlobPath: String) {
    val startTime: TimeSource.Monotonic.ValueTimeMark = TimeSource.Monotonic.markNow()

    try {
      val doneBlobUri: BlobUri = SelectedStorageClient.parseBlobUri(doneBlobPath)
      val folderPrefix: String = doneBlobUri.key.substringBeforeLast("/")

      val blobKeys = mutableListOf<String>()
      storageClient.listBlobs(folderPrefix).collect { blob ->
        if (!isDoneMarker(blob.blobKey)) {
          blobKeys.add(blob.blobKey)
        }
      }

      if (blobKeys.isEmpty()) {
        logger.info("No raw impression files found in $folderPrefix")
        recordUploadDuration(startTime, UPLOAD_STATUS_SUCCESS)
        return
      }

      metrics.filesProcessedCounter.add(
        blobKeys.size.toLong(),
        Attributes.of(DATA_PROVIDER_ATTR, dataProviderName),
      )

      val rawImpressionUpload = createRawImpressionUpload(doneBlobPath)
      val uploadId = rawImpressionUpload.name.substringAfterLast("/")

      val resolvedModelLines = resolveModelLines()

      if (resolvedModelLines.isEmpty()) {
        logger.info("No active model lines resolved for $modelSuiteName")
        recordUploadDuration(startTime, UPLOAD_STATUS_SUCCESS)
        return
      }

      // TODO(world-federation-of-advertisers/cross-media-measurement#3899): Check
      // memoized_vid_assignment_enabled on ModelShard (available in API v0.94.0).
      // For memoized model lines, create PoolAssignmentJobs instead of WorkItems.

      var totalWorkItems = 0
      for (resolvedModelLine in resolvedModelLines) {
        for (shardIndex in 0 until numberOfShards) {
          createWorkItem(resolvedModelLine, shardIndex, uploadId)
          totalWorkItems++
        }
      }

      // TODO(world-federation-of-advertisers/cross-media-measurement#3899): Create
      // RawImpressionUploadFile resources for each blob URI.
      // TODO(world-federation-of-advertisers/cross-media-measurement#3899): Create
      // RawImpressionUploadModelLine resources for each active model line.
      // TODO(world-federation-of-advertisers/cross-media-measurement#3899): For memoized
      // model lines, create PoolAssignmentJobs instead of WorkItems.

      metrics.workItemsCreatedCounter.add(
        totalWorkItems.toLong(),
        Attributes.of(DATA_PROVIDER_ATTR, dataProviderName),
      )

      recordUploadDuration(startTime, UPLOAD_STATUS_SUCCESS)
    } catch (e: Exception) {
      recordUploadDuration(startTime, UPLOAD_STATUS_FAILED)
      throw e
    }
  }

  /**
   * Resolves active model lines with their model blob paths from the VID Repository API.
   *
   * If [overrideModelLines] is non-empty, uses those directly without active window filtering.
   * This supports backfilling past data where the model line may no longer be in the active window.
   *
   * Resolution chain: ListModelLines -> ListModelRollouts (to find active ModelRelease for each
   * ModelLine) -> ListModelShards (for this DataProvider, filtered by ModelRelease) ->
   * model_blob_path.
   *
   * @return list of resolved model lines with their blob paths.
   */
  private suspend fun resolveModelLines(): List<ResolvedModelLine> {
    val activeModelLineNames: List<String> =
      if (overrideModelLines.isNotEmpty()) {
        // Override model lines bypass active window checks to support backfilling past data.
        logger.info("Using ${overrideModelLines.size} override model lines")
        overrideModelLines
      } else {
        resolveActiveModelLinesFromApi()
      }

    if (activeModelLineNames.isEmpty()) return emptyList()

    val resolved = mutableListOf<ResolvedModelLine>()
    for (modelLineName in activeModelLineNames) {
      val modelBlobPath = resolveModelBlobPath(modelLineName)
      if (modelBlobPath != null) {
        resolved.add(ResolvedModelLine(modelLineName, modelBlobPath))
      } else {
        logger.warning("Could not resolve model blob path for $modelLineName, skipping")
      }
    }

    logger.info("Resolved ${resolved.size} model lines with blob paths")
    return resolved
  }

  /**
   * Lists active PROD model lines from the VID Repository API.
   *
   * @return list of active model line resource names that have entries in [modelLineConfigs].
   */
  private suspend fun resolveActiveModelLinesFromApi(): List<String> {
    val now: Timestamp = Timestamps.fromMillis(clock.millis())
    val activeModelLines = mutableListOf<String>()
    var pageToken = ""

    do {
      val request = listModelLinesRequest {
        parent = modelSuiteName
        if (pageToken.isNotEmpty()) {
          this.pageToken = pageToken
        }
      }

      val response =
        try {
          modelLinesStub.listModelLines(request)
        } catch (e: StatusException) {
          throw Exception("Error listing model lines for $modelSuiteName", e)
        }

      for (modelLine in response.modelLinesList) {
        if (modelLine.type != ModelLine.Type.PROD) continue
        if (!isWithinActiveWindow(modelLine, now)) continue
        if (modelLine.name !in modelLineConfigs) {
          logger.warning("Skipping model line ${modelLine.name}: no config entry")
          continue
        }
        activeModelLines.add(modelLine.name)
      }

      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())

    logger.info("Found ${activeModelLines.size} active PROD model lines from API")
    return activeModelLines
  }

  /**
   * Resolves the model blob path for a model line by traversing the ModelRollout -> ModelShard
   * chain.
   *
   * @param modelLineName resource name of the model line.
   * @return the model blob path, or null if no active rollout or shard is found.
   */
  private suspend fun resolveModelBlobPath(modelLineName: String): String? {
    val modelReleaseName = resolveActiveModelRelease(modelLineName) ?: return null
    return resolveModelBlobPathFromShards(modelReleaseName)
  }

  /**
   * Finds the active `ModelRelease` for a model line via ListModelRollouts.
   *
   * @param modelLineName resource name of the model line.
   * @return the model release resource name, or null if no rollout is found.
   */
  private suspend fun resolveActiveModelRelease(modelLineName: String): String? {
    var pageToken = ""

    do {
      val request = listModelRolloutsRequest {
        parent = modelLineName
        if (pageToken.isNotEmpty()) {
          this.pageToken = pageToken
        }
      }

      val response =
        try {
          modelRolloutsStub.listModelRollouts(request)
        } catch (e: StatusException) {
          throw Exception("Error listing model rollouts for $modelLineName", e)
        }

      for (rollout in response.modelRolloutsList) {
        if (rollout.modelRelease.isNotEmpty()) {
          return rollout.modelRelease
        }
      }

      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())

    logger.warning("No model rollout found for model line $modelLineName")
    return null
  }

  /**
   * Resolves the model blob path from `ModelShard` resources for this `DataProvider` and
   * `ModelRelease`.
   *
   * @param modelReleaseName resource name of the model release.
   * @return the model blob path, or null if no matching shard is found.
   */
  private suspend fun resolveModelBlobPathFromShards(modelReleaseName: String): String? {
    var pageToken = ""

    do {
      val request = listModelShardsRequest {
        parent = dataProviderName
        if (pageToken.isNotEmpty()) {
          this.pageToken = pageToken
        }
      }

      val response =
        try {
          modelShardsStub.listModelShards(request)
        } catch (e: StatusException) {
          throw Exception(
            "Error listing model shards for $dataProviderName release $modelReleaseName",
            e,
          )
        }

      for (shard in response.modelShardsList) {
        if (shard.modelRelease == modelReleaseName && shard.hasModelBlob()) {
          return shard.modelBlob.modelBlobPath
        }
      }

      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())

    logger.warning("No model shard found for release $modelReleaseName on $dataProviderName")
    return null
  }

  /**
   * Creates a `RawImpressionUpload` resource to track this upload.
   *
   * @param doneBlobPath the full storage URI of the "done" blob.
   * @return the created `RawImpressionUpload`.
   */
  private suspend fun createRawImpressionUpload(
    doneBlobPath: String
  ): org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload {
    val requestId = UUID.nameUUIDFromBytes(doneBlobPath.toByteArray()).toString()
    val request = createRawImpressionUploadRequest {
      parent = dataProviderName
      rawImpressionUpload = rawImpressionUpload { doneBlobUri = doneBlobPath }
      this.requestId = requestId
    }

    try {
      return rawImpressionUploadStub.createRawImpressionUpload(request)
    } catch (e: StatusException) {
      throw Exception("Error creating RawImpressionUpload for $doneBlobPath", e)
    }
  }

  /**
   * Creates a WorkItem in the Secure Computation control plane for a single model line shard.
   *
   * @param resolvedModelLine the resolved model line with its blob path.
   * @param shardIndex zero-based index of this shard.
   * @param uploadId unique identifier for this upload, used to prevent WorkItem ID collisions
   *   across multiple uploads by the same `DataProvider`.
   */
  private suspend fun createWorkItem(
    resolvedModelLine: ResolvedModelLine,
    shardIndex: Int,
    uploadId: String,
  ) {
    val modelLineName = resolvedModelLine.modelLineName
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
      modelLineConfigs[modelLineName] = VidLabelerParamsKt.modelLineConfig {
        labelerInputFieldMapping.putAll(modelLineConfig.labelerInputFieldMappingMap)
        eventTemplateFieldMapping.putAll(modelLineConfig.eventTemplateFieldMappingMap)
      }
      overrideModelLines += listOf(modelLineName)
      this.shardIndex = shardIndex
      totalShards = numberOfShards
      modelBlobPaths[modelLineName] = resolvedModelLine.modelBlobPath
    }

    val workItemId =
      "vid-labeling-$uploadId-${modelLineName.substringAfterLast("/")}-shard-$shardIndex"
    val packedWorkItemParams = workItemParams { appParams = params.pack() }.pack()

    val request = createWorkItemRequest {
      this.workItemId = workItemId
      workItem = workItem {
        queue = queueName
        workItemParams = packedWorkItemParams
      }
    }

    try {
      workItemsStub.createWorkItem(request)
    } catch (e: StatusException) {
      throw Exception("Error creating WorkItem $workItemId", e)
    }
    logger.info("Created WorkItem $workItemId for model line $modelLineName shard $shardIndex")
  }

  private fun recordUploadDuration(
    startTime: TimeSource.Monotonic.ValueTimeMark,
    status: String,
  ) {
    val duration: Double = startTime.elapsedNow().inWholeMilliseconds / 1000.0
    metrics.uploadDurationHistogram.record(
      duration,
      Attributes.of(DATA_PROVIDER_ATTR, dataProviderName, UPLOAD_STATUS_ATTR, status),
    )
  }

  private fun isDoneMarker(blobKey: String): Boolean {
    return blobKey.substringAfterLast("/").equals(DONE_MARKER_FILE_NAME, ignoreCase = true)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private const val DONE_MARKER_FILE_NAME = "done"

    private val DATA_PROVIDER_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.vid_labeling_dispatcher.data_provider")
    private val UPLOAD_STATUS_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.vid_labeling_dispatcher.dispatch_status")
    private const val UPLOAD_STATUS_SUCCESS = "success"
    private const val UPLOAD_STATUS_FAILED = "failed"

    private fun isWithinActiveWindow(modelLine: ModelLine, now: Timestamp): Boolean {
      if (!modelLine.hasActiveStartTime()) return false
      if (Timestamps.compare(now, modelLine.activeStartTime) < 0) return false
      if (modelLine.hasActiveEndTime() && Timestamps.compare(now, modelLine.activeEndTime) >= 0) {
        return false
      }
      return true
    }
  }
}
