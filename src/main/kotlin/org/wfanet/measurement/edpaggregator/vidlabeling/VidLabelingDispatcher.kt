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
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelShardsGrpcKt
import org.wfanet.measurement.api.v2alpha.listModelLinesRequest
import org.wfanet.measurement.api.v2alpha.listModelRolloutsRequest
import org.wfanet.measurement.api.v2alpha.listModelShardsRequest
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.edpaggregator.BlobUris
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createRawImpressionUploadFileRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createRawImpressionUploadModelLineRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.storage.BlobUri
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient

/**
 * Uploads VID labeling work items to the Secure Computation control plane.
 *
 * Processes "done" blob events by crawling directories for raw impression files, resolving active
 * model lines via the VID Repository API (ListModelLines -> ListModelRollouts -> ListModelShards),
 * and registering per-model-line state for downstream processing.
 *
 * @param storageClient client for crawling raw impressions directory.
 * @param rawImpressionUploadStub gRPC stub for the `RawImpressionUploadService`.
 * @param rawImpressionUploadFilesStub gRPC stub for the `RawImpressionUploadFileService`.
 * @param rawImpressionUploadModelLineStub gRPC stub for the
 *   `RawImpressionUploadModelLineService`.
 * @param modelLinesStub gRPC stub for the VID Repository ModelLines API.
 * @param modelRolloutsStub gRPC stub for the VID Repository ModelRollouts API.
 * @param modelShardsStub gRPC stub for the VID Repository ModelShards API.
 * @param dataProviderName resource name of the `DataProvider`.
 * @param modelSuiteName resource name of the model suite for ListModelLines.
 * @param overrideModelLines if non-empty, use these model lines instead of querying the API.
 *   Overrides bypass active window checks to support backfilling past data.
 * @param modelLineConfigs field mapping configuration keyed by model line resource name.
 * @param clock clock for determining active model line windows.
 * @param metrics OpenTelemetry metrics recorder.
 */
class VidLabelingDispatcher(
  private val storageClient: StorageClient,
  private val rawImpressionUploadStub:
    RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub,
  private val rawImpressionUploadFilesStub:
    RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub,
  private val rawImpressionUploadModelLineStub:
    RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub,
  private val modelLinesStub: ModelLinesGrpcKt.ModelLinesCoroutineStub,
  private val modelRolloutsStub: ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub,
  private val modelShardsStub: ModelShardsGrpcKt.ModelShardsCoroutineStub,
  private val dataProviderName: String,
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
    val memoizationEnabled: Boolean,
  )

  /**
   * Uploads VID labeling work for raw impression files in the directory containing the done blob.
   *
   * @param doneBlobPath the full storage URI of the "done" blob that triggered this upload.
   * @param doneBlobGeneration GCS object generation number of the done blob. Used to produce
   *   idempotent request IDs that handle both Pub/Sub redelivery (same generation = same ID) and
   *   EDP re-uploads to the same path (new generation = new ID).
   * @throws IllegalArgumentException if [doneBlobPath] uses an unsupported URI scheme or
   *   [doneBlobGeneration] is null.
   */
  suspend fun upload(doneBlobPath: String, doneBlobGeneration: Long) {
    val startTime: TimeSource.Monotonic.ValueTimeMark = TimeSource.Monotonic.markNow()

    try {
      val doneBlobUri: BlobUri = SelectedStorageClient.parseBlobUri(doneBlobPath)
      val folderPrefix: String = doneBlobUri.key.substringBeforeLast("/")

      val blobKeys: List<String> =
        storageClient
          .listBlobs(folderPrefix)
          .filter { !isDoneMarker(it.blobKey) }
          .map { it.blobKey }
          .toList()

      if (blobKeys.isEmpty()) {
        logger.info("No raw impression files found in $folderPrefix")
        recordUploadDuration(startTime, UPLOAD_STATUS_SUCCESS)
        return
      }

      metrics.filesProcessedCounter.add(
        blobKeys.size.toLong(),
        Attributes.of(DATA_PROVIDER_ATTR, dataProviderName),
      )

      val rawImpressionUpload = createRawImpressionUpload(doneBlobPath, doneBlobGeneration)
      val uploadId = rawImpressionUpload.name.substringAfterLast("/")

      createRawImpressionUploadFiles(rawImpressionUpload.name, blobKeys, doneBlobUri)

      val resolvedModelLines = resolveModelLines()

      if (resolvedModelLines.isEmpty()) {
        logger.info("No active model lines resolved for $modelSuiteName")
        recordUploadDuration(startTime, UPLOAD_STATUS_SUCCESS)
        return
      }

      createRawImpressionUploadModelLines(rawImpressionUpload.name, resolvedModelLines)

      // Registration complete. WorkItem and PoolAssignmentJob creation is handled by
      // VidLabelingMonitorFunction (#3958), which checks for uploads without WorkItems,
      // verifies no concurrent dispatch for the same (DataProvider, ModelLine), and drives
      // work forward. This prevents cross-dispatch concurrency corruption on concurrent uploads.

      logger.info(
        "Registered upload ${rawImpressionUpload.name} with ${blobKeys.size} files and " +
          "${resolvedModelLines.size} model lines"
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
   * If [overrideModelLines] is non-empty, uses those directly without active window filtering. This
   * supports backfilling past data where the model line may no longer be in the active window.
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

    val resolved: List<ResolvedModelLine> = buildList {
      for (modelLineName in activeModelLineNames) {
        val shardInfo = resolveShardInfo(modelLineName)
        if (shardInfo != null) {
          add(
            ResolvedModelLine(
              modelLineName = modelLineName,
              modelBlobPath = shardInfo.modelBlobPath,
              memoizationEnabled = shardInfo.memoizationEnabled,
            )
          )
        } else {
          logger.warning("Could not resolve model shard for $modelLineName, skipping")
        }
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
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun resolveActiveModelLinesFromApi(): List<String> {
    val now: Timestamp = Timestamps.fromMillis(clock.millis())

    val activeModelLines: List<String> =
      modelLinesStub
        .listResources { pageToken: String ->
          val response =
            try {
              modelLinesStub.listModelLines(
                listModelLinesRequest {
                  parent = modelSuiteName
                  if (pageToken.isNotEmpty()) {
                    this.pageToken = pageToken
                  }
                }
              )
            } catch (e: StatusException) {
              throw Exception("Error listing model lines for $modelSuiteName", e)
            }
          ResourceList(response.modelLinesList, response.nextPageToken)
        }
        .flattenConcat()
        .filter { modelLine -> isActiveProdModelLineWithConfig(modelLine, now) }
        .map { it.name }
        .toList()

    logger.info("Found ${activeModelLines.size} active PROD model lines from API")
    return activeModelLines
  }

  /**
   * Returns whether [modelLine] is an active PROD model line that has a [modelLineConfigs] entry.
   *
   * @param modelLine the model line to check.
   * @param now the current time used for active window evaluation.
   */
  private fun isActiveProdModelLineWithConfig(modelLine: ModelLine, now: Timestamp): Boolean {
    if (modelLine.type != ModelLine.Type.PROD) return false
    if (!isWithinActiveWindow(modelLine, now)) return false
    // TODO(world-federation-of-advertisers/cross-media-measurement#3956): Remove the static
    // modelLineConfigs dependency. Field mappings should come from ModelShard or be
    // convention-based so adding a new model line in the VID Repository doesn't require a
    // Cloud Function config redeploy.
    if (modelLine.name !in modelLineConfigs) {
      logger.warning("Skipping model line ${modelLine.name}: no config entry")
      return false
    }
    return true
  }

  /**
   * Resolved model shard info from the VID Repository.
   *
   * @param modelBlobPath path to the compiled model blob.
   * @param memoizationEnabled whether this model shard requires memoized VID assignment.
   */
  private data class ResolvedShardInfo(
    val modelBlobPath: String,
    val memoizationEnabled: Boolean,
  )

  /**
   * Resolves model shard info for a model line by traversing the ModelRollout -> ModelShard chain.
   *
   * @param modelLineName resource name of the model line.
   * @return resolved shard info, or null if no active rollout or shard is found.
   */
  private suspend fun resolveShardInfo(modelLineName: String): ResolvedShardInfo? {
    val modelReleaseName = resolveActiveModelRelease(modelLineName) ?: return null
    return resolveShardInfoFromShards(modelReleaseName)
  }

  /**
   * Finds the active `ModelRelease` for a model line via ListModelRollouts.
   *
   * @param modelLineName resource name of the model line.
   * @return the model release resource name, or null if no rollout is found.
   */
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

  /**
   * Resolves model shard info from `ModelShard` resources for this `DataProvider` and
   * `ModelRelease`.
   *
   * @param modelReleaseName resource name of the model release.
   * @return resolved shard info, or null if no matching shard is found.
   */
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
   * Creates a `RawImpressionUpload` resource to track this upload.
   *
   * Uses the done blob path and GCS generation number to produce an idempotent request ID. Same
   * (path, generation) → same request ID → idempotent on Pub/Sub redelivery. New generation at the
   * same path → new request ID → new upload for EDP re-uploads.
   *
   * @param doneBlobPath the full storage URI of the "done" blob.
   * @param generation GCS object generation number.
   * @return the created `RawImpressionUpload`.
   */
  private suspend fun createRawImpressionUpload(
    doneBlobPath: String,
    generation: Long,
  ): RawImpressionUpload {
    val requestId = UUID.nameUUIDFromBytes("$doneBlobPath:$generation".toByteArray()).toString()
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
   * Creates a `RawImpressionUploadFile` for each raw impression blob in the upload.
   *
   * @param uploadName resource name of the parent `RawImpressionUpload`.
   * @param blobKeys storage keys of the raw impression files in the upload.
   * @param doneBlobUri parsed URI of the "done" blob, used to reconstruct full blob URIs.
   */
  private suspend fun createRawImpressionUploadFiles(
    uploadName: String,
    blobKeys: List<String>,
    doneBlobUri: BlobUri,
  ) {
    for (chunk in blobKeys.chunked(RAW_IMPRESSION_UPLOAD_FILE_BATCH_SIZE)) {
      val request = batchCreateRawImpressionUploadFilesRequest {
        parent = uploadName
        for (blobKey in chunk) {
          val fileBlobUri = BlobUris.buildUri(doneBlobUri, blobKey)
          requests += createRawImpressionUploadFileRequest {
            parent = uploadName
            rawImpressionUploadFile = rawImpressionUploadFile { blobUri = fileBlobUri }
            requestId = UUID.nameUUIDFromBytes(fileBlobUri.toByteArray()).toString()
          }
        }
      }

      try {
        rawImpressionUploadFilesStub.batchCreateRawImpressionUploadFiles(request)
      } catch (e: StatusException) {
        throw Exception("Error creating RawImpressionUploadFiles for $uploadName", e)
      }
    }
  }

  /**
   * Creates a `RawImpressionUploadModelLine` for each resolved model line.
   *
   * @param uploadName resource name of the parent `RawImpressionUpload`.
   * @param resolvedModelLines the resolved model lines to register.
   */
  private suspend fun createRawImpressionUploadModelLines(
    uploadName: String,
    resolvedModelLines: List<ResolvedModelLine>,
  ) {
    for (chunk in resolvedModelLines.chunked(RAW_IMPRESSION_UPLOAD_MODEL_LINE_BATCH_SIZE)) {
      val request = batchCreateRawImpressionUploadModelLinesRequest {
        parent = uploadName
        for (resolvedModelLine in chunk) {
          requests += createRawImpressionUploadModelLineRequest {
            parent = uploadName
            rawImpressionUploadModelLine = rawImpressionUploadModelLine {
              cmmsModelLine = resolvedModelLine.modelLineName
            }
            requestId =
              UUID.nameUUIDFromBytes(
                "$uploadName:${resolvedModelLine.modelLineName}".toByteArray()
              ).toString()
          }
        }
      }

      try {
        rawImpressionUploadModelLineStub.batchCreateRawImpressionUploadModelLines(request)
      } catch (e: StatusException) {
        throw Exception("Error creating RawImpressionUploadModelLines for $uploadName", e)
      }
    }

    logger.info("Created ${resolvedModelLines.size} RawImpressionUploadModelLines for $uploadName")
  }

  private fun recordUploadDuration(startTime: TimeSource.Monotonic.ValueTimeMark, status: String) {
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

    private const val RAW_IMPRESSION_UPLOAD_FILE_BATCH_SIZE = 100
    private const val RAW_IMPRESSION_UPLOAD_MODEL_LINE_BATCH_SIZE = 50

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
