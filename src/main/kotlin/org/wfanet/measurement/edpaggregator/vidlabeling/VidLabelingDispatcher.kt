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
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.edpaggregator.BlobUris
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreatePoolAssignmentJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createPoolAssignmentJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.poolAssignmentJob
import org.wfanet.measurement.edpaggregator.v1alpha.createRawImpressionUploadFileRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadFile
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
 * @param rawImpressionUploadFilesStub gRPC stub for the `RawImpressionUploadFileService`.
 * @param poolAssignmentJobStub gRPC stub for the `PoolAssignmentJobService`.
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
  private val rawImpressionUploadFilesStub:
    RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub,
  private val poolAssignmentJobStub:
    PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub,
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
    val memoizationEnabled: Boolean,
  )

  /**
   * Uploads VID labeling work for raw impression files in the directory containing the done blob.
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

      val rawImpressionUpload = createRawImpressionUpload(doneBlobPath)
      val uploadId = rawImpressionUpload.name.substringAfterLast("/")

      createRawImpressionUploadFiles(rawImpressionUpload.name, blobKeys, doneBlobUri)

      val resolvedModelLines = resolveModelLines()

      if (resolvedModelLines.isEmpty()) {
        logger.info("No active model lines resolved for $modelSuiteName")
        recordUploadDuration(startTime, UPLOAD_STATUS_SUCCESS)
        return
      }

      // TODO(world-federation-of-advertisers/cross-media-measurement#3899): Create
      // RawImpressionUploadModelLine resources for each active model line.

      val (memoizedLines, nonMemoizedLines) =
        resolvedModelLines.partition { it.memoizationEnabled }

      var totalWorkItems = 0
      for (resolvedModelLine in nonMemoizedLines) {
        for (shardIndex in 0 until numberOfShards) {
          createWorkItem(resolvedModelLine, shardIndex, uploadId)
          totalWorkItems++
        }
      }

      for (resolvedModelLine in memoizedLines) {
        createPoolAssignmentJobs(rawImpressionUpload.name, resolvedModelLine)
      }

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
   * @param doneBlobPath the full storage URI of the "done" blob.
   * @return the created `RawImpressionUpload`.
   */
  private suspend fun createRawImpressionUpload(doneBlobPath: String): RawImpressionUpload {
    val requestId = UUID.randomUUID().toString()
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
   * Creates a WorkItem in the Secure Computation control plane for a single model line shard.
   *
   * @param resolvedModelLine the resolved model line with its blob path.
  /**
   * Creates `PoolAssignmentJob` resources for a memoized model line — one per shard.
   *
   * @param uploadName resource name of the parent `RawImpressionUpload`.
   * @param resolvedModelLine the resolved model line with memoization enabled.
   */
  private suspend fun createPoolAssignmentJobs(
    uploadName: String,
    resolvedModelLine: ResolvedModelLine,
  ) {
    for (shardChunk in (0 until numberOfShards).chunked(POOL_ASSIGNMENT_JOB_BATCH_SIZE)) {
      val request = batchCreatePoolAssignmentJobsRequest {
        parent = uploadName
        for (shardIndex in shardChunk) {
          requests += createPoolAssignmentJobRequest {
            parent = uploadName
            poolAssignmentJob = poolAssignmentJob {
              cmmsModelLine = resolvedModelLine.modelLineName
              this.shardIndex = shardIndex
            }
            requestId = UUID.randomUUID().toString()
          }
        }
      }

      try {
        poolAssignmentJobStub.batchCreatePoolAssignmentJobs(request)
      } catch (e: StatusException) {
        throw Exception(
          "Error creating PoolAssignmentJobs for ${resolvedModelLine.modelLineName}",
          e,
        )
      }
    }

    logger.info(
      "Created $numberOfShards PoolAssignmentJobs for memoized model line " +
        "${resolvedModelLine.modelLineName}"
    )
  }

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
      modelLineConfigs[modelLineName] =
        VidLabelerParamsKt.modelLineConfig {
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
    private const val POOL_ASSIGNMENT_JOB_BATCH_SIZE = 100

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
