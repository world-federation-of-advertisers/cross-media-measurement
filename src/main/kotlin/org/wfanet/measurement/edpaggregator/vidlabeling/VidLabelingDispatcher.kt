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
import java.util.logging.Logger
import kotlin.time.TimeSource
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelShardsGrpcKt
import org.wfanet.measurement.api.v2alpha.listModelLinesRequest
import org.wfanet.measurement.api.v2alpha.listModelShardsRequest
import org.wfanet.measurement.common.pack
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
 * Dispatches VID labeling work items to the Secure Computation control plane.
 *
 * Processes "done" blob events by crawling directories for raw impression files, resolving active
 * model lines via the VID Repository API, and creating one WorkItem per shard per model line for
 * TEE processing.
 *
 * @param storageClient client for crawling raw impressions directory.
 * @param workItemsStub gRPC stub for creating WorkItems via Secure Computation API.
 * @param modelLinesStub gRPC stub for the VID Repository ModelLines API.
 * @param modelShardsStub gRPC stub for the VID Repository ModelShards API.
 * @param dataProviderName resource name of the DataProvider.
 * @param vidLabelerParamsTemplate template [VidLabelerParams] with storage and connection fields.
 * @param queueName resource name of the Secure Computation queue.
 * @param numberOfShards static number of shards per model line.
 * @param modelSuiteName resource name of the model suite for ListModelLines.
 * @param overrideModelLines if non-empty, use these model lines instead of querying the API.
 * @param modelLineConfigs field mapping configuration keyed by model line resource name.
 * @param clock clock for determining active model line windows.
 * @param metrics OpenTelemetry metrics recorder.
 */
class VidLabelingDispatcher(
  private val storageClient: StorageClient,
  private val workItemsStub: WorkItemsGrpcKt.WorkItemsCoroutineStub,
  private val modelLinesStub: ModelLinesGrpcKt.ModelLinesCoroutineStub,
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
   * Dispatches VID labeling work for raw impression files in the directory containing the done
   * blob.
   *
   * @param doneBlobPath the full storage URI of the "done" blob that triggered this dispatch.
   * @throws IllegalArgumentException if [doneBlobPath] uses an unsupported URI scheme.
   * @throws Exception if a WorkItem creation fails via the Secure Computation API.
   */
  suspend fun dispatch(doneBlobPath: String) {
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
        recordDispatchDuration(startTime, DISPATCH_STATUS_SUCCESS)
        return
      }

      metrics.filesProcessedCounter.add(
        blobKeys.size.toLong(),
        Attributes.of(DATA_PROVIDER_ATTR, dataProviderName),
      )

      // TODO(world-federation-of-advertisers/cross-media-measurement#3899): Create
      // RawImpressionUpload using RawImpressionUploadService from PR #3818.

      val activeModelLines = resolveActiveModelLines()

      if (activeModelLines.isEmpty()) {
        logger.info("No active model lines found for $modelSuiteName")
        recordDispatchDuration(startTime, DISPATCH_STATUS_SUCCESS)
        return
      }

      // TODO(world-federation-of-advertisers/cross-media-measurement#3899): Check
      // memoized_vid_assignment_enabled on ModelShard once the field is added.
      // For now, all model lines are treated as non-memoized.

      val blobUris = blobKeys.map { buildBlobUri(doneBlobUri, it) }

      var totalWorkItems = 0
      for (modelLineName in activeModelLines) {
        for (shardIndex in 0 until numberOfShards) {
          createWorkItem(modelLineName, shardIndex, blobUris)
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

      recordDispatchDuration(startTime, DISPATCH_STATUS_SUCCESS)
    } catch (e: Exception) {
      recordDispatchDuration(startTime, DISPATCH_STATUS_FAILED)
      throw e
    }
  }

  /**
   * Resolves active model lines from the VID Repository API.
   *
   * If [overrideModelLines] is non-empty, returns those directly without calling the API.
   * Otherwise, calls ListModelLines filtering for type=PROD with active time windows containing
   * the current time, then filters to model lines present in [modelLineConfigs].
   *
   * @return list of active model line resource names.
   */
  private suspend fun resolveActiveModelLines(): List<String> {
    if (overrideModelLines.isNotEmpty()) {
      logger.info("Using ${overrideModelLines.size} override model lines")
      return overrideModelLines
    }

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
          logger.fine("Skipping model line ${modelLine.name}: no config entry")
          continue
        }
        activeModelLines.add(modelLine.name)
      }

      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())

    logger.info("Resolved ${activeModelLines.size} active model lines")
    return activeModelLines
  }

  /**
   * Creates a WorkItem in the Secure Computation control plane for a single model line shard.
   *
   * @param modelLineName resource name of the model line.
   * @param shardIndex zero-based index of this shard.
   * @param blobUris all blob URIs for this dispatch.
   */
  private suspend fun createWorkItem(
    modelLineName: String,
    shardIndex: Int,
    blobUris: List<String>,
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
      modelLineConfigs[modelLineName] = VidLabelerParamsKt.modelLineConfig {
        labelerInputFieldMapping.putAll(modelLineConfig.labelerInputFieldMappingMap)
        eventTemplateFieldMapping.putAll(modelLineConfig.eventTemplateFieldMappingMap)
      }
      overrideModelLines += listOf(modelLineName)
    }

    val workItemId = "vid-labeling-${modelLineName.substringAfterLast("/")}-shard-$shardIndex"
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

  private fun recordDispatchDuration(
    startTime: TimeSource.Monotonic.ValueTimeMark,
    status: String,
  ) {
    val duration: Double = startTime.elapsedNow().inWholeMilliseconds / 1000.0
    metrics.dispatchDurationHistogram.record(
      duration,
      Attributes.of(DATA_PROVIDER_ATTR, dataProviderName, DISPATCH_STATUS_ATTR, status),
    )
  }

  private fun buildBlobUri(doneBlobUri: BlobUri, blobKey: String): String {
    return when (doneBlobUri.scheme) {
      "gs" -> "${doneBlobUri.scheme}://${doneBlobUri.bucket}/$blobKey"
      "file" -> "${doneBlobUri.scheme}:///${doneBlobUri.bucket}/$blobKey"
      else -> throw IllegalArgumentException("Unsupported scheme: ${doneBlobUri.scheme}")
    }
  }

  private fun isDoneMarker(blobKey: String): Boolean {
    return blobKey.substringAfterLast("/").equals(DONE_MARKER_FILE_NAME, ignoreCase = true)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private const val DONE_MARKER_FILE_NAME = "done"

    private val DATA_PROVIDER_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.vid_labeling_dispatcher.data_provider")
    private val DISPATCH_STATUS_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.vid_labeling_dispatcher.dispatch_status")
    private const val DISPATCH_STATUS_SUCCESS = "success"
    private const val DISPATCH_STATUS_FAILED = "failed"

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
