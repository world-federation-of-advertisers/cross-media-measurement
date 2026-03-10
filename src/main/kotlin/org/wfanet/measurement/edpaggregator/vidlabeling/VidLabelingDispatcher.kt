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

import io.grpc.StatusException
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import java.security.MessageDigest
import java.util.UUID
import java.util.logging.Logger
import kotlin.time.TimeSource
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
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
 * Processes "done" blob events by crawling directories for raw impression files, partitioning them
 * into batches, and creating WorkItems for TEE processing via the Secure Computation API.
 *
 * @param storageClient client for crawling raw impressions directory and reading file sizes.
 * @param workItemsStub gRPC stub for creating WorkItems via Secure Computation API.
 * @param dataProviderName resource name of the DataProvider.
 * @param vidLabelerParamsTemplate template [VidLabelerParams] with static fields populated.
 *   Per-batch fields ([VidLabelerParams.getInputBlobUrisList] and
 *   [VidLabelerParams.getBatchIndex]) are set by the dispatcher for each batch.
 * @param batchMaxSizeBytes maximum batch size in bytes. If null, all files go into a single batch.
 * @param metrics OpenTelemetry metrics recorder.
 */
class VidLabelingDispatcher(
  private val storageClient: StorageClient,
  private val workItemsStub: WorkItemsGrpcKt.WorkItemsCoroutineStub,
  private val dataProviderName: String,
  private val vidLabelerParamsTemplate: VidLabelerParams,
  private val batchMaxSizeBytes: Long? = null,
  private val metrics: VidLabelingDispatcherMetrics = VidLabelingDispatcherMetrics(),
) {
  /** Holds a blob key and its size for batching. */
  private data class BlobInfo(val blobKey: String, val sizeBytes: Long)

  /**
   * Dispatches VID labeling work for raw impression files in the directory containing the done blob.
   *
   * @param doneBlobPath the full storage URI of the "done" blob that triggered this dispatch.
   */
  suspend fun dispatch(doneBlobPath: String) {
    val startTime = TimeSource.Monotonic.markNow()

    try {
      val doneBlobUri = SelectedStorageClient.parseBlobUri(doneBlobPath)
      val folderPrefix = doneBlobUri.key.substringBeforeLast("/")

      // Crawl for raw impression files, excluding the done marker.
      val blobInfos = mutableListOf<BlobInfo>()
      storageClient.listBlobs(folderPrefix).collect { blob ->
        if (!isDoneMarker(blob.blobKey)) {
          blobInfos.add(BlobInfo(blob.blobKey, blob.size))
        }
      }

      if (blobInfos.isEmpty()) {
        logger.info("No raw impression files found in $folderPrefix")
        recordDispatchDuration(startTime, DISPATCH_STATUS_SUCCESS)
        return
      }

      // Sort by key for deterministic batching.
      blobInfos.sortBy { it.blobKey }

      metrics.filesProcessedCounter.add(
        blobInfos.size.toLong(),
        Attributes.of(DATA_PROVIDER_ATTR, dataProviderName),
      )

      val batches = partitionIntoBatches(blobInfos)

      for (batch in batches) {
        val batchBlobUris = batch.map { buildBlobUri(doneBlobUri, it.blobKey) }
        val batchId = generateDeterministicId(batchBlobUris)

        val params = vidLabelerParams {
          dataProvider = vidLabelerParamsTemplate.dataProvider
          storageParams = vidLabelerParamsTemplate.storageParams
          vidRepoConnection = vidLabelerParamsTemplate.vidRepoConnection
          modelLineConfigs.putAll(vidLabelerParamsTemplate.modelLineConfigsMap)
          overrideModelLines += vidLabelerParamsTemplate.overrideModelLinesList
          inputBlobUris += batchBlobUris
          batchIndex = batchId
        }

        // TODO(world-federation-of-advertisers/cross-media-measurement#3584): Persist batch
        //   metadata via RawImpressionMetadataService once the service is implemented.

        createWorkItem(batchId, params)
      }

      metrics.batchesCreatedCounter.add(
        batches.size.toLong(),
        Attributes.of(DATA_PROVIDER_ATTR, dataProviderName),
      )

      recordDispatchDuration(startTime, DISPATCH_STATUS_SUCCESS)
    } catch (e: Exception) {
      recordDispatchDuration(startTime, DISPATCH_STATUS_FAILED)
      throw e
    }
  }

  /**
   * Partitions blob infos into batches based on [batchMaxSizeBytes].
   *
   * If [batchMaxSizeBytes] is null, all blobs are placed in a single batch. Files that individually
   * exceed [batchMaxSizeBytes] are placed in their own batch and an oversized file alert is emitted.
   *
   * @param blobInfos sorted list of blob infos to partition.
   * @return list of batches, where each batch is a list of [BlobInfo].
   */
  private fun partitionIntoBatches(blobInfos: List<BlobInfo>): List<List<BlobInfo>> {
    if (batchMaxSizeBytes == null) {
      return listOf(blobInfos)
    }

    val batches = mutableListOf<MutableList<BlobInfo>>()
    var currentBatch = mutableListOf<BlobInfo>()
    var currentBatchSize = 0L

    for (blobInfo in blobInfos) {
      if (blobInfo.sizeBytes > batchMaxSizeBytes) {
        logger.warning(
          "File ${blobInfo.blobKey} (${blobInfo.sizeBytes} bytes) exceeds batch max size " +
            "($batchMaxSizeBytes bytes)"
        )
        metrics.oversizedFileAlertsCounter.add(
          1,
          Attributes.of(DATA_PROVIDER_ATTR, dataProviderName),
        )
        if (currentBatch.isNotEmpty()) {
          batches.add(currentBatch)
          currentBatch = mutableListOf()
          currentBatchSize = 0L
        }
        batches.add(mutableListOf(blobInfo))
        continue
      }

      if (currentBatchSize + blobInfo.sizeBytes > batchMaxSizeBytes && currentBatch.isNotEmpty()) {
        batches.add(currentBatch)
        currentBatch = mutableListOf()
        currentBatchSize = 0L
      }

      currentBatch.add(blobInfo)
      currentBatchSize += blobInfo.sizeBytes
    }

    if (currentBatch.isNotEmpty()) {
      batches.add(currentBatch)
    }

    return batches
  }

  /**
   * Creates a WorkItem in the Secure Computation control plane.
   *
   * @param batchId deterministic batch identifier used to generate the work item ID.
   * @param params the [VidLabelerParams] for this batch.
   */
  private suspend fun createWorkItem(batchId: String, params: VidLabelerParams) {
    val workItemId = "vid-labeling-$batchId"
    val packedWorkItemParams =
      workItemParams { appParams = params.pack() }.pack()

    val request = createWorkItemRequest {
      this.workItemId = workItemId
      workItem = workItem {
        queue = VID_LABELER_QUEUE
        workItemParams = packedWorkItemParams
      }
    }

    try {
      workItemsStub.createWorkItem(request)
      logger.info("Created WorkItem $workItemId with ${params.inputBlobUrisList.size} files")
    } catch (e: StatusException) {
      metrics.rpcErrorsCounter.add(
        1,
        Attributes.of(
          DATA_PROVIDER_ATTR,
          dataProviderName,
          RPC_METHOD_ATTR,
          RPC_METHOD_CREATE_WORK_ITEM,
          STATUS_CODE_ATTR,
          e.status.code.name,
        ),
      )
      throw Exception("Error creating WorkItem $workItemId", e)
    }
  }

  private fun recordDispatchDuration(
    startTime: TimeSource.Monotonic.ValueTimeMark,
    status: String,
  ) {
    val duration = startTime.elapsedNow().inWholeMilliseconds / 1000.0
    metrics.dispatchDurationHistogram.record(
      duration,
      Attributes.of(DATA_PROVIDER_ATTR, dataProviderName, DISPATCH_STATUS_ATTR, status),
    )
  }

  /**
   * Generates a deterministic ID from a sorted list of blob URIs using SHA-256.
   *
   * @param blobUris the blob URIs to hash.
   * @return a 32-character hex string.
   */
  private fun generateDeterministicId(blobUris: List<String>): String {
    val hash =
      MessageDigest.getInstance("SHA-256")
        .digest(blobUris.sorted().joinToString("\n").toByteArray())
    return hash.take(16).joinToString("") { "%02x".format(it) }
  }

  private fun buildBlobUri(doneBlobUri: BlobUri, blobKey: String): String {
    return when (doneBlobUri.scheme) {
      "gs" -> "${doneBlobUri.scheme}://${doneBlobUri.bucket}/$blobKey"
      "file" -> "${doneBlobUri.scheme}:///${doneBlobUri.bucket}/$blobKey"
      else -> throw IllegalArgumentException("Unsupported scheme: ${doneBlobUri.scheme}")
    }
  }

  private fun isDoneMarker(blobKey: String): Boolean {
    return blobKey.substringAfterLast("/").lowercase() == DONE_MARKER_FILE_NAME
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private const val VID_LABELER_QUEUE = "queues/vid-labeler-queue"
    private const val DONE_MARKER_FILE_NAME = "done"

    private val DATA_PROVIDER_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.vid_labeling_dispatcher.data_provider")
    private val DISPATCH_STATUS_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.vid_labeling_dispatcher.dispatch_status")
    private val RPC_METHOD_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.vid_labeling_dispatcher.rpc_method")
    private val STATUS_CODE_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.vid_labeling_dispatcher.status_code")
    private const val DISPATCH_STATUS_SUCCESS = "success"
    private const val DISPATCH_STATUS_FAILED = "failed"
    private const val RPC_METHOD_CREATE_WORK_ITEM = "CreateWorkItem"
  }
}
