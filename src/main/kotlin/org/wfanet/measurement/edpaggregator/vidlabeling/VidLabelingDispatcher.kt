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
import java.util.logging.Logger
import kotlin.time.TimeSource
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionMetadataBatch
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionMetadataBatchFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionMetadataBatchServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRawImpressionMetadataBatchFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createRawImpressionMetadataBatchFileRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createRawImpressionMetadataBatchRequest
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionMetadataBatchFile
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
 * @param rawImpressionMetadataBatchStub gRPC stub for creating RawImpressionMetadataBatch
 *   resources.
 * @param rawImpressionMetadataBatchFileStub gRPC stub for creating RawImpressionMetadataBatchFile
 *   resources.
 * @param dataProviderName resource name of the DataProvider.
 * @param vidLabelerParamsTemplate template [VidLabelerParams] with static fields populated.
 *   Per-batch field [VidLabelerParams.getRawImpressionMetadataBatch] is set by the dispatcher for
 *   each batch.
 * @param queueName resource name of the Secure Computation queue for VID labeling work items.
 * @param batchMaxSizeBytes maximum batch size in bytes.
 * @param metrics OpenTelemetry metrics recorder.
 */
class VidLabelingDispatcher(
  private val storageClient: StorageClient,
  private val workItemsStub: WorkItemsGrpcKt.WorkItemsCoroutineStub,
  private val rawImpressionMetadataBatchStub:
    RawImpressionMetadataBatchServiceGrpcKt.RawImpressionMetadataBatchServiceCoroutineStub,
  private val rawImpressionMetadataBatchFileStub:
    RawImpressionMetadataBatchFileServiceGrpcKt.RawImpressionMetadataBatchFileServiceCoroutineStub,
  private val dataProviderName: String,
  private val vidLabelerParamsTemplate: VidLabelerParams,
  private val queueName: String,
  private val batchMaxSizeBytes: Long,
  private val metrics: VidLabelingDispatcherMetrics = VidLabelingDispatcherMetrics(),
) {
  /** Holds a blob key and its size for batching. */
  private data class BlobInfo(val blobKey: String, val sizeBytes: Long)

  /** A batch of blobs with tracked total size. */
  private class Batch {
    private val mutableBlobs = mutableListOf<BlobInfo>()

    val blobs: List<BlobInfo>
      get() = mutableBlobs

    var sizeBytes: Long = 0L
      private set

    fun addBlob(blob: BlobInfo) {
      mutableBlobs.add(blob)
      sizeBytes += blob.sizeBytes
    }
  }

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

      metrics.filesProcessedCounter.add(
        blobInfos.size.toLong(),
        Attributes.of(DATA_PROVIDER_ATTR, dataProviderName),
      )

      val batches: List<List<BlobInfo>> = partitionIntoBatches(blobInfos)

      for (batch in batches) {
        val batchBlobUris: List<String> = batch.map { buildBlobUri(doneBlobUri, it.blobKey) }

        val createdBatch = createBatch()
        val batchResourceName: String = createdBatch.name
        createBatchFiles(batchResourceName, batchBlobUris)

        val params = vidLabelerParams {
          dataProvider = vidLabelerParamsTemplate.dataProvider
          storageParams = vidLabelerParamsTemplate.storageParams
          vidRepoConnection = vidLabelerParamsTemplate.vidRepoConnection
          modelLineConfigs.putAll(vidLabelerParamsTemplate.modelLineConfigsMap)
          overrideModelLines += vidLabelerParamsTemplate.overrideModelLinesList
          rawImpressionMetadataBatch = batchResourceName
        }

        createWorkItem(batchResourceName, params)
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
   * Partitions blob infos into batches using Best-Fit Decreasing bin-packing.
   *
   * Files that individually exceed [batchMaxSizeBytes] are placed in their own batch and an
   * oversized file alert is emitted. Remaining files are sorted by size descending (with blob key
   * as tiebreaker) and each file is placed into the batch with the smallest remaining capacity that
   * can still fit it.
   *
   * @param blobInfos list of blob infos to partition.
   * @return list of batches, where each batch is a list of [BlobInfo].
   */
  private fun partitionIntoBatches(blobInfos: List<BlobInfo>): List<List<BlobInfo>> {
    val oversizedBatches = mutableListOf<List<BlobInfo>>()
    val fittingBlobs = mutableListOf<BlobInfo>()

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
        oversizedBatches.add(listOf(blobInfo))
      } else {
        fittingBlobs.add(blobInfo)
      }
    }

    // Sort by size descending for Best-Fit Decreasing, with blob key as tiebreaker.
    val sortedBlobs =
      fittingBlobs.sortedWith(compareByDescending<BlobInfo> { it.sizeBytes }.thenBy { it.blobKey })

    val batches = mutableListOf<Batch>()

    for (blobInfo in sortedBlobs) {
      // Find the batch with the smallest remaining capacity that can fit this file.
      var bestFitIndex = -1
      var bestFitRemaining = Long.MAX_VALUE

      for (i in batches.indices) {
        val remaining = batchMaxSizeBytes - batches[i].sizeBytes
        if (blobInfo.sizeBytes <= remaining && remaining < bestFitRemaining) {
          bestFitIndex = i
          bestFitRemaining = remaining
        }
      }

      if (bestFitIndex >= 0) {
        batches[bestFitIndex].addBlob(blobInfo)
      } else {
        val batch = Batch()
        batch.addBlob(blobInfo)
        batches.add(batch)
      }
    }

    return oversizedBatches + batches.map { it.blobs }
  }

  /**
   * Creates a [RawImpressionMetadataBatch] via the gRPC API.
   *
   * @return the created [RawImpressionMetadataBatch] with server-assigned name.
   */
  private suspend fun createBatch(): RawImpressionMetadataBatch {
    val request = createRawImpressionMetadataBatchRequest {
      parent = dataProviderName
      rawImpressionMetadataBatch = RawImpressionMetadataBatch.getDefaultInstance()
    }
    try {
      return rawImpressionMetadataBatchStub.createRawImpressionMetadataBatch(request)
    } catch (e: StatusException) {
      throw Exception("Error creating RawImpressionMetadataBatch for $dataProviderName", e)
    }
  }

  /**
   * Creates [RawImpressionMetadataBatchFile] entries for each blob URI in the batch.
   *
   * @param batchResourceName the parent batch resource name.
   * @param blobUris the blob URIs to register as files.
   */
  private suspend fun createBatchFiles(batchResourceName: String, blobUris: List<String>) {
    val request = batchCreateRawImpressionMetadataBatchFilesRequest {
      parent = batchResourceName
      requests +=
        blobUris.map { uri ->
          createRawImpressionMetadataBatchFileRequest {
            parent = batchResourceName
            rawImpressionMetadataBatchFile = rawImpressionMetadataBatchFile { blobUri = uri }
          }
        }
    }
    try {
      rawImpressionMetadataBatchFileStub.batchCreateRawImpressionMetadataBatchFiles(request)
    } catch (e: StatusException) {
      throw Exception(
        "Error creating RawImpressionMetadataBatchFiles for batch $batchResourceName",
        e,
      )
    }
  }

  /**
   * Creates a WorkItem in the Secure Computation control plane.
   *
   * @param batchResourceName resource name of the [RawImpressionMetadataBatch].
   * @param params the [VidLabelerParams] for this batch.
   */
  private suspend fun createWorkItem(batchResourceName: String, params: VidLabelerParams) {
    val workItemId = "vid-labeling-$batchResourceName"
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
      throw Exception("Error creating WorkItem $workItemId for batch $batchResourceName", e)
    }
    logger.info("Created WorkItem $workItemId for batch ${params.rawImpressionMetadataBatch}")
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
  }
}
