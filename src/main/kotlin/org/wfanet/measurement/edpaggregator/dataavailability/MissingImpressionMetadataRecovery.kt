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

package org.wfanet.measurement.edpaggregator.dataavailability

import io.grpc.StatusException
import io.opentelemetry.api.common.Attributes
import java.time.LocalDate
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.coroutines.cancellation.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.edpaggregator.BlobUris
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequestKt.filter
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataRequest
import org.wfanet.measurement.storage.BlobUri
import org.wfanet.measurement.storage.StorageClient

/**
 * Recovers finalized metadata blobs that have no corresponding `ImpressionMetadata` resource.
 *
 * Also reports deleted resources whose metadata blobs still exist. Deleted resources count as
 * registered and are not reactivated.
 *
 * @param storageClient Client used to list metadata and completion blobs.
 * @param storageRootUri Storage scheme and bucket used to build metadata blob URIs.
 * @param edpImpressionPath Prefix containing the EDP's impression metadata.
 * @param impressionMetadataStub Client for listing registered metadata.
 * @param dataProviderName Parent resource name for metadata list requests.
 * @param throttler Throttles metadata list requests.
 * @param impressionMetadataBatchSize Maximum blob URIs per list request.
 * @param earliestDataDate Earliest date folder included in the reconciliation.
 * @param latestDataDate Latest date folder included in the reconciliation.
 * @param sync Re-runs data availability sync for a completion blob and selected metadata keys.
 * @param metrics Records reconciliation results.
 */
class MissingImpressionMetadataRecovery(
  private val storageClient: StorageClient,
  private val storageRootUri: BlobUri,
  private val edpImpressionPath: String,
  private val impressionMetadataStub: ImpressionMetadataServiceCoroutineStub,
  private val dataProviderName: String,
  private val throttler: Throttler,
  private val impressionMetadataBatchSize: Int,
  private val earliestDataDate: LocalDate,
  private val latestDataDate: LocalDate,
  private val sync: suspend (doneBlobUri: String, metadataBlobKeys: Set<String>) -> Unit,
  private val metrics: MissingImpressionMetadataRecoveryMetrics,
) {
  init {
    require(edpImpressionPath.isNotEmpty()) { "edpImpressionPath must not be empty" }
    require(!edpImpressionPath.startsWith("/")) { "edpImpressionPath cannot start with a slash" }
    require(!edpImpressionPath.endsWith("/")) { "edpImpressionPath cannot end with a slash" }
    require(impressionMetadataBatchSize > 0) {
      "impressionMetadataBatchSize must be greater than zero"
    }
    require(!latestDataDate.isBefore(earliestDataDate)) { "data date range must not be empty" }
  }

  /**
   * @property doneBlobUri Completion marker whose resynchronization failed.
   * @property message Human-readable failure summary.
   */
  data class RecoveryError(val doneBlobUri: String, val message: String)

  /**
   * Result of one recovery scan.
   *
   * @property finalizedMetadataBlobs Metadata blobs eligible for missing-record recovery.
   * @property missingBlobs Finalized blobs with no active or deleted record.
   * @property deletedRecordsWithBlobs Deleted records whose metadata blobs still exist.
   * @property recoveredBlobs Missing blobs in successfully resynchronized folders.
   * @property failedBlobs Missing blobs in folders that failed resynchronization.
   * @property dateFoldersResynced Number of date folders successfully resynchronized.
   * @property errors Per-folder resynchronization failures.
   */
  data class RecoveryResult(
    val finalizedMetadataBlobs: Int,
    val missingBlobs: Int,
    val deletedRecordsWithBlobs: Int,
    val recoveredBlobs: Int,
    val failedBlobs: Int,
    val dateFoldersResynced: Int,
    val errors: List<RecoveryError>,
  )

  /** Finds missing resources and re-runs data availability sync for each affected date folder. */
  suspend fun recover(): RecoveryResult {
    val blobs = listBlobsInLookbackWindow()
    val doneBlobKeys = blobs.filter { it.blobKey.endsWith(DONE_SUFFIX) }.map { it.blobKey }.toSet()
    val storageMetadataBlobs = blobs.filter(::hasMetadataFileName)
    val finalizedMetadataBlobs =
      storageMetadataBlobs.filter(DataAvailabilityBlobs::isMetadataBlob).filter { blob ->
        "${blob.blobKey.substringBeforeLast('/')}$DONE_SUFFIX" in doneBlobKeys
      }
    val finalizedBlobUris =
      finalizedMetadataBlobs.map { BlobUris.buildUri(storageRootUri, it.blobKey) }.toSet()
    val storageBlobUris =
      storageMetadataBlobs.map { BlobUris.buildUri(storageRootUri, it.blobKey) }.toSet()
    val registeredMetadata = listRegisteredMetadata(storageBlobUris)
    val missingBlobUris = finalizedBlobUris.minus(registeredMetadata.keys).sorted()
    val deletedRecordsWithBlobs =
      registeredMetadata.values.count { it.state == ImpressionMetadata.State.DELETED }
    val metricAttributes =
      Attributes.of(
        MissingImpressionMetadataRecoveryMetrics.EDP_IMPRESSION_PATH_ATTR,
        edpImpressionPath,
      )

    metrics.missingBlobsGauge.set(missingBlobUris.size.toLong(), metricAttributes)
    metrics.deletedRecordsWithBlobsGauge.set(deletedRecordsWithBlobs.toLong(), metricAttributes)
    if (deletedRecordsWithBlobs > 0) {
      logger.log(
        Level.SEVERE,
        "ALERT: Found $deletedRecordsWithBlobs deleted ImpressionMetadata resources whose " +
          "blobs still exist",
      )
    }
    if (missingBlobUris.isEmpty()) {
      metrics.recoveredBlobsGauge.set(0, metricAttributes)
      metrics.failedBlobsGauge.set(0, metricAttributes)
      return RecoveryResult(
        finalizedMetadataBlobs = finalizedMetadataBlobs.size,
        missingBlobs = 0,
        deletedRecordsWithBlobs = deletedRecordsWithBlobs,
        recoveredBlobs = 0,
        failedBlobs = 0,
        dateFoldersResynced = 0,
        errors = emptyList(),
      )
    }

    logger.log(
      Level.SEVERE,
      "ALERT: Found ${missingBlobUris.size} finalized metadata blobs without " +
        "ImpressionMetadata resources",
    )

    var recoveredBlobs = 0
    var failedBlobs = 0
    var dateFoldersResynced = 0
    val errors = mutableListOf<RecoveryError>()
    val missingBlobUriSet = missingBlobUris.toSet()
    val missingMetadataBlobs =
      finalizedMetadataBlobs.filter {
        BlobUris.buildUri(storageRootUri, it.blobKey) in missingBlobUriSet
      }
    for ((folder, folderBlobs) in
      missingMetadataBlobs.groupBy { it.blobKey.substringBeforeLast('/') }) {
      val doneBlobUri = BlobUris.buildUri(storageRootUri, "$folder$DONE_SUFFIX")
      try {
        sync(doneBlobUri, folderBlobs.mapTo(mutableSetOf()) { it.blobKey })
        recoveredBlobs += folderBlobs.size
        dateFoldersResynced++
      } catch (e: CancellationException) {
        throw e
      } catch (e: Exception) {
        failedBlobs += folderBlobs.size
        errors += RecoveryError(doneBlobUri, e.message ?: e::class.java.simpleName)
      }
    }

    metrics.recoveredBlobsGauge.set(recoveredBlobs.toLong(), metricAttributes)
    metrics.failedBlobsGauge.set(failedBlobs.toLong(), metricAttributes)
    return RecoveryResult(
      finalizedMetadataBlobs = finalizedMetadataBlobs.size,
      missingBlobs = missingBlobUris.size,
      deletedRecordsWithBlobs = deletedRecordsWithBlobs,
      recoveredBlobs = recoveredBlobs,
      failedBlobs = failedBlobs,
      dateFoldersResynced = dateFoldersResynced,
      errors = errors,
    )
  }

  /** Lists objects only for date folders inside the requested window. */
  private suspend fun listBlobsInLookbackWindow(): List<StorageClient.Blob> {
    val prefixesToVisit = ArrayDeque<String>()
    val dateFolderPrefixes = mutableListOf<String>()
    prefixesToVisit.add("$edpImpressionPath/")

    while (prefixesToVisit.isNotEmpty()) {
      val prefix = prefixesToVisit.removeFirst()
      for (keyOrPrefix in storageClient.listBlobKeysAndPrefixes(prefix).toList()) {
        if (!keyOrPrefix.endsWith(StorageClient.DELIMITER) || keyOrPrefix.length <= prefix.length) {
          continue
        }

        val folderName =
          keyOrPrefix
            .removeSuffix(StorageClient.DELIMITER)
            .substringAfterLast(StorageClient.DELIMITER)
        val date = runCatching { LocalDate.parse(folderName) }.getOrNull()
        if (date == null) {
          prefixesToVisit.addLast(keyOrPrefix)
        } else if (date in earliestDataDate..latestDataDate) {
          dateFolderPrefixes += keyOrPrefix
        }
      }
    }

    return dateFolderPrefixes
      .sorted()
      .chunked(DATE_FOLDER_LIST_CONCURRENCY)
      .flatMap { prefixChunk ->
        coroutineScope {
          prefixChunk
            .map { prefix -> async { storageClient.listBlobs(prefix).toList() } }
            .awaitAll()
            .flatten()
        }
      }
      .filter(::isInLookbackWindow)
  }

  private suspend fun listRegisteredMetadata(
    blobUris: Set<String>
  ): Map<String, ImpressionMetadata> {
    val activeMetadata = listRegisteredMetadata(blobUris, showDeleted = false)
    val deletedMetadata = listRegisteredMetadata(blobUris, showDeleted = true)
    return (activeMetadata + deletedMetadata).associateBy { it.blobUri }
  }

  @OptIn(ExperimentalCoroutinesApi::class)
  private suspend fun listRegisteredMetadata(
    blobUris: Set<String>,
    showDeleted: Boolean,
  ): List<ImpressionMetadata> {
    return blobUris.chunked(impressionMetadataBatchSize).flatMap { blobUriChunk ->
      impressionMetadataStub
        .listResources<ImpressionMetadata, String, ImpressionMetadataServiceCoroutineStub> {
          pageToken ->
          val response =
            try {
              throttler.onReady {
                impressionMetadataStub.listImpressionMetadata(
                  listImpressionMetadataRequest {
                    parent = dataProviderName
                    pageSize = impressionMetadataBatchSize
                    this.showDeleted = showDeleted
                    this.pageToken = pageToken
                    filter = filter { this.blobUris += blobUriChunk }
                  }
                )
              }
            } catch (e: StatusException) {
              throw Exception("Error listing ImpressionMetadata", e)
            }
          ResourceList(response.impressionMetadataList, response.nextPageToken)
        }
        .flattenConcat()
        .toList()
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val DONE_SUFFIX = "/done"
    private const val DATE_FOLDER_LIST_CONCURRENCY = 16
    private const val METADATA_FILE_NAME = "metadata"

    private fun hasMetadataFileName(blob: StorageClient.Blob): Boolean =
      !blob.blobKey.endsWith(DONE_SUFFIX) &&
        METADATA_FILE_NAME in blob.blobKey.substringAfterLast('/').lowercase()

    private fun dataDate(blobKey: String): LocalDate? =
      runCatching { LocalDate.parse(blobKey.substringBeforeLast('/').substringAfterLast('/')) }
        .getOrNull()
  }

  private fun isInLookbackWindow(blob: StorageClient.Blob): Boolean {
    val date = dataDate(blob.blobKey) ?: return false
    return date in earliestDataDate..latestDataDate
  }
}
