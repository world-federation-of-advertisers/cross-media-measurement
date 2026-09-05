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

import io.grpc.Status
import io.grpc.StatusException
import io.opentelemetry.api.common.Attributes
import java.time.LocalDate
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.coroutines.cancellation.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
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
import org.wfanet.measurement.edpaggregator.v1alpha.undeleteImpressionMetadataRequest
import org.wfanet.measurement.storage.BlobUri
import org.wfanet.measurement.storage.StorageClient

/**
 * Recovers finalized metadata blobs that have no corresponding `ImpressionMetadata` resource.
 *
 * Also restores deleted resources whose metadata blobs still exist. Date folders are reconciled
 * sequentially so memory usage is bounded by the number of objects and records in one folder. A
 * folder whose metadata was persisted but whose Kingdom publication did not complete is retried
 * with one representative metadata blob per model line.
 *
 * @param storageClient Client used to list metadata and completion blobs.
 * @param storageRootUri Storage scheme and bucket used to build metadata blob URIs.
 * @param edpImpressionPath Prefix containing the EDP's impression metadata.
 * @param impressionMetadataStub Client for listing registered metadata.
 * @param dataProviderName Parent resource name for metadata list requests.
 * @param throttler Throttles metadata list and mutation requests.
 * @param impressionMetadataBatchSize Maximum results per metadata list page.
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
   * @property target Resource name, folder, or completion marker whose recovery failed.
   * @property message Human-readable failure summary.
   */
  data class RecoveryError(val target: String, val message: String)

  /**
   * Result of one recovery scan.
   *
   * @property finalizedMetadataBlobs Metadata blobs eligible for missing-record recovery.
   * @property missingBlobs Finalized blobs with no active or deleted record when scanned.
   * @property deletedRecordsWithBlobs Deleted records whose metadata blobs still exist.
   * @property undeletedRecords Deleted records successfully restored to the active state.
   * @property failedUndeletes Deleted records that could not be restored.
   * @property recoveredBlobs Missing blobs verified active after resynchronization.
   * @property failedBlobs Missing blobs not verified active after resynchronization.
   * @property dateFoldersResynced Number of date folders successfully resynchronized and verified.
   * @property incompleteFullSyncFolders Finalized folders whose metadata was persisted but whose
   *   Kingdom data availability publication was not marked complete when scanned.
   * @property errors Per-target recovery failures.
   */
  data class RecoveryResult(
    val finalizedMetadataBlobs: Int,
    val missingBlobs: Int,
    val deletedRecordsWithBlobs: Int,
    val undeletedRecords: Int,
    val failedUndeletes: Int,
    val recoveredBlobs: Int,
    val failedBlobs: Int,
    val dateFoldersResynced: Int,
    val incompleteFullSyncFolders: Int,
    val errors: List<RecoveryError>,
  )

  private data class FolderRecoveryResult(
    val finalizedMetadataBlobs: Int,
    val missingBlobs: Int,
    val deletedRecordsWithBlobs: Int,
    val undeletedRecords: Int,
    val failedUndeletes: Int,
    val recoveredBlobs: Int,
    val failedBlobs: Int,
    val dateFoldersResynced: Int,
    val incompleteFullSyncFolders: Int,
    val errors: List<RecoveryError>,
  )

  /** Restores deleted resources and re-runs incomplete metadata or availability synchronization. */
  suspend fun recover(): RecoveryResult {
    var finalizedMetadataBlobs = 0
    var missingBlobs = 0
    var deletedRecordsWithBlobs = 0
    var undeletedRecords = 0
    var failedUndeletes = 0
    var recoveredBlobs = 0
    var failedBlobs = 0
    var dateFoldersResynced = 0
    var incompleteFullSyncFolders = 0
    val errors = mutableListOf<RecoveryError>()

    for (dateFolderPrefix in listDateFolderPrefixes()) {
      val result =
        try {
          recoverDateFolder(dateFolderPrefix)
        } catch (e: CancellationException) {
          throw e
        } catch (e: Exception) {
          logger.log(Level.SEVERE, "Failed to reconcile date folder $dateFolderPrefix", e)
          FolderRecoveryResult(
            finalizedMetadataBlobs = 0,
            missingBlobs = 0,
            deletedRecordsWithBlobs = 0,
            undeletedRecords = 0,
            failedUndeletes = 0,
            recoveredBlobs = 0,
            failedBlobs = 0,
            dateFoldersResynced = 0,
            incompleteFullSyncFolders = 0,
            errors = listOf(RecoveryError(dateFolderPrefix, e.message ?: e::class.java.simpleName)),
          )
        }

      finalizedMetadataBlobs += result.finalizedMetadataBlobs
      missingBlobs += result.missingBlobs
      deletedRecordsWithBlobs += result.deletedRecordsWithBlobs
      undeletedRecords += result.undeletedRecords
      failedUndeletes += result.failedUndeletes
      recoveredBlobs += result.recoveredBlobs
      failedBlobs += result.failedBlobs
      dateFoldersResynced += result.dateFoldersResynced
      incompleteFullSyncFolders += result.incompleteFullSyncFolders
      errors += result.errors
    }

    val metricAttributes =
      Attributes.of(
        MissingImpressionMetadataRecoveryMetrics.EDP_IMPRESSION_PATH_ATTR,
        edpImpressionPath,
      )
    metrics.missingBlobsGauge.set(missingBlobs.toLong(), metricAttributes)
    metrics.deletedRecordsWithBlobsGauge.set(deletedRecordsWithBlobs.toLong(), metricAttributes)
    metrics.failedUndeletesGauge.set(failedUndeletes.toLong(), metricAttributes)
    metrics.failedBlobsGauge.set(failedBlobs.toLong(), metricAttributes)

    if (deletedRecordsWithBlobs > 0) {
      logger.log(
        Level.SEVERE,
        "ALERT: Found $deletedRecordsWithBlobs deleted ImpressionMetadata resources whose " +
          "blobs still exist; restored $undeletedRecords and failed to restore $failedUndeletes",
      )
    }
    if (missingBlobs > 0) {
      logger.log(
        Level.SEVERE,
        "ALERT: Found $missingBlobs finalized metadata blobs without ImpressionMetadata " +
          "resources; verified $recoveredBlobs recovered and $failedBlobs still missing",
      )
    }

    return RecoveryResult(
      finalizedMetadataBlobs = finalizedMetadataBlobs,
      missingBlobs = missingBlobs,
      deletedRecordsWithBlobs = deletedRecordsWithBlobs,
      undeletedRecords = undeletedRecords,
      failedUndeletes = failedUndeletes,
      recoveredBlobs = recoveredBlobs,
      failedBlobs = failedBlobs,
      dateFoldersResynced = dateFoldersResynced,
      incompleteFullSyncFolders = incompleteFullSyncFolders,
      errors = errors,
    )
  }

  private suspend fun recoverDateFolder(dateFolderPrefix: String): FolderRecoveryResult {
    val blobs = storageClient.listBlobs(dateFolderPrefix).toList()
    val storageMetadataBlobs = blobs.filter(::hasMetadataFileName)
    val storageBlobUris =
      storageMetadataBlobs.mapTo(mutableSetOf()) { BlobUris.buildUri(storageRootUri, it.blobKey) }
    val registeredMetadata = listRegisteredMetadata(dateFolderPrefix)
    val deletedMetadataWithBlobs =
      registeredMetadata.values.filter {
        it.state == ImpressionMetadata.State.DELETED && it.blobUri in storageBlobUris
      }

    var undeletedRecords = 0
    var failedUndeletes = 0
    val undeletedBlobUris = mutableSetOf<String>()
    val errors = mutableListOf<RecoveryError>()
    for (metadata in deletedMetadataWithBlobs) {
      try {
        throttler.onReady {
          impressionMetadataStub.undeleteImpressionMetadata(
            undeleteImpressionMetadataRequest { name = metadata.name }
          )
        }
        undeletedRecords++
        undeletedBlobUris += metadata.blobUri
      } catch (e: CancellationException) {
        throw e
      } catch (e: StatusException) {
        if (e.status.code == Status.Code.ALREADY_EXISTS) {
          undeletedRecords++
          undeletedBlobUris += metadata.blobUri
        } else {
          logger.log(Level.SEVERE, "Failed to undelete ${metadata.name}", e)
          failedUndeletes++
          errors += RecoveryError(metadata.name, e.message ?: e::class.java.simpleName)
        }
      } catch (e: Exception) {
        logger.log(Level.SEVERE, "Failed to undelete ${metadata.name}", e)
        failedUndeletes++
        errors += RecoveryError(metadata.name, e.message ?: e::class.java.simpleName)
      }
    }

    val doneBlobKey = "${dateFolderPrefix.removeSuffix("/")}$DONE_SUFFIX"
    val doneBlob = blobs.firstOrNull { it.blobKey == doneBlobKey }
    val finalizedMetadataBlobs =
      if (doneBlob != null) {
        storageMetadataBlobs.filter(DataAvailabilityBlobs::isMetadataBlob)
      } else {
        emptyList()
      }
    val finalizedBlobUris =
      finalizedMetadataBlobs.mapTo(mutableSetOf()) { BlobUris.buildUri(storageRootUri, it.blobKey) }
    val missingBlobUris = finalizedBlobUris.minus(registeredMetadata.keys)
    val unsyncedRegisteredBlobUris =
      finalizedMetadataBlobs
        .asSequence()
        .filterNot(DataAvailabilityBlobs::isSynced)
        .map { BlobUris.buildUri(storageRootUri, it.blobKey) }
        .filterTo(mutableSetOf()) { blobUri ->
          val metadata = registeredMetadata[blobUri]
          metadata?.state == ImpressionMetadata.State.ACTIVE || blobUri in undeletedBlobUris
        }
    val incompleteFullSync =
      doneBlob != null &&
        finalizedMetadataBlobs.isNotEmpty() &&
        !DataAvailabilityBlobs.isDataAvailabilityPublished(doneBlob)
    val representativeBlobUris =
      if (incompleteFullSync) {
        registeredMetadata.values
          .asSequence()
          .filter {
            it.blobUri in finalizedBlobUris &&
              (it.state != ImpressionMetadata.State.DELETED || it.blobUri in undeletedBlobUris)
          }
          .associateBy { it.modelLine }
          .values
          .mapTo(mutableSetOf()) { it.blobUri }
      } else {
        emptySet()
      }
    val blobUrisToSync =
      missingBlobUris +
        undeletedBlobUris.intersect(finalizedBlobUris) +
        unsyncedRegisteredBlobUris +
        representativeBlobUris
    if (blobUrisToSync.isEmpty()) {
      return FolderRecoveryResult(
        finalizedMetadataBlobs = finalizedMetadataBlobs.size,
        missingBlobs = 0,
        deletedRecordsWithBlobs = deletedMetadataWithBlobs.size,
        undeletedRecords = undeletedRecords,
        failedUndeletes = failedUndeletes,
        recoveredBlobs = 0,
        failedBlobs = 0,
        dateFoldersResynced = 0,
        incompleteFullSyncFolders = if (incompleteFullSync) 1 else 0,
        errors = errors,
      )
    }

    val metadataBlobsToSync =
      finalizedMetadataBlobs.filter {
        BlobUris.buildUri(storageRootUri, it.blobKey) in blobUrisToSync
      }
    val doneBlobUri = BlobUris.buildUri(storageRootUri, doneBlobKey)
    try {
      sync(doneBlobUri, metadataBlobsToSync.mapTo(mutableSetOf()) { it.blobKey })
    } catch (e: CancellationException) {
      throw e
    } catch (e: Exception) {
      logger.log(Level.SEVERE, "Failed to resynchronize $doneBlobUri", e)
      return FolderRecoveryResult(
        finalizedMetadataBlobs = finalizedMetadataBlobs.size,
        missingBlobs = missingBlobUris.size,
        deletedRecordsWithBlobs = deletedMetadataWithBlobs.size,
        undeletedRecords = undeletedRecords,
        failedUndeletes = failedUndeletes,
        recoveredBlobs = 0,
        failedBlobs = missingBlobUris.size,
        dateFoldersResynced = 0,
        incompleteFullSyncFolders = if (incompleteFullSync) 1 else 0,
        errors = errors + RecoveryError(doneBlobUri, e.message ?: e::class.java.simpleName),
      )
    }

    val activeBlobUris =
      listRegisteredMetadata(dateFolderPrefix)
        .values
        .filter { it.state == ImpressionMetadata.State.ACTIVE }
        .mapTo(mutableSetOf()) { it.blobUri }
    val unrecoveredBlobUris = missingBlobUris.minus(activeBlobUris)
    val inactiveUndeletedBlobUris =
      undeletedBlobUris.intersect(finalizedBlobUris).minus(activeBlobUris)
    val unverifiedBlobUris = unrecoveredBlobUris + inactiveUndeletedBlobUris
    if (unverifiedBlobUris.isNotEmpty()) {
      errors +=
        RecoveryError(
          doneBlobUri,
          "Sync completed but ${unverifiedBlobUris.size} metadata blobs are still missing or " +
            "inactive",
        )
    }

    val remainingUnmarkedBlobUris =
      if (unsyncedRegisteredBlobUris.isEmpty()) {
        emptySet()
      } else {
        storageClient
          .listBlobs(dateFolderPrefix)
          .toList()
          .asSequence()
          .filter(DataAvailabilityBlobs::isMetadataBlob)
          .filterNot(DataAvailabilityBlobs::isSynced)
          .map { BlobUris.buildUri(storageRootUri, it.blobKey) }
          .filterTo(mutableSetOf()) { it in unsyncedRegisteredBlobUris }
      }
    if (remainingUnmarkedBlobUris.isNotEmpty()) {
      errors +=
        RecoveryError(
          doneBlobUri,
          "Sync completed but ${remainingUnmarkedBlobUris.size} metadata blobs remain unmarked",
        )
    }

    val availabilityPublicationCompleted =
      storageClient.getBlob(doneBlobKey)?.let(DataAvailabilityBlobs::isDataAvailabilityPublished) ==
        true
    if (!availabilityPublicationCompleted) {
      errors +=
        RecoveryError(
          doneBlobUri,
          "Sync returned without completing Kingdom data availability publication",
        )
    }

    return FolderRecoveryResult(
      finalizedMetadataBlobs = finalizedMetadataBlobs.size,
      missingBlobs = missingBlobUris.size,
      deletedRecordsWithBlobs = deletedMetadataWithBlobs.size,
      undeletedRecords = undeletedRecords - inactiveUndeletedBlobUris.size,
      failedUndeletes = failedUndeletes + inactiveUndeletedBlobUris.size,
      recoveredBlobs = missingBlobUris.size - unrecoveredBlobUris.size,
      failedBlobs = unrecoveredBlobUris.size,
      dateFoldersResynced =
        if (
          unverifiedBlobUris.isEmpty() &&
            remainingUnmarkedBlobUris.isEmpty() &&
            availabilityPublicationCompleted
        )
          1
        else 0,
      incompleteFullSyncFolders = if (incompleteFullSync) 1 else 0,
      errors = errors,
    )
  }

  /** Lists date folders in the configured window without loading their contents. */
  private suspend fun listDateFolderPrefixes(): List<String> {
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

    return dateFolderPrefixes.sorted()
  }

  @OptIn(ExperimentalCoroutinesApi::class)
  private suspend fun listRegisteredMetadata(
    dateFolderPrefix: String
  ): Map<String, ImpressionMetadata> {
    val blobUriPrefix = BlobUris.buildUri(storageRootUri, dateFolderPrefix)
    return impressionMetadataStub
      .listResources<ImpressionMetadata, String, ImpressionMetadataServiceCoroutineStub> { pageToken
        ->
        val response =
          try {
            throttler.onReady {
              impressionMetadataStub.listImpressionMetadata(
                listImpressionMetadataRequest {
                  parent = dataProviderName
                  pageSize = impressionMetadataBatchSize
                  showDeleted = true
                  this.pageToken = pageToken
                  filter = filter { this.blobUriPrefix = blobUriPrefix }
                }
              )
            }
          } catch (e: StatusException) {
            throw Exception("Error listing ImpressionMetadata for $blobUriPrefix", e)
          }
        ResourceList(response.impressionMetadataList, response.nextPageToken)
      }
      .flattenConcat()
      .toList()
      .associateBy { it.blobUri }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val DONE_SUFFIX = "/done"
    private const val METADATA_FILE_NAME = "metadata"

    private fun hasMetadataFileName(blob: StorageClient.Blob): Boolean =
      !blob.blobKey.endsWith(DONE_SUFFIX) &&
        METADATA_FILE_NAME in blob.blobKey.substringAfterLast('/').lowercase()
  }
}
