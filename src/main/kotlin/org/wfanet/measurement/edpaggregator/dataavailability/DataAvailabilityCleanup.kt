/*
 * Copyright 2025 The Cross-Media Measurement Authors
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
import java.util.logging.Logger
import kotlin.time.TimeSource
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.getImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markImpressionMetadataDeletedRequest
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient

/**
 * Soft-deletes metadata for removed objects while preserving synchronized replacements.
 *
 * Cleanup reads the metadata version before checking storage and deletes only if that version still
 * matches. Noncurrent object deletion and archive notifications are safe to process repeatedly.
 *
 * @property impressionMetadataServiceStub Stub for the ImpressionMetadata service.
 * @property dataProviderName Resource name of the data provider.
 * @property storageClient Storage client for checking whether a live object remains.
 * @property throttler Regulates each metadata RPC, including rechecks after a version conflict.
 * @property metrics Metrics recorder for telemetry.
 */
class DataAvailabilityCleanup(
  private val impressionMetadataServiceStub:
    ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub,
  private val dataProviderName: String,
  private val storageClient: StorageClient,
  private val throttler: Throttler,
  private val metrics: DataAvailabilityCleanupMetrics = DataAvailabilityCleanupMetrics(),
) {

  /**
   * Result of a cleanup operation.
   *
   * @property status The outcome of the cleanup operation.
   */
  data class CleanupResult(val status: CleanupStatus)

  /** Status of a cleanup operation. */
  enum class CleanupStatus {
    /** The ImpressionMetadata record was successfully deleted. */
    SUCCESS,
    /** The cleanup was skipped (e.g., record not found or already deleted). */
    SKIPPED,
    /** The cleanup failed due to an error. */
    FAILED,
  }

  /**
   * Cleans up an ImpressionMetadata record after its corresponding GCS object is deleted.
   *
   * The metadata is read before checking for a live object. Its etag guards the subsequent deletion
   * against synchronization that commits after the read. A conflict restarts the checks once;
   * further conflicts fail so that the notification can be retried.
   *
   * @param deletedBlobPath The full Cloud Storage object path of the deleted blob.
   * @param impressionMetadataResourceId Optional. The resource name of the ImpressionMetadata to
   *   delete. If not provided, the function will look up the record by blob URI.
   * @return A [CleanupResult] indicating the outcome of the cleanup operation.
   */
  suspend fun cleanup(
    deletedBlobPath: String,
    impressionMetadataResourceId: String? = null,
  ): CleanupResult {
    val startTime = TimeSource.Monotonic.markNow()
    var cleanupStatus = CleanupStatus.SUCCESS

    try {
      for (attempt in 1..MAX_CLEANUP_ATTEMPTS) {
        try {
          cleanupStatus = cleanupOnce(deletedBlobPath, impressionMetadataResourceId)
          return CleanupResult(cleanupStatus)
        } catch (e: StatusException) {
          if (
            attempt == MAX_CLEANUP_ATTEMPTS ||
              e.status.code != Status.Code.ABORTED ||
              e.errorInfo?.domain != ERROR_DOMAIN ||
              e.errorInfo?.reason != ETAG_MISMATCH_REASON
          ) {
            throw e
          }
          logger.info("ImpressionMetadata changed during cleanup. Rechecking $deletedBlobPath")
        }
      }
      error("Cleanup attempts exhausted")
    } catch (e: Exception) {
      cleanupStatus = CleanupStatus.FAILED
      // Record RPC error metric if it's a gRPC error
      if (e is StatusException) {
        metrics.cleanupErrorsCounter.add(
          1,
          Attributes.of(
            DataAvailabilityCleanupMetrics.DATA_PROVIDER_KEY_ATTR,
            dataProviderName,
            DataAvailabilityCleanupMetrics.ERROR_TYPE_ATTR,
            DataAvailabilityCleanupMetrics.ERROR_TYPE_RPC_ERROR,
          ),
        )
      }
      throw e
    } finally {
      recordCleanupDuration(startTime, cleanupStatus)
    }
  }

  private suspend fun cleanupOnce(
    deletedBlobPath: String,
    impressionMetadataResourceId: String?,
  ): CleanupStatus {
    val metadata: ImpressionMetadata? =
      if (!impressionMetadataResourceId.isNullOrEmpty()) {
        getImpressionMetadata(impressionMetadataResourceId)
      } else {
        lookupMetadataByBlobUri(deletedBlobPath)
      }
    if (metadata == null || metadata.state == ImpressionMetadata.State.DELETED) {
      return CleanupStatus.SKIPPED
    }

    // A live object means this notification removed a noncurrent version.
    val blobUri = SelectedStorageClient.parseBlobUri(deletedBlobPath)
    if (storageClient.getBlob(blobUri.key) != null) {
      logger.info(
        "Live version still exists for $deletedBlobPath. " +
          "Skipping cleanup (noncurrent version deletion)."
      )
      metrics.cleanupSkippedCounter.add(
        1,
        Attributes.of(
          DataAvailabilityCleanupMetrics.DATA_PROVIDER_KEY_ATTR,
          dataProviderName,
          DataAvailabilityCleanupMetrics.CLEANUP_STATUS_ATTR,
          DataAvailabilityCleanupMetrics.CLEANUP_SKIP_REASON_NONCURRENT_VERSION,
        ),
      )
      return CleanupStatus.SKIPPED
    }

    if (metadata.etag.isEmpty()) {
      throw Status.UNAVAILABLE.withDescription(
          "Upgrade the EDPA public API server before retrying cleanup."
        )
        .asException()
    }
    return deleteImpressionMetadata(metadata)
  }

  /** Returns the named metadata, or null if it no longer exists. */
  private suspend fun getImpressionMetadata(name: String): ImpressionMetadata? {
    return try {
      throttler.onReady {
        impressionMetadataServiceStub.getImpressionMetadata(
          getImpressionMetadataRequest { this.name = name }
        )
      }
    } catch (e: StatusException) {
      if (e.status.code == Status.Code.NOT_FOUND) {
        null
      } else {
        throw e
      }
    }
  }

  /** Returns the metadata for the exact blob URI, or null if it no longer exists. */
  private suspend fun lookupMetadataByBlobUri(blobUri: String): ImpressionMetadata? {
    val listResponse =
      throttler.onReady {
        impressionMetadataServiceStub.listImpressionMetadata(
          listImpressionMetadataRequest {
            parent = dataProviderName
            filter = ListImpressionMetadataRequestKt.filter { blobUris += blobUri }
          }
        )
      }

    val results = listResponse.impressionMetadataList
    if (results.isEmpty()) {
      logger.warning("No ImpressionMetadata found for blob URI: $blobUri. Skipping cleanup.")
      metrics.cleanupErrorsCounter.add(
        1,
        Attributes.of(
          DataAvailabilityCleanupMetrics.DATA_PROVIDER_KEY_ATTR,
          dataProviderName,
          DataAvailabilityCleanupMetrics.ERROR_TYPE_ATTR,
          DataAvailabilityCleanupMetrics.ERROR_TYPE_NOT_FOUND,
        ),
      )
      return null
    }
    return results.single()
  }

  /** Deletes the observed version, skipping metadata that was deleted concurrently. */
  private suspend fun deleteImpressionMetadata(metadata: ImpressionMetadata): CleanupStatus {
    val resourceId = metadata.name
    return try {
      throttler.onReady {
        impressionMetadataServiceStub.markImpressionMetadataDeleted(
          markImpressionMetadataDeletedRequest {
            name = resourceId
            etag = metadata.etag
          }
        )
      }
      logger.info("Successfully soft-deleted ImpressionMetadata: $resourceId")
      // Record successful deletion
      metrics.recordsDeletedCounter.add(
        1,
        Attributes.of(
          DataAvailabilityCleanupMetrics.DATA_PROVIDER_KEY_ATTR,
          dataProviderName,
          DataAvailabilityCleanupMetrics.CLEANUP_STATUS_ATTR,
          DataAvailabilityCleanupMetrics.CLEANUP_STATUS_SUCCESS,
        ),
      )
      CleanupStatus.SUCCESS
    } catch (e: StatusException) {
      when {
        e.status.code == Status.Code.NOT_FOUND -> {
          logger.info("ImpressionMetadata not found (already deleted): $resourceId")
          CleanupStatus.SKIPPED
        }
        e.status.code == Status.Code.UNIMPLEMENTED ->
          throw Status.UNAVAILABLE.withDescription(
              "Upgrade the EDPA public API server before retrying cleanup."
            )
            .withCause(e)
            .asException()
        else -> throw e
      }
    }
  }

  /**
   * Records the cleanup duration metric.
   *
   * @param startTime The start time of the cleanup operation.
   * @param cleanupStatus The status of the cleanup operation.
   */
  private fun recordCleanupDuration(
    startTime: TimeSource.Monotonic.ValueTimeMark,
    cleanupStatus: CleanupStatus,
  ) {
    val durationSeconds = startTime.elapsedNow().inWholeMilliseconds / 1000.0
    val statusString =
      when (cleanupStatus) {
        CleanupStatus.SUCCESS -> DataAvailabilityCleanupMetrics.CLEANUP_STATUS_SUCCESS
        CleanupStatus.SKIPPED -> DataAvailabilityCleanupMetrics.CLEANUP_STATUS_SKIPPED
        CleanupStatus.FAILED -> DataAvailabilityCleanupMetrics.CLEANUP_STATUS_FAILED
      }
    metrics.cleanupDurationHistogram.record(
      durationSeconds,
      Attributes.of(
        DataAvailabilityCleanupMetrics.DATA_PROVIDER_KEY_ATTR,
        dataProviderName,
        DataAvailabilityCleanupMetrics.CLEANUP_STATUS_ATTR,
        statusString,
      ),
    )
  }

  companion object {
    private const val MAX_CLEANUP_ATTEMPTS = 2
    private const val ERROR_DOMAIN = "edpaggregator.halo-cmm.org"
    private const val ETAG_MISMATCH_REASON = "ETAG_MISMATCH"
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
