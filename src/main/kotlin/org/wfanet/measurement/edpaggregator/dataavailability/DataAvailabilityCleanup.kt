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
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.time.TimeSource
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.deleteImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataRequest

/**
 * Handles cleanup of ImpressionMetadata records when GCS objects are deleted.
 *
 * This class coordinates the workflow that occurs after an OBJECT_DELETE event is detected in GCS.
 * It handles:
 * - Looking up ImpressionMetadata records by blob URI when a resource ID is not provided.
 * - Calling DeleteImpressionMetadata to soft-delete the corresponding Spanner record.
 * - Recording cleanup metrics for observability.
 *
 * The cleanup operation is idempotent - if the record has already been deleted, the operation
 * succeeds silently.
 *
 * Typical workflow:
 * 1. A deleted blob path and optional resource ID are passed to [cleanup].
 * 2. If a resource ID is provided, it is used directly for deletion.
 * 3. If no resource ID is provided, the ImpressionMetadata is looked up by blob URI.
 * 4. The ImpressionMetadata record is soft-deleted.
 * 5. Metrics are recorded for the cleanup operation.
 *
 * @property impressionMetadataServiceStub gRPC stub for interacting with the ImpressionMetadata
 *   service.
 * @property dataProviderName The resource name of the data provider, used as a parent identifier in
 *   gRPC requests.
 * @property metrics Metrics recorder for telemetry.
 */
class DataAvailabilityCleanup(
  private val impressionMetadataServiceStub:
    ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub,
  private val dataProviderName: String,
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
   * This function is triggered when an OBJECT_DELETE event is detected in GCS. It soft-deletes the
   * ImpressionMetadata record either by resource ID (if provided) or by looking up the record using
   * the blob URI.
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
      val resourceIdToDelete: String? =
        if (!impressionMetadataResourceId.isNullOrEmpty()) {
          logger.info(
            "Using resource ID from header for deleted blob: $deletedBlobPath, " +
              "resourceId: $impressionMetadataResourceId"
          )
          impressionMetadataResourceId
        } else {
          // Look up the ImpressionMetadata by blob URI
          logger.info(
            "No resource ID provided. Looking up ImpressionMetadata by blob URI: $deletedBlobPath"
          )
          lookupResourceIdByBlobUri(deletedBlobPath).also {
            if (it == null) {
              cleanupStatus = CleanupStatus.SKIPPED
            }
          }
        }

      if (resourceIdToDelete != null) {
        cleanupStatus = deleteImpressionMetadata(resourceIdToDelete)
      }

      return CleanupResult(cleanupStatus)
    } catch (e: Exception) {
      cleanupStatus = CleanupStatus.FAILED
      // Log the exception with context before rethrowing
      logger.log(
        Level.SEVERE,
        "Cleanup failed for blob: $deletedBlobPath, resourceId: $impressionMetadataResourceId",
        e,
      )
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

  /**
   * Looks up an ImpressionMetadata resource ID by blob URI.
   *
   * @param blobUri The blob URI to search for.
   * @return The resource name of the ImpressionMetadata, or null if not found.
   */
  private suspend fun lookupResourceIdByBlobUri(blobUri: String): String? {
    val listResponse =
      impressionMetadataServiceStub.listImpressionMetadata(
        listImpressionMetadataRequest {
          parent = dataProviderName
          filter = ListImpressionMetadataRequestKt.filter { blobUriPrefix = blobUri }
        }
      )

    val results = listResponse.impressionMetadataList
    return when {
      results.isEmpty() -> {
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
        null
      }
      results.size > 1 -> {
        logger.warning(
          "Multiple ImpressionMetadata records (${results.size}) found for blob URI: $blobUri. " +
            "Using first match: ${results.first().name}"
        )
        metrics.cleanupErrorsCounter.add(
          1,
          Attributes.of(
            DataAvailabilityCleanupMetrics.DATA_PROVIDER_KEY_ATTR,
            dataProviderName,
            DataAvailabilityCleanupMetrics.ERROR_TYPE_ATTR,
            DataAvailabilityCleanupMetrics.ERROR_TYPE_MULTIPLE_MATCHES,
          ),
        )
        results.first().name
      }
      else -> results.first().name
    }
  }

  /**
   * Deletes an ImpressionMetadata record by resource ID.
   *
   * @param resourceId The resource name of the ImpressionMetadata to delete.
   * @return The cleanup status after attempting deletion.
   */
  private suspend fun deleteImpressionMetadata(resourceId: String): CleanupStatus {
    return try {
      impressionMetadataServiceStub.deleteImpressionMetadata(
        deleteImpressionMetadataRequest { name = resourceId }
      )
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
      if (e.status.code == Status.Code.NOT_FOUND) {
        // Idempotent - the record may have already been deleted
        logger.info("ImpressionMetadata not found (already deleted): $resourceId")
        CleanupStatus.SKIPPED
      } else {
        throw e
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
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
