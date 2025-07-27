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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.type.Interval
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.logging.Logger
import kotlin.streams.asSequence
 
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.storage.SelectedStorageClient

/**
 * Storage-backed [ImpressionMetadataService].
 *
 * Resolves per-day metadata paths using an [EventPathResolver], reads [BlobDetails] messages from
 * storage, and emits sources with UTC day intervals. Date expansion uses [zoneIdForDates].
 */
class StorageImpressionMetadataService(
  private val pathResolver: EventPathResolver,
  private val impressionDekStorageConfig: StorageConfig,
  private val zoneIdForDates: ZoneId = ZoneOffset.UTC,
) : ImpressionMetadataService {

  /**
   * Lists impression data sources for an event group within a period.
   *
   * Expands [period] to per-day boundaries using [zoneIdForDates], reads `BlobDetails` for each day
   * from storage, and returns a source per day with UTC closed-open intervals [start, end).
   *
   * @param eventGroupReferenceId event group reference identifier.
   * @param period closed-open time interval in UTC.
   * @return sources covering the requested period; empty if the period maps to no dates.
   * @throws ImpressionReadException if a required metadata blob does not exist.
   * @throws com.google.protobuf.InvalidProtocolBufferException if a metadata blob is present but
   *   contains invalid `BlobDetails`.
   */
  override suspend fun listImpressionDataSources(
    eventGroupReferenceId: String,
    period: Interval,
  ): List<ImpressionDataSource> {
    val startDate = LocalDate.ofInstant(period.startTime.toInstant(), zoneIdForDates)
    val endDateInclusive = LocalDate.ofInstant(period.endTime.toInstant().minusSeconds(1), zoneIdForDates)
    if (startDate.isAfter(endDateInclusive)) return emptyList()

    val dates = startDate.datesUntil(endDateInclusive.plusDays(1)).asSequence().toList()

    return dates.map { date ->
      val paths = pathResolver.resolvePaths(date, eventGroupReferenceId)
      val blobDetails = readBlobDetails(paths.metadataPath)

      val dayStartUtc = date.atStartOfDay(ZoneOffset.UTC).toInstant()
      val dayEndUtc = date.plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant()

      ImpressionDataSource(
        interval = Interval.newBuilder()
          .setStartTime(dayStartUtc.toProtoTime())
          .setEndTime(dayEndUtc.toProtoTime())
          .build(),
        blobDetails = blobDetails,
      )
    }
  }

  /**
   * Reads `BlobDetails` from a metadata blob.
   *
   * @param metadataPath blob URI for the metadata (e.g., `file:///bucket/key`).
   * @return parsed [BlobDetails].
   * @throws ImpressionReadException with [ImpressionReadException.Code.BLOB_NOT_FOUND] when the
   *   blob is missing.
   * @throws com.google.protobuf.InvalidProtocolBufferException if parsing fails due to invalid
   *   metadata contents.
   */
  private suspend fun readBlobDetails(metadataPath: String): BlobDetails {
    val storageClientUri = SelectedStorageClient.parseBlobUri(metadataPath)
    val impressionsDekStorageClient =
      SelectedStorageClient(
        storageClientUri,
        impressionDekStorageConfig.rootDirectory,
        impressionDekStorageConfig.projectId,
      )
    logger.info("Reading impression metadata from $metadataPath")
    val blob =
      impressionsDekStorageClient.getBlob(storageClientUri.key)
        ?: throw ImpressionReadException(
          metadataPath,
          ImpressionReadException.Code.BLOB_NOT_FOUND,
          "BlobDetails metadata not found",
        )
    return BlobDetails.parseFrom(blob.read().flatten())
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
