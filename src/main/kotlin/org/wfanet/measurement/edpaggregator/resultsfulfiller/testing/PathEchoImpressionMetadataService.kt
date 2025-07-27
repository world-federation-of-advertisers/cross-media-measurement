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

package org.wfanet.measurement.edpaggregator.resultsfulfiller.testing

import com.google.type.Interval
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZoneOffset
import kotlin.streams.asSequence
 
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.resultsfulfiller.EventPathResolver
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ImpressionDataSource
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ImpressionMetadataService
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek

/**
 * Resolver-backed [ImpressionMetadataService] for tests.
 *
 * Expands the requested [Interval] into per-day UTC intervals and returns a
 * source per day. The returned [BlobDetails.blobUri] is set to the resolved
 * metadata path, with an empty [EncryptedDek]. No storage I/O is performed.
 */
class PathEchoImpressionMetadataService(
  private val pathResolver: EventPathResolver,
  private val zoneIdForDates: ZoneId = ZoneOffset.UTC,
) : ImpressionMetadataService {

  override suspend fun listImpressionDataSources(
    eventGroupReferenceId: String,
    period: Interval,
  ): List<ImpressionDataSource> {
    val startDate = LocalDate.ofInstant(period.startTime.toInstant(), zoneIdForDates)
    val endDateInclusive = LocalDate.ofInstant(period.endTime.toInstant().minusSeconds(1), zoneIdForDates)
    if (startDate.isAfter(endDateInclusive)) return emptyList()

    return startDate
      .datesUntil(endDateInclusive.plusDays(1))
      .asSequence()
      .toList()
      .map { date ->
        val paths = pathResolver.resolvePaths(date, eventGroupReferenceId)
        val dayStartUtc = date.atStartOfDay(ZoneOffset.UTC).toInstant()
        val dayEndUtc = date.plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant()

        val blobDetails = BlobDetails.newBuilder()
          .setBlobUri(paths.metadataPath)
          .setEncryptedDek(EncryptedDek.getDefaultInstance())
          .build()

        ImpressionDataSource(
          interval = Interval.newBuilder()
            .setStartTime(dayStartUtc.toProtoTime())
            .setEndTime(dayEndUtc.toProtoTime())
            .build(),
          blobDetails = blobDetails,
        )
      }
  }
}
