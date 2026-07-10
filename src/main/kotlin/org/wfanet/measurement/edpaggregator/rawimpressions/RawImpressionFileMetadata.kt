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

package org.wfanet.measurement.edpaggregator.rawimpressions

import java.time.LocalDate
import java.time.format.DateTimeParseException

/**
 * Per-file metadata for a single raw-impression file, read from the file's plaintext Parquet footer
 * key-value metadata.
 *
 * A raw-impression file is exactly one `EventGroup`, so it carries one [eventDate]. Entity keys are
 * NOT carried here — they are read per impression from dedicated Parquet columns (see
 * [EntityKeyMapper]); the footer no longer holds them.
 *
 * @property eventDate UTC calendar date of the file's impressions (a raw-impression file holds one
 *   day of events). Places the labeled output under `model-line/<id>/<YYYY-MM-DD>/` so
 *   `DataAvailabilitySync` can classify it by date.
 */
data class RawImpressionFileMetadata(val eventDate: LocalDate) {
  companion object {
    /** Footer key holding the file's event date (one day per file) as an ISO `YYYY-MM-DD` (UTC). */
    const val EVENT_DATE_KEY = "event_date"

    /**
     * Parses [RawImpressionFileMetadata] from a raw-impression file's footer [metadata].
     *
     * Fails loudly if the event date is missing: every labeled impression must be dated, so an
     * undated input file is a producer bug, not a per-row condition.
     */
    fun fromFooterMetadata(metadata: Map<String, String>): RawImpressionFileMetadata {
      return RawImpressionFileMetadata(parseEventDate(metadata))
    }

    /**
     * Parses just the file's event date from its footer [metadata].
     *
     * Split out of [fromFooterMetadata] so callers that only need the date (e.g. the dispatcher's
     * registration path) can reuse the footer key name and date parsing. Fails loudly if the event
     * date is missing or not an ISO `YYYY-MM-DD` date, since an undated input file is a producer
     * bug, not a per-row condition.
     */
    fun parseEventDate(metadata: Map<String, String>): LocalDate {
      val eventDateString =
        requireNotNull(metadata[EVENT_DATE_KEY]?.takeIf { it.isNotEmpty() }) {
          "raw-impression footer is missing the '$EVENT_DATE_KEY' metadata entry; the producer " +
            "must write each file's event date (ISO YYYY-MM-DD, UTC) into its plaintext footer"
        }
      return try {
        LocalDate.parse(eventDateString)
      } catch (e: DateTimeParseException) {
        throw IllegalArgumentException(
          "raw-impression footer '$EVENT_DATE_KEY' is not an ISO YYYY-MM-DD date: " + eventDateString,
          e,
        )
      }
    }
  }
}
