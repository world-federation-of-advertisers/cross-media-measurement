/*
 * Copyright 2023 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.common

import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset

/**
 * An open-ended range of [Instant]s.
 *
 * TODO(@SanjayVas): Move to common-jvm.
 */
data class OpenEndedTimeRange(val start: Instant, val endExclusive: Instant) {
  operator fun contains(value: Instant): Boolean {
    return value >= start && value < endExclusive
  }

  fun overlaps(other: OpenEndedTimeRange): Boolean {
    return max(start, other.start) < min(endExclusive, other.endExclusive)
  }

  companion object {
    private fun min(lhs: Instant, rhs: Instant): Instant {
      return if (lhs < rhs) lhs else rhs
    }

    private fun max(lhs: Instant, rhs: Instant): Instant {
      return if (lhs > rhs) lhs else rhs
    }

    fun fromClosedDateRange(
      dateRange: ClosedRange<LocalDate>,
      zoneOffset: ZoneOffset = ZoneOffset.UTC
    ) =
      OpenEndedTimeRange(
        dateRange.start.atStartOfDay().toInstant(zoneOffset),
        dateRange.endInclusive.plusDays(1).atStartOfDay().toInstant(zoneOffset)
      )
  }
}
