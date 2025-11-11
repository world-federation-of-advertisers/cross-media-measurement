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

package org.wfanet.measurement.common

import com.google.protobuf.Timestamp
import com.google.type.DateTime
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime

fun DateTime.toTimestamp(): Timestamp {
  val source = this
  return if (source.hasUtcOffset()) {
    val offset = ZoneOffset.ofTotalSeconds(source.utcOffset.seconds.toInt())
    val offsetDateTime =
      OffsetDateTime.of(
        source.year,
        source.month,
        source.day,
        source.hours,
        source.minutes,
        source.seconds,
        source.nanos,
        offset,
      )
    offsetDateTime.toInstant().toProtoTime()
  } else if (source.hasTimeZone()) {
    val id = ZoneId.of(source.timeZone.id)
    val zonedDateTime =
      ZonedDateTime.of(
        source.year,
        source.month,
        source.day,
        source.hours,
        source.minutes,
        source.seconds,
        source.nanos,
        id,
      )
    zonedDateTime.toInstant().toProtoTime()
  } else {
    throw IllegalArgumentException("utc_offset or time_zone required")
  }
}
