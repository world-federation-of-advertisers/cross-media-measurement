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
import com.google.protobuf.util.Durations
import com.google.type.Date
import com.google.type.DateTime
import com.google.type.dateTime
import com.google.type.timeZone
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime

fun DateTime.toZonedDateTime(): ZonedDateTime {
  val zoneId: ZoneId =
    when (timeOffsetCase) {
      DateTime.TimeOffsetCase.UTC_OFFSET -> ZoneOffset.ofTotalSeconds(utcOffset.seconds.toInt())
      DateTime.TimeOffsetCase.TIME_ZONE -> ZoneId.of(timeZone.id)
      DateTime.TimeOffsetCase.TIMEOFFSET_NOT_SET -> error("time_offset not set")
    }
  return ZonedDateTime.of(year, month, day, hours, minutes, seconds, nanos, zoneId)
}

fun DateTime.toTimestamp(): Timestamp {
  return toZonedDateTime().toInstant().toProtoTime()
}

fun ZonedDateTime.toProtoDateTime(): DateTime {
  val source = this
  return dateTime {
    year = source.year
    month = source.monthValue
    day = source.dayOfMonth
    hours = source.hour
    minutes = source.minute
    seconds = source.second
    nanos = source.nano

    val zoneId: ZoneId = source.zone
    if (zoneId is ZoneOffset) {
      utcOffset = Durations.fromSeconds(zoneId.totalSeconds.toLong())
    } else {
      timeZone = timeZone { id = zoneId.id }
    }
  }
}

fun Date.toLocalDate(): LocalDate {
  return LocalDate.of(year, month, day)
}
