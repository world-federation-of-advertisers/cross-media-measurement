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

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.util.Durations
import com.google.type.date
import com.google.type.dateTime
import com.google.type.timeZone
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class TimestampsTest {
  @Test
  fun `toZonedDateTime returns value with offset`() {
    val dateTime = dateTime {
      year = 2025
      month = 11
      day = 19
      hours = 10
      minutes = 4
      seconds = 11
      nanos = 13
      utcOffset = Durations.fromHours(-8)
    }

    val zonedDateTime = dateTime.toZonedDateTime()

    assertThat(zonedDateTime)
      .isEqualTo(
        ZonedDateTime.of(
          /* year = */ 2025,
          /* month = */ 11,
          /* dayOfMonth = */ 19,
          /* hour = */ 10,
          /* minute = */ 4,
          /* second = */ 11,
          /* nanoOfSecond = */ 13,
          /* zone = */ ZoneOffset.ofHours(-8),
        )
      )
  }

  @Test
  fun `toZonedDateTime returns value with time zone`() {
    val zoneId = "America/Los_Angeles"
    val dateTime = dateTime {
      year = 2025
      month = 11
      day = 19
      hours = 10
      minutes = 4
      seconds = 11
      nanos = 13
      timeZone = timeZone { id = zoneId }
    }

    val zonedDateTime = dateTime.toZonedDateTime()

    assertThat(zonedDateTime)
      .isEqualTo(
        ZonedDateTime.of(
          /* year = */ 2025,
          /* month = */ 11,
          /* dayOfMonth = */ 19,
          /* hour = */ 10,
          /* minute = */ 4,
          /* second = */ 11,
          /* nanoOfSecond = */ 13,
          /* zone = */ ZoneId.of(zoneId),
        )
      )
  }

  @Test
  fun `toProtoDateTime returns value with offset`() {
    val zonedDateTime =
      ZonedDateTime.of(
        /* year = */ 2025,
        /* month = */ 11,
        /* dayOfMonth = */ 19,
        /* hour = */ 10,
        /* minute = */ 4,
        /* second = */ 11,
        /* nanoOfSecond = */ 13,
        /* zone = */ ZoneOffset.ofHours(-8),
      )

    val dateTime = zonedDateTime.toProtoDateTime()

    assertThat(dateTime)
      .isEqualTo(
        dateTime {
          year = 2025
          month = 11
          day = 19
          hours = 10
          minutes = 4
          seconds = 11
          nanos = 13
          utcOffset = Durations.fromHours(-8)
        }
      )
  }

  @Test
  fun `toProtoDateTime returns value with time zone`() {
    val zoneId = "America/Los_Angeles"
    val zonedDateTime =
      ZonedDateTime.of(
        /* year = */ 2025,
        /* month = */ 11,
        /* dayOfMonth = */ 19,
        /* hour = */ 10,
        /* minute = */ 4,
        /* second = */ 11,
        /* nanoOfSecond = */ 13,
        /* zone = */ ZoneId.of(zoneId),
      )

    val dateTime = zonedDateTime.toProtoDateTime()

    assertThat(dateTime)
      .isEqualTo(
        dateTime {
          year = 2025
          month = 11
          day = 19
          hours = 10
          minutes = 4
          seconds = 11
          nanos = 13
          timeZone = timeZone { id = zoneId }
        }
      )
  }

  @Test
  fun `toLocalDate returns equivalent date`() {
    val date = date {
      year = 2025
      month = 11
      day = 19
    }

    val localDate = date.toLocalDate()

    assertThat(localDate).isEqualTo(LocalDate.of(2025, 11, 19))
  }
}
