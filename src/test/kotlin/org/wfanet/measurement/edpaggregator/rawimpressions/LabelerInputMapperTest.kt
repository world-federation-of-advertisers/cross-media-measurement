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

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Timestamp
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.storage.ParquetValue
import org.wfanet.measurement.storage.parquetValue

@RunWith(JUnit4::class)
class LabelerInputMapperTest {
  @Test
  fun `projects scalar and nested message fields`() {
    val mapper =
      LabelerInputMapper(
        mapOf(
          "event_id.id" to "eid",
          "timestamp_usec" to "ts",
          "profile_info.email_user_info.user_id" to "email",
        )
      )

    val row =
      mapOf(
        "eid" to parquetValue { stringValue = "event-1" },
        "ts" to parquetValue { int64Value = 1_700_000_000_000_000L },
        "email" to parquetValue { stringValue = "user@example.com" },
      )

    val input = mapper.project(row)

    assertThat(input.eventId.id).isEqualTo("event-1")
    assertThat(input.timestampUsec).isEqualTo(1_700_000_000_000_000L)
    assertThat(input.profileInfo.emailUserInfo.userId).isEqualTo("user@example.com")
  }

  @Test
  fun `skips columns that are absent or NULL`() {
    val mapper = LabelerInputMapper(mapOf("event_id.id" to "eid", "timestamp_usec" to "ts"))

    val row =
      mapOf(
        "eid" to parquetValue { stringValue = "event-1" },
        // "ts" present but NULL (no kind set).
        "ts" to ParquetValue.getDefaultInstance(),
      )

    val input = mapper.project(row)

    assertThat(input.eventId.id).isEqualTo("event-1")
    assertThat(input.hasTimestampUsec()).isFalse()
  }

  @Test
  fun `coerces a parquet timestamp into timestamp_usec micros`() {
    val mapper = LabelerInputMapper(mapOf("timestamp_usec" to "ts"))
    val row =
      mapOf(
        "ts" to
          parquetValue {
            timestampValue = Timestamp.newBuilder().setSeconds(1_700L).setNanos(123_000).build()
          }
      )

    val input = mapper.project(row)

    assertThat(input.timestampUsec).isEqualTo(1_700_000_123L)
  }

  @Test
  fun `widens int32 column to int64 field`() {
    val mapper = LabelerInputMapper(mapOf("timestamp_usec" to "ts"))
    val input = mapper.project(mapOf("ts" to parquetValue { int32Value = 42 }))
    assertThat(input.timestampUsec).isEqualTo(42L)
  }

  @Test
  fun `stores an unsigned 32-bit value above Int_MAX without overflow`() {
    // geo.country_id is a proto uint32; 3_000_000_000 is a valid uint32 but exceeds Int.MAX.
    val mapper = LabelerInputMapper(mapOf("geo.country_id" to "country"))
    val unsignedValue = 3_000_000_000L

    val input =
      mapper.project(mapOf("country" to parquetValue { uint32Value = unsignedValue.toInt() }))

    assertThat(Integer.toUnsignedLong(input.geo.countryId)).isEqualTo(unsignedValue)
  }

  @Test
  fun `rejects an unknown field path at construction`() {
    assertFailsWith<IllegalArgumentException> { LabelerInputMapper(mapOf("not_a_field" to "c")) }
  }

  @Test
  fun `rejects a message-typed leaf at construction`() {
    assertFailsWith<IllegalArgumentException> { LabelerInputMapper(mapOf("event_id" to "c")) }
  }

  @Test
  fun `rejects a type-incompatible column at projection`() {
    val mapper = LabelerInputMapper(mapOf("event_id.id" to "eid"))
    assertFailsWith<IllegalArgumentException> {
      mapper.project(mapOf("eid" to parquetValue { int64Value = 5 }))
    }
  }
}
