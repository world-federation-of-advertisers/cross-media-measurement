// Copyright 2020 The Measurement System Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.gcloud.spanner

import com.google.cloud.Timestamp
import com.google.cloud.spanner.Struct
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class SpannerStructsTest {
  private val timestamp = Timestamp.now()
  private val str = "abcdefg"
  private val int64 = 405060708090100L
  private val struct = Struct.newBuilder()
    .set("nullString").to(null as String?)
    .set("stringValue").to(str)
    .set("nullInt64").to(null as Long?)
    .set("int64Value").to(int64)
    .set("nullTimestamp").to(null as Timestamp?)
    .set("timestampValue").to(timestamp)
    .build()

  @Test
  fun getNullableString() {
    assertNull(struct.getNullableString("nullString"))
    assertEquals(str, struct.getNullableString("stringValue"))
    assertFailsWith<IllegalStateException> { struct.getNullableString("timestampValue") }
    assertFailsWith<IllegalStateException> { struct.getNullableString("nullInt64") }
  }

  @Test
  fun getNullableInt64() {
    assertNull(struct.getNullableLong("nullInt64"))
    assertEquals(int64, struct.getNullableLong("int64Value"))
    assertFailsWith<IllegalStateException> { struct.getNullableLong("timestampValue") }
    assertFailsWith<IllegalStateException> { struct.getNullableLong("nullString") }
  }

  @Test
  fun getNullableTimestamp() {
    assertNull(struct.getNullableTimestamp("nullTimestamp"))
    assertEquals(timestamp, struct.getNullableTimestamp("timestampValue"))
    assertFailsWith<IllegalStateException> { struct.getNullableTimestamp("int64Value") }
    assertFailsWith<IllegalStateException> { struct.getNullableTimestamp("nullString") }
  }
}
