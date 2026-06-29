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

package org.wfanet.measurement.edpaggregator.eventgroupactivities

import com.google.common.truth.Truth.assertThat
import java.io.ByteArrayInputStream
import java.time.Instant
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class SpotDataParserTest {

  private fun parse(json: String): List<SpotRecord> =
    SpotDataParser.parseJson(ByteArrayInputStream(json.toByteArray(Charsets.UTF_8)))

  @Test
  fun `parseJson parses valid JSON array`() {
    val json =
      """
      [
        {"parent": "dataProviders/dp1/eventGroups/eg1", "event_group_activity_date": "2026-01-01T00:00:00Z"},
        {"parent": "dataProviders/dp1/eventGroups/eg2", "event_group_activity_date": "2026-01-02T12:30:00Z"}
      ]
      """
        .trimIndent()

    val records = parse(json)

    assertThat(records)
      .containsExactly(
        SpotRecord("dataProviders/dp1/eventGroups/eg1", Instant.parse("2026-01-01T00:00:00Z")),
        SpotRecord("dataProviders/dp1/eventGroups/eg2", Instant.parse("2026-01-02T12:30:00Z")),
      )
      .inOrder()
  }

  @Test
  fun `parseJson handles empty array`() {
    assertThat(parse("[]")).isEmpty()
  }

  @Test
  fun `parseJson fails on missing parent field`() {
    val json = """[{"event_group_activity_date": "2026-01-01T00:00:00Z"}]"""

    val exception = assertFailsWith<IllegalArgumentException> { parse(json) }

    assertThat(exception).hasMessageThat().contains("parent")
  }

  @Test
  fun `parseJson fails on missing event_group_activity_date field`() {
    val json = """[{"parent": "dataProviders/dp1/eventGroups/eg1"}]"""

    val exception = assertFailsWith<IllegalArgumentException> { parse(json) }

    assertThat(exception).hasMessageThat().contains("event_group_activity_date")
  }

  @Test
  fun `parseJson fails on invalid date format`() {
    val json =
      """[{"parent": "dataProviders/dp1/eventGroups/eg1", "event_group_activity_date": "not-a-date"}]"""

    val exception = assertFailsWith<IllegalArgumentException> { parse(json) }

    assertThat(exception).hasMessageThat().contains("event_group_activity_date")
  }

  @Test
  fun `parseJson fails on non-array top-level JSON`() {
    val exception = assertFailsWith<IllegalArgumentException> { parse("{}") }

    assertThat(exception).hasMessageThat().contains("Malformed spot-data JSON")
  }

  @Test
  fun `parseJson fails on wrong-typed parent`() {
    val json = """[{"parent": 123, "event_group_activity_date": "2026-01-01T00:00:00Z"}]"""

    val exception = assertFailsWith<IllegalArgumentException> { parse(json) }

    assertThat(exception).hasMessageThat().contains("Malformed spot-data JSON")
  }

  @Test
  fun `parseJson fails on truncated or garbage JSON`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        parse("""[{"parent": "dataProviders/dp1/eventGroups/eg1", """)
      }

    assertThat(exception).hasMessageThat().contains("Malformed spot-data JSON")
  }

  @Test
  fun `parseJson handles large file (200k records)`() {
    val count = 200_000
    val builder = StringBuilder()
    builder.append("[")
    for (i in 0 until count) {
      if (i > 0) builder.append(",")
      builder.append(
        """{"parent": "dataProviders/dp1/eventGroups/eg$i", "event_group_activity_date": "2026-01-01T00:00:00Z"}"""
      )
    }
    builder.append("]")

    val records = parse(builder.toString())

    assertThat(records).hasSize(count)
  }
}
