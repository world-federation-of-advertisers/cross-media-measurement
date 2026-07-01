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

package org.wfanet.measurement.edpaggregator.vidlabeler

import com.google.common.truth.Truth.assertThat
import java.time.LocalDate
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class FileEntityKeysTest {
  @Test
  fun `fromFooterMetadata parses event group reference id, entity keys, and max event date`() {
    val metadata =
      mapOf(
        "event_group_reference_id" to "eg-1",
        "entity_keys" to
          """[{"entity_type":"creative","entity_id":"c-1"},{"entity_type":"placement","entity_id":"p-9"}]""",
        "event_date" to "2026-06-30",
      )

    val fileEntityKeys = FileEntityKeys.fromFooterMetadata(metadata)

    assertThat(fileEntityKeys.eventGroupReferenceId).isEqualTo("eg-1")
    assertThat(fileEntityKeys.entityKeys.map { it.entityType to it.entityId })
      .containsExactly("creative" to "c-1", "placement" to "p-9")
      .inOrder()
    assertThat(fileEntityKeys.eventDate).isEqualTo(LocalDate.of(2026, 6, 30))
  }

  @Test
  fun `fromFooterMetadata throws when event_date is missing`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        FileEntityKeys.fromFooterMetadata(
          mapOf(
            "event_group_reference_id" to "eg-1",
            "entity_keys" to """[{"entity_type":"creative","entity_id":"c-1"}]""",
          )
        )
      }
    assertThat(exception).hasMessageThat().contains("event_date")
  }

  @Test
  fun `fromFooterMetadata throws when event_date is not an ISO date`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        FileEntityKeys.fromFooterMetadata(
          mapOf(
            "event_group_reference_id" to "eg-1",
            "entity_keys" to """[{"entity_type":"creative","entity_id":"c-1"}]""",
            "event_date" to "30-06-2026",
          )
        )
      }
    assertThat(exception).hasMessageThat().contains("event_date")
  }

  @Test
  fun `fromFooterMetadata throws when entity_keys is missing`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        FileEntityKeys.fromFooterMetadata(mapOf("event_group_reference_id" to "eg-1"))
      }
    assertThat(exception).hasMessageThat().contains("entity_keys")
  }

  @Test
  fun `fromFooterMetadata throws when entity_keys is an empty array`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        FileEntityKeys.fromFooterMetadata(
          mapOf("event_group_reference_id" to "eg-1", "entity_keys" to "[]")
        )
      }
    assertThat(exception).hasMessageThat().contains("at least one entity key")
  }

  @Test
  fun `fromFooterMetadata throws when event_group_reference_id is missing`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        FileEntityKeys.fromFooterMetadata(
          mapOf("entity_keys" to """[{"entity_type":"creative","entity_id":"c-1"}]""")
        )
      }
    assertThat(exception).hasMessageThat().contains("event_group_reference_id")
  }

  @Test
  fun `fromFooterMetadata throws when an entity key is missing a field`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        FileEntityKeys.fromFooterMetadata(
          mapOf(
            "event_group_reference_id" to "eg-1",
            "entity_keys" to """[{"entity_type":"creative"}]""",
          )
        )
      }
    assertThat(exception).hasMessageThat().contains("entity_id")
  }
}
