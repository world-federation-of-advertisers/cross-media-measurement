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
import com.google.protobuf.ByteString
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.storage.parquetValue

@RunWith(JUnit4::class)
class EntityKeyMapperTest {
  // entity_type -> raw-impression column name; LinkedHashMap so iteration order is deterministic.
  private val required = linkedMapOf("person" to "person_col")
  private val optional = linkedMapOf("household" to "hh_col", "creative" to "cr_col")

  @Test
  fun `maps required then optional columns to entity keys in order`() {
    val row =
      mapOf(
        "person_col" to parquetValue { stringValue = "p-1" },
        "hh_col" to parquetValue { stringValue = "hh-1" },
        "cr_col" to parquetValue { stringValue = "cr-9" },
      )

    val keys = EntityKeyMapper(required, optional).map(row)

    assertThat(keys.map { it.entityType to it.entityId })
      .containsExactly("person" to "p-1", "household" to "hh-1", "creative" to "cr-9")
      .inOrder()
  }

  @Test
  fun `omits an optional entity type whose column is unset`() {
    // hh_col is populated; cr_col is explicitly unset (KIND_NOT_SET).
    val row =
      mapOf(
        "person_col" to parquetValue { stringValue = "p-1" },
        "hh_col" to parquetValue { stringValue = "hh-1" },
        "cr_col" to parquetValue {},
      )

    val keys = EntityKeyMapper(required, optional).map(row)

    assertThat(keys.map { it.entityType to it.entityId })
      .containsExactly("person" to "p-1", "household" to "hh-1")
      .inOrder()
  }

  @Test
  fun `omits optional entity types whose columns are absent`() {
    val row = mapOf("person_col" to parquetValue { stringValue = "p-1" })

    val keys = EntityKeyMapper(required, optional).map(row)

    assertThat(keys.map { it.entityType to it.entityId }).containsExactly("person" to "p-1")
  }

  @Test
  fun `omits optional entity types whose columns are empty`() {
    val row =
      mapOf(
        "person_col" to parquetValue { stringValue = "p-1" },
        "hh_col" to parquetValue { stringValue = "" },
        "cr_col" to parquetValue { stringValue = "" },
      )

    val keys = EntityKeyMapper(required, optional).map(row)

    assertThat(keys.map { it.entityType to it.entityId }).containsExactly("person" to "p-1")
  }

  @Test
  fun `throws when a required hierarchy column is absent`() {
    val row = mapOf("person_col" to parquetValue { stringValue = "p-1" })

    val exception =
      assertFailsWith<IllegalArgumentException> {
        EntityKeyMapper(mapOf("campaign" to "campaign_col"), mapOf("person" to "person_col"))
          .map(row)
      }

    assertThat(exception).hasMessageThat().contains("campaign_col")
  }

  @Test
  fun `throws when a required column is empty`() {
    val row =
      mapOf(
        "person_col" to parquetValue { stringValue = "" },
        "hh_col" to parquetValue { stringValue = "hh-1" },
      )

    val exception =
      assertFailsWith<IllegalArgumentException> { EntityKeyMapper(required, optional).map(row) }
    assertThat(exception).hasMessageThat().contains("person_col")
  }

  @Test
  fun `throws when a required column is unset`() {
    val row = mapOf("person_col" to parquetValue {})

    val exception =
      assertFailsWith<IllegalArgumentException> { EntityKeyMapper(required, optional).map(row) }

    assertThat(exception).hasMessageThat().contains("person_col")
  }

  @Test
  fun `throws when a required column is not a string`() {
    val row = mapOf("person_col" to parquetValue { int64Value = 1L })

    assertFailsWith<IllegalStateException> { EntityKeyMapper(required, optional).map(row) }
  }

  @Test
  fun `throws when no required types and every optional column is unset`() {
    val row = mapOf("cr_col" to parquetValue { stringValue = "" })

    assertFailsWith<IllegalArgumentException> { EntityKeyMapper(emptyMap(), optional).map(row) }
  }

  @Test
  fun `throws on a non-string column kind`() {
    val row =
      mapOf(
        "person_col" to parquetValue { stringValue = "p-1" },
        "hh_col" to parquetValue { bytesValue = ByteString.copyFromUtf8("hh-1") },
      )

    assertFailsWith<IllegalStateException> { EntityKeyMapper(required, optional).map(row) }
  }

  @Test
  fun `throws at construction when required and optional keys overlap`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        EntityKeyMapper(linkedMapOf("person" to "a"), linkedMapOf("person" to "b"))
      }
    assertThat(exception).hasMessageThat().contains("person")
  }

  @Test
  fun `throws at construction when both mappings are empty`() {
    assertFailsWith<IllegalArgumentException> { EntityKeyMapper(emptyMap(), emptyMap()) }
  }
}
