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
  private val mapping = linkedMapOf("household" to "hh_col", "creative" to "cr_col")

  @Test
  fun `maps all set columns to entity keys in mapping order`() {
    val row =
      mapOf(
        "hh_col" to parquetValue { stringValue = "hh-1" },
        "cr_col" to parquetValue { stringValue = "cr-9" },
      )

    val keys = EntityKeyMapper(mapping).map(row)

    assertThat(keys.map { it.entityType to it.entityId })
      .containsExactly("household" to "hh-1", "creative" to "cr-9")
      .inOrder()
  }

  @Test
  fun `omits entity types whose column is absent or unset`() {
    // hh_col set; cr_col explicitly unset (KIND_NOT_SET). An absent column behaves the same way.
    val row = mapOf("hh_col" to parquetValue { stringValue = "hh-1" }, "cr_col" to parquetValue {})

    val keys = EntityKeyMapper(mapping).map(row)

    assertThat(keys.map { it.entityType to it.entityId }).containsExactly("household" to "hh-1")
  }

  @Test
  fun `treats an empty-string cell as unset`() {
    val row =
      mapOf(
        "hh_col" to parquetValue { stringValue = "hh-1" },
        "cr_col" to parquetValue { stringValue = "" },
      )

    val keys = EntityKeyMapper(mapping).map(row)

    assertThat(keys.map { it.entityType to it.entityId }).containsExactly("household" to "hh-1")
  }

  @Test
  fun `throws when all mapped columns are null or empty`() {
    // hh_col absent; cr_col empty string -> both unset.
    val row = mapOf("cr_col" to parquetValue { stringValue = "" })

    assertFailsWith<IllegalArgumentException> { EntityKeyMapper(mapping).map(row) }
  }

  @Test
  fun `throws on a non-string column kind`() {
    val row =
      mapOf(
        "hh_col" to parquetValue { stringValue = "hh-1" },
        "cr_col" to parquetValue { bytesValue = ByteString.copyFromUtf8("cr-9") },
      )

    assertFailsWith<IllegalStateException> { EntityKeyMapper(mapping).map(row) }
  }

  @Test
  fun `throws on an empty mapping`() {
    assertFailsWith<IllegalArgumentException> {
      EntityKeyMapper(emptyMap()).map(mapOf("x" to parquetValue { stringValue = "y" }))
    }
  }
}
