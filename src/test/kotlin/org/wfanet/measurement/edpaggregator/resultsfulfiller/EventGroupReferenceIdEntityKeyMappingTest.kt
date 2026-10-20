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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class EventGroupReferenceIdEntityKeyMappingTest {
  @Test
  fun `toEventGroupReferenceId and toEntityId preserve ID when both prefixes are empty`() {
    val mapping =
      EventGroupReferenceIdEntityKeyMapping(
        entityType = ENTITY_TYPE,
        eventGroupReferenceIdPrefix = "",
        entityIdPrefix = "",
      )

    assertThat(mapping.toEventGroupReferenceId(ENTITY_TYPE, "123")).isEqualTo("123")
    assertThat(mapping.toEntityId("123")).isEqualTo("123")
  }

  @Test
  fun `toEventGroupReferenceId and toEntityId convert when only reference prefix is set`() {
    val mapping =
      EventGroupReferenceIdEntityKeyMapping(
        entityType = ENTITY_TYPE,
        eventGroupReferenceIdPrefix = "ref/",
        entityIdPrefix = "",
      )

    assertThat(mapping.toEventGroupReferenceId(ENTITY_TYPE, "123")).isEqualTo("ref/123")
    assertThat(mapping.toEntityId("ref/123")).isEqualTo("123")
    assertThat(mapping.toEntityId("123")).isNull()
  }

  @Test
  fun `toEventGroupReferenceId and toEntityId convert when only entity prefix is set`() {
    val mapping =
      EventGroupReferenceIdEntityKeyMapping(
        entityType = ENTITY_TYPE,
        eventGroupReferenceIdPrefix = "",
        entityIdPrefix = "entity/",
      )

    assertThat(mapping.toEventGroupReferenceId(ENTITY_TYPE, "entity/123")).isEqualTo("123")
    assertThat(mapping.toEntityId("123")).isEqualTo("entity/123")
    assertThat(mapping.toEventGroupReferenceId(ENTITY_TYPE, "123")).isNull()
  }

  @Test
  fun `toEventGroupReferenceId and toEntityId replace prefixes when both are set`() {
    val mapping =
      EventGroupReferenceIdEntityKeyMapping(
        entityType = ENTITY_TYPE,
        eventGroupReferenceIdPrefix = "ref/",
        entityIdPrefix = "entity/",
      )

    assertThat(mapping.toEventGroupReferenceId(ENTITY_TYPE, "entity/123")).isEqualTo("ref/123")
    assertThat(mapping.toEntityId("ref/123")).isEqualTo("entity/123")
    assertThat(mapping.toEventGroupReferenceId(ENTITY_TYPE, "123")).isNull()
    assertThat(mapping.toEntityId("123")).isNull()
  }

  @Test
  fun `toEventGroupReferenceId returns null when replacement is blank`() {
    val emptyPrefixMapping =
      EventGroupReferenceIdEntityKeyMapping(
        entityType = ENTITY_TYPE,
        eventGroupReferenceIdPrefix = "",
        entityIdPrefix = "",
      )
    val entityPrefixMapping =
      EventGroupReferenceIdEntityKeyMapping(
        entityType = ENTITY_TYPE,
        eventGroupReferenceIdPrefix = "",
        entityIdPrefix = "entity/",
      )

    assertThat(emptyPrefixMapping.toEventGroupReferenceId(ENTITY_TYPE, "   ")).isNull()
    assertThat(entityPrefixMapping.toEventGroupReferenceId(ENTITY_TYPE, "entity/")).isNull()
  }

  @Test
  fun `toEntityId returns null when replacement is blank`() {
    val emptyPrefixMapping =
      EventGroupReferenceIdEntityKeyMapping(
        entityType = ENTITY_TYPE,
        eventGroupReferenceIdPrefix = "",
        entityIdPrefix = "",
      )
    val referencePrefixMapping =
      EventGroupReferenceIdEntityKeyMapping(
        entityType = ENTITY_TYPE,
        eventGroupReferenceIdPrefix = "ref/",
        entityIdPrefix = "",
      )

    assertThat(emptyPrefixMapping.toEntityId("   ")).isNull()
    assertThat(referencePrefixMapping.toEntityId("ref/")).isNull()
  }

  @Test
  fun `construction rejects blank entity type`() {
    assertFailsWith<IllegalArgumentException> {
      EventGroupReferenceIdEntityKeyMapping(
        entityType = "   ",
        eventGroupReferenceIdPrefix = "ref/",
        entityIdPrefix = "entity/",
      )
    }
  }

  companion object {
    private const val ENTITY_TYPE = "campaign"
  }
}
