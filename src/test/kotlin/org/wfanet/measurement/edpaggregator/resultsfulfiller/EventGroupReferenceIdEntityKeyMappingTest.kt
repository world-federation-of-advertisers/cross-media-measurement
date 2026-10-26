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
import org.wfanet.measurement.edpaggregator.EventGroupReferenceIdEntityKeyMapping

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

  @Test
  fun `required conversions accept every prefix combination`() {
    for (referencePrefix in listOf("", "ref/")) {
      for (entityPrefix in listOf("", "entity/")) {
        val mapping =
          EventGroupReferenceIdEntityKeyMapping(ENTITY_TYPE, referencePrefix, entityPrefix)

        assertThat(mapping.requireEntityId(referencePrefix + "123")).isEqualTo(entityPrefix + "123")
        assertThat(mapping.requireEventGroupReferenceId(ENTITY_TYPE, entityPrefix + "123"))
          .isEqualTo(referencePrefix + "123")
      }
    }
  }

  @Test
  fun `required conversions throw on source prefix mismatch while nullable conversions remain unchanged`() {
    val mapping = EventGroupReferenceIdEntityKeyMapping(ENTITY_TYPE, "ref/", "entity/")

    assertFailsWith<IllegalArgumentException> { mapping.requireEntityId("wrong/123") }
    assertFailsWith<IllegalArgumentException> {
      mapping.requireEventGroupReferenceId(ENTITY_TYPE, "wrong/123")
    }
    assertThat(mapping.toEntityId("wrong/123")).isNull()
    assertThat(mapping.toEventGroupReferenceId(ENTITY_TYPE, "wrong/123")).isNull()
  }

  @Test
  fun `required reference ID conversion rejects other entity types`() {
    val mapping = EventGroupReferenceIdEntityKeyMapping(ENTITY_TYPE, "ref/", "entity/")

    assertFailsWith<IllegalArgumentException> {
      mapping.requireEventGroupReferenceId("ad", "entity/123")
    }
    assertThat(mapping.toEventGroupReferenceId("ad", "entity/123")).isNull()
  }

  @Test
  fun `required conversions reject blank source IDs with empty prefixes`() {
    val referencePrefixMapping = EventGroupReferenceIdEntityKeyMapping(ENTITY_TYPE, "ref/", "")
    val entityPrefixMapping = EventGroupReferenceIdEntityKeyMapping(ENTITY_TYPE, "", "entity/")

    for (blankId in listOf("", "   ")) {
      assertFailsWith<IllegalArgumentException> {
        referencePrefixMapping.requireEventGroupReferenceId(ENTITY_TYPE, blankId)
      }
      assertFailsWith<IllegalArgumentException> { entityPrefixMapping.requireEntityId(blankId) }
    }
  }

  @Test
  fun `required conversions reject blank destination IDs`() {
    val referencePrefixMapping = EventGroupReferenceIdEntityKeyMapping(ENTITY_TYPE, "ref/", "")
    val entityPrefixMapping = EventGroupReferenceIdEntityKeyMapping(ENTITY_TYPE, "", "entity/")

    for (suffix in listOf("", "   ")) {
      assertFailsWith<IllegalArgumentException> {
        referencePrefixMapping.requireEntityId("ref/" + suffix)
      }
      assertFailsWith<IllegalArgumentException> {
        entityPrefixMapping.requireEventGroupReferenceId(ENTITY_TYPE, "entity/" + suffix)
      }
    }
  }

  @Test
  fun `validateEntityId and validateEventGroupReferenceId accept every prefix combination`() {
    for (referencePrefix in listOf("", "ref/")) {
      for (entityPrefix in listOf("", "entity/")) {
        val mapping =
          EventGroupReferenceIdEntityKeyMapping(ENTITY_TYPE, referencePrefix, entityPrefix)

        mapping.validateEntityId(ENTITY_TYPE, entityPrefix + "123")
        mapping.validateEventGroupReferenceId(referencePrefix + "123")
      }
    }
  }

  @Test
  fun `validateEntityId rejects prefix mismatch even when reference prefix is empty`() {
    val mapping = EventGroupReferenceIdEntityKeyMapping(ENTITY_TYPE, "", "entity/")

    assertFailsWith<IllegalArgumentException> { mapping.validateEntityId(ENTITY_TYPE, "wrong/123") }
  }

  @Test
  fun `validateEventGroupReferenceId rejects prefix mismatch even when entity prefix is empty`() {
    val mapping = EventGroupReferenceIdEntityKeyMapping(ENTITY_TYPE, "ref/", "")

    assertFailsWith<IllegalArgumentException> { mapping.validateEventGroupReferenceId("wrong/123") }
  }

  @Test
  fun `validateEntityId and validateEventGroupReferenceId reject blank IDs with empty prefixes`() {
    for (referencePrefix in listOf("", "ref/")) {
      for (entityPrefix in listOf("", "entity/")) {
        val mapping =
          EventGroupReferenceIdEntityKeyMapping(ENTITY_TYPE, referencePrefix, entityPrefix)
        for (blankId in listOf("", "   ", "\t\u00a0\u2003")) {
          assertFailsWith<IllegalArgumentException> {
            mapping.validateEntityId(ENTITY_TYPE, blankId)
          }
          assertFailsWith<IllegalArgumentException> {
            mapping.validateEventGroupReferenceId(blankId)
          }
        }
      }
    }
  }

  @Test
  fun `validateEntityId and validateEventGroupReferenceId reject blank mapped IDs`() {
    for (targetPrefix in listOf("", " \t")) {
      val referencePrefixMapping =
        EventGroupReferenceIdEntityKeyMapping(ENTITY_TYPE, "ref/", targetPrefix)
      val entityPrefixMapping =
        EventGroupReferenceIdEntityKeyMapping(ENTITY_TYPE, targetPrefix, "entity/")
      for (suffix in listOf("", "   ", "\t\u00a0\u2003")) {
        assertFailsWith<IllegalArgumentException> {
          referencePrefixMapping.validateEventGroupReferenceId("ref/" + suffix)
        }
        assertFailsWith<IllegalArgumentException> {
          entityPrefixMapping.validateEntityId(ENTITY_TYPE, "entity/" + suffix)
        }
      }
    }
  }

  @Test
  fun `validateEntityId and validateEventGroupReferenceId accept blank suffix with nonblank destination prefix`() {
    val mapping = EventGroupReferenceIdEntityKeyMapping(ENTITY_TYPE, "ref/", "entity/")

    for (suffix in listOf("", "   ", "\t\u00a0\u2003")) {
      mapping.validateEntityId(ENTITY_TYPE, "entity/" + suffix)
      mapping.validateEventGroupReferenceId("ref/" + suffix)
      assertThat(mapping.requireEventGroupReferenceId(ENTITY_TYPE, "entity/" + suffix))
        .isEqualTo("ref/" + suffix)
      assertThat(mapping.requireEntityId("ref/" + suffix)).isEqualTo("entity/" + suffix)
    }
  }

  @Test
  fun `validateEntityId rejects other entity types`() {
    val mapping = EventGroupReferenceIdEntityKeyMapping(ENTITY_TYPE, "", "")

    assertFailsWith<IllegalArgumentException> { mapping.validateEntityId("ad", "123") }
  }

  @Test
  fun `validateEntityId and validateEventGroupReferenceId preserve conversion acceptance`() {
    val prefixes = listOf("", "ref/", "entity/", " \t", "\u00a0")
    val ids =
      listOf(
        "",
        "   ",
        "\t\u00a0\u2003",
        "123",
        " \t123",
        "ref/",
        "ref/123",
        "ref/   ",
        "entity/",
        "entity/123",
        "entity/\u00a0",
        "wrong/123",
        "\u00a0123",
      )
    for (referencePrefix in prefixes) {
      for (entityPrefix in prefixes) {
        val mapping =
          EventGroupReferenceIdEntityKeyMapping(ENTITY_TYPE, referencePrefix, entityPrefix)
        for (id in ids) {
          if (id.isNotBlank() && mapping.toEntityId(id) != null) {
            mapping.validateEventGroupReferenceId(id)
            assertThat(mapping.requireEntityId(id)).isEqualTo(mapping.toEntityId(id))
          } else {
            assertFailsWith<IllegalArgumentException> { mapping.validateEventGroupReferenceId(id) }
          }
          for (entityType in listOf(ENTITY_TYPE, "ad")) {
            if (id.isNotBlank() && mapping.toEventGroupReferenceId(entityType, id) != null) {
              mapping.validateEntityId(entityType, id)
              assertThat(mapping.requireEventGroupReferenceId(entityType, id))
                .isEqualTo(mapping.toEventGroupReferenceId(entityType, id))
            } else {
              assertFailsWith<IllegalArgumentException> { mapping.validateEntityId(entityType, id) }
            }
          }
        }
      }
    }
  }

  companion object {
    private const val ENTITY_TYPE = "campaign"
  }
}
