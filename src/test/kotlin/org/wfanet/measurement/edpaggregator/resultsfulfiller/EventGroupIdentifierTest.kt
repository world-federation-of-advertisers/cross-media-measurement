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
import org.wfanet.measurement.edpaggregator.v1alpha.EntityKeyGroup
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpressionKt
import org.wfanet.measurement.edpaggregator.v1alpha.entityKeyGroup

@RunWith(JUnit4::class)
class EventGroupIdentifierTest {
  @Test
  fun `construction accepts canonical entity-key groups`() {
    val campaignGroup = makeEntityKeyGroup("campaign", "A", "B")

    val identifier =
      EventGroupIdentifier(
        eventGroupReferenceId = null,
        entityKeys = setOf(makeEntityKey("campaign", "A"), makeEntityKey("campaign", "B")),
        wholeBlobEntityKeyGroups = setOf(campaignGroup),
      )

    assertThat(identifier.wholeBlobEntityKeyGroups).containsExactly(campaignGroup)
  }

  @Test
  fun `construction rejects duplicate entity types`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        EventGroupIdentifier(
          eventGroupReferenceId = null,
          entityKeys = setOf(makeEntityKey("campaign", "A"), makeEntityKey("campaign", "B")),
          wholeBlobEntityKeyGroups =
            setOf(makeEntityKeyGroup("campaign", "A"), makeEntityKeyGroup("campaign", "B")),
        )
      }

    assertThat(exception).hasMessageThat().contains("unique")
  }

  @Test
  fun `construction rejects duplicate entity IDs`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        EventGroupIdentifier(
          eventGroupReferenceId = null,
          entityKeys = setOf(makeEntityKey("campaign", "A")),
          wholeBlobEntityKeyGroups = setOf(makeEntityKeyGroup("campaign", "A", "A")),
        )
      }

    assertThat(exception).hasMessageThat().contains("unique within")
  }

  @Test
  fun `construction rejects noncanonical entity ID order`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        EventGroupIdentifier(
          eventGroupReferenceId = null,
          entityKeys = setOf(makeEntityKey("campaign", "A"), makeEntityKey("campaign", "B")),
          wholeBlobEntityKeyGroups = setOf(makeEntityKeyGroup("campaign", "B", "A")),
        )
      }

    assertThat(exception).hasMessageThat().contains("canonical order")
  }

  @Test
  fun `construction rejects empty entity ID group`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        EventGroupIdentifier(
          eventGroupReferenceId = null,
          entityKeys = emptySet(),
          wholeBlobEntityKeyGroups = setOf(makeEntityKeyGroup("campaign")),
        )
      }

    assertThat(exception).hasMessageThat().contains("must not be empty")
  }

  @Test
  fun `construction rejects blank entity type`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        EventGroupIdentifier(
          eventGroupReferenceId = null,
          entityKeys = setOf(makeEntityKey("   ", "A")),
          wholeBlobEntityKeyGroups = setOf(makeEntityKeyGroup("   ", "A")),
        )
      }

    assertThat(exception).hasMessageThat().contains("types must not be blank")
  }

  @Test
  fun `construction rejects blank entity ID`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        EventGroupIdentifier(
          eventGroupReferenceId = null,
          entityKeys = setOf(makeEntityKey("campaign", "   ")),
          wholeBlobEntityKeyGroups = setOf(makeEntityKeyGroup("campaign", "   ")),
        )
      }

    assertThat(exception).hasMessageThat().contains("IDs must not be blank")
  }

  @Test
  fun `construction rejects whole-blob key missing from entity keys`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        EventGroupIdentifier(
          eventGroupReferenceId = null,
          entityKeys = emptySet(),
          wholeBlobEntityKeyGroups = setOf(makeEntityKeyGroup("campaign", "A")),
        )
      }

    assertThat(exception).hasMessageThat().contains("present in the blob")
  }

  private fun makeEntityKeyGroup(entityType: String, vararg entityIds: String): EntityKeyGroup =
    entityKeyGroup {
      this.entityType = entityType
      this.entityIds += entityIds.toList()
    }

  private fun makeEntityKey(entityType: String, entityId: String): LabeledImpression.EntityKey =
    LabeledImpressionKt.entityKey {
      this.entityType = entityType
      this.entityId = entityId
    }
}
