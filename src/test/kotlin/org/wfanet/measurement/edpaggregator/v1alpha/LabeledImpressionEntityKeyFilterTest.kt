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

package org.wfanet.measurement.edpaggregator.v1alpha

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.Timestamp
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpressionKt.entityKey

/**
 * Demonstrates filtering [LabeledImpression] records by [LabeledImpression.EntityKey].
 *
 * This test is intentionally self-contained: it exercises the proto field directly with a
 * locally-defined matcher, without coupling to the
 * [org.wfanet.measurement.edpaggregator.resultsfulfiller] filter pipeline. Once entity-key
 * filtering is wired into the ResultsFulfiller (e.g. as part of `FilterSpec`/`FilterProcessor`),
 * this matcher can be deleted in favor of tests in `FilterProcessorTest`.
 */
@RunWith(JUnit4::class)
class LabeledImpressionEntityKeyFilterTest {

  /**
   * Returns true iff [impression] has at least one [LabeledImpression.EntityKey] whose
   * (entity_type, entity_id) pair is contained in [targetKeys].
   *
   * This mirrors the "any-of" semantics that downstream consumers are expected to use when
   * filtering impressions to a subset of entities.
   */
  private fun matchesAnyEntityKey(
    impression: LabeledImpression,
    targetKeys: Set<Pair<String, String>>,
  ): Boolean {
    return impression.entityKeysList.any { key -> (key.entityType to key.entityId) in targetKeys }
  }

  @Test
  fun `entity_keys round-trips through the proto`() {
    val impression = labeledImpression {
      eventTime = Timestamp.getDefaultInstance()
      vid = 1L
      event = Any.getDefaultInstance()
      eventGroupReferenceId = "eg-1"
      entityKeys += entityKey {
        entityType = "creative"
        entityId = "creative-123"
      }
      entityKeys += entityKey {
        entityType = "ad-set"
        entityId = "ad-set-456"
      }
    }

    assertThat(impression.entityKeysList).hasSize(2)
    assertThat(impression.entityKeysList.map { it.entityType to it.entityId })
      .containsExactly("creative" to "creative-123", "ad-set" to "ad-set-456")
      .inOrder()
  }

  @Test
  fun `filter keeps impressions that match any target entity key`() {
    val creativeImpression = labeledImpression {
      eventTime = Timestamp.getDefaultInstance()
      vid = 1L
      event = Any.getDefaultInstance()
      eventGroupReferenceId = "eg-1"
      entityKeys += entityKey {
        entityType = "creative"
        entityId = "creative-123"
      }
    }
    val adSetImpression = labeledImpression {
      eventTime = Timestamp.getDefaultInstance()
      vid = 2L
      event = Any.getDefaultInstance()
      eventGroupReferenceId = "eg-1"
      entityKeys += entityKey {
        entityType = "ad-set"
        entityId = "ad-set-456"
      }
    }
    val otherImpression = labeledImpression {
      eventTime = Timestamp.getDefaultInstance()
      vid = 3L
      event = Any.getDefaultInstance()
      eventGroupReferenceId = "eg-1"
      entityKeys += entityKey {
        entityType = "campaign"
        entityId = "campaign-999"
      }
    }

    val impressions = listOf(creativeImpression, adSetImpression, otherImpression)
    val targetKeys = setOf("creative" to "creative-123", "ad-set" to "ad-set-456")

    val filtered = impressions.filter { matchesAnyEntityKey(it, targetKeys) }

    assertThat(filtered.map { it.vid }).containsExactly(1L, 2L).inOrder()
  }

  @Test
  fun `filter matches when an impression has multiple entity keys and only one matches`() {
    val multiKeyImpression = labeledImpression {
      eventTime = Timestamp.getDefaultInstance()
      vid = 42L
      event = Any.getDefaultInstance()
      eventGroupReferenceId = "eg-1"
      entityKeys += entityKey {
        entityType = "creative"
        entityId = "creative-abc"
      }
      entityKeys += entityKey {
        entityType = "ad-set"
        entityId = "ad-set-xyz"
      }
      entityKeys += entityKey {
        entityType = "campaign"
        entityId = "campaign-789"
      }
    }

    val targetKeys = setOf("ad-set" to "ad-set-xyz")

    assertThat(matchesAnyEntityKey(multiKeyImpression, targetKeys)).isTrue()
  }

  @Test
  fun `filter drops impressions with no entity keys`() {
    val impression = labeledImpression {
      eventTime = Timestamp.getDefaultInstance()
      vid = 7L
      event = Any.getDefaultInstance()
      eventGroupReferenceId = "eg-1"
    }

    val targetKeys = setOf("creative" to "creative-123")

    assertThat(impression.entityKeysList).isEmpty()
    assertThat(matchesAnyEntityKey(impression, targetKeys)).isFalse()
  }

  @Test
  fun `filter distinguishes entity_type from entity_id`() {
    val impression = labeledImpression {
      eventTime = Timestamp.getDefaultInstance()
      vid = 9L
      event = Any.getDefaultInstance()
      eventGroupReferenceId = "eg-1"
      entityKeys += entityKey {
        entityType = "creative"
        entityId = "creative-id-123"
      }
    }

    val mismatchingType = setOf("ad-set" to "creative-id-123")
    assertThat(matchesAnyEntityKey(impression, mismatchingType)).isFalse()

    val matchingType = setOf("creative" to "creative-id-123")
    assertThat(matchesAnyEntityKey(impression, matchingType)).isTrue()
  }
}
