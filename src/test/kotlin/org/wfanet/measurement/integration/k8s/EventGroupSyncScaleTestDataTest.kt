/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.integration.k8s

import com.google.common.truth.Truth.assertThat
import org.junit.Test

class EventGroupSyncScaleTestDataTest {
  @Test
  fun `selectMutations returns reproducible disjoint sets`() {
    val current = (0 until 5_000).map { "campaign-scale-test-initial-$it" }

    val first =
      EventGroupSyncScaleTestData.selectMutations(
        currentReferenceIds = current,
        seed = "run-123",
        mutationCount = 50,
      )
    val second =
      EventGroupSyncScaleTestData.selectMutations(
        currentReferenceIds = current,
        seed = "run-123",
        mutationCount = 50,
      )

    assertThat(first).isEqualTo(second)
    assertThat(first.updatedReferenceIds).hasSize(50)
    assertThat(first.deletedReferenceIds).hasSize(50)
    assertThat(first.omittedReferenceIds).hasSize(50)
    assertThat(first.addedReferenceIds).hasSize(50)
    assertThat(
        first.updatedReferenceIds +
          first.deletedReferenceIds +
          first.omittedReferenceIds +
          first.addedReferenceIds
      )
      .hasSize(200)
    assertThat(first.addedReferenceIds).containsNoneIn(current)
  }

  @Test
  fun `selectMutations changes selections with seed`() {
    val current = (0 until 5_000).map { "campaign-scale-test-initial-$it" }

    val first =
      EventGroupSyncScaleTestData.selectMutations(
        currentReferenceIds = current,
        seed = "run-123",
        mutationCount = 50,
      )
    val second =
      EventGroupSyncScaleTestData.selectMutations(
        currentReferenceIds = current,
        seed = "run-456",
        mutationCount = 50,
      )

    assertThat(first).isNotEqualTo(second)
  }

  @Test
  fun `newReferenceIds avoids existing identifiers`() {
    val existing =
      setOf(
        "campaign-scale-test-run-123-bootstrap-00000",
        "campaign-scale-test-run-123-bootstrap-00001",
      )

    val result =
      EventGroupSyncScaleTestData.newReferenceIds(
        existingReferenceIds = existing,
        seed = "run-123",
        label = "bootstrap",
        count = 3,
      )

    assertThat(result)
      .containsExactly(
        "campaign-scale-test-run-123-bootstrap-00002",
        "campaign-scale-test-run-123-bootstrap-00003",
        "campaign-scale-test-run-123-bootstrap-00004",
      )
      .inOrder()
  }
}
