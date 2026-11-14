// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.vidlabeling

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.timestamp
import org.junit.Test
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.rankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUpload

class HistoricalAvailabilitySnapshotsTest {
  private fun source(registered: Boolean = true) = rawImpressionUpload {
    registrationComplete = registered
    createTime = timestamp { seconds = 10L }
  }

  private fun checkpoint(pool: Long, created: Long?, deleted: Boolean = false) = rankIndexBlob {
    poolOffset = pool
    blobType = RankIndexBlob.BlobType.SNAPSHOT
    if (created != null) createTime = timestamp { seconds = created }
    if (deleted) deleteTime = timestamp { seconds = 100L }
  }

  @Test
  fun `deleted same-pool source checkpoint remains a historical boundary`() {
    assertThat(
        HistoricalAvailabilitySnapshots.hasHistoricalBasis(
          source(),
          listOf(checkpoint(0L, 20L, true)),
          listOf(checkpoint(0L, 15L)),
        )
      )
      .isTrue()
  }

  @Test
  fun `equal or newer owner checkpoints fail the historical boundary`() {
    for (created in listOf(20L, 21L)) {
      assertThat(
          HistoricalAvailabilitySnapshots.hasHistoricalBasis(
            source(),
            listOf(checkpoint(0L, 20L)),
            listOf(checkpoint(0L, created)),
          )
        )
        .isFalse()
    }
  }

  @Test
  fun `absent original pool uses first original checkpoint rather than registration`() {
    assertThat(
        HistoricalAvailabilitySnapshots.hasHistoricalBasis(
          source(),
          listOf(checkpoint(0L, 20L)),
          listOf(checkpoint(1L, 15L)),
        )
      )
      .isTrue()
  }

  @Test
  fun `absent original pool cannot use a later original checkpoint`() {
    assertThat(
        HistoricalAvailabilitySnapshots.hasHistoricalBasis(
          source(),
          listOf(checkpoint(0L, 20L), checkpoint(1L, 30L)),
          listOf(checkpoint(2L, 25L)),
        )
      )
      .isFalse()
  }

  @Test
  fun `same-pool original checkpoint takes precedence over other original pools`() {
    assertThat(
        HistoricalAvailabilitySnapshots.hasHistoricalBasis(
          source(),
          listOf(checkpoint(0L, 20L), checkpoint(1L, 30L)),
          listOf(checkpoint(1L, 25L)),
        )
      )
      .isTrue()
  }

  @Test
  fun `no original checkpoint requires owner history before source creation`() {
    assertThat(
        HistoricalAvailabilitySnapshots.hasHistoricalBasis(
          source(),
          emptyList(),
          listOf(checkpoint(0L, 9L)),
        )
      )
      .isTrue()
    assertThat(
        HistoricalAvailabilitySnapshots.hasHistoricalBasis(
          source(),
          emptyList(),
          listOf(checkpoint(0L, 11L)),
        )
      )
      .isFalse()
  }

  @Test
  fun `missing owner checkpoint or timestamp is unproven`() {
    assertThat(
        HistoricalAvailabilitySnapshots.hasHistoricalBasis(source(), emptyList(), emptyList())
      )
      .isFalse()
    assertThat(
        HistoricalAvailabilitySnapshots.hasHistoricalBasis(
          source(),
          listOf(checkpoint(0L, 20L)),
          listOf(checkpoint(0L, null)),
        )
      )
      .isFalse()
  }

  @Test
  fun `unregistered original source is unproven`() {
    assertThat(
        HistoricalAvailabilitySnapshots.hasHistoricalBasis(
          source(registered = false),
          listOf(checkpoint(0L, 20L)),
          listOf(checkpoint(0L, 15L)),
        )
      )
      .isFalse()
  }

  @Test
  fun `missing original creation time is unproven`() {
    val original = rawImpressionUpload { registrationComplete = true }
    assertThat(
        HistoricalAvailabilitySnapshots.hasHistoricalBasis(
          original,
          listOf(checkpoint(0L, 20L)),
          listOf(checkpoint(0L, 15L)),
        )
      )
      .isFalse()
  }

  @Test
  fun `missing original checkpoint time cannot be replaced by another pool boundary`() {
    assertThat(
        HistoricalAvailabilitySnapshots.hasHistoricalBasis(
          source(),
          listOf(checkpoint(0L, null), checkpoint(1L, 20L)),
          listOf(checkpoint(0L, 15L)),
        )
      )
      .isFalse()
  }

  @Test
  fun `every owner pool must meet its historical boundary`() {
    assertThat(
        HistoricalAvailabilitySnapshots.hasHistoricalBasis(
          source(),
          listOf(checkpoint(0L, 20L)),
          listOf(checkpoint(0L, 15L), checkpoint(1L, 21L)),
        )
      )
      .isFalse()
  }
}
