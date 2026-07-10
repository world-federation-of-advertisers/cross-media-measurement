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
class RawImpressionFileMetadataTest {
  @Test
  fun `fromFooterMetadata parses event date`() {
    val metadata = mapOf("event_date" to "2026-06-30")

    val fileMetadata = RawImpressionFileMetadata.fromFooterMetadata(metadata)

    assertThat(fileMetadata.eventDate).isEqualTo(LocalDate.of(2026, 6, 30))
  }

  @Test
  fun `fromFooterMetadata throws when event_date is missing`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        RawImpressionFileMetadata.fromFooterMetadata(emptyMap())
      }
    assertThat(exception).hasMessageThat().contains("event_date")
  }

  @Test
  fun `fromFooterMetadata throws when event_date is not an ISO date`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        RawImpressionFileMetadata.fromFooterMetadata(mapOf("event_date" to "30-06-2026"))
      }
    assertThat(exception).hasMessageThat().contains("event_date")
  }
}
