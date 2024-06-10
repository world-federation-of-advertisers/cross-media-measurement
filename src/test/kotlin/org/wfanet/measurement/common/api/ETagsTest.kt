/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.api

import com.google.common.truth.Truth.assertThat
import java.time.Instant
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class ETagsTest {
  @Test
  fun `computeETag returns deterministic ETag`() {
    val etag = ETags.computeETag(UPDATE_TIME_1)

    assertThat(etag).isEqualTo(ETAG_1)
  }

  @Test
  fun `computeETag returns different etag for different update time`() {
    val etag1 = ETags.computeETag(UPDATE_TIME_1)
    val etag2 = ETags.computeETag(UPDATE_TIME_2)

    assertThat(etag1).isNotEqualTo(etag2)
  }

  companion object {
    private val UPDATE_TIME_1 = Instant.parse("2024-06-07T20:00:39.035048707Z")
    private val UPDATE_TIME_2 = Instant.parse("2024-06-07T20:02:25.869628909Z")
    private const val ETAG_1 =
      "W/\"0CD880487A4CDDA3ACD433A17DCDEB02B527717CDDA4A55C25C11F358E708C56\""
  }
}
