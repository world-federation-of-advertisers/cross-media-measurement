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

package org.wfanet.measurement.edpaggregator

import com.google.common.truth.Truth.assertThat
import java.time.Duration
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class VidLabelingRpcThrottlersTest {
  @Test
  fun `default intervals match the process rate budgets`() {
    assertThat(VidLabelingRpcThrottlers.DEFAULT_KINGDOM_MIN_INTERVAL)
      .isEqualTo(Duration.ofMillis(500))
    assertThat(VidLabelingRpcThrottlers.DEFAULT_METADATA_READ_MIN_INTERVAL)
      .isEqualTo(Duration.ofMillis(500))
    assertThat(VidLabelingRpcThrottlers.DEFAULT_METADATA_WRITE_MIN_INTERVAL)
      .isEqualTo(Duration.ofMillis(1000))
    assertThat(VidLabelingRpcThrottlers.DEFAULT_CONTROL_PLANE_MIN_INTERVAL)
      .isEqualTo(Duration.ofMillis(500))
  }

  @Test
  fun `fromMinimumIntervals rejects non-positive intervals`() {
    assertFailsWith<IllegalArgumentException> {
      VidLabelingRpcThrottlers.fromMinimumIntervals(kingdom = Duration.ZERO)
    }
    assertFailsWith<IllegalArgumentException> {
      VidLabelingRpcThrottlers.fromMinimumIntervals(metadataRead = Duration.ZERO)
    }
    assertFailsWith<IllegalArgumentException> {
      VidLabelingRpcThrottlers.fromMinimumIntervals(metadataWrite = Duration.ZERO)
    }
    assertFailsWith<IllegalArgumentException> {
      VidLabelingRpcThrottlers.fromMinimumIntervals(controlPlane = Duration.ZERO)
    }
  }

  @Test
  fun `fromEnvironment rejects a non-positive configured interval`() {
    assertFailsWith<IllegalArgumentException> {
      VidLabelingRpcThrottlers.fromEnvironment { name ->
        if (name == VidLabelingRpcThrottlers.METADATA_READ_MIN_INTERVAL_ENV) "0s" else null
      }
    }
  }
}
