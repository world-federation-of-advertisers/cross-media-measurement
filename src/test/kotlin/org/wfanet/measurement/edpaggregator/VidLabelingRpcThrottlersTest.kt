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
import kotlinx.coroutines.test.runTest
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.testing.VidLabelingRpcThrottlersTestHelper

@RunWith(JUnit4::class)
class VidLabelingRpcThrottlersTest {
  @Test
  fun `default intervals match the process rate budgets`() {
    assertThat(VidLabelingRpcThrottlers.DEFAULT_KINGDOM_MIN_INTERVAL)
      .isEqualTo(Duration.ofMillis(500))
    assertThat(VidLabelingRpcThrottlers.DEFAULT_METADATA_READ_MIN_INTERVAL)
      .isEqualTo(Duration.ofMillis(100))
    assertThat(VidLabelingRpcThrottlers.DEFAULT_METADATA_WRITE_MIN_INTERVAL)
      .isEqualTo(Duration.ofMillis(200))
    assertThat(VidLabelingRpcThrottlers.DEFAULT_CONTROL_PLANE_MIN_INTERVAL)
      .isEqualTo(Duration.ofMillis(250))
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
  fun `alwaysReady executes all RPC classes`() = runTest {
    val throttlers = VidLabelingRpcThrottlersTestHelper.alwaysReady()
    val calls = mutableListOf<String>()

    throttlers.kingdom.onReady { calls += "kingdom" }
    throttlers.metadataRead.onReady { calls += "metadataRead" }
    throttlers.metadataWrite.onReady { calls += "metadataWrite" }
    throttlers.controlPlane.onReady { calls += "controlPlane" }

    assertThat(calls)
      .containsExactly("kingdom", "metadataRead", "metadataWrite", "controlPlane")
      .inOrder()
  }

  @Test
  fun `recording tracks RPC classes independently`() = runTest {
    val recording = VidLabelingRpcThrottlersTestHelper.recording()

    recording.throttlers.kingdom.onReady {}
    recording.throttlers.kingdom.onReady {}
    recording.throttlers.metadataRead.onReady {}
    recording.throttlers.metadataWrite.onReady {}
    recording.throttlers.metadataWrite.onReady {}
    recording.throttlers.metadataWrite.onReady {}
    recording.throttlers.controlPlane.onReady {}

    assertThat(recording.kingdom.invocationCount).isEqualTo(2)
    assertThat(recording.metadataRead.invocationCount).isEqualTo(1)
    assertThat(recording.metadataWrite.invocationCount).isEqualTo(3)
    assertThat(recording.controlPlane.invocationCount).isEqualTo(1)
  }
}
