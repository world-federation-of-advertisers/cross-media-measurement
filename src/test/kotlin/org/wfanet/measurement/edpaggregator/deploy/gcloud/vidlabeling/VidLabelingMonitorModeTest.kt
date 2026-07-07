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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.vidlabeling

import com.google.common.truth.Truth.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class VidLabelingMonitorModeTest {
  @Test
  fun `parses the dispatch mode`() {
    assertThat(parseMonitorMode("dispatch")).isEqualTo(MonitorMode.DISPATCH)
  }

  @Test
  fun `parses the health mode`() {
    assertThat(parseMonitorMode("health")).isEqualTo(MonitorMode.HEALTH)
  }

  @Test
  fun `returns null for an unrecognized mode`() {
    assertThat(parseMonitorMode("both")).isNull()
  }

  @Test
  fun `returns null for a null mode`() {
    assertThat(parseMonitorMode(null)).isNull()
  }

  @Test
  fun `returns null for an empty mode`() {
    assertThat(parseMonitorMode("")).isNull()
  }
}
