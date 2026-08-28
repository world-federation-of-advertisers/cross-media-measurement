/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams.ImpressionCapMode

@RunWith(JUnit4::class)
class DefaultFulfillerSelectorTest {

  @Test
  fun `DYNAMIC keeps the vector uncapped whatever the configured cap`() {
    for (configured in listOf(null, -1, 1, 5, 127)) {
      assertThat(frequencyVectorCap(ImpressionCapMode.DYNAMIC, configured)).isEqualTo(UNCAPPED)
    }
  }

  @Test
  fun `modes that ignore the configured cap reject one`() {
    for (mode in
      listOf(
        ImpressionCapMode.UNCAPPED,
        ImpressionCapMode.USE_MEASUREMENT_SPEC_CAP,
        ImpressionCapMode.DYNAMIC,
      )) {
      requireCapMatchesMode(mode, 0)
      for (configured in listOf(-1, 1, 127)) {
        assertFailsWith<IllegalArgumentException>("$mode with cap $configured") {
          requireCapMatchesMode(mode, configured)
        }
      }
    }
  }

  @Test
  fun `CUSTOM_CAP requires a positive cap`() {
    requireCapMatchesMode(ImpressionCapMode.CUSTOM_CAP, 1)
    for (configured in listOf(-1, 0)) {
      assertFailsWith<IllegalArgumentException> {
        requireCapMatchesMode(ImpressionCapMode.CUSTOM_CAP, configured)
      }
    }
  }

  @Test
  fun `UNSPECIFIED accepts any configured cap`() {
    for (configured in listOf(-1, 0, 1, 127)) {
      requireCapMatchesMode(ImpressionCapMode.UNSPECIFIED, configured)
    }
  }

  @Test
  fun `UNSPECIFIED and CUSTOM_CAP keep the configured cap`() {
    for (mode in listOf(ImpressionCapMode.UNSPECIFIED, ImpressionCapMode.CUSTOM_CAP)) {
      for (configured in listOf(null, -1, 1, 5, 127)) {
        assertThat(frequencyVectorCap(mode, configured)).isEqualTo(configured)
      }
    }
  }

  @Test
  fun `UNCAPPED and USE_MEASUREMENT_SPEC_CAP defer to the MeasurementSpec`() {
    for (mode in listOf(ImpressionCapMode.UNCAPPED, ImpressionCapMode.USE_MEASUREMENT_SPEC_CAP)) {
      for (configured in listOf(null, -1, 1, 5, 127)) {
        assertThat(frequencyVectorCap(mode, configured)).isNull()
      }
    }
  }
}
