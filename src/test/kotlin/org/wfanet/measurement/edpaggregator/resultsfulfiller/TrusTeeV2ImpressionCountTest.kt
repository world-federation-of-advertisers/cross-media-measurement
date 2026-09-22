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
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams.ImpressionCapMode

@RunWith(JUnit4::class)
class TrusTeeV2ImpressionCountTest {

  @Test
  fun `a custom cap clips the population, noises the count and reports the clip`() {
    val details =
      buildTrusTeeV2FulfillmentDetails(ImpressionCapMode.CUSTOM_CAP, configuredCap = 3, VECTOR)

    assertThat(details.impression.noiseMechanism)
      .isEqualTo(ProtocolConfig.NoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE)
    // The clip is the sensitivity the TEE needs to reason about the value it was given.
    assertThat(details.impression.deterministicCount.customMaximumFrequencyPerUser).isEqualTo(3)
    assertThat(details.impression.hasCustomDirectMethodology()).isFalse()
    // The draw is seeded from the vector, so the same population repeats the same count.
    assertThat(
        buildTrusTeeV2FulfillmentDetails(ImpressionCapMode.CUSTOM_CAP, configuredCap = 3, VECTOR)
      )
      .isEqualTo(details)
  }

  @Test
  fun `a wider cap counts more impressions`() {
    val narrow =
      buildTrusTeeV2FulfillmentDetails(ImpressionCapMode.CUSTOM_CAP, configuredCap = 1, VECTOR)
    val wide =
      buildTrusTeeV2FulfillmentDetails(ImpressionCapMode.CUSTOM_CAP, configuredCap = 100, VECTOR)

    // Clipped sums of 4 and 113 before noise, so the gap survives any single draw.
    assertThat(wide.impression.value).isGreaterThan(narrow.impression.value)
  }

  @Test
  fun `uncapped reports every impression and no clip`() {
    val details =
      buildTrusTeeV2FulfillmentDetails(ImpressionCapMode.UNCAPPED, configuredCap = 0, VECTOR)

    // Every impression, including the ones past the cell's own ceiling of 127.
    assertThat(details.impression.value).isEqualTo(213L)
    assertThat(details.impression.noiseMechanism).isEqualTo(ProtocolConfig.NoiseMechanism.NONE)
    assertThat(details.impression.hasDeterministicCount()).isTrue()
    assertThat(details.impression.deterministicCount.customMaximumFrequencyPerUser).isEqualTo(0)
  }

  @Test
  fun `dynamic clipping reports a variance and repeats itself`() {
    val details =
      buildTrusTeeV2FulfillmentDetails(ImpressionCapMode.DYNAMIC, configuredCap = 0, VECTOR)

    assertThat(details.impression.noiseMechanism)
      .isEqualTo(ProtocolConfig.NoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE)
    // The clip came from the data, so the variance travels instead of the clip.
    assertThat(details.impression.customDirectMethodology.variance.scalar).isGreaterThan(0.0)
    assertThat(details.impression.hasDeterministicCount()).isFalse()
    assertThat(
        buildTrusTeeV2FulfillmentDetails(ImpressionCapMode.DYNAMIC, configuredCap = 0, VECTOR)
      )
      .isEqualTo(details)
  }

  @Test
  fun `the MeasurementSpec cap has nothing to read`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        requireTrusTeeV2CapModeSupported(ImpressionCapMode.USE_MEASUREMENT_SPEC_CAP)
      }

    assertThat(exception).hasMessageThat().contains("MultiMeasurementSpec carries no cap")
  }

  @Test
  fun `a cap mode is required`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        requireTrusTeeV2CapModeSupported(ImpressionCapMode.UNSPECIFIED)
      }

    assertThat(exception).hasMessageThat().contains("must be set explicitly")
  }

  companion object {
    /** Four VIDs reached 5, 1, 200 and 7 times. */
    private val VECTOR =
      StripedByteFrequencyVector(size = 4).apply {
        repeat(5) { increment(0) }
        increment(1)
        repeat(200) { increment(2) }
        repeat(7) { increment(3) }
      }
  }
}
