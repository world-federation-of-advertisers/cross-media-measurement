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
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams.ImpressionCapMode
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams.NoiseParams.NoiseType
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParamsKt

@RunWith(JUnit4::class)
class TrusTeeV2ImpressionCountTest {

  @Test
  fun `custom cap sums the clipped population and reports the clip`() {
    val details =
      buildTrusTeeV2FulfillmentDetails(params(ImpressionCapMode.CUSTOM_CAP, cap = 3), VECTOR)

    // 3 + 1 + 3 + 3, clipping the VIDs reached 5 and 200 times.
    assertThat(details.impression.value).isEqualTo(10L)
    assertThat(details.impression.noiseMechanism).isEqualTo(ProtocolConfig.NoiseMechanism.NONE)
    assertThat(details.impression.deterministicCount.customMaximumFrequencyPerUser).isEqualTo(3)
    assertThat(details.impression.hasCustomDirectMethodology()).isFalse()
  }

  @Test
  fun `uncapped reports every impression and no clip`() {
    val details = buildTrusTeeV2FulfillmentDetails(params(ImpressionCapMode.UNCAPPED), VECTOR)

    // Every impression, including the ones past the cell's own ceiling of 127.
    assertThat(details.impression.value).isEqualTo(213L)
    assertThat(details.impression.noiseMechanism).isEqualTo(ProtocolConfig.NoiseMechanism.NONE)
    assertThat(details.impression.hasDeterministicCount()).isTrue()
    assertThat(details.impression.deterministicCount.customMaximumFrequencyPerUser).isEqualTo(0)
  }

  @Test
  fun `dynamic clipping reports a variance and repeats itself`() {
    val params =
      params(ImpressionCapMode.DYNAMIC, noiseType = NoiseType.DETERMINISTIC_TRUNCATED_LAPLACE)

    val details = buildTrusTeeV2FulfillmentDetails(params, VECTOR)

    assertThat(details.impression.noiseMechanism)
      .isEqualTo(ProtocolConfig.NoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE)
    assertThat(details.impression.customDirectMethodology.variance.scalar).isGreaterThan(0.0)
    assertThat(details.impression.hasDeterministicCount()).isFalse()
    // The draw is seeded from the vector, so the same population repeats the same count.
    assertThat(buildTrusTeeV2FulfillmentDetails(params, VECTOR)).isEqualTo(details)
  }

  @Test
  fun `a capped count refuses to be noised by this EDP`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        validateImpressionCountsParams(
          params(
            ImpressionCapMode.CUSTOM_CAP,
            cap = 3,
            noiseType = NoiseType.DETERMINISTIC_TRUNCATED_LAPLACE,
          )
        )
      }

    assertThat(exception).hasMessageThat().contains("requires noise_type NONE")
  }

  @Test
  fun `dynamic clipping refuses to go unnoised`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        validateImpressionCountsParams(params(ImpressionCapMode.DYNAMIC))
      }

    assertThat(exception).hasMessageThat().contains("DYNAMIC requires noise_type")
  }

  @Test
  fun `the MeasurementSpec cap has nothing to read`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        validateImpressionCountsParams(params(ImpressionCapMode.USE_MEASUREMENT_SPEC_CAP))
      }

    assertThat(exception).hasMessageThat().contains("MultiMeasurementSpec carries no cap")
  }

  @Test
  fun `a cap mode is required`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        validateImpressionCountsParams(params(ImpressionCapMode.UNSPECIFIED))
      }

    assertThat(exception).hasMessageThat().contains("cap_mode must be set explicitly")
  }

  @Test
  fun `a cap beyond what a cell holds is refused`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        validateImpressionCountsParams(params(ImpressionCapMode.CUSTOM_CAP, cap = 128))
      }

    assertThat(exception).hasMessageThat().contains("must be in 1..127")
  }

  @Test
  fun `a cap outside CUSTOM_CAP is refused`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        validateImpressionCountsParams(params(ImpressionCapMode.UNCAPPED, cap = 3))
      }

    assertThat(exception).hasMessageThat().contains("read only under CUSTOM_CAP")
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

    private fun params(
      capMode: ImpressionCapMode,
      cap: Int = 0,
      noiseType: NoiseType = NoiseType.NONE,
    ): ResultsFulfillerParams.ImpressionCountsParams =
      ResultsFulfillerParamsKt.impressionCountsParams {
        this.capMode = capMode
        maxFrequencyPerUser = cap
        noiseParams = ResultsFulfillerParamsKt.noiseParams { this.noiseType = noiseType }
      }
  }
}
