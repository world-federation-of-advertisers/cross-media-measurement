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
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams.TrusTeeV2Config.ImpressionCountMode

@RunWith(JUnit4::class)
class TrusTeeV2ImpressionCountTest {

  @Test
  fun `the unnoised mode reports every impression and no clip`() {
    val details =
      TrusTeeV2ImpressionCount.buildFulfillmentDetails(
        ImpressionCountMode.UNNOISED,
        maxFrequencyPerUser = null,
        VECTOR.getByteArray(),
        VECTOR.getTotalUncappedImpressions(),
      )

    // Every impression, including the ones past the cell's own ceiling of 127.
    assertThat(details.impression.value).isEqualTo(213L)
    assertThat(details.impression.noiseMechanism).isEqualTo(ProtocolConfig.NoiseMechanism.NONE)
    assertThat(details.impression.hasDeterministicCount()).isTrue()
    assertThat(details.impression.deterministicCount.customMaximumFrequencyPerUser).isEqualTo(0)
  }

  @Test
  fun `a configured clip is applied, noised and reported`() {
    val details =
      TrusTeeV2ImpressionCount.buildFulfillmentDetails(
        ImpressionCountMode.NOISED,
        maxFrequencyPerUser = 3,
        VECTOR.getByteArray(),
        VECTOR.getTotalUncappedImpressions(),
      )

    assertThat(details.impression.noiseMechanism)
      .isEqualTo(ProtocolConfig.NoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE)
    // Clipped sum of 3 + 1 + 3 + 3 = 10, plus a seeded draw of -5 at sensitivity 3. Pins the whole
    // chain: the clip, the seed encoding and the sampler. An unnoised count would read 10.
    assertThat(details.impression.value).isEqualTo(5L)
    // The clip is the sensitivity the TEE needs to reason about the value it was given.
    assertThat(details.impression.deterministicCount.customMaximumFrequencyPerUser).isEqualTo(3)
    assertThat(details.impression.hasCustomDirectMethodology()).isFalse()
    // The draw is seeded from the vector, so the same population repeats the same count.
    assertThat(
        TrusTeeV2ImpressionCount.buildFulfillmentDetails(
          ImpressionCountMode.NOISED,
          maxFrequencyPerUser = 3,
          VECTOR.getByteArray(),
          VECTOR.getTotalUncappedImpressions(),
        )
      )
      .isEqualTo(details)
  }

  @Test
  fun `an unset clip is chosen from the data and reported as a variance`() {
    val details =
      TrusTeeV2ImpressionCount.buildFulfillmentDetails(
        ImpressionCountMode.NOISED,
        maxFrequencyPerUser = null,
        VECTOR.getByteArray(),
        VECTOR.getTotalUncappedImpressions(),
      )

    assertThat(details.impression.noiseMechanism)
      .isEqualTo(ProtocolConfig.NoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE)
    // The clip came from the data, so the variance travels instead of the clip.
    assertThat(details.impression.customDirectMethodology.variance.scalar).isGreaterThan(0.0)
    assertThat(details.impression.hasDeterministicCount()).isFalse()
    assertThat(
        TrusTeeV2ImpressionCount.buildFulfillmentDetails(
          ImpressionCountMode.NOISED,
          maxFrequencyPerUser = null,
          VECTOR.getByteArray(),
          VECTOR.getTotalUncappedImpressions(),
        )
      )
      .isEqualTo(details)
  }

  @Test
  fun `a clip is read only under the noised mode`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        TrusTeeV2ImpressionCount.validateConfig(
          ImpressionCountMode.UNNOISED,
          maxFrequencyPerUser = 3,
        )
      }

    assertThat(exception).hasMessageThat().contains("read only under NOISED")
  }

  @Test
  fun `a zero clip is refused rather than read as unset`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        TrusTeeV2ImpressionCount.validateConfig(ImpressionCountMode.NOISED, maxFrequencyPerUser = 0)
      }

    assertThat(exception).hasMessageThat().contains("must be in 1..127")
  }

  @Test
  fun `a clip beyond what a cell holds is refused`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        TrusTeeV2ImpressionCount.validateConfig(
          ImpressionCountMode.NOISED,
          maxFrequencyPerUser = 128,
        )
      }

    assertThat(exception).hasMessageThat().contains("saturates at the largest signed byte")
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
