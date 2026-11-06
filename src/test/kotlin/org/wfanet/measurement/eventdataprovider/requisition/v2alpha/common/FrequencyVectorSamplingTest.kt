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

package org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common

import com.google.common.truth.Truth.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.impression
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.multiMeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reach
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.computation.ResultMinimumThresholds

@RunWith(JUnit4::class)
class FrequencyVectorSamplingTest {
  @Test
  fun `sample selects and caps a non-wrapping interval`() {
    val sampling = sampling(reachFrequencySpec(0.2f, 0.5f, 4), null, null)
    assertThat(sampling.sample(FREQUENCIES)).isEqualTo(byteArrayOf(2, 3, 4, 4, 4))
  }

  @Test
  fun `sample emits primary indexes before wrapped indexes`() {
    val sampling = sampling(reachFrequencySpec(0.8f, 0.5f, 100), null, null)
    assertThat(sampling.sample(FREQUENCIES)).isEqualTo(byteArrayOf(8, 9, 0, 1, 2))
  }

  @Test
  fun `sample preserves full-width wrapping order`() {
    val sampling = sampling(reachFrequencySpec(0.3f, 1.0f, 100), null, null)
    assertThat(sampling.sample(FREQUENCIES)).isEqualTo(byteArrayOf(3, 4, 5, 6, 7, 8, 9, 0, 1, 2))
  }

  @Test
  fun `sample wraps an interval starting at one`() {
    val sampling = sampling(reachFrequencySpec(1.0f, 1.0f, 100), null, null)
    assertThat(sampling.sample(FREQUENCIES)).isEqualTo(FREQUENCIES)
  }

  @Test
  fun `sample preserves float interval rounding`() {
    val sampling = sampling(reachFrequencySpec(0.19f, 0.29f, 100), null, null)
    assertThat(sampling.sample(FREQUENCIES)).isEqualTo(byteArrayOf(1, 2, 3))
  }

  @Test
  fun `sample preserves unsigned frequencies through 255`() {
    val sampling = sampling(reachFrequencySpec(0.0f, 1.0f, 255), null, null)
    val frequencies = byteArrayOf(0, 1, 127, 128.toByte(), 200.toByte(), 255.toByte())
    assertThat(sampling.sample(frequencies))
      .isEqualTo(byteArrayOf(0, 1, 127, 128.toByte(), 200.toByte(), 255.toByte(), 0, 0, 0, 0))
  }

  @Test
  fun `sample pads missing source indexes with zeros`() {
    val sampling = sampling(reachFrequencySpec(0.8f, 0.5f, 100), null, null)
    assertThat(sampling.sample(byteArrayOf(1, 2))).isEqualTo(byteArrayOf(0, 0, 1, 2, 0))
  }

  @Test
  fun `sample caps reach frequencies at one`() {
    val spec = measurementSpec {
      reach = reach {}
      vidSamplingInterval = vidSamplingInterval { width = 1.0f }
    }
    assertThat(sampling(spec, null, null).sample(FREQUENCIES))
      .isEqualTo(byteArrayOf(0, 1, 1, 1, 1, 1, 1, 1, 1, 1))
  }

  @Test
  fun `sample preserves the reach threshold contribution cap`() {
    val spec = measurementSpec {
      reach = reach {}
      vidSamplingInterval = vidSamplingInterval { width = 1.0f }
    }
    val thresholds = ResultMinimumThresholds(reachMaxFrequencyPerUser = 3)
    assertThat(sampling(spec, thresholds, null).sample(FREQUENCIES))
      .isEqualTo(byteArrayOf(0, 1, 2, 3, 3, 3, 3, 3, 3, 3))
  }

  @Test
  fun `sample applies an explicit impression cap`() {
    val spec = measurementSpec {
      impression = impression { maximumFrequencyPerUser = 5 }
      vidSamplingInterval = vidSamplingInterval { width = 1.0f }
    }
    assertThat(sampling(spec, null, 2).sample(FREQUENCIES))
      .isEqualTo(byteArrayOf(0, 1, 2, 2, 2, 2, 2, 2, 2, 2))
  }

  @Test
  fun `sample caps multimeasurement cells at 127`() {
    val spec = measurementSpec {
      multi = multiMeasurementSpec {}
      vidSamplingInterval = vidSamplingInterval { width = 1.0f }
    }
    assertThat(sampling(spec, null, null).sample(byteArrayOf(127, 200.toByte(), 255.toByte())))
      .isEqualTo(byteArrayOf(127, 127, 127, 0, 0, 0, 0, 0, 0, 0))
  }

  @Test
  fun `sample produces independent payloads without mutating source frequencies`() {
    val frequencies = FREQUENCIES.clone()
    val sampling = sampling(reachFrequencySpec(0.0f, 1.0f, 3), null, null)
    val first = sampling.sample(frequencies)
    first.fill(0)
    assertThat(frequencies).isEqualTo(FREQUENCIES)
    assertThat(sampling.sample(frequencies)).isEqualTo(byteArrayOf(0, 1, 2, 3, 3, 3, 3, 3, 3, 3))
  }

  private fun sampling(spec: MeasurementSpec, thresholds: ResultMinimumThresholds?, cap: Int?) =
    FrequencyVectorSampling(POPULATION, spec, thresholds, cap)

  private fun reachFrequencySpec(start: Float, width: Float, cap: Int): MeasurementSpec =
    measurementSpec {
      reachAndFrequency = reachAndFrequency { maximumFrequency = cap }
      vidSamplingInterval = vidSamplingInterval {
        this.start = start
        this.width = width
      }
    }

  companion object {
    private val FREQUENCIES = byteArrayOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    private val POPULATION = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1
          endVidInclusive = 10
        }
      }
    }
  }
}
