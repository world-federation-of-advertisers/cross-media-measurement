// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.computation

import com.google.common.truth.Truth.assertThat
import kotlin.test.Test
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.FrequencyVectorBuilder

class FrequencyVectorAnonymizationTest {
  @Test
  fun `kAnonymizeFrequencyVector returns original vector when reach meets threshold`() {
    val measurementSpec = measurementSpec {
      vidSamplingInterval = MeasurementSpecKt.vidSamplingInterval { width = 1.0f }
      reach = MeasurementSpecKt.reach {}
    }
    val populationSpec = populationSpec { 
      subpopulations += populationSpec { 
        vidRanges += vidRange { start = 0; endExclusive = 1000 }
      }.subpopulations
    }
    val kAnonymityParams = KAnonymityParams(minUsers = 2, minImpressions = 10, reachMaxFrequencyPerUser = 10)

    // Create a frequency vector with sufficient reach
    val frequencyData = ByteArray(100) { if (it < 50) 1.toByte() else 0.toByte() }
    val builder = FrequencyVectorBuilder(
      measurementSpec = measurementSpec,
      populationSpec = populationSpec,
      frequencyDataBytes = frequencyData,
      strict = false,
      kAnonymityParams = kAnonymityParams,
      overrideImpressionMaxFrequencyPerUser = null,
    )

    val result = kAnonymizeFrequencyVector(
      measurementSpec,
      populationSpec,
      builder,
      kAnonymityParams,
      maxPopulation = null,
    )

    // Should return the original vector since reach is sufficient
    assertThat(result.dataCount).isEqualTo(100)
    assertThat(result.dataList.take(50).all { it == 1 }).isTrue()
  }

  @Test
  fun `kAnonymizeFrequencyVector returns empty vector when reach below threshold`() {
    val measurementSpec = measurementSpec {
      vidSamplingInterval = MeasurementSpecKt.vidSamplingInterval { width = 1.0f }
      reach = MeasurementSpecKt.reach {}
    }
    val populationSpec = populationSpec { 
      subpopulations += populationSpec { 
        vidRanges += vidRange { start = 0; endExclusive = 1000 }
      }.subpopulations
    }
    // High threshold that won't be met
    val kAnonymityParams = KAnonymityParams(minUsers = 1000, minImpressions = 10000, reachMaxFrequencyPerUser = 10)

    // Create a frequency vector with insufficient reach
    val frequencyData = ByteArray(100) { if (it < 5) 1.toByte() else 0.toByte() }
    val builder = FrequencyVectorBuilder(
      measurementSpec = measurementSpec,
      populationSpec = populationSpec,
      frequencyDataBytes = frequencyData,
      strict = false,
      kAnonymityParams = kAnonymityParams,
      overrideImpressionMaxFrequencyPerUser = null,
    )

    val result = kAnonymizeFrequencyVector(
      measurementSpec,
      populationSpec,
      builder,
      kAnonymityParams,
      maxPopulation = null,
    )

    // Should return an empty vector since reach is below threshold
    assertThat(result.dataCount).isEqualTo(0)
  }
}
