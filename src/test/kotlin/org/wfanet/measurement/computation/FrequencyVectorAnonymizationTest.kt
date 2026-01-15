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
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.FrequencyVectorBuilder

class FrequencyVectorAnonymizationTest {
  private val measurementSpec = measurementSpec {
    vidSamplingInterval = MeasurementSpecKt.vidSamplingInterval { width = 1.0f }
    reach = MeasurementSpecKt.reach {}
  }

  private val populationSpec = populationSpec {
    subpopulations +=
      PopulationSpecKt.subPopulation {
        vidRanges +=
          PopulationSpecKt.vidRange {
            startVid = 1
            endVidInclusive = 100
          }
      }
  }

  @Test
  fun kAnonymizeFrequencyVectorReturnsFrequencyVector() {
    val kAnonymityParams =
      KAnonymityParams(minUsers = 1, minImpressions = 1, reachMaxFrequencyPerUser = 10)

    // Create a frequency vector with sufficient reach (many non-zero entries)
    val frequencyData = ByteArray(100) { if (it < 50) 1.toByte() else 0.toByte() }
    val builder =
      FrequencyVectorBuilder(
        measurementSpec = measurementSpec,
        populationSpec = populationSpec,
        frequencyDataBytes = frequencyData,
        strict = false,
        kAnonymityParams = kAnonymityParams,
        overrideImpressionMaxFrequencyPerUser = null,
      )

    val result =
      KAnonymizer.kAnonymizeFrequencyVector(
        measurementSpec,
        populationSpec,
        builder,
        kAnonymityParams,
        maxPopulation = null,
      )

    // Should return a frequency vector with population size data points
    assertThat(result.dataCount).isEqualTo(100)
  }
}
