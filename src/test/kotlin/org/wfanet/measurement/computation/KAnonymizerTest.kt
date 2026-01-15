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
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.FrequencyVectorBuilder

@RunWith(JUnit4::class)
class KAnonymizerTest {
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
  fun `kAnonymizeFrequencyVector returns original vector when reach meets threshold`() {
    val kAnonymityParams =
      KAnonymityParams(minUsers = 2, minImpressions = 10, reachMaxFrequencyPerUser = 10)

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

    // Should return the original vector since reach is sufficient
    assertThat(result.dataCount).isEqualTo(100)
    assertThat(result.dataList.take(50).all { it == 1 }).isTrue()
  }

  @Test
  fun `kAnonymizeFrequencyVector returns empty vector when reach below threshold`() {
    // Very high threshold that wont be met with only 3 users
    val kAnonymityParams =
      KAnonymityParams(minUsers = 1000, minImpressions = 1000, reachMaxFrequencyPerUser = 10)

    // Create a frequency vector builder and add only 3 increments (below threshold)
    val builder =
      FrequencyVectorBuilder(
        measurementSpec = measurementSpec,
        populationSpec = populationSpec,
        strict = false,
        kAnonymityParams = kAnonymityParams,
        overrideImpressionMaxFrequencyPerUser = null,
      )
    // Add only 3 users - below the minUsers threshold of 1000
    listOf(4, 5, 6).forEach { builder.increment(it) }

    val result =
      KAnonymizer.kAnonymizeFrequencyVector(
        measurementSpec,
        populationSpec,
        builder,
        kAnonymityParams,
        maxPopulation = null,
      )

    // Build an expected empty vector (all zeros) for comparison
    val expectedEmptyVector =
      FrequencyVectorBuilder(
          measurementSpec = measurementSpec,
          populationSpec = populationSpec,
          strict = false,
          overrideImpressionMaxFrequencyPerUser = null,
        )
        .build()

    // Should return an empty (all-zero) vector since reach (3) is below threshold (1000)
    assertThat(result).isEqualTo(expectedEmptyVector)
  }
}
