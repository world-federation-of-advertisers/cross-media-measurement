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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.common.truth.Truth.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.frequencycount.FrequencyVector
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.computation.ResultMinimumThresholds
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.FrequencyVectorBuilder

@RunWith(JUnit4::class)
class ResultMinimumThresholderTest {
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

  private fun buildEmptyVector(): FrequencyVector {
    return FrequencyVectorBuilder(
        measurementSpec = measurementSpec,
        populationSpec = populationSpec,
        strict = false,
        overrideImpressionMaxFrequencyPerUser = null,
      )
      .build()
  }

  @Test
  fun `applyThresholds passes through when protocol thresholds are sufficient`() {
    val edpaThresholds =
      ResultMinimumThresholds(minUsers = 100, minImpressions = 10, reachMaxFrequencyPerUser = 5)

    val builder =
      FrequencyVectorBuilder(
        measurementSpec = measurementSpec,
        populationSpec = populationSpec,
        strict = false,
        resultMinimumThresholds = edpaThresholds,
        overrideImpressionMaxFrequencyPerUser = null,
      )
    listOf(4, 5, 6).forEach { builder.increment(it) }

    val result =
      ResultMinimumThresholder.applyThresholds(
        measurementSpec,
        populationSpec,
        builder,
        edpaThresholds,
        maxPopulation = null,
        protocolMinUsers = 100,
        protocolMinImpressions = 10,
      )

    assertThat(result.dataCount).isGreaterThan(0)
    assertThat(result.dataList.count { it != 0 }).isEqualTo(3)
  }

  @Test
  fun `applyThresholds passes through when protocol thresholds exceed EDPA thresholds`() {
    val edpaThresholds =
      ResultMinimumThresholds(minUsers = 50, minImpressions = 5, reachMaxFrequencyPerUser = 5)

    val builder =
      FrequencyVectorBuilder(
        measurementSpec = measurementSpec,
        populationSpec = populationSpec,
        strict = false,
        resultMinimumThresholds = edpaThresholds,
        overrideImpressionMaxFrequencyPerUser = null,
      )
    listOf(1, 2, 3).forEach { builder.increment(it) }

    val result =
      ResultMinimumThresholder.applyThresholds(
        measurementSpec,
        populationSpec,
        builder,
        edpaThresholds,
        maxPopulation = null,
        protocolMinUsers = 200,
        protocolMinImpressions = 100,
      )

    assertThat(result.dataList.count { it != 0 }).isEqualTo(3)
  }

  @Test
  fun `applyThresholds returns original when no protocol thresholds and total meets EDPA thresholds`() {
    val edpaThresholds =
      ResultMinimumThresholds(minUsers = 2, minImpressions = 2, reachMaxFrequencyPerUser = 10)

    val frequencyData = ByteArray(100) { if (it < 50) 1.toByte() else 0.toByte() }
    val builder =
      FrequencyVectorBuilder(
        measurementSpec = measurementSpec,
        populationSpec = populationSpec,
        frequencyDataBytes = frequencyData,
        strict = false,
        resultMinimumThresholds = edpaThresholds,
        overrideImpressionMaxFrequencyPerUser = null,
      )

    val result =
      ResultMinimumThresholder.applyThresholds(
        measurementSpec,
        populationSpec,
        builder,
        edpaThresholds,
        maxPopulation = null,
        protocolMinUsers = 0,
        protocolMinImpressions = 0,
      )

    assertThat(result.dataCount).isEqualTo(100)
    assertThat(result.dataList.take(50).all { it == 1 }).isTrue()
  }

  @Test
  fun `applyThresholds zeros vector when no protocol thresholds and total reach below EDPA threshold`() {
    val edpaThresholds =
      ResultMinimumThresholds(minUsers = 1000, minImpressions = 1000, reachMaxFrequencyPerUser = 10)

    val builder =
      FrequencyVectorBuilder(
        measurementSpec = measurementSpec,
        populationSpec = populationSpec,
        strict = false,
        resultMinimumThresholds = edpaThresholds,
        overrideImpressionMaxFrequencyPerUser = null,
      )
    listOf(4, 5, 6).forEach { builder.increment(it) }

    val result =
      ResultMinimumThresholder.applyThresholds(
        measurementSpec,
        populationSpec,
        builder,
        edpaThresholds,
        maxPopulation = null,
        protocolMinUsers = 0,
        protocolMinImpressions = 0,
      )

    assertThat(result).isEqualTo(buildEmptyVector())
  }

  @Test
  fun `applyThresholds zeros vector when protocol thresholds insufficient and total reach below EDPA threshold`() {
    val edpaThresholds =
      ResultMinimumThresholds(minUsers = 1000, minImpressions = 1000, reachMaxFrequencyPerUser = 10)

    val builder =
      FrequencyVectorBuilder(
        measurementSpec = measurementSpec,
        populationSpec = populationSpec,
        strict = false,
        resultMinimumThresholds = edpaThresholds,
        overrideImpressionMaxFrequencyPerUser = null,
      )
    listOf(4, 5, 6).forEach { builder.increment(it) }

    val result =
      ResultMinimumThresholder.applyThresholds(
        measurementSpec,
        populationSpec,
        builder,
        edpaThresholds,
        maxPopulation = null,
        protocolMinUsers = 50,
        protocolMinImpressions = 50,
      )

    assertThat(result).isEqualTo(buildEmptyVector())
  }

  @Test
  fun `applyThresholds zeros vector when total impressions below EDPA threshold but reach above`() {
    val edpaThresholds =
      ResultMinimumThresholds(minUsers = 1, minImpressions = 10000, reachMaxFrequencyPerUser = 10)

    val builder =
      FrequencyVectorBuilder(
        measurementSpec = measurementSpec,
        populationSpec = populationSpec,
        strict = false,
        resultMinimumThresholds = edpaThresholds,
        overrideImpressionMaxFrequencyPerUser = null,
      )
    listOf(4, 5, 6).forEach { builder.increment(it) }

    val result =
      ResultMinimumThresholder.applyThresholds(
        measurementSpec,
        populationSpec,
        builder,
        edpaThresholds,
        maxPopulation = null,
        protocolMinUsers = 0,
        protocolMinImpressions = 0,
      )

    assertThat(result).isEqualTo(buildEmptyVector())
  }

  @Test
  fun `applyThresholds zeros vector when total reach below EDPA threshold but impressions above`() {
    val edpaThresholds =
      ResultMinimumThresholds(minUsers = 1000, minImpressions = 1, reachMaxFrequencyPerUser = 10)

    val builder =
      FrequencyVectorBuilder(
        measurementSpec = measurementSpec,
        populationSpec = populationSpec,
        strict = false,
        resultMinimumThresholds = edpaThresholds,
        overrideImpressionMaxFrequencyPerUser = null,
      )
    listOf(4, 5, 6).forEach { builder.increment(it) }

    val result =
      ResultMinimumThresholder.applyThresholds(
        measurementSpec,
        populationSpec,
        builder,
        edpaThresholds,
        maxPopulation = null,
        protocolMinUsers = 0,
        protocolMinImpressions = 0,
      )

    assertThat(result).isEqualTo(buildEmptyVector())
  }

  @Test
  fun `applyThresholds returns empty vector for empty input data`() {
    val edpaThresholds =
      ResultMinimumThresholds(minUsers = 10, minImpressions = 10, reachMaxFrequencyPerUser = 5)

    val builder =
      FrequencyVectorBuilder(
        measurementSpec = measurementSpec,
        populationSpec = populationSpec,
        strict = false,
        overrideImpressionMaxFrequencyPerUser = null,
      )

    val result =
      ResultMinimumThresholder.applyThresholds(
        measurementSpec,
        populationSpec,
        builder,
        edpaThresholds,
        maxPopulation = null,
        protocolMinUsers = 0,
        protocolMinImpressions = 0,
      )

    assertThat(result).isEqualTo(buildEmptyVector())
  }

  @Test
  fun `applyThresholds zeros vector when protocol min_users sufficient but min_impressions not`() {
    val edpaThresholds =
      ResultMinimumThresholds(minUsers = 100, minImpressions = 100, reachMaxFrequencyPerUser = 5)

    val builder =
      FrequencyVectorBuilder(
        measurementSpec = measurementSpec,
        populationSpec = populationSpec,
        strict = false,
        resultMinimumThresholds = edpaThresholds,
        overrideImpressionMaxFrequencyPerUser = null,
      )
    listOf(4, 5, 6).forEach { builder.increment(it) }

    val result =
      ResultMinimumThresholder.applyThresholds(
        measurementSpec,
        populationSpec,
        builder,
        edpaThresholds,
        maxPopulation = null,
        protocolMinUsers = 200,
        protocolMinImpressions = 50,
      )

    assertThat(result).isEqualTo(buildEmptyVector())
  }

  @Test
  fun `applyThresholds zeros vector when protocol min_impressions sufficient but min_users not`() {
    val edpaThresholds =
      ResultMinimumThresholds(minUsers = 100, minImpressions = 100, reachMaxFrequencyPerUser = 5)

    val builder =
      FrequencyVectorBuilder(
        measurementSpec = measurementSpec,
        populationSpec = populationSpec,
        strict = false,
        resultMinimumThresholds = edpaThresholds,
        overrideImpressionMaxFrequencyPerUser = null,
      )
    listOf(4, 5, 6).forEach { builder.increment(it) }

    val result =
      ResultMinimumThresholder.applyThresholds(
        measurementSpec,
        populationSpec,
        builder,
        edpaThresholds,
        maxPopulation = null,
        protocolMinUsers = 50,
        protocolMinImpressions = 200,
      )

    assertThat(result).isEqualTo(buildEmptyVector())
  }

  @Test
  fun `applyThresholds zeros vector when maxPopulation caps reach below threshold`() {
    val edpaThresholds =
      ResultMinimumThresholds(minUsers = 100, minImpressions = 10, reachMaxFrequencyPerUser = 10)

    val frequencyData = ByteArray(100) { if (it < 50) 1.toByte() else 0.toByte() }
    val builder =
      FrequencyVectorBuilder(
        measurementSpec = measurementSpec,
        populationSpec = populationSpec,
        frequencyDataBytes = frequencyData,
        strict = false,
        resultMinimumThresholds = edpaThresholds,
        overrideImpressionMaxFrequencyPerUser = null,
      )

    val result =
      ResultMinimumThresholder.applyThresholds(
        measurementSpec,
        populationSpec,
        builder,
        edpaThresholds,
        maxPopulation = 2,
        protocolMinUsers = 0,
        protocolMinImpressions = 0,
      )

    assertThat(result).isEqualTo(buildEmptyVector())
  }
}
