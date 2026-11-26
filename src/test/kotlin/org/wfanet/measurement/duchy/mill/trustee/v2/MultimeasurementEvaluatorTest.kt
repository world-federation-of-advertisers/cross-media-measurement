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

package org.wfanet.measurement.duchy.mill.trustee.v2

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.Measurement.Result.MultimeasurementResult
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.MultimeasurementResultKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.MultimeasurementResultKt.QueryResultKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.multimeasurementResult
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.populationAttributeValue
import org.wfanet.measurement.computation.ResultMinimumThresholds
import org.wfanet.measurement.internal.duchy.NoiseMechanism
import org.wfanet.measurement.internal.duchy.Query
import org.wfanet.measurement.internal.duchy.query
import org.wfanet.measurement.internal.duchy.vectorSet

@RunWith(JUnit4::class)
class MultimeasurementEvaluatorTest {
  @Test
  fun `evaluate returns exact cells for every group without noise or thresholds`() {
    val query = testQuery(groupingPaths = listOf(AGE_GROUP_PATH))

    val result = evaluate(query, NO_NOISE)

    // Even positions sum to 1, 4, 3; odd ones to 1, 2, 0.
    assertThat(result)
      .isEqualTo(
        multimeasurementResult {
          queryResults +=
            MultimeasurementResultKt.queryResult {
              queryId = QUERY_ID
              groupingPaths += AGE_GROUP_PATH
              maxFrequency = 3
              cells += exactCell(YOUNG, bins = listOf(1L, 0L, 2L), impressions = 8L)
              cells += exactCell(OLD, bins = listOf(1L, 1L, 0L), impressions = 3L)
              reachNoiseMechanism = ProtocolConfig.NoiseMechanism.NONE
              frequencyNoiseMechanism = ProtocolConfig.NoiseMechanism.NONE
              impressionNoiseMechanism = ProtocolConfig.NoiseMechanism.NONE
            }
        }
      )
  }

  @Test
  fun `evaluate rebuilds a cell without the DataProvider whose slice failed`() {
    // DATA_PROVIDER_A reaches 4 VIDs and DATA_PROVIDER_B 2, so only B's slice fails.
    val params =
      CellPrivacyParams(
        NoiseMechanism.NONE,
        ResultMinimumThresholds(minUsers = 3, minImpressions = 1),
      )

    val cell = evaluateSingleCell(params)

    // Without vector 2 the counts are 1, 1, 3, 0, 3, 0. Bin 3 holds 2 and bin 2 then 2, both below
    // min_users, so they fold into bin 1.
    assertThat(cell.reach).isEqualTo(4L)
    assertThat(cell.impressions).isEqualTo(8L)
    assertThat(cell.frequencyBinsList).containsExactly(4L, 0L, 0L).inOrder()
    assertThat(cell.suppressed).isFalse()
  }

  @Test
  fun `evaluate suppresses a cell whose slices all fail`() {
    val params =
      CellPrivacyParams(
        NoiseMechanism.NONE,
        ResultMinimumThresholds(minUsers = 5, minImpressions = 1),
      )

    val cell = evaluateSingleCell(params)

    assertThat(cell.suppressed).isTrue()
    assertThat(cell.reach).isEqualTo(0L)
  }

  @Test
  fun `evaluate is deterministic under truncated Laplace noise`() {
    val params =
      CellPrivacyParams(
        NoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE,
        ResultMinimumThresholds(minUsers = 1, minImpressions = 1),
      )
    val query = testQuery(groupingPaths = listOf(AGE_GROUP_PATH))

    assertThat(evaluate(query, params)).isEqualTo(evaluate(query, params))
    assertThat(evaluate(query, params).queryResultsList.single().reachNoiseMechanism)
      .isEqualTo(ProtocolConfig.NoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE)
  }

  @Test
  fun `evaluate throws when a vector is missing`() {
    val evaluator =
      MultimeasurementEvaluator(
        listOf(testQuery(groupingPaths = listOf())),
        DATA_PROVIDERS,
        ParitySubpopulationIndex,
        FULL_SAMPLING_WIDTH,
        NO_NOISE,
        MEASUREMENT,
      )

    assertFailsWith<IllegalArgumentException> { evaluator.evaluate(VECTORS.take(2)) }
  }

  /** Even positions fall in subpopulation 0 ([YOUNG]), odd ones in subpopulation 1 ([OLD]). */
  private object ParitySubpopulationIndex : SubpopulationIndex {
    override val subpopulationCount: Int = 2

    override fun subpopulationOf(position: Int): Int = position % 2

    override fun attributeValue(subpopulation: Int, path: String): String? =
      if (path == AGE_GROUP_PATH) listOf(YOUNG, OLD)[subpopulation] else null

    override fun populationSize(subpopulation: Int): Long =
      listOf(YOUNG_POPULATION_SIZE, OLD_POPULATION_SIZE)[subpopulation]
  }

  companion object {
    private const val MEASUREMENT = "measurementConsumers/mc/measurements/m"
    private const val QUERY_ID = "query"
    private const val FULL_SAMPLING_WIDTH = 1.0
    private const val AGE_GROUP_PATH = "person.age_group"
    private const val YOUNG = "YEARS_18_TO_34"
    private const val OLD = "YEARS_35_TO_54"
    private const val YOUNG_POPULATION_SIZE = 100L
    private const val OLD_POPULATION_SIZE = 200L
    private const val DATA_PROVIDER_A = "dataProviders/a"
    private const val DATA_PROVIDER_B = "dataProviders/b"
    private val NO_NOISE = CellPrivacyParams(NoiseMechanism.NONE, resultMinimumThresholds = null)

    /** Base vectors 0 and 1 come from [DATA_PROVIDER_A], vector 2 from [DATA_PROVIDER_B]. */
    private val DATA_PROVIDERS = listOf(DATA_PROVIDER_A, DATA_PROVIDER_A, DATA_PROVIDER_B)
    private val VECTORS: List<ByteArray> =
      listOf(
        byteArrayOf(1, 0, 2, 0, 3, 0),
        byteArrayOf(0, 1, 1, 0, 0, 0),
        byteArrayOf(0, 0, 1, 2, 0, 0),
      )

    private fun testQuery(groupingPaths: List<String>): Query = query {
      id = QUERY_ID
      summands = vectorSet { baseVectors += listOf(0, 1, 2) }
      this.groupingPaths += groupingPaths
      maxFrequency = 3
    }

    private fun evaluate(query: Query, params: CellPrivacyParams): MultimeasurementResult =
      MultimeasurementEvaluator(
          listOf(query),
          DATA_PROVIDERS,
          ParitySubpopulationIndex,
          FULL_SAMPLING_WIDTH,
          params,
          MEASUREMENT,
        )
        .evaluate(VECTORS)

    private fun evaluateSingleCell(
      params: CellPrivacyParams
    ): MultimeasurementResult.QueryResult.Cell {
      val result = evaluate(testQuery(groupingPaths = listOf()), params)
      return result.queryResultsList.single().cellsList.single()
    }

    private fun exactCell(ageGroup: String, bins: List<Long>, impressions: Long) =
      QueryResultKt.cell {
        attributeValues += populationAttributeValue { enumValue = ageGroup }
        reach = bins.sum()
        reachVariance = 0.0
        this.impressions = impressions
        impressionVariance = 0.0
        frequencyBins += bins
        frequencyBinVariance += List(bins.size) { 0.0 }
      }
  }
}
