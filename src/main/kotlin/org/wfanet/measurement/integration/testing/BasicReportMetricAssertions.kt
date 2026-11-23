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

package org.wfanet.measurement.integration.testing

import com.google.common.truth.Truth.assertWithMessage
import org.wfanet.measurement.reporting.v2alpha.ResultGroup
import org.wfanet.measurement.reporting.v2alpha.ResultGroupMetricSpec

/** Verifies requested BasicReport metric structure and optional source-derived value ranges. */
object BasicReportMetricAssertions {
  data class MetricRanges(
    val reach: ClosedFloatingPointRange<Double>? = null,
    val kPlusReach: List<ClosedFloatingPointRange<Double>>? = null,
    val impressions: ClosedFloatingPointRange<Double>? = null,
    val percentReach: ClosedFloatingPointRange<Double>? = null,
    val averageFrequency: ClosedFloatingPointRange<Double>? = null,
  )

  data class ResultRanges(
    val cumulative: MetricRanges? = null,
    val nonCumulative: MetricRanges? = null,
    val componentCumulativeReach: Map<String, ClosedFloatingPointRange<Double>> = emptyMap(),
    val componentNonCumulativeReach: Map<String, ClosedFloatingPointRange<Double>> = emptyMap(),
    val stackedIncrementalReach: List<ClosedFloatingPointRange<Double>>? = null,
  )

  private data class Comparison(
    val label: String,
    val actual: Double,
    val expected: ClosedFloatingPointRange<Double>,
  ) {
    val inRange: Boolean
      get() = actual.isFinite() && actual in expected
  }

  /** Returns the number of numeric comparisons after verifying structure and values. */
  fun assertMatches(
    metrics: ResultGroup.MetricSet,
    requested: ResultGroupMetricSpec,
    components: Collection<String>,
    label: String,
    expected: ResultRanges = ResultRanges(),
  ): Int {
    val comparisons = mutableListOf<Comparison>()
    assertWithMessage("$label reporting unit").that(metrics.hasReportingUnit()).isTrue()
    val unit = metrics.reportingUnit
    if (requested.reportingUnit.hasCumulative()) {
      assertWithMessage("$label cumulative").that(unit.hasCumulative()).isTrue()
      comparisons +=
        compareBasic(
          unit.cumulative,
          requested.reportingUnit.cumulative,
          metrics.populationSize,
          "$label cumulative",
          expected.cumulative,
        )
    }
    if (requested.reportingUnit.hasNonCumulative()) {
      assertWithMessage("$label non-cumulative").that(unit.hasNonCumulative()).isTrue()
      comparisons +=
        compareBasic(
          unit.nonCumulative,
          requested.reportingUnit.nonCumulative,
          metrics.populationSize,
          "$label non-cumulative",
          expected.nonCumulative,
        )
    }
    if (requested.reportingUnit.stackedIncrementalReach) {
      assertWithMessage("$label incremental contributions")
        .that(unit.stackedIncrementalReachList)
        .hasSize(components.size)
      expected.stackedIncrementalReach?.forEachIndexed { index, range ->
        comparisons +=
          Comparison(
            "$label incremental $index",
            unit.stackedIncrementalReachList[index].toDouble(),
            range,
          )
      }
    }
    assertWithMessage("$label components")
      .that(metrics.componentsList.map { it.key })
      .containsExactlyElementsIn(components)
    for (component in metrics.componentsList) {
      assertWithMessage("$label ${component.key} value").that(component.hasValue()).isTrue()
      if (requested.component.hasCumulative()) {
        assertWithMessage("$label ${component.key} cumulative")
          .that(component.value.hasCumulative())
          .isTrue()
        comparisons +=
          compareBasic(
            component.value.cumulative,
            requested.component.cumulative,
            metrics.populationSize,
            "$label ${component.key} cumulative",
            expected.componentCumulativeReach[component.key]?.let { MetricRanges(reach = it) },
          )
      }
      if (requested.component.hasNonCumulative()) {
        assertWithMessage("$label ${component.key} non-cumulative")
          .that(component.value.hasNonCumulative())
          .isTrue()
        comparisons +=
          compareBasic(
            component.value.nonCumulative,
            requested.component.nonCumulative,
            metrics.populationSize,
            "$label ${component.key} non-cumulative",
            expected.componentNonCumulativeReach[component.key]?.let { MetricRanges(reach = it) },
          )
      }
    }
    val outside = comparisons.filter { !it.inRange }
    assertWithMessage(
        outside.joinToString("\n", prefix = "$label metrics outside expected ranges:\n")
      )
      .that(outside)
      .isEmpty()
    return comparisons.size
  }

  private fun compareBasic(
    actual: ResultGroup.MetricSet.BasicMetricSet,
    requested: ResultGroupMetricSpec.BasicMetricSetSpec,
    population: Long,
    label: String,
    expected: MetricRanges?,
  ): List<Comparison> = buildList {
    if (requested.kPlusReach > 0) {
      assertWithMessage("$label K+ reach").that(actual.kPlusReachList).hasSize(requested.kPlusReach)
    }
    // Derived values remain checkable when noisy zero counts make a truth-based ratio unstable.
    if (requested.averageFrequency && requested.reach && requested.impressions) {
      val ratio =
        if (actual.reach != 0L && actual.impressions != 0L)
          actual.impressions.toDouble() / actual.reach
        else 0.0
      add(
        Comparison(
          "$label derived average frequency",
          actual.averageFrequency.toDouble(),
          floatRange(ratio),
        )
      )
    }
    if (requested.percentReach && requested.reach) {
      val percent = if (population > 0) actual.reach.toDouble() / population * 100.0 else 0.0
      add(
        Comparison(
          "$label derived percent reach",
          actual.percentReach.toDouble(),
          floatRange(percent),
        )
      )
    }
    if (expected == null) return@buildList
    expected.reach?.let { add(Comparison("$label reach", actual.reach.toDouble(), it)) }
    expected.kPlusReach?.forEachIndexed { index, range ->
      add(Comparison("$label ${index + 1}+ reach", actual.kPlusReachList[index].toDouble(), range))
    }
    expected.impressions?.let {
      add(Comparison("$label impressions", actual.impressions.toDouble(), it))
    }
    expected.percentReach?.let {
      add(Comparison("$label percent reach", actual.percentReach.toDouble(), it))
    }
    // Near the noise floor, verify the reported ratio above rather than compare it with zero.
    if (expected.reach?.start?.let { it > 0.0 } == true) {
      expected.averageFrequency?.let {
        add(Comparison("$label average frequency", actual.averageFrequency.toDouble(), it))
      }
    }
  }

  private fun floatRange(value: Double): ClosedFloatingPointRange<Double> {
    val rounded = value.toFloat().toDouble()
    val margin = maxOf(0.000001, Math.ulp(value.toFloat()).toDouble() * 2)
    return (rounded - margin)..(rounded + margin)
  }
}
