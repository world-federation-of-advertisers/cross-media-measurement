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

package org.wfanet.measurement.integration.testing

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.integration.testing.BasicReportMetricAssertions.MetricRanges
import org.wfanet.measurement.integration.testing.BasicReportMetricAssertions.ResultRanges
import org.wfanet.measurement.reporting.v2alpha.ResultGroup
import org.wfanet.measurement.reporting.v2alpha.ResultGroupKt
import org.wfanet.measurement.reporting.v2alpha.ResultGroupMetricSpec
import org.wfanet.measurement.reporting.v2alpha.ResultGroupMetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.resultGroupMetricSpec

@RunWith(JUnit4::class)
class BasicReportMetricAssertionsTest {
  @Test
  fun `assertMatches accepts complete weekly and total metrics`() {
    assertThat(check(metrics(false), false, ranges(false))).isGreaterThan(0)
    assertThat(check(metrics(true), true, ranges(true))).isGreaterThan(0)
  }

  @Test
  fun `assertMatches rejects a missing reporting unit`() {
    val actual = metrics(false).copy { clearReportingUnit() }
    assertFailsWith<AssertionError> { check(actual, false) }
  }

  @Test
  fun `assertMatches rejects a component with no value`() {
    val actual = metrics(true).copy { components[0] = components[0].copy { clearValue() } }
    assertFailsWith<AssertionError> { check(actual, true) }
  }

  @Test
  fun `assertMatches rejects a non-reference cell missing cumulative metrics`() {
    val actual = metrics(false).toBuilder().apply { reportingUnitBuilder.clearCumulative() }.build()
    assertFailsWith<AssertionError> { check(actual, false) }
  }

  @Test
  fun `assertMatches rejects a non-reference cell missing weekly metrics`() {
    val actual =
      metrics(false).toBuilder().apply { reportingUnitBuilder.clearNonCumulative() }.build()
    assertFailsWith<AssertionError> { check(actual, false) }
  }

  @Test
  fun `assertMatches rejects a shortened weekly K plus vector`() {
    val actual =
      metrics(false)
        .toBuilder()
        .apply { reportingUnitBuilder.nonCumulativeBuilder.clearKPlusReach().addKPlusReach(20) }
        .build()
    assertFailsWith<AssertionError> { check(actual, false) }
  }

  @Test
  fun `assertMatches rejects a shortened total K plus vector`() {
    val actual =
      metrics(true)
        .toBuilder()
        .apply { reportingUnitBuilder.cumulativeBuilder.clearKPlusReach() }
        .build()
    assertFailsWith<AssertionError> { check(actual, true) }
  }

  @Test
  fun `assertMatches rejects missing total incremental contributions without numerical ranges`() {
    val actual =
      metrics(true)
        .toBuilder()
        .apply {
          reportingUnitBuilder
            .clearStackedIncrementalReach()
            .addAllStackedIncrementalReach(listOf(5L, 5L, 5L))
        }
        .build()
    assertFailsWith<AssertionError> { check(actual, true) }
  }

  @Test
  fun `assertMatches rejects a component missing requested weekly metrics`() {
    val actual =
      metrics(false)
        .toBuilder()
        .apply { getComponentsBuilder(0).valueBuilder.clearNonCumulative() }
        .build()
    assertFailsWith<AssertionError> { check(actual, false) }
  }

  @Test
  fun `assertMatches rejects a component missing requested total metrics`() {
    val actual =
      metrics(true)
        .toBuilder()
        .apply { getComponentsBuilder(0).valueBuilder.clearCumulative() }
        .build()
    assertFailsWith<AssertionError> { check(actual, true) }
  }

  @Test
  fun `assertMatches rejects duplicate component keys`() {
    val actual =
      metrics(true).toBuilder().apply { getComponentsBuilder(3).key = COMPONENTS.first() }.build()
    assertFailsWith<AssertionError> { check(actual, true) }
  }

  @Test
  fun `assertMatches rejects a corrupt earlier device week even when the last week matches`() {
    val earlier =
      metrics(false)
        .toBuilder()
        .apply { reportingUnitBuilder.nonCumulativeBuilder.setReach(10).setImpressions(20) }
        .build()
    check(metrics(false), false, ranges(false))
    assertFailsWith<AssertionError> { check(earlier, false, ranges(false)) }
  }

  @Test
  fun `assertMatches rejects a component value outside its expected range`() {
    val actual =
      metrics(true)
        .toBuilder()
        .apply { getComponentsBuilder(0).valueBuilder.cumulativeBuilder.reach = 9 }
        .build()
    assertFailsWith<AssertionError> { check(actual, true, ranges(true)) }
  }

  @Test
  fun `assertMatches rejects corrupt total impressions with consistent reach and ratios`() {
    val actual =
      metrics(true)
        .toBuilder()
        .apply { reportingUnitBuilder.cumulativeBuilder.setImpressions(60).setAverageFrequency(3F) }
        .build()
    // Keep the derived ratio consistent so the impression comparison must detect this corruption.
    val expected = ranges(true)
    val countRanges =
      expected.copy(cumulative = checkNotNull(expected.cumulative).copy(averageFrequency = null))
    assertFailsWith<AssertionError> { check(actual, true, countRanges) }
  }

  @Test
  fun `assertMatches rejects a total average frequency inconsistent with its counts`() {
    val actual =
      metrics(true)
        .toBuilder()
        .apply { reportingUnitBuilder.cumulativeBuilder.averageFrequency = 3F }
        .build()
    assertFailsWith<AssertionError> { check(actual, true) }
  }

  @Test
  fun `assertMatches rejects a total percent reach inconsistent with its population`() {
    val actual =
      metrics(true)
        .toBuilder()
        .apply { reportingUnitBuilder.cumulativeBuilder.percentReach = 25F }
        .build()
    assertFailsWith<AssertionError> { check(actual, true) }
  }

  @Test
  fun `assertMatches accepts a zero reach with noisy impressions and zero average frequency`() {
    val actual =
      metrics(true)
        .toBuilder()
        .apply {
          reportingUnitBuilder.cumulativeBuilder
            .setReach(0)
            .setImpressions(3)
            .setAverageFrequency(0F)
            .setPercentReach(0F)
        }
        .build()
    check(
      actual,
      true,
      ResultRanges(
        cumulative =
          MetricRanges(reach = -5.0..5.0, impressions = -5.0..5.0, averageFrequency = 0.0..0.0)
      ),
    )
  }

  @Test
  fun `assertMatches verifies a noisy sparse ratio instead of assuming it is zero`() {
    val actual =
      metrics(true)
        .toBuilder()
        .apply {
          reportingUnitBuilder.cumulativeBuilder
            .setReach(2)
            .setImpressions(3)
            .setAverageFrequency(1.5F)
            .setPercentReach(2F)
        }
        .build()
    check(
      actual,
      true,
      ResultRanges(
        cumulative =
          MetricRanges(reach = -5.0..5.0, impressions = -5.0..5.0, averageFrequency = 0.0..0.0)
      ),
    )
  }

  @Test
  fun `assertMatches permits finite negative noisy counts with their derived ratio`() {
    val actual =
      metrics(true)
        .toBuilder()
        .apply {
          reportingUnitBuilder.cumulativeBuilder
            .setReach(-2)
            .setImpressions(3)
            .setAverageFrequency(-1.5F)
            .setPercentReach(-2F)
        }
        .build()
    check(actual, true)
  }

  @Test
  fun `assertMatches rejects a non-finite derived average frequency`() {
    val actual =
      metrics(true)
        .toBuilder()
        .apply { reportingUnitBuilder.cumulativeBuilder.averageFrequency = Float.NaN }
        .build()
    assertFailsWith<AssertionError> { check(actual, true) }
  }

  private fun check(
    actual: ResultGroup.MetricSet,
    total: Boolean,
    expected: ResultRanges = ResultRanges(),
  ) =
    BasicReportMetricAssertions.assertMatches(
      actual,
      requested(total),
      COMPONENTS,
      "device/media cell",
      expected,
    )

  private fun requested(total: Boolean): ResultGroupMetricSpec = resultGroupMetricSpec {
    populationSize = true
    reportingUnit =
      ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
        cumulative =
          ResultGroupMetricSpecKt.basicMetricSetSpec {
            reach = true
            percentReach = true
            if (total) {
              kPlusReach = 2
              impressions = true
              averageFrequency = true
            }
          }
        if (!total)
          nonCumulative =
            ResultGroupMetricSpecKt.basicMetricSetSpec {
              reach = true
              kPlusReach = 2
              impressions = true
              averageFrequency = true
            }
        stackedIncrementalReach = total
      }
    component =
      ResultGroupMetricSpecKt.componentMetricSetSpec {
        if (total) cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
        else nonCumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
      }
  }

  private fun metrics(total: Boolean): ResultGroup.MetricSet =
    ResultGroupKt.metricSet {
      populationSize = 100
      reportingUnit =
        ResultGroupKt.MetricSetKt.reportingUnitMetricSet {
          cumulative = basic()
          if (total) stackedIncrementalReach += List(4) { 5L } else nonCumulative = basic()
        }
      for (name in COMPONENTS) {
        components +=
          ResultGroupKt.MetricSetKt.componentMetricSetMapEntry {
            key = name
            value =
              ResultGroupKt.MetricSetKt.componentMetricSet {
                val reach = ResultGroupKt.MetricSetKt.basicMetricSet { reach = 5 }
                if (total) cumulative = reach else nonCumulative = reach
              }
          }
      }
    }

  private fun basic() =
    ResultGroupKt.MetricSetKt.basicMetricSet {
      reach = 20
      kPlusReach += listOf(20L, 10L)
      impressions = 40
      averageFrequency = 2F
      percentReach = 20F
    }

  private fun ranges(total: Boolean): ResultRanges {
    val basic =
      MetricRanges(
        reach = 19.0..21.0,
        kPlusReach = listOf(19.0..21.0, 9.0..11.0),
        impressions = 39.0..41.0,
        averageFrequency = 1.9..2.1,
        percentReach = 19.0..21.0,
      )
    return if (total)
      ResultRanges(
        cumulative = basic,
        componentCumulativeReach = COMPONENTS.associateWith { 4.0..6.0 },
        stackedIncrementalReach = List(4) { 4.0..6.0 },
      )
    else
      ResultRanges(
        cumulative = MetricRanges(reach = basic.reach, percentReach = basic.percentReach),
        nonCumulative = basic.copy(percentReach = null),
        componentNonCumulativeReach = COMPONENTS.associateWith { 4.0..6.0 },
      )
  }

  companion object {
    private val COMPONENTS = (1..4).map { "dataProviders/$it" }
  }
}
