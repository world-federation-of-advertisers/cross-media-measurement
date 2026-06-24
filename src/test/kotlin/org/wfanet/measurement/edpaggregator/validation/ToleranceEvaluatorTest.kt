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

package org.wfanet.measurement.edpaggregator.validation

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.config.edpaggregator.toleranceBand
import org.wfanet.measurement.config.edpaggregator.toleranceConfig

@RunWith(JUnit4::class)
class ToleranceEvaluatorTest {

  // Absolute-deviation floors are set to 1 so the fractional threshold is the deciding factor for
  // these large-count cases; dedicated tests below exercise the floor.
  private val defaultConfig = toleranceConfig {
    warning = toleranceBand {
      thresholdFraction = 0.02
      minimumAbsoluteDeviation = 1
    }
    failure = toleranceBand {
      thresholdFraction = 0.10
      minimumAbsoluteDeviation = 1
    }
  }

  @Test
  fun `evaluate returns PASS when counts match exactly`() {
    val result =
      ToleranceEvaluator.evaluate(
        reportedCount = 10_000_000,
        expectedCount = 10_000_000,
        config = defaultConfig,
      )
    assertThat(result.verdict).isEqualTo(ToleranceEvaluator.Verdict.PASS)
    assertThat(result.deviationFraction).isEqualTo(0.0)
  }

  @Test
  fun `evaluate returns PASS when deviation is within warning threshold`() {
    val result =
      ToleranceEvaluator.evaluate(
        reportedCount = 10_100_000,
        expectedCount = 10_000_000,
        config = defaultConfig,
      )
    assertThat(result.verdict).isEqualTo(ToleranceEvaluator.Verdict.PASS)
    assertThat(result.deviationFraction).isWithin(0.001).of(0.01)
  }

  @Test
  fun `evaluate returns WARNING when deviation exceeds warning but not failure`() {
    val result =
      ToleranceEvaluator.evaluate(
        reportedCount = 10_500_000,
        expectedCount = 10_000_000,
        config = defaultConfig,
      )
    assertThat(result.verdict).isEqualTo(ToleranceEvaluator.Verdict.WARNING)
    assertThat(result.deviationFraction).isWithin(0.001).of(0.05)
    assertThat(result.effectiveTolerance).isEqualTo(0.02)
  }

  @Test
  fun `evaluate returns FAIL when deviation exceeds failure threshold`() {
    val result =
      ToleranceEvaluator.evaluate(
        reportedCount = 8_500_000,
        expectedCount = 10_000_000,
        config = defaultConfig,
      )
    assertThat(result.verdict).isEqualTo(ToleranceEvaluator.Verdict.FAIL)
    assertThat(result.deviationFraction).isWithin(0.001).of(0.15)
    assertThat(result.effectiveTolerance).isEqualTo(0.10)
  }

  @Test
  fun `evaluate returns WARNING instead of FAIL when failure band is unset`() {
    val config = toleranceConfig {
      warning = toleranceBand {
        thresholdFraction = 0.02
        minimumAbsoluteDeviation = 1
      }
    }
    val result =
      ToleranceEvaluator.evaluate(
        reportedCount = 8_500_000,
        expectedCount = 10_000_000,
        config = config,
      )
    assertThat(result.verdict).isEqualTo(ToleranceEvaluator.Verdict.WARNING)
    assertThat(result.deviationFraction).isWithin(0.001).of(0.15)
    assertThat(result.effectiveTolerance).isEqualTo(0.02)
  }

  @Test
  fun `evaluate returns PASS when deviation exactly equals warning fraction`() {
    // 0.5 is exact in binary and 500 / 1000 == 0.5 exactly, so this pins the strict-greater-than
    // boundary without floating-point fragility.
    val config = toleranceConfig {
      warning = toleranceBand {
        thresholdFraction = 0.5
        minimumAbsoluteDeviation = 1
      }
      failure = toleranceBand {
        thresholdFraction = 0.75
        minimumAbsoluteDeviation = 1
      }
    }
    val result =
      ToleranceEvaluator.evaluate(reportedCount = 1_500, expectedCount = 1_000, config = config)
    assertThat(result.deviationFraction).isEqualTo(0.5)
    assertThat(result.verdict).isEqualTo(ToleranceEvaluator.Verdict.PASS)
  }

  @Test
  fun `evaluate returns WARNING when deviation exactly equals failure fraction`() {
    val config = toleranceConfig {
      warning = toleranceBand {
        thresholdFraction = 0.25
        minimumAbsoluteDeviation = 1
      }
      failure = toleranceBand {
        thresholdFraction = 0.5
        minimumAbsoluteDeviation = 1
      }
    }
    val result =
      ToleranceEvaluator.evaluate(reportedCount = 1_500, expectedCount = 1_000, config = config)
    assertThat(result.deviationFraction).isEqualTo(0.5)
    assertThat(result.verdict).isEqualTo(ToleranceEvaluator.Verdict.WARNING)
  }

  @Test
  fun `evaluate downgrades FAIL to WARNING when absolute deviation is within the failure floor`() {
    // Steven's case: expected 1001 vs reported 900 is 10.1% but only 101 impressions absolute.
    val config = toleranceConfig {
      warning = toleranceBand {
        thresholdFraction = 0.02
        minimumAbsoluteDeviation = 50
      }
      failure = toleranceBand {
        thresholdFraction = 0.10
        minimumAbsoluteDeviation = 500
      }
    }
    val result =
      ToleranceEvaluator.evaluate(reportedCount = 900, expectedCount = 1_001, config = config)
    // Failure fraction is exceeded but its 500-impression floor is not (abs deviation 101), so the
    // verdict falls through to WARNING, whose 50-impression floor is exceeded.
    assertThat(result.verdict).isEqualTo(ToleranceEvaluator.Verdict.WARNING)
  }

  @Test
  fun `evaluate returns PASS when fractional deviation exceeds warning but absolute is within floor`() {
    val config = toleranceConfig {
      warning = toleranceBand {
        thresholdFraction = 0.02
        minimumAbsoluteDeviation = 500
      }
      failure = toleranceBand {
        thresholdFraction = 0.10
        minimumAbsoluteDeviation = 1_000
      }
    }
    val result =
      ToleranceEvaluator.evaluate(reportedCount = 900, expectedCount = 1_001, config = config)
    // Both bands' absolute floors exceed the 101-impression deviation, so no band trips.
    assertThat(result.verdict).isEqualTo(ToleranceEvaluator.Verdict.PASS)
  }

  @Test
  fun `evaluate returns SKIPPED when expected count is zero`() {
    val result =
      ToleranceEvaluator.evaluate(reportedCount = 100_000, expectedCount = 0, config = defaultConfig)
    assertThat(result.verdict).isEqualTo(ToleranceEvaluator.Verdict.SKIPPED)
    assertThat(result.deviationFraction).isNull()
    assertThat(result.effectiveTolerance).isNull()
  }

  @Test
  fun `evaluate throws when failure fraction is below warning fraction`() {
    val config = toleranceConfig {
      warning = toleranceBand {
        thresholdFraction = 0.10
        minimumAbsoluteDeviation = 1
      }
      failure = toleranceBand {
        thresholdFraction = 0.05
        minimumAbsoluteDeviation = 1
      }
    }
    assertFailsWith<IllegalArgumentException> {
      ToleranceEvaluator.evaluate(reportedCount = 900, expectedCount = 1_000, config = config)
    }
  }
}
