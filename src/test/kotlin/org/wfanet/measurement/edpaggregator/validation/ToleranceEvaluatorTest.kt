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
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.config.edpaggregator.toleranceConfig

@RunWith(JUnit4::class)
class ToleranceEvaluatorTest {

  private val defaultConfig = toleranceConfig {
    warningThresholdFraction = 0.02
    failureThresholdFraction = 0.10
    minimumImpressionCount = 1000
  }

  @Test
  fun `evaluate returns PASS when counts match exactly`() {
    val result =
      ToleranceEvaluator.evaluate(
        reportedCount = 10_000_000,
        publisherCount = 10_000_000,
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
        publisherCount = 10_000_000,
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
        publisherCount = 10_000_000,
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
        publisherCount = 10_000_000,
        config = defaultConfig,
      )
    assertThat(result.verdict).isEqualTo(ToleranceEvaluator.Verdict.FAIL)
    assertThat(result.deviationFraction).isWithin(0.001).of(0.15)
    assertThat(result.effectiveTolerance).isEqualTo(0.10)
  }

  @Test
  fun `evaluate returns WARNING instead of FAIL when failure threshold is unset`() {
    val config = toleranceConfig {
      warningThresholdFraction = 0.02
      minimumImpressionCount = 1000
    }
    val result =
      ToleranceEvaluator.evaluate(
        reportedCount = 8_500_000,
        publisherCount = 10_000_000,
        config = config,
      )
    assertThat(result.verdict).isEqualTo(ToleranceEvaluator.Verdict.WARNING)
    assertThat(result.deviationFraction).isWithin(0.001).of(0.15)
    assertThat(result.effectiveTolerance).isEqualTo(0.02)
  }

  @Test
  fun `evaluate returns SKIPPED when publisher count below minimum`() {
    val result =
      ToleranceEvaluator.evaluate(reportedCount = 500, publisherCount = 800, config = defaultConfig)
    assertThat(result.verdict).isEqualTo(ToleranceEvaluator.Verdict.SKIPPED)
    assertThat(result.deviationFraction).isNull()
    assertThat(result.effectiveTolerance).isNull()
  }

  @Test
  fun `evaluate applies VID sampling width to publisher count`() {
    val result =
      ToleranceEvaluator.evaluate(
        reportedCount = 1_000_000,
        publisherCount = 10_000_000,
        config = defaultConfig,
        vidSamplingWidth = 0.1,
      )
    assertThat(result.verdict).isEqualTo(ToleranceEvaluator.Verdict.PASS)
    assertThat(result.deviationFraction).isEqualTo(0.0)
  }

  @Test
  fun `evaluate returns SKIPPED when publisher count is zero`() {
    val result =
      ToleranceEvaluator.evaluate(
        reportedCount = 0,
        publisherCount = 0,
        config =
          toleranceConfig {
            warningThresholdFraction = 0.02
            failureThresholdFraction = 0.10
            minimumImpressionCount = 0
          },
      )
    assertThat(result.verdict).isEqualTo(ToleranceEvaluator.Verdict.SKIPPED)
    assertThat(result.deviationFraction).isNull()
  }

  @Test
  fun `evaluate returns SKIPPED when publisher count is zero even if reported is non-zero`() {
    val result =
      ToleranceEvaluator.evaluate(
        reportedCount = 100_000,
        publisherCount = 0,
        config =
          toleranceConfig {
            warningThresholdFraction = 0.02
            failureThresholdFraction = 0.10
            minimumImpressionCount = 0
          },
      )
    assertThat(result.verdict).isEqualTo(ToleranceEvaluator.Verdict.SKIPPED)
    assertThat(result.deviationFraction).isNull()
  }

  @Test
  fun `evaluate returns SKIPPED when sampling width rounds expected count to zero`() {
    val result =
      ToleranceEvaluator.evaluate(
        reportedCount = 0,
        publisherCount = 1_000,
        config = defaultConfig,
        vidSamplingWidth = 0.0,
      )
    assertThat(result.verdict).isEqualTo(ToleranceEvaluator.Verdict.SKIPPED)
    assertThat(result.deviationFraction).isNull()
  }
}
