/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.measurementconsumer.stats

import com.google.common.truth.Truth.assertThat
import kotlin.math.abs
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class CovariancesTest {
  @Test
  fun `computeDeterministicCovariance returns a value for reach when small reaches overlap and small sampling widths overlap `() {
    val reachCovarianceParams = ReachCovarianceParams(1, 2, 2, 2e-4, 3e-4, 4e-4)
    val covariance = Covariances.computeDeterministicCovariance(reachCovarianceParams)
    val expect = 1665.6666666666665
    val percentageError = percentageError(covariance, expect)
    assertThat(percentageError).isLessThan(ERROR_TOLERANCE)
  }

  @Test
  fun `computeDeterministicCovariance returns a value for reach when small reaches not overlap and large sampling widths not overlap `() {
    val reachCovarianceParams = ReachCovarianceParams(1, 2, 3, 0.5, 0.5, 1.0)
    val covariance = Covariances.computeDeterministicCovariance(reachCovarianceParams)
    val expect = 0.0
    val percentageError = percentageError(covariance, expect)
    assertThat(percentageError).isLessThan(ERROR_TOLERANCE)
  }

  @Test
  fun `computeDeterministicCovariance returns a value for reach when large reaches overlap and small sampling widths not overlap`() {
    val reachCovarianceParams =
      ReachCovarianceParams(3e8.toInt(), 3e8.toInt(), 4e8.toInt(), 1e-4, 1e-4, 2e-4)
    val covariance = Covariances.computeDeterministicCovariance(reachCovarianceParams)

    val expect = -2e+8
    val percentageError = percentageError(covariance, expect)
    assertThat(percentageError).isLessThan(ERROR_TOLERANCE)
  }

  @Test
  fun `computeDeterministicCovariance returns a value for reach when one reach is small`() {
    val reachCovarianceParams = ReachCovarianceParams(1, 3e6.toInt(), 3e6.toInt(), 0.5, 0.4, 0.7)
    val covariance = Covariances.computeDeterministicCovariance(reachCovarianceParams)
    val expect = 2.220446049250313e-16
    val percentageError = percentageError(covariance, expect)
    assertThat(percentageError).isLessThan(ERROR_TOLERANCE)
  }

  @Test
  fun `computeDeterministicCovariance returns a value for reach when large reaches not overlap and large sampling widths overlap`() {
    val reachCovarianceParams =
      ReachCovarianceParams(
        3e8.toInt(),
        3e8.toInt(),
        6e8.toInt(),
        0.7,
        0.7,
        0.7,
      )
    val covariance = Covariances.computeDeterministicCovariance(reachCovarianceParams)
    val expect = 0.0
    assertThat(covariance).isEqualTo(expect)
  }

  @Test
  fun `computeLiquidLegionsCovariance returns a value for reach when small reaches overlap and small sampling widths not overlap`() {
    val decayRate = 1e-3
    val sketchSize = 1e5
    val sketchParams = LiquidLegionsSketchParams(decayRate, sketchSize)
    val reachCovarianceParams = ReachCovarianceParams(5, 5, 5, 0.01, 0.01, 0.02)
    val covariance = Covariances.computeLiquidLegionsCovariance(sketchParams, reachCovarianceParams)

    val expect = -4.849999813860015
    val percentageError = percentageError(covariance, expect)
    assertThat(percentageError).isLessThan(ERROR_TOLERANCE)
  }

  @Test
  fun `computeLiquidLegionsCovariance returns a value for reach when small reaches not overlap and large sampling widths overlap `() {
    val decayRate = 15.0
    val sketchSize = 1e5
    val sketchParams = LiquidLegionsSketchParams(decayRate, sketchSize)
    val reachCovarianceParams = ReachCovarianceParams(1, 1, 2, 0.5, 0.4, 0.5)
    val covariance = Covariances.computeLiquidLegionsCovariance(sketchParams, reachCovarianceParams)

    val expect = 0.000562525536043962
    val percentageError = percentageError(covariance, expect)
    assertThat(percentageError).isLessThan(ERROR_TOLERANCE)
  }

  @Test
  fun `computeLiquidLegionsCovariance returns a value for reach when large reaches overlap and small sampling widths overlap`() {
    val decayRate = 15.0
    val sketchSize = 1e5
    val sketchParams = LiquidLegionsSketchParams(decayRate, sketchSize)
    val reachCovarianceParams =
      ReachCovarianceParams(1e6.toInt(), 3e8.toInt(), 3e8.toInt(), 0.02, 0.01, 0.02)
    val covariance = Covariances.computeLiquidLegionsCovariance(sketchParams, reachCovarianceParams)

    val expect = 156079036.5929788
    val percentageError = percentageError(covariance, expect)
    assertThat(percentageError).isLessThan(ERROR_TOLERANCE)
  }

  @Test
  fun `computeLiquidLegionsCovariance returns a value for reach when large reaches not overlap and large sampling widths not overlap`() {
    val decayRate = 100.0
    val sketchSize = 1e5
    val sketchParams = LiquidLegionsSketchParams(decayRate, sketchSize)
    val reachCovarianceParams =
      ReachCovarianceParams(3e8.toInt(), 3e8.toInt(), 6e8.toInt(), 0.3, 0.4, 0.7)
    val covariance = Covariances.computeLiquidLegionsCovariance(sketchParams, reachCovarianceParams)

    val expect = 428562.31521871715
    val percentageError = percentageError(covariance, expect)
    assertThat(percentageError).isLessThan(ERROR_TOLERANCE)
  }

  @Test
  fun `computeLiquidLegionsCovariance returns a value for reach when one reach is small`() {
    val decayRate = 1.0
    val sketchSize = 1e5
    val sketchParams = LiquidLegionsSketchParams(decayRate, sketchSize)
    val reachCovarianceParams = ReachCovarianceParams(1, 3e6.toInt(), 3e6.toInt(), 0.5, 0.4, 0.7)
    val covariance = Covariances.computeLiquidLegionsCovariance(sketchParams, reachCovarianceParams)

    val expect = 0.0002056053253447586
    val percentageError = percentageError(covariance, expect)
    assertThat(percentageError).isLessThan(ERROR_TOLERANCE)
  }

  @Test
  fun `computeLiquidLegionsCovariance returns a value for reach when large reaches not overlap and large sampling widths overlap`() {
    val decayRate = 100.0
    val sketchSize = 1e5
    val sketchParams = LiquidLegionsSketchParams(decayRate, sketchSize)
    val reachCovarianceParams =
      ReachCovarianceParams(3e8.toInt(), 3e8.toInt(), 6e8.toInt(), 0.7, 0.7, 0.7)
    val covariance = Covariances.computeLiquidLegionsCovariance(sketchParams, reachCovarianceParams)

    val expect = 214283.93565983986
    val percentageError = percentageError(covariance, expect)
    assertThat(percentageError).isLessThan(ERROR_TOLERANCE)
  }

  companion object {
    fun percentageError(estimate: Double, truth: Double): Double {
      return if (truth == 0.0) {
        estimate
      } else if (estimate == 0.0) {
        truth
      } else {
        abs(1.0 - (estimate / truth))
      }
    }

    const val ERROR_TOLERANCE = 5e-3
  }
}
