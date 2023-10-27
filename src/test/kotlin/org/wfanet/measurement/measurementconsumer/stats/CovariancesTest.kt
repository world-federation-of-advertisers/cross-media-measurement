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
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.eventdataprovider.noiser.DpParams

@RunWith(JUnit4::class)
class CovariancesTest {
  @Test
  fun `computeDeterministicCovariance returns a value for reach when small reaches overlap and small sampling widths overlap `() {
    val reachMeasurementCovarianceParams =
      ReachMeasurementCovarianceParams(1, 2, 2, 2e-4, 3e-4, 4e-4)
    val covariance = Covariances.computeDeterministicCovariance(reachMeasurementCovarianceParams)
    val expected = 1665.6666666666665
    val tolerance = computeErrorTolerance(covariance, expected)
    assertThat(covariance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeDeterministicCovariance returns a value for reach when small reaches not overlap and large sampling widths not overlap `() {
    val reachMeasurementCovarianceParams = ReachMeasurementCovarianceParams(1, 2, 3, 0.5, 0.5, 1.0)
    val covariance = Covariances.computeDeterministicCovariance(reachMeasurementCovarianceParams)
    val expected = 0.0
    val tolerance = computeErrorTolerance(covariance, expected)
    assertThat(covariance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeDeterministicCovariance returns a value for reach when large reaches overlap and small sampling widths not overlap`() {
    val reachMeasurementCovarianceParams =
      ReachMeasurementCovarianceParams(3e8.toLong(), 3e8.toLong(), 4e8.toLong(), 1e-4, 1e-4, 2e-4)
    val covariance = Covariances.computeDeterministicCovariance(reachMeasurementCovarianceParams)

    val expected = -2e+8
    val tolerance = computeErrorTolerance(covariance, expected)
    assertThat(covariance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeDeterministicCovariance returns a value for reach when one reach is small`() {
    val reachMeasurementCovarianceParams =
      ReachMeasurementCovarianceParams(1, 3e6.toLong(), 3e6.toLong(), 0.5, 0.4, 0.7)
    val covariance = Covariances.computeDeterministicCovariance(reachMeasurementCovarianceParams)
    val expected = 2.220446049250313e-16
    val tolerance = computeErrorTolerance(covariance, expected)
    assertThat(covariance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeDeterministicCovariance returns a value for reach when large reaches not overlap and large sampling widths overlap`() {
    val reachMeasurementCovarianceParams =
      ReachMeasurementCovarianceParams(
        3e8.toLong(),
        3e8.toLong(),
        6e8.toLong(),
        0.7,
        0.7,
        0.7,
      )
    val covariance = Covariances.computeDeterministicCovariance(reachMeasurementCovarianceParams)
    val expected = 0.0
    assertThat(covariance).isEqualTo(expected)
  }

  @Test
  fun `computeLiquidLegionsCovariance returns a value for reach when small reaches overlap and small sampling widths not overlap`() {
    val decayRate = 1e-3
    val sketchSize = 100000L
    val sketchParams = LiquidLegionsSketchParams(decayRate, sketchSize)
    val reachMeasurementCovarianceParams =
      ReachMeasurementCovarianceParams(5, 5, 5, 0.01, 0.01, 0.02)
    val covariance =
      Covariances.computeLiquidLegionsCovariance(sketchParams, reachMeasurementCovarianceParams)

    val expected = -4.849999813860015
    val tolerance = computeErrorTolerance(covariance, expected)
    assertThat(covariance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeLiquidLegionsCovariance returns a value for reach when small reaches not overlap and large sampling widths overlap `() {
    val decayRate = 15.0
    val sketchSize = 100000L
    val sketchParams = LiquidLegionsSketchParams(decayRate, sketchSize)
    val reachMeasurementCovarianceParams = ReachMeasurementCovarianceParams(1, 1, 2, 0.5, 0.4, 0.5)
    val covariance =
      Covariances.computeLiquidLegionsCovariance(sketchParams, reachMeasurementCovarianceParams)

    val expected = 0.000562525536043962
    val tolerance = computeErrorTolerance(covariance, expected)
    assertThat(covariance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeLiquidLegionsCovariance returns a value for reach when large reaches overlap and small sampling widths overlap`() {
    val decayRate = 15.0
    val sketchSize = 100000L
    val sketchParams = LiquidLegionsSketchParams(decayRate, sketchSize)
    val reachMeasurementCovarianceParams =
      ReachMeasurementCovarianceParams(1e6.toLong(), 3e8.toLong(), 3e8.toLong(), 0.02, 0.01, 0.02)
    val covariance =
      Covariances.computeLiquidLegionsCovariance(sketchParams, reachMeasurementCovarianceParams)

    val expected = 156079036.5929788
    val tolerance = computeErrorTolerance(covariance, expected)
    assertThat(covariance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeLiquidLegionsCovariance returns a value for reach when large reaches not overlap and large sampling widths not overlap`() {
    val decayRate = 100.0
    val sketchSize = 100000L
    val sketchParams = LiquidLegionsSketchParams(decayRate, sketchSize)
    val reachMeasurementCovarianceParams =
      ReachMeasurementCovarianceParams(3e8.toLong(), 3e8.toLong(), 6e8.toLong(), 0.3, 0.4, 0.7)
    val covariance =
      Covariances.computeLiquidLegionsCovariance(sketchParams, reachMeasurementCovarianceParams)

    val expected = 428562.31521871715
    val tolerance = computeErrorTolerance(covariance, expected)
    assertThat(covariance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeLiquidLegionsCovariance returns a value for reach when one reach is small`() {
    val decayRate = 1.0
    val sketchSize = 100000L
    val sketchParams = LiquidLegionsSketchParams(decayRate, sketchSize)
    val reachMeasurementCovarianceParams =
      ReachMeasurementCovarianceParams(1, 3e6.toLong(), 3e6.toLong(), 0.5, 0.4, 0.7)
    val covariance =
      Covariances.computeLiquidLegionsCovariance(sketchParams, reachMeasurementCovarianceParams)

    val expected = 0.0002056053253447586
    val tolerance = computeErrorTolerance(covariance, expected)
    assertThat(covariance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeLiquidLegionsCovariance returns a value for reach when large reaches not overlap and large sampling widths overlap`() {
    val decayRate = 100.0
    val sketchSize = 100000L
    val sketchParams = LiquidLegionsSketchParams(decayRate, sketchSize)
    val reachMeasurementCovarianceParams =
      ReachMeasurementCovarianceParams(3e8.toLong(), 3e8.toLong(), 6e8.toLong(), 0.7, 0.7, 0.7)
    val covariance =
      Covariances.computeLiquidLegionsCovariance(sketchParams, reachMeasurementCovarianceParams)

    val expected = 214283.93565983986
    val tolerance = computeErrorTolerance(covariance, expected)
    assertThat(covariance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementCovariance returns zero for reach when one is from custom direct methodology`() {
    val weightedReachMeasurementVarianceParams =
      WeightedReachMeasurementVarianceParams(
        binaryRepresentation = 1,
        weight = 1,
        measurementVarianceParams =
          ReachMeasurementVarianceParams(
            reach = 1,
            measurementParams =
              ReachMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.0, 2e-4),
                dpParams = DpParams(1.0, 1e-5),
                noiseMechanism = NoiseMechanism.LAPLACE
              )
          ),
        methodology = CustomDirectScalarMethodology(0.0)
      )

    val otherWeightedReachMeasurementVarianceParams =
      WeightedReachMeasurementVarianceParams(
        binaryRepresentation = 2,
        weight = 1,
        measurementVarianceParams =
          ReachMeasurementVarianceParams(
            reach = 2,
            measurementParams =
              ReachMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(1e-4, 3e-4),
                dpParams = DpParams(1.0, 1e-5),
                noiseMechanism = NoiseMechanism.LAPLACE
              )
          ),
        methodology = DeterministicMethodology
      )

    val unionWeightedReachMeasurementVarianceParams =
      WeightedReachMeasurementVarianceParams(
        binaryRepresentation = 3,
        weight = -1,
        measurementVarianceParams =
          ReachMeasurementVarianceParams(
            reach = 2,
            measurementParams =
              ReachMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.0, 4e-4),
                dpParams = DpParams(1.0, 1e-5),
                noiseMechanism = NoiseMechanism.LAPLACE
              )
          ),
        methodology = DeterministicMethodology
      )

    val covariance =
      Covariances.computeMeasurementCovariance(
        weightedReachMeasurementVarianceParams,
        otherWeightedReachMeasurementVarianceParams,
        unionWeightedReachMeasurementVarianceParams
      )

    val expected = 0.0

    val tolerance = computeErrorTolerance(covariance, expected)
    assertThat(covariance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementCovariance returns a value for reach when two reach measurements are deterministic`() {
    val weightedReachMeasurementVarianceParams =
      WeightedReachMeasurementVarianceParams(
        binaryRepresentation = 1,
        weight = 1,
        measurementVarianceParams =
          ReachMeasurementVarianceParams(
            reach = 1,
            measurementParams =
              ReachMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.0, 2e-4),
                dpParams = DpParams(1.0, 1e-5),
                noiseMechanism = NoiseMechanism.LAPLACE
              )
          ),
        methodology = DeterministicMethodology
      )

    val otherWeightedReachMeasurementVarianceParams =
      WeightedReachMeasurementVarianceParams(
        binaryRepresentation = 2,
        weight = 1,
        measurementVarianceParams =
          ReachMeasurementVarianceParams(
            reach = 2,
            measurementParams =
              ReachMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(1e-4, 3e-4),
                dpParams = DpParams(1.0, 1e-5),
                noiseMechanism = NoiseMechanism.LAPLACE
              )
          ),
        methodology = DeterministicMethodology
      )

    val unionWeightedReachMeasurementVarianceParams =
      WeightedReachMeasurementVarianceParams(
        binaryRepresentation = 3,
        weight = -1,
        measurementVarianceParams =
          ReachMeasurementVarianceParams(
            reach = 2,
            measurementParams =
              ReachMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.0, 4e-4),
                dpParams = DpParams(1.0, 1e-5),
                noiseMechanism = NoiseMechanism.LAPLACE
              )
          ),
        methodology = DeterministicMethodology
      )

    val covariance =
      Covariances.computeMeasurementCovariance(
        weightedReachMeasurementVarianceParams,
        otherWeightedReachMeasurementVarianceParams,
        unionWeightedReachMeasurementVarianceParams
      )

    val expected = 1665.6666666666665

    val tolerance = computeErrorTolerance(covariance, expected)
    assertThat(covariance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementCovariance returns a value for reach when one is LiquidLegionsSketch and one is deterministic`() {
    val weightedReachMeasurementVarianceParams =
      WeightedReachMeasurementVarianceParams(
        binaryRepresentation = 1,
        weight = 1,
        measurementVarianceParams =
          ReachMeasurementVarianceParams(
            reach = 1,
            measurementParams =
              ReachMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.0, 2e-4),
                dpParams = DpParams(1.0, 1e-5),
                noiseMechanism = NoiseMechanism.LAPLACE
              )
          ),
        methodology = LiquidLegionsSketchMethodology(100.0, 100000L)
      )

    val otherWeightedReachMeasurementVarianceParams =
      WeightedReachMeasurementVarianceParams(
        binaryRepresentation = 2,
        weight = 1,
        measurementVarianceParams =
          ReachMeasurementVarianceParams(
            reach = 2,
            measurementParams =
              ReachMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(1e-4, 3e-4),
                dpParams = DpParams(1.0, 1e-5),
                noiseMechanism = NoiseMechanism.LAPLACE
              )
          ),
        methodology = DeterministicMethodology
      )

    val unionWeightedReachMeasurementVarianceParams =
      WeightedReachMeasurementVarianceParams(
        binaryRepresentation = 3,
        weight = -1,
        measurementVarianceParams =
          ReachMeasurementVarianceParams(
            reach = 2,
            measurementParams =
              ReachMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.0, 4e-4),
                dpParams = DpParams(1.0, 1e-5),
                noiseMechanism = NoiseMechanism.LAPLACE
              )
          ),
        methodology = DeterministicMethodology
      )

    val covariance =
      Covariances.computeMeasurementCovariance(
        weightedReachMeasurementVarianceParams,
        otherWeightedReachMeasurementVarianceParams,
        unionWeightedReachMeasurementVarianceParams
      )

    val expected = 1665.6666666666665

    val tolerance = computeErrorTolerance(covariance, expected)
    assertThat(covariance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementCovariance returns a value for reach when two reach measurements are LiquidLegionsSketch`() {
    val decayRate = 100.0
    val sketchSize = 100000L

    val weightedReachMeasurementVarianceParams =
      WeightedReachMeasurementVarianceParams(
        binaryRepresentation = 1,
        weight = 1,
        measurementVarianceParams =
          ReachMeasurementVarianceParams(
            reach = 3e8.toLong(),
            measurementParams =
              ReachMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.0, 0.7),
                dpParams = DpParams(1.0, 1e-5),
                noiseMechanism = NoiseMechanism.LAPLACE
              )
          ),
        methodology = LiquidLegionsSketchMethodology(decayRate, sketchSize)
      )

    val otherWeightedReachMeasurementVarianceParams =
      WeightedReachMeasurementVarianceParams(
        binaryRepresentation = 2,
        weight = 1,
        measurementVarianceParams =
          ReachMeasurementVarianceParams(
            reach = 3e8.toLong(),
            measurementParams =
              ReachMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.0, 0.7),
                dpParams = DpParams(1.0, 1e-5),
                noiseMechanism = NoiseMechanism.LAPLACE
              )
          ),
        methodology = LiquidLegionsSketchMethodology(decayRate, sketchSize)
      )

    val unionWeightedReachMeasurementVarianceParams =
      WeightedReachMeasurementVarianceParams(
        binaryRepresentation = 3,
        weight = -1,
        measurementVarianceParams =
          ReachMeasurementVarianceParams(
            reach = 6e8.toLong(),
            measurementParams =
              ReachMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.0, 0.7),
                dpParams = DpParams(1.0, 1e-5),
                noiseMechanism = NoiseMechanism.LAPLACE
              )
          ),
        methodology = LiquidLegionsSketchMethodology(decayRate, sketchSize)
      )

    val covariance =
      Covariances.computeMeasurementCovariance(
        weightedReachMeasurementVarianceParams,
        otherWeightedReachMeasurementVarianceParams,
        unionWeightedReachMeasurementVarianceParams
      )

    val expected = 214283.93565983986

    val tolerance = computeErrorTolerance(covariance, expected)
    assertThat(covariance).isWithin(tolerance).of(expected)
  }

  @Test
  fun `computeMeasurementCovariance throws IllegalArgumentException for reach when sketch params are not matched`() {
    val decayRate = 100.0
    val sketchSize = 100000L

    val weightedReachMeasurementVarianceParams =
      WeightedReachMeasurementVarianceParams(
        binaryRepresentation = 1,
        weight = 1,
        measurementVarianceParams =
          ReachMeasurementVarianceParams(
            reach = 3e8.toLong(),
            measurementParams =
              ReachMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.0, 0.7),
                dpParams = DpParams(1.0, 1e-5),
                noiseMechanism = NoiseMechanism.LAPLACE
              )
          ),
        methodology = LiquidLegionsSketchMethodology(decayRate, sketchSize)
      )

    val otherWeightedReachMeasurementVarianceParams =
      WeightedReachMeasurementVarianceParams(
        binaryRepresentation = 2,
        weight = 1,
        measurementVarianceParams =
          ReachMeasurementVarianceParams(
            reach = 3e8.toLong(),
            measurementParams =
              ReachMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.0, 0.7),
                dpParams = DpParams(1.0, 1e-5),
                noiseMechanism = NoiseMechanism.LAPLACE
              )
          ),
        methodology = LiquidLegionsSketchMethodology(decayRate + 1, sketchSize)
      )

    val unionWeightedReachMeasurementVarianceParams =
      WeightedReachMeasurementVarianceParams(
        binaryRepresentation = 3,
        weight = -1,
        measurementVarianceParams =
          ReachMeasurementVarianceParams(
            reach = 6e8.toLong(),
            measurementParams =
              ReachMeasurementParams(
                vidSamplingInterval = VidSamplingInterval(0.0, 0.7),
                dpParams = DpParams(1.0, 1e-5),
                noiseMechanism = NoiseMechanism.LAPLACE
              )
          ),
        methodology = LiquidLegionsV2Methodology(decayRate + 2, sketchSize, 100000L)
      )

    assertFailsWith<IllegalArgumentException> {
      Covariances.computeMeasurementCovariance(
        weightedReachMeasurementVarianceParams,
        otherWeightedReachMeasurementVarianceParams,
        unionWeightedReachMeasurementVarianceParams
      )
    }
  }

  companion object {
    fun computeErrorTolerance(actual: Double, expected: Double): Double {
      return if (expected == 0.0 || actual == 0.0) {
        ERROR_TOLERANCE_PERCENTAGE
      } else {
        abs(expected * ERROR_TOLERANCE_PERCENTAGE)
      }
    }

    private const val ERROR_TOLERANCE_PERCENTAGE = 5e-3
  }
}
