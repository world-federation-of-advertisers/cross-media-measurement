/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.wfanet.measurement.eventdataprovider.differentialprivacy

import com.google.privacy.differentialprivacy.GaussianNoise

/**
 * Noises the histogram bars [DynamicClipping] searches.
 *
 * The source owns the calibration rather than receiving a standard deviation, because a mechanism
 * hardened against the floating-point attacks on differential privacy has to control the lattice
 * its output lands on, which it cannot do if the caller rescales the draw afterwards.
 *
 * Draws are addressed by ([pass], [barIndex]) rather than taken from a stream, because
 * [DynamicClipping] noises the bars more than once and how many passes it makes depends on the
 * data. A source that reproduces a draw from its address therefore reproduces the whole algorithm,
 * while one that reproduces a stream in order would not.
 */
fun interface DynamicClippingNoiseSource {
  /**
   * Returns [bar] with noise, for the bar at [barIndex] in noising [pass], charged [rho] against
   * [l2Sensitivity].
   *
   * Draws must be independent across distinct ([pass], [barIndex]) pairs: [DynamicClipping]
   * combines the passes by inverse-variance weights, which assumes they are independent estimates.
   */
  fun noise(pass: Int, barIndex: Int, bar: Double, l2Sensitivity: Double, rho: Double): Double
}

/**
 * The default [DynamicClippingNoiseSource], drawing fresh randomness per call.
 *
 * Delegates to the same [GaussianNoise] the Direct result noisers use, which draws from
 * [java.security.SecureRandom] and snaps its output to a power-of-two lattice chosen independently
 * of the value being noised.
 */
class StochasticStandardNormalNoiseSource : DynamicClippingNoiseSource {
  private val gaussianNoise = GaussianNoise()

  override fun noise(
    pass: Int,
    barIndex: Int,
    bar: Double,
    l2Sensitivity: Double,
    rho: Double,
  ): Double = gaussianNoise.addNoiseDefinedByRho(bar, l2Sensitivity, rho)
}
