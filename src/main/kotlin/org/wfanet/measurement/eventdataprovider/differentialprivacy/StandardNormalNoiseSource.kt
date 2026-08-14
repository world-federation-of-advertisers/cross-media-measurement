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

import org.apache.commons.math3.distribution.NormalDistribution

/**
 * Supplies the standard-normal draws [DynamicClipping] adds to its histogram bars.
 *
 * Draws are addressed by ([pass], [barIndex]) rather than taken from a stream, because
 * [DynamicClipping] noises the bars more than once and how many passes it makes depends on the
 * data. A source that reproduces a draw from its address therefore reproduces the whole algorithm,
 * while one that reproduces a stream in order would not.
 */
fun interface StandardNormalNoiseSource {
  /**
   * Returns the standard-normal draw for the bar at [barIndex] in noising [pass].
   *
   * Draws must be independent across distinct ([pass], [barIndex]) pairs: [DynamicClipping]
   * combines the passes by inverse-variance weights, which assumes they are independent estimates.
   */
  fun sample(pass: Int, barIndex: Int): Double
}

/** A [StandardNormalNoiseSource] drawing fresh randomness per call. */
class StochasticStandardNormalNoiseSource : StandardNormalNoiseSource {
  private val distribution = NormalDistribution(0.0, 1.0)

  override fun sample(pass: Int, barIndex: Int): Double = distribution.sample()
}
