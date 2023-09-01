/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.eventdataprovider.noiser

/** Internal Differential Privacy(DP) parameters. */
data class DpParams(val epsilon: Double, val delta: Double)

/** Noise mechanism for generating publisher noise for direct measurements. */
enum class DirectNoiseMechanism {
  /** NONE mechanism is testing only. */
  NONE,
  CONTINUOUS_LAPLACE,
  CONTINUOUS_GAUSSIAN,
}

/** A base Noiser interface for direct measurements. */
interface Noiser {

  /** Returns a random value sampled from the distribution. */
  fun sample(): Double

  /** The variance of the noiser. */
  val variance: Double
}
