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

package org.wfanet.measurement.computation

import kotlin.math.exp
import org.wfanet.measurement.eventdataprovider.noiser.DpParams
import org.wfanet.measurement.eventdataprovider.noiser.GaussianNoiser

/** Computes the variance of noise mechanisms used by Direct measurements. */
object DirectNoiseVariances {
  /** Returns continuous-Laplace variance for [epsilon] and L1 [sensitivity]. */
  fun continuousLaplace(epsilon: Double, sensitivity: Double): Double {
    require(epsilon > 0.0) { "epsilon must be positive, got $epsilon" }
    require(sensitivity > 0.0) { "sensitivity must be positive, got $sensitivity" }
    val scale = sensitivity / epsilon
    return 2.0 * scale * scale
  }

  /** Returns continuous-Gaussian variance for the privacy parameters and L2 [sensitivity]. */
  fun continuousGaussian(epsilon: Double, delta: Double, sensitivity: Double): Double {
    require(sensitivity > 0.0) { "sensitivity must be positive, got $sensitivity" }
    val sigma = GaussianNoiser.getSigma(DpParams(epsilon, delta)) * sensitivity
    return sigma * sigma
  }

  /** Returns deterministic truncated-Laplace variance for L1 [sensitivity]. */
  fun deterministicTruncatedLaplace(sensitivity: Double): Double {
    require(sensitivity > 0.0) { "sensitivity must be positive, got $sensitivity" }
    val scale = sensitivity / DeterministicTruncatedLaplaceParams.EPSILON
    val bound = DeterministicTruncatedLaplaceParams.truncationBound(sensitivity)
    val tailMass = exp(-bound / scale)
    val normalizer = 1.0 - tailMass
    val untruncatedVariance = 2.0 * scale * scale
    val truncatedTail = tailMass * (bound * bound + 2.0 * bound * scale + 2.0 * scale * scale)
    return (untruncatedVariance - truncatedTail) / normalizer
  }
}
