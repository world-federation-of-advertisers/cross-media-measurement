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

package org.wfanet.measurement.computation

import java.nio.ByteBuffer
import org.wfanet.measurement.eventdataprovider.differentialprivacy.DynamicClippingNoiseSource

/**
 * A [DynamicClippingNoiseSource] whose draws are a pure function of [fingerprint] and the draw's
 * address, so a mechanism taking a varying number of passes still reproduces each draw.
 *
 * [fingerprint] identifies the data being noised, and is what makes the draws non-averageable: seed
 * it from the frequency vector the consumer never sees, via
 * [DeterministicTruncatedLaplaceResultNoiser.fingerprint].
 *
 * @param domain separates these draws from any other drawn against the same fingerprint.
 */
class DeterministicDynamicClippingNoiseSource(
  private val fingerprint: ByteArray,
  private val domain: Int,
) : DynamicClippingNoiseSource {
  private val sampler = DeterministicGaussianNoiseSampler()

  override fun sample(pass: Int, barIndex: Int): Double =
    sampler.sample(fingerprint, label(domain), label(pass), label(barIndex))

  private fun label(value: Int): ByteArray =
    ByteBuffer.allocate(Int.SIZE_BYTES).putInt(value).array()
}
