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

package org.wfanet.measurement.edpaggregator.resultsfulfiller.compute.protocols.direct

import java.nio.ByteBuffer
import org.wfanet.measurement.computation.DeterministicGaussianNoiseSampler
import org.wfanet.measurement.computation.DeterministicTruncatedLaplaceParams
import org.wfanet.measurement.computation.DeterministicTruncatedLaplaceResultNoiser
import org.wfanet.measurement.eventdataprovider.differentialprivacy.DynamicClipping
import org.wfanet.measurement.eventdataprovider.differentialprivacy.StandardNormalNoiseSource
import org.wfanet.measurement.eventdataprovider.noiser.DpParams
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.AcdpParamsConverter

/**
 * A Direct measurement is produced by a single EDP, so every draw is seeded from one contribution.
 */
private const val DIRECT_CONTRIBUTION_COUNT = 1

/**
 * Separates these draws from the result draws taken against the same frequency vector, so the clip
 * search and the noise on a released quantity never share a draw.
 */
private const val CLIP_SEARCH_DOMAIN = 1

/**
 * One user moves any single bar of the cumulative histogram by at most one, which is the
 * sensitivity the charge conversion is calibrated to. [DynamicClipping] scales it up for the number
 * of bars it releases.
 */
private const val BAR_SENSITIVITY = 1.0

/**
 * Returns a [DynamicClipping] whose threshold and noised histogram are a deterministic function of
 * [frequencyData], so a re-run of the same measurement yields the same clip and the same count.
 *
 * The charge comes from the privacy parameters compiled into this image rather than from the
 * measurement spec, so a measurement consumer cannot widen it by asking for a larger epsilon. The
 * distribution stays Gaussian, which leaves [DynamicClipping]'s calibration, stopping rule and
 * remaining-charge weighting exactly as analyzed; only the source of the draws changes.
 */
fun buildDeterministicDynamicClipping(frequencyData: IntArray): DynamicClipping {
  val acdpCharge =
    AcdpParamsConverter.getDirectAcdpCharge(
      DpParams(
        DeterministicTruncatedLaplaceParams.EPSILON,
        DeterministicTruncatedLaplaceParams.DELTA,
      ),
      BAR_SENSITIVITY,
    )
  return DynamicClipping(
    queryRho = acdpCharge.rho,
    measurementType = DynamicClipping.MeasurementType.IMPRESSION,
    noiseSource =
      FrequencyVectorSeededNoiseSource(
        DeterministicTruncatedLaplaceResultNoiser.fingerprint(
          frequencyData,
          DIRECT_CONTRIBUTION_COUNT,
        )
      ),
  )
}

/**
 * A [StandardNormalNoiseSource] whose draws are a pure function of [fingerprint] and the draw's
 * address, so a varying number of passes still reproduces each draw.
 */
private class FrequencyVectorSeededNoiseSource(private val fingerprint: ByteArray) :
  StandardNormalNoiseSource {
  private val sampler = DeterministicGaussianNoiseSampler()

  override fun sample(pass: Int, barIndex: Int): Double =
    sampler.sample(fingerprint, label(CLIP_SEARCH_DOMAIN), label(pass), label(barIndex))

  private fun label(value: Int): ByteArray =
    ByteBuffer.allocate(Int.SIZE_BYTES).putInt(value).array()
}
