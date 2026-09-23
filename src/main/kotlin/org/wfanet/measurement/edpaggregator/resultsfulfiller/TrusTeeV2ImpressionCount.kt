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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import org.wfanet.measurement.api.v2alpha.CustomDirectMethodologyKt
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.impression
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.customDirectMethodology
import org.wfanet.measurement.api.v2alpha.deterministicCount
import org.wfanet.measurement.computation.DeterministicTruncatedLaplaceResultNoiser
import org.wfanet.measurement.computation.HistogramComputations
import org.wfanet.measurement.computation.ImpressionComputations
import org.wfanet.measurement.edpaggregator.resultsfulfiller.compute.protocols.direct.computeDirectDynamicallyClippedImpressions
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams.TrusTeeV2Config
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism

/** The clip a frequency vector cell can represent, since a cell is one signed byte. */
private const val MAX_REPRESENTABLE_CLIP = 127

/** One released quantity draws from the seed, as on the Direct path. */
private const val CONTRIBUTION_COUNT = 1

/**
 * Builds the impression count a `TrusTeeV2` fulfillment carries alongside its frequency vector.
 *
 * The count covers the whole population, so it carries no sampling error.
 * [TrusTeeV2Config.ImpressionCountMode.UNNOISED] reports the true uncapped total, which
 * [StripedByteFrequencyVector] accumulates before a cell saturates
 * at 127. [TrusTeeV2Config.ImpressionCountMode.NOISED] sums the saturated cells under a clip.
 *
 * @throws IllegalArgumentException if [mode] and [maxFrequencyPerUser] are a combination
 *   [requireTrusTeeV2ImpressionCountConfig] rejects
 */
fun buildTrusTeeV2FulfillmentDetails(
  mode: TrusTeeV2Config.ImpressionCountMode,
  maxFrequencyPerUser: Int,
  frequencyVector: StripedByteFrequencyVector,
): FulfillRequisitionRequest.Header.TrusTeeV2.FulfillmentDetails {
  requireTrusTeeV2ImpressionCountConfig(mode, maxFrequencyPerUser)

  return FulfillRequisitionRequestKt.HeaderKt.TrusTeeV2Kt.fulfillmentDetails {
    impression =
      when {
        mode == TrusTeeV2Config.ImpressionCountMode.UNNOISED ->
          impression {
            value = frequencyVector.getTotalUncappedImpressions()
            noiseMechanism = ProtocolConfig.NoiseMechanism.NONE
            // No per-user clip accompanies an uncapped count, so none is reported with it.
            deterministicCount = deterministicCount {}
          }
        maxFrequencyPerUser > 0 -> {
          val frequencyData: IntArray = readFrequencyData(frequencyVector)
          val histogram: LongArray =
            HistogramComputations.buildHistogram(
              frequencyVector = frequencyData,
              maxFrequency = maxFrequencyPerUser,
            )
          impression {
            value =
              ImpressionComputations.computeImpressionCount(
                rawHistogram = histogram,
                // The count spans the whole population, so nothing scales it.
                vidSamplingIntervalWidth = 1.0,
                noiser =
                  DeterministicTruncatedLaplaceResultNoiser(
                    combinedFrequencyVector = frequencyData,
                    contributionCount = CONTRIBUTION_COUNT,
                    maxFrequencyPerUser = maxFrequencyPerUser,
                  ),
                // This count is not thresholded here.
                resultMinimumThresholds = null,
              )
            noiseMechanism = ProtocolConfig.NoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE
            deterministicCount = deterministicCount {
              customMaximumFrequencyPerUser = maxFrequencyPerUser
            }
          }
        }
        else -> {
          val clipped =
            computeDirectDynamicallyClippedImpressions(
              directNoiseMechanism = DirectNoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE,
              frequencyData = readFrequencyData(frequencyVector),
              dpParams = null,
              // The count spans the whole population, so nothing scales it.
              vidSamplingIntervalWidth = 1.0,
              // This count is not thresholded here.
              resultMinimumThresholds = null,
            )
          impression {
            value = clipped.value
            noiseMechanism = ProtocolConfig.NoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE
            // The clip came from the data, so the variance travels instead of the clip.
            customDirectMethodology = customDirectMethodology {
              variance = CustomDirectMethodologyKt.variance { scalar = clipped.variance }
            }
          }
        }
      }
  }
}

/**
 * Checks that [mode] and [maxFrequencyPerUser] are a combination a count can be built from.
 *
 * Called at config load so a provider learns of a bad combination before a requisition arrives, and
 * again where the count is built.
 *
 * @throws IllegalArgumentException with what to set instead
 */
fun requireTrusTeeV2ImpressionCountConfig(
  mode: TrusTeeV2Config.ImpressionCountMode,
  maxFrequencyPerUser: Int,
) {
  when (mode) {
    TrusTeeV2Config.ImpressionCountMode.NOISED ->
      require(maxFrequencyPerUser in 0..MAX_REPRESENTABLE_CLIP) {
        "max_frequency_per_user must be in 1..$MAX_REPRESENTABLE_CLIP under NOISED, or unset to " +
          "clip dynamically, got $maxFrequencyPerUser. A frequency vector cell saturates at the " +
          "largest signed byte."
      }
    TrusTeeV2Config.ImpressionCountMode.UNSPECIFIED,
    TrusTeeV2Config.ImpressionCountMode.UNNOISED ->
      require(maxFrequencyPerUser == 0) {
        "max_frequency_per_user is read only under NOISED, got $maxFrequencyPerUser under $mode."
      }
    TrusTeeV2Config.ImpressionCountMode.UNRECOGNIZED ->
      throw IllegalArgumentException("Unrecognized impression_count_mode")
  }
}

/**
 * Returns the per-VID frequencies of [frequencyVector] as an [IntArray].
 *
 * Called from the branches that read the array rather than once up front: the copy is the size of
 * the population, and the unnoised mode never reads it.
 */
private fun readFrequencyData(frequencyVector: StripedByteFrequencyVector): IntArray {
  val bytes: ByteArray = frequencyVector.getByteArray()
  return IntArray(bytes.size) { bytes[it].toInt() }
}
