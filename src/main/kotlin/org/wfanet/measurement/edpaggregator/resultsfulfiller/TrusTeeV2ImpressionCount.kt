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
import org.wfanet.measurement.edpaggregator.resultsfulfiller.compute.protocols.direct.computeDeterministicDynamicallyClippedImpressions
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams.TrusTeeV2Config

/** The impression count a `TrusTeeV2` fulfillment carries alongside its frequency vector. */
object TrusTeeV2ImpressionCount {
  /** The clip a frequency vector cell can represent, since a cell is one signed byte. */
  private const val MAX_REPRESENTABLE_CLIP = 127

  /** One released quantity draws from the seed, as on the Direct path. */
  private const val CONTRIBUTION_COUNT = 1

  /**
   * Builds the impression count a `TrusTeeV2` fulfillment carries alongside its frequency vector.
   *
   * [frequencyData] is the caller's own snapshot of the population, one byte per VID, so nothing
   * here clones the vector a second time.
   *
   * The count covers the whole population, so it carries no sampling error.
   * [TrusTeeV2Config.ImpressionCountMode.UNNOISED] reports [totalUncappedImpressions], which
   * `StripedByteFrequencyVector` accumulates before a cell saturates
   * at 127. [TrusTeeV2Config.ImpressionCountMode.NOISED] sums the saturated cells under a clip.
   *
   * @throws IllegalArgumentException if [mode] and [maxFrequencyPerUser] are a combination
   *   [validateConfig] rejects
   */
  fun buildFulfillmentDetails(
    mode: TrusTeeV2Config.ImpressionCountMode,
    maxFrequencyPerUser: Int?,
    frequencyData: ByteArray,
    totalUncappedImpressions: Long,
  ): FulfillRequisitionRequest.Header.TrusTeeV2.FulfillmentDetails {
    validateConfig(mode, maxFrequencyPerUser)

    return FulfillRequisitionRequestKt.HeaderKt.TrusTeeV2Kt.fulfillmentDetails {
      impression =
        when {
          mode == TrusTeeV2Config.ImpressionCountMode.UNNOISED ->
            impression {
              value = totalUncappedImpressions
              noiseMechanism = ProtocolConfig.NoiseMechanism.NONE
              // No per-user clip accompanies an uncapped count, so none is reported with it.
              deterministicCount = deterministicCount {}
            }
          maxFrequencyPerUser != null -> {
            val clip: Int = maxFrequencyPerUser
            val perVidFrequencies: IntArray = toIntArray(frequencyData)
            val histogram: LongArray =
              HistogramComputations.buildHistogram(
                frequencyVector = perVidFrequencies,
                maxFrequency = clip,
              )
            impression {
              value =
                ImpressionComputations.computeImpressionCount(
                  rawHistogram = histogram,
                  // The count spans the whole population, so nothing scales it.
                  vidSamplingIntervalWidth = 1.0,
                  noiser =
                    DeterministicTruncatedLaplaceResultNoiser(
                      combinedFrequencyVector = perVidFrequencies,
                      contributionCount = CONTRIBUTION_COUNT,
                      maxFrequencyPerUser = clip,
                    ),
                  // This count is not thresholded here.
                  resultMinimumThresholds = null,
                )
              noiseMechanism = ProtocolConfig.NoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE
              deterministicCount = deterministicCount { customMaximumFrequencyPerUser = clip }
            }
          }
          else -> {
            val clipped =
              computeDeterministicDynamicallyClippedImpressions(
                frequencyData = toIntArray(frequencyData),
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
   * Called at config load so a provider learns of a bad combination before a requisition arrives,
   * and again where the count is built.
   *
   * @throws IllegalArgumentException with what to set instead
   */
  fun validateConfig(mode: TrusTeeV2Config.ImpressionCountMode, maxFrequencyPerUser: Int?) {
    when (mode) {
      TrusTeeV2Config.ImpressionCountMode.NOISED ->
        require(maxFrequencyPerUser == null || maxFrequencyPerUser in 1..MAX_REPRESENTABLE_CLIP) {
          "max_frequency_per_user must be in 1..$MAX_REPRESENTABLE_CLIP under NOISED, or null to " +
            "clip dynamically, got $maxFrequencyPerUser. A frequency vector cell saturates at the " +
            "largest signed byte."
        }
      TrusTeeV2Config.ImpressionCountMode.UNSPECIFIED,
      TrusTeeV2Config.ImpressionCountMode.UNNOISED ->
        require(maxFrequencyPerUser == null) {
          "max_frequency_per_user is read only under NOISED, got $maxFrequencyPerUser under $mode."
        }
      TrusTeeV2Config.ImpressionCountMode.UNRECOGNIZED ->
        throw IllegalArgumentException("Unrecognized impression_count_mode")
    }
  }

  // TODO(world-federation-of-advertisers/cross-media-measurement#4596): Drop this conversion once
  // the TrusTee fulfillment path carries bytes. The histogram, the clip search and the seed take an
  // IntArray only because TrusTee v1 and HMSS do, and this copy is four times the population.
  /**
   * Returns [frequencyData] as an [IntArray].
   *
   * Called from the branches that read the array rather than once up front: the copy is the size of
   * the population, and the unnoised mode never reads it.
   */
  private fun toIntArray(frequencyData: ByteArray): IntArray =
    IntArray(frequencyData.size) { frequencyData[it].toInt() }
}

/**
 * The configured per-user clip, or null when `max_frequency_per_user` is unset.
 *
 * The proto encodes "unset" as 0, since a scalar field cannot be absent. Null is what the
 * computation code reads, so the sentinel stops at this boundary.
 */
internal val TrusTeeV2Config.maxFrequencyPerUserOrNull: Int?
  get() = if (maxFrequencyPerUser == 0) null else maxFrequencyPerUser
