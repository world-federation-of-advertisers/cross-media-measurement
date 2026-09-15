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
import org.wfanet.measurement.computation.HistogramComputations
import org.wfanet.measurement.computation.ImpressionComputations
import org.wfanet.measurement.computation.NoNoise
import org.wfanet.measurement.edpaggregator.resultsfulfiller.compute.protocols.direct.computeDirectDynamicallyClippedImpressions
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams.ImpressionCapMode
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism
import org.wfanet.measurement.eventdataprovider.noiser.DpParams

/** The cap a frequency vector cell can represent, since a cell is one signed byte. */
private const val MAX_REPRESENTABLE_CAP = 127

/**
 * Builds the impression count a `TrusTeeV2` fulfillment carries alongside its frequency vector.
 *
 * The count covers the whole population rather than the sampling interval the vector covers, so it
 * carries no sampling error. [ImpressionCapMode.UNCAPPED] reports the true uncapped total, which
 * [StripedByteFrequencyVector] accumulates as it ingests, before a cell saturates at 127. The other
 * two modes sum the saturated cells, so a VID seen more than 127 times counts as 127.
 * `DirectImpressionResultBuilder` splits an uncapped total from a capped sum the same way.
 *
 * Who noises the count follows `noise_params.noise_type`:
 * * NONE leaves the value unnoised and reports the clip, so the TEE noises it once per released
 *   figure using that clip as the sensitivity bound.
 * * DETERMINISTIC_TRUNCATED_LAPLACE noises here and reports the variance, so the TEE adds nothing.
 *
 * @throws IllegalArgumentException if [params] is a combination [validateImpressionCountsParams]
 *   rejects
 */
fun buildTrusTeeV2FulfillmentDetails(
  params: ResultsFulfillerParams.ImpressionCountsParams,
  frequencyVector: StripedByteFrequencyVector,
): FulfillRequisitionRequest.Header.TrusTeeV2.FulfillmentDetails {
  validateImpressionCountsParams(params)

  return FulfillRequisitionRequestKt.HeaderKt.TrusTeeV2Kt.fulfillmentDetails {
    impression =
      when (params.capMode) {
        ImpressionCapMode.UNCAPPED ->
          impression {
            value = frequencyVector.getTotalUncappedImpressions()
            noiseMechanism = ProtocolConfig.NoiseMechanism.NONE
            // No per-user bound accompanies an uncapped count, so the TEE has no sensitivity to
            // noise it with. Only NONE reaches here.
            deterministicCount = deterministicCount {}
          }
        ImpressionCapMode.CUSTOM_CAP -> {
          val cap = params.maxFrequencyPerUser
          val histogram: LongArray =
            HistogramComputations.buildHistogram(
              frequencyVector = readFrequencyData(frequencyVector),
              maxFrequency = cap,
            )
          impression {
            value =
              ImpressionComputations.computeImpressionCount(
                rawHistogram = histogram,
                // The count spans the whole population, so nothing scales it.
                vidSamplingIntervalWidth = 1.0,
                // The TEE noises the figure it composes, using the reported clip as the bound.
                noiser = NoNoise,
                // The TEE thresholds the aggregate it composes from these counts.
                resultMinimumThresholds = null,
              )
            noiseMechanism = ProtocolConfig.NoiseMechanism.NONE
            deterministicCount = deterministicCount { customMaximumFrequencyPerUser = cap }
          }
        }
        ImpressionCapMode.DYNAMIC -> {
          val clipped =
            computeDirectDynamicallyClippedImpressions(
              directNoiseMechanism = DirectNoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE,
              frequencyData = readFrequencyData(frequencyVector),
              // Unread for this mechanism, which takes its parameters from the attested image.
              dpParams = DpParams(1.0, 1.0),
              // The count spans the whole population, so nothing scales it.
              vidSamplingIntervalWidth = 1.0,
              // The TEE thresholds the aggregate it composes from these counts.
              resultMinimumThresholds = null,
            )
          impression {
            value = clipped.value
            noiseMechanism = ProtocolConfig.NoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE
            customDirectMethodology = customDirectMethodology {
              variance = CustomDirectMethodologyKt.variance { scalar = clipped.variance }
            }
          }
        }
        else -> error("Unreachable: ${params.capMode} is rejected at config load")
      }
  }
}

/**
 * Checks that [params] is a combination the `TrusTeeV2` path can fulfill.
 *
 * Called at config load so a provider learns of a bad combination before a requisition arrives, and
 * again where the count is built.
 *
 * @throws IllegalArgumentException with what to set instead
 */
fun validateImpressionCountsParams(params: ResultsFulfillerParams.ImpressionCountsParams) {
  require(params.hasNoiseParams()) {
    "noise_params is required for a TrusTeeV2 impression count. Set noise_type NONE under " +
      "UNCAPPED or CUSTOM_CAP, and DETERMINISTIC_TRUNCATED_LAPLACE under DYNAMIC."
  }
  val noiseType = params.noiseParams.noiseType
  when (params.capMode) {
    ImpressionCapMode.UNCAPPED,
    ImpressionCapMode.CUSTOM_CAP ->
      require(noiseType == ResultsFulfillerParams.NoiseParams.NoiseType.NONE) {
        "${params.capMode} requires noise_type NONE, got $noiseType. The TEE noises the count it " +
          "composes, using the reported clip as the sensitivity bound."
      }
    ImpressionCapMode.DYNAMIC ->
      require(
        noiseType == ResultsFulfillerParams.NoiseParams.NoiseType.DETERMINISTIC_TRUNCATED_LAPLACE
      ) {
        "DYNAMIC requires noise_type DETERMINISTIC_TRUNCATED_LAPLACE, got $noiseType. Choosing " +
          "the clip reads the frequency distribution, so the choice itself has to be noised."
      }
    ImpressionCapMode.USE_MEASUREMENT_SPEC_CAP ->
      throw IllegalArgumentException(
        "USE_MEASUREMENT_SPEC_CAP has nothing to read: a MultiMeasurementSpec carries no cap. " +
          "Set CUSTOM_CAP, UNCAPPED or DYNAMIC."
      )
    ImpressionCapMode.UNSPECIFIED,
    ImpressionCapMode.UNRECOGNIZED ->
      throw IllegalArgumentException(
        "cap_mode must be set explicitly for a TrusTeeV2 impression count. Set CUSTOM_CAP, " +
          "UNCAPPED or DYNAMIC."
      )
  }
  if (params.capMode == ImpressionCapMode.CUSTOM_CAP) {
    require(params.maxFrequencyPerUser in 1..MAX_REPRESENTABLE_CAP) {
      "max_frequency_per_user must be in 1..$MAX_REPRESENTABLE_CAP under CUSTOM_CAP, got " +
        "${params.maxFrequencyPerUser}. A frequency vector cell saturates at the largest signed " +
        "byte."
    }
  } else {
    require(params.maxFrequencyPerUser == 0) {
      "max_frequency_per_user is read only under CUSTOM_CAP, got ${params.maxFrequencyPerUser} " +
        "under ${params.capMode}."
    }
  }
}

/**
 * Returns the per-VID frequencies of [frequencyVector] as an [IntArray].
 *
 * Called from the branches that read the array rather than once up front: the copy is the size of
 * the population, and [ImpressionCapMode.UNCAPPED] never reads it.
 */
private fun readFrequencyData(frequencyVector: StripedByteFrequencyVector): IntArray {
  val bytes: ByteArray = frequencyVector.getByteArray()
  return IntArray(bytes.size) { bytes[it].toInt() }
}
