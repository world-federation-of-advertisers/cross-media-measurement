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
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams.ImpressionCapMode
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism
import org.wfanet.measurement.eventdataprovider.noiser.DpParams

/** One released quantity draws from the seed, as on the Direct path. */
private const val CONTRIBUTION_COUNT = 1

/**
 * Builds the impression count a `TrusTeeV2` fulfillment carries alongside its frequency vector.
 *
 * The count covers the whole population rather than the sampling interval the vector covers, so it
 * carries no sampling error. [capMode] and [configuredCap] are the same fields the Direct path
 * clips with.
 *
 * The cap mode decides who noises the count, because only one mechanism is available to each:
 * * [ImpressionCapMode.UNCAPPED] has no per-user bound to calibrate a sampler to, so the count goes
 *   out unnoised and the TEE noises the figure it composes from these counts. It reports the true
 *   uncapped total, which [StripedByteFrequencyVector] accumulates before a cell saturates at 127.
 * * [ImpressionCapMode.CUSTOM_CAP] and [ImpressionCapMode.DYNAMIC] noise here with deterministic
 *   truncated Laplace, whose parameters are compiled into the attested image. Continuous Gaussian
 *   needs privacy params from the `MeasurementSpec`, and a `MultiMeasurementSpec` carries none.
 *
 * @throws IllegalArgumentException if [capMode] is one [requireTrusTeeV2CapModeSupported] rejects
 */
fun buildTrusTeeV2FulfillmentDetails(
  capMode: ImpressionCapMode,
  configuredCap: Int,
  frequencyVector: StripedByteFrequencyVector,
): FulfillRequisitionRequest.Header.TrusTeeV2.FulfillmentDetails {
  requireTrusTeeV2CapModeSupported(capMode)

  return FulfillRequisitionRequestKt.HeaderKt.TrusTeeV2Kt.fulfillmentDetails {
    impression =
      when (capMode) {
        ImpressionCapMode.UNCAPPED ->
          impression {
            value = frequencyVector.getTotalUncappedImpressions()
            noiseMechanism = ProtocolConfig.NoiseMechanism.NONE
            // No per-user bound accompanies an uncapped count, so no clip is reported with it.
            deterministicCount = deterministicCount {}
          }
        ImpressionCapMode.CUSTOM_CAP -> {
          val frequencyData: IntArray = readFrequencyData(frequencyVector)
          val histogram: LongArray =
            HistogramComputations.buildHistogram(
              frequencyVector = frequencyData,
              maxFrequency = configuredCap,
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
                    maxFrequencyPerUser = configuredCap,
                  ),
                // The TEE thresholds the aggregate it composes from these counts.
                resultMinimumThresholds = null,
              )
            noiseMechanism = ProtocolConfig.NoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE
            deterministicCount = deterministicCount {
              customMaximumFrequencyPerUser = configuredCap
            }
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
            // The clip came from the data, so the variance travels instead of the clip.
            customDirectMethodology = customDirectMethodology {
              variance = CustomDirectMethodologyKt.variance { scalar = clipped.variance }
            }
          }
        }
        else -> error("Unreachable: $capMode is rejected at config load")
      }
  }
}

/**
 * Checks that [capMode] is one a `TrusTeeV2` impression count can be built under.
 *
 * Called at config load so a provider learns of an unusable mode before a requisition arrives, and
 * again where the count is built. The cap value itself is checked by `requireCapMatchesMode`, which
 * the Direct path shares.
 *
 * @throws IllegalArgumentException with what to set instead
 */
fun requireTrusTeeV2CapModeSupported(capMode: ImpressionCapMode) {
  when (capMode) {
    ImpressionCapMode.UNCAPPED,
    ImpressionCapMode.CUSTOM_CAP,
    ImpressionCapMode.DYNAMIC -> {}
    ImpressionCapMode.USE_MEASUREMENT_SPEC_CAP ->
      throw IllegalArgumentException(
        "USE_MEASUREMENT_SPEC_CAP has nothing to read for a TrusTeeV2 impression count: a " +
          "MultiMeasurementSpec carries no cap. Set CUSTOM_CAP, UNCAPPED or DYNAMIC."
      )
    ImpressionCapMode.UNSPECIFIED,
    ImpressionCapMode.UNRECOGNIZED ->
      throw IllegalArgumentException(
        "impression_cap_mode must be set explicitly to send a TrusTeeV2 impression count. Set " +
          "CUSTOM_CAP, UNCAPPED or DYNAMIC."
      )
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
