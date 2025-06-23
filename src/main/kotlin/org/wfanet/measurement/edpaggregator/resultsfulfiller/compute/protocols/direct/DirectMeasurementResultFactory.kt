/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.resultsfulfiller.compute.protocols.direct

import java.security.SecureRandom
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism

/** Factory for creating direct measurement results. */
object DirectMeasurementResultFactory {
  private val logger: Logger = Logger.getLogger(this::class.java.name)

  /**
   * Build [Measurement.Result] of the measurement type specified in [MeasurementSpec].
   *
   * @param directProtocolConfig The direct protocol configuration.
   * @param directNoiseMechanism The direct noise mechanism to use.
   * @param measurementSpec The measurement specification.
   * @param sampledVids The sampled VIDs.
   * @param random The random number generator to use.
   * @return The measurement result.
   */
  suspend fun buildMeasurementResult(
    directProtocolConfig: ProtocolConfig.Direct,
    directNoiseMechanism: DirectNoiseMechanism,
    measurementSpec: MeasurementSpec,
    sampledVids: Flow<Long>,
    random: SecureRandom = SecureRandom(),
  ): Measurement.Result {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields cannot be null.
    return when (measurementSpec.measurementTypeCase) {
      MeasurementSpec.MeasurementTypeCase.REACH_AND_FREQUENCY -> {
        val reachAndFrequencyResultBuilder =
          DirectReachAndFrequencyResultBuilder(
            directProtocolConfig,
            sampledVids,
            measurementSpec.reachAndFrequency.maximumFrequency,
            measurementSpec.reachAndFrequency.reachPrivacyParams,
            measurementSpec.reachAndFrequency.frequencyPrivacyParams,
            measurementSpec.vidSamplingInterval.width,
            directNoiseMechanism,
            random,
          )
        reachAndFrequencyResultBuilder.buildMeasurementResult()
      }
      MeasurementSpec.MeasurementTypeCase.IMPRESSION -> {
        MeasurementKt.result { TODO("Not yet implemented") }
      }
      MeasurementSpec.MeasurementTypeCase.DURATION -> {
        MeasurementKt.result { TODO("Not yet implemented") }
      }
      MeasurementSpec.MeasurementTypeCase.POPULATION -> {
        MeasurementKt.result { TODO("Not yet implemented") }
      }
      MeasurementSpec.MeasurementTypeCase.REACH -> {
        MeasurementKt.result { TODO("Not yet implemented") }
      }
      MeasurementSpec.MeasurementTypeCase.MEASUREMENTTYPE_NOT_SET -> {
        error("Measurement type not set.")
      }
    }
  }
}
