/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.config

import org.wfanet.measurement.config.reporting.MeasurementSpecConfig

/**
 * Contains a single method that validates a [MeasurementSpecConfig].
 */
object MeasurementSpecConfigValidator {
  fun MeasurementSpecConfig.validate() {
    if (!this.reachSingleDataProvider.privacyParams.isValid()) {
      throw IllegalArgumentException("reach_single_data_provider privacy_params is invalid.")
    }
    if (!this.reachSingleDataProvider.vidSamplingInterval.isValid()) {
      throw IllegalArgumentException(
        "reach_single_data_provider vid_sampling_interval is invalid."
      )
    }

    if (!this.reach.privacyParams.isValid()) {
      throw IllegalArgumentException("reach privacy_params is invalid.")
    }
    if (!this.reach.vidSamplingInterval.isValid()) {
      throw IllegalArgumentException("reach vid_sampling_interval is invalid.")
    }

    if (!this.reachAndFrequencySingleDataProvider.reachPrivacyParams.isValid()) {
      throw IllegalArgumentException(
        "reach_and_frequency_single_data_provider reach_privacy_params is invalid."
      )
    }
    if (
      !this.reachAndFrequencySingleDataProvider.frequencyPrivacyParams.isValid()
    ) {
      throw IllegalArgumentException(
        "reach_and_frequency_single_data_provider frequency_privacy_params is invalid."
      )
    }
    if (
      !this.reachAndFrequencySingleDataProvider.vidSamplingInterval.isValid()
    ) {
      throw IllegalArgumentException(
        "reach_and_frequency_single_data_provider vid_sampling_interval is invalid."
      )
    }

    if (!this.reachAndFrequency.reachPrivacyParams.isValid()) {
      throw IllegalArgumentException("reach_and_frequency reach_privacy_params is invalid.")
    }
    if (!this.reachAndFrequency.frequencyPrivacyParams.isValid()) {
      throw IllegalArgumentException("reach_and_frequency frequency_privacy_params is invalid.")
    }
    if (!this.reachAndFrequency.vidSamplingInterval.isValid()) {
      throw IllegalArgumentException("reach_and_frequency vid_sampling_interval is invalid.")
    }

    if (!this.impression.privacyParams.isValid()) {
      throw IllegalArgumentException("impression privacy_params is invalid.")
    }
    if (!this.impression.vidSamplingInterval.isValid()) {
      throw IllegalArgumentException("impression vid_sampling_interval is invalid.")
    }

    if (!this.duration.privacyParams.isValid()) {
      throw IllegalArgumentException("duration privacy_params is invalid.")
    }
    if (!this.duration.vidSamplingInterval.isValid()) {
      throw IllegalArgumentException("duration vid_sampling_interval is invalid.")
    }
  }

  private fun MeasurementSpecConfig.DifferentialPrivacyParams.isValid(): Boolean {
    return (this.epsilon > 0 && this.delta >= 0)
  }

  private fun MeasurementSpecConfig.VidSamplingInterval.isValid(): Boolean {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    return when (this.startCase) {
      MeasurementSpecConfig.VidSamplingInterval.StartCase.FIXED_START -> this.fixedStart.isValid()
      MeasurementSpecConfig.VidSamplingInterval.StartCase.RANDOM_START -> this.randomStart.isValid()
      MeasurementSpecConfig.VidSamplingInterval.StartCase.START_NOT_SET -> true
    }
  }

  private fun MeasurementSpecConfig.VidSamplingInterval.FixedStart.isValid(): Boolean {
    if (this.start < 0.0) {
      return false
    }

    if (this.width <= 0.0) {
      return false
    }

    if (this.start + this.width > 1.0) {
      return false
    }

    return true
  }

  private fun MeasurementSpecConfig.VidSamplingInterval.RandomStart.isValid(): Boolean {
    if (this.numVidBuckets <= 0) {
      return false
    }

    if (this.width <= 0 || this.width > this.numVidBuckets) {
      return false
    }

    return true
  }
}
