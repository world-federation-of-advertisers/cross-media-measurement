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

package org.wfanet.measurement.reporting.service.api.v2alpha

import org.wfanet.measurement.config.reporting.MetricSpecConfig
import org.wfanet.measurement.reporting.v2alpha.MetricSpec
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.metricSpec

class MetricSpecBuildingException(message: String? = null, cause: Throwable? = null) :
  Exception(message, cause)

class MetricSpecBuilder(private val metricSpecConfig: MetricSpecConfig) {
  /**
   * Builds a [MetricSpec] by checking the given [MetricSpec] and filling unset values with default
   * values in [MetricSpecConfig].
   */
  fun buildMetricSpec(metricSpec: MetricSpec): MetricSpec {
    return metricSpec {
      val defaultVidSamplingInterval: MetricSpecConfig.VidSamplingInterval =
        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
        when (metricSpec.typeCase) {
          MetricSpec.TypeCase.REACH -> {
            reach = buildReachParams(metricSpec.reach)
            metricSpecConfig.reachVidSamplingInterval
          }
          MetricSpec.TypeCase.FREQUENCY_HISTOGRAM -> {
            frequencyHistogram = buildFrequencyHistogramParams(metricSpec.frequencyHistogram)
            metricSpecConfig.frequencyHistogramVidSamplingInterval
          }
          MetricSpec.TypeCase.IMPRESSION_COUNT -> {
            impressionCount = buildImpressionCountParams(metricSpec.impressionCount)
            metricSpecConfig.impressionCountVidSamplingInterval
          }
          MetricSpec.TypeCase.WATCH_DURATION -> {
            watchDuration = buildWatchDurationParams(metricSpec.watchDuration)
            metricSpecConfig.watchDurationVidSamplingInterval
          }
          MetricSpec.TypeCase.TYPE_NOT_SET ->
            throw MetricSpecBuildingException(
              "Invalid metric spec type",
              IllegalArgumentException("The metric type in Metric is not specified.")
            )
        }

      vidSamplingInterval =
        if (metricSpec.hasVidSamplingInterval()) {
          metricSpec.vidSamplingInterval
        } else defaultVidSamplingInterval.toVidSamplingInterval()

      if (vidSamplingInterval.start < 0) {
        throw MetricSpecBuildingException(
          "Invalid vidSamplingInterval",
          IllegalArgumentException("vidSamplingInterval.start cannot be negative.")
        )
      }
      if (vidSamplingInterval.start >= 1) {
        throw MetricSpecBuildingException(
          "Invalid vidSamplingInterval",
          IllegalArgumentException("vidSamplingInterval.start must be smaller than 1.")
        )
      }
      if (vidSamplingInterval.width <= 0) {
        throw MetricSpecBuildingException(
          "Invalid vidSamplingInterval",
          IllegalArgumentException("vidSamplingInterval.width must be greater than 0.")
        )
      }
      if (vidSamplingInterval.start + vidSamplingInterval.width > 1) {
        throw MetricSpecBuildingException(
          "Invalid vidSamplingInterval",
          IllegalArgumentException("vidSamplingInterval start + width cannot be greater than 1.")
        )
      }
    }
  }

  /** Builds a [MetricSpec.ReachParams]. */
  private fun buildReachParams(reachParams: MetricSpec.ReachParams): MetricSpec.ReachParams {
    if (!reachParams.hasPrivacyParams()) {
      throw MetricSpecBuildingException(
        "Invalid privacy params",
        IllegalArgumentException("privacyParams in reach is not set.")
      )
    }

    return MetricSpecKt.reachParams {
      privacyParams =
        buildDifferentialPrivacyParams(
          reachParams.privacyParams,
          metricSpecConfig.reachParams.privacyParams.epsilon,
          metricSpecConfig.reachParams.privacyParams.delta
        )
    }
  }

  /** Builds a [MetricSpec.FrequencyHistogramParams]. */
  private fun buildFrequencyHistogramParams(
    frequencyHistogramParams: MetricSpec.FrequencyHistogramParams
  ): MetricSpec.FrequencyHistogramParams {
    if (!frequencyHistogramParams.hasReachPrivacyParams()) {
      throw MetricSpecBuildingException(
        "Invalid privacy params",
        IllegalArgumentException("reachPrivacyParams in frequency histogram is not set.")
      )
    }
    if (!frequencyHistogramParams.hasFrequencyPrivacyParams()) {
      throw MetricSpecBuildingException(
        "Invalid privacy params",
        IllegalArgumentException("frequencyPrivacyParams in frequency histogram is not set.")
      )
    }

    return MetricSpecKt.frequencyHistogramParams {
      reachPrivacyParams =
        buildDifferentialPrivacyParams(
          frequencyHistogramParams.reachPrivacyParams,
          metricSpecConfig.frequencyHistogramParams.reachPrivacyParams.epsilon,
          metricSpecConfig.frequencyHistogramParams.reachPrivacyParams.delta
        )
      frequencyPrivacyParams =
        buildDifferentialPrivacyParams(
          frequencyHistogramParams.frequencyPrivacyParams,
          metricSpecConfig.frequencyHistogramParams.frequencyPrivacyParams.epsilon,
          metricSpecConfig.frequencyHistogramParams.frequencyPrivacyParams.delta
        )
      maximumFrequencyPerUser =
        if (frequencyHistogramParams.hasMaximumFrequencyPerUser()) {
          frequencyHistogramParams.maximumFrequencyPerUser
        } else {
          metricSpecConfig.frequencyHistogramParams.maximumFrequencyPerUser
        }
    }
  }

  /** Builds an [MetricSpec.WatchDurationParams]. */
  private fun buildWatchDurationParams(
    watchDurationParams: MetricSpec.WatchDurationParams
  ): MetricSpec.WatchDurationParams {
    if (!watchDurationParams.hasPrivacyParams()) {
      throw MetricSpecBuildingException(
        "Invalid privacy params",
        IllegalArgumentException("privacyParams in watch duration is not set.")
      )
    }

    return MetricSpecKt.watchDurationParams {
      privacyParams =
        buildDifferentialPrivacyParams(
          watchDurationParams.privacyParams,
          metricSpecConfig.watchDurationParams.privacyParams.epsilon,
          metricSpecConfig.watchDurationParams.privacyParams.delta
        )
      maximumWatchDurationPerUser =
        if (watchDurationParams.hasMaximumWatchDurationPerUser()) {
          watchDurationParams.maximumWatchDurationPerUser
        } else {
          metricSpecConfig.watchDurationParams.maximumWatchDurationPerUser
        }
    }
  }

  /** Builds a [MetricSpec.ImpressionCountParams]. */
  private fun buildImpressionCountParams(
    impressionCountParams: MetricSpec.ImpressionCountParams
  ): MetricSpec.ImpressionCountParams {
    if (!impressionCountParams.hasPrivacyParams()) {
      throw MetricSpecBuildingException(
        "Invalid privacy params",
        IllegalArgumentException("privacyParams in impression count is not set.")
      )
    }

    return MetricSpecKt.impressionCountParams {
      privacyParams =
        buildDifferentialPrivacyParams(
          impressionCountParams.privacyParams,
          metricSpecConfig.impressionCountParams.privacyParams.epsilon,
          metricSpecConfig.impressionCountParams.privacyParams.delta
        )
      maximumFrequencyPerUser =
        if (impressionCountParams.hasMaximumFrequencyPerUser()) {
          impressionCountParams.maximumFrequencyPerUser
        } else {
          metricSpecConfig.impressionCountParams.maximumFrequencyPerUser
        }
    }
  }
}

/**
 * Build an [MetricSpec.DifferentialPrivacyParams] given [MetricSpec.DifferentialPrivacyParams]. If
 * any field in the given [MetricSpec.DifferentialPrivacyParams] is unspecified, it will use the
 * provided default value.
 */
private fun buildDifferentialPrivacyParams(
  dpParams: MetricSpec.DifferentialPrivacyParams,
  defaultEpsilon: Double,
  defaultDelta: Double
): MetricSpec.DifferentialPrivacyParams {
  return MetricSpecKt.differentialPrivacyParams {
    epsilon = if (dpParams.hasEpsilon()) dpParams.epsilon else defaultEpsilon
    delta = if (dpParams.hasDelta()) dpParams.delta else defaultDelta
  }
}



/** Converts an [MetricSpecConfig.VidSamplingInterval] to an [MetricSpec.VidSamplingInterval]. */
private fun MetricSpecConfig.VidSamplingInterval.toVidSamplingInterval(): MetricSpec.VidSamplingInterval {
  val source = this
  return MetricSpecKt.vidSamplingInterval {
    start = source.start
    width = source.width
  }
}
