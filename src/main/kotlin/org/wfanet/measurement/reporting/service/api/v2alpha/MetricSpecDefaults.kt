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

import kotlin.random.Random
import org.wfanet.measurement.config.reporting.MetricSpecConfig
import org.wfanet.measurement.reporting.v2alpha.MetricSpec
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.copy

private const val NUM_BUCKETS = 10000

class MetricSpecDefaultsException(message: String? = null, cause: Throwable? = null) :
  Exception(message, cause)

/**
 * Validates a [MetricSpecConfig].
 *
 * @throws [IllegalArgumentException] if validation fails.
 */
fun MetricSpecConfig.validate() {
  if (hasReachParams()) {
    if (reachParams.hasMultipleDataProviderParams()) {
      try {
        reachParams.multipleDataProviderParams.validate()
      } catch (e: IllegalArgumentException) {
        throw IllegalArgumentException("reach_params: " + e.message)
      }
    } else {
      throw IllegalArgumentException("reach_params: missing multiple_data_provider_params.")
    }

    if (reachParams.hasSingleDataProviderParams()) {
      try {
        reachParams.singleDataProviderParams.validate()
      } catch (e: IllegalArgumentException) {
        throw IllegalArgumentException("reach_params: " + e.message)
      }
    } else {
      throw IllegalArgumentException("reach_params: missing single_data_provider_params.")
    }
  } else {
    throw IllegalArgumentException("Missing reach_params.")
  }

  if (hasReachAndFrequencyParams()) {
    if (reachAndFrequencyParams.hasMultipleDataProviderParams()) {
      try {
        reachAndFrequencyParams.multipleDataProviderParams.validate()
      } catch (e: IllegalArgumentException) {
        throw IllegalArgumentException("reach_and_frequency_params: " + e.message)
      }
    } else {
      throw IllegalArgumentException(
        "reach_and_frequency_params: missing multiple_data_provider_params."
      )
    }

    if (reachAndFrequencyParams.hasSingleDataProviderParams()) {
      try {
        reachAndFrequencyParams.singleDataProviderParams.validate()
      } catch (e: IllegalArgumentException) {
        throw IllegalArgumentException("reach_and_frequency_params: " + e.message)
      }
    } else {
      throw IllegalArgumentException(
        "reach_and_frequency_params: missing single_data_provider_params."
      )
    }

    if (reachAndFrequencyParams.maximumFrequency <= 0) {
      throw IllegalArgumentException("reach_and_frequency_params: missing maximum_frequency.")
    }
  } else {
    throw IllegalArgumentException("Missing reach_and_frequency_params.")
  }

  if (hasImpressionCountParams()) {
    if (impressionCountParams.hasParams()) {
      try {
        impressionCountParams.params.validate()
      } catch (e: IllegalArgumentException) {
        throw IllegalArgumentException("impression_count_params: " + e.message)
      }
    } else {
      throw IllegalArgumentException("impression_count_params: missing params.")
    }

    if (impressionCountParams.maximumFrequencyPerUser <= 0) {
      throw IllegalArgumentException("impression_count_params: missing maximum_frequency_per_user.")
    }
  } else {
    throw IllegalArgumentException("Missing impression_count_params.")
  }

  if (hasWatchDurationParams()) {
    if (watchDurationParams.hasParams()) {
      try {
        watchDurationParams.params.validate()
      } catch (e: IllegalArgumentException) {
        throw IllegalArgumentException("watch_duration_params: " + e.message)
      }
    } else {
      throw IllegalArgumentException("watch_duration_params: missing params.")
    }

    if (!watchDurationParams.hasMaximumWatchDurationPerUser()) {
      throw IllegalArgumentException(
        "watch_duration_params: missing maximum_watch_duration_per_user."
      )
    }
  } else {
    throw IllegalArgumentException("Missing watch_duration_params.")
  }

  if (!hasPopulationCountParams()) {
    throw IllegalArgumentException("Missing population_count_params.")
  }
}

/**
 * Validates a [MetricSpecConfig.SamplingAndPrivacyParams].
 *
 * @throws [IllegalArgumentException] if validation fails.
 */
fun MetricSpecConfig.SamplingAndPrivacyParams.validate() {
  if (hasPrivacyParams()) {
    privacyParams.validate()
  } else {
    throw IllegalArgumentException("SamplingAndPrivacyParams missing privacy_params.")
  }

  if (hasVidSamplingInterval()) {
    vidSamplingInterval.validate()
  } else {
    throw IllegalArgumentException("SamplingAndPrivacyParams missing vid_sampling_interval.")
  }
}

/**
 * Validates a [MetricSpecConfig.ReachAndFrequencySamplingAndPrivacyParams].
 *
 * @throws [IllegalArgumentException] if validation fails.
 */
fun MetricSpecConfig.ReachAndFrequencySamplingAndPrivacyParams.validate() {
  if (hasReachPrivacyParams()) {
    reachPrivacyParams.validate()
  } else {
    throw IllegalArgumentException(
      "ReachAndFrequencySamplingAndPrivacyParams missing reach_privacy_params."
    )
  }

  if (hasFrequencyPrivacyParams()) {
    frequencyPrivacyParams.validate()
  } else {
    throw IllegalArgumentException(
      "ReachAndFrequencySamplingAndPrivacyParams missing frequency_privacy_params."
    )
  }

  if (hasVidSamplingInterval()) {
    vidSamplingInterval.validate()
  } else {
    throw IllegalArgumentException(
      "ReachAndFrequencySamplingAndPrivacyParams missing vid_sampling_interval."
    )
  }
}

/**
 * Validates a [MetricSpecConfig.DifferentialPrivacyParams].
 *
 * @throws [IllegalArgumentException] if validation fails.
 */
fun MetricSpecConfig.DifferentialPrivacyParams.validate() {
  if (!hasEpsilon()) {
    throw IllegalArgumentException("DifferentialPrivacyParams missing epsilon.")
  }

  if (!hasDelta()) {
    throw IllegalArgumentException("DifferentialPrivacyParams missing delta.")
  }
}

/**
 * Validates a [MetricSpecConfig.VidSamplingInterval].
 *
 * @throws [IllegalArgumentException] if validation fails.
 */
fun MetricSpecConfig.VidSamplingInterval.validate() {
  if (hasFixedStart()) {
    if (fixedStart.hasStart()) {
      if (fixedStart.hasWidth()) {
        if (fixedStart.start < 0) {
          throw IllegalArgumentException("VidSamplingInterval.FixedStart.start cannot be negative.")
        }
        if (fixedStart.start >= 1) {
          throw IllegalArgumentException(
            "VidSamplingInterval.FixedStart.start must be smaller than 1."
          )
        }
        if (fixedStart.width <= 0) {
          throw IllegalArgumentException(
            "VidSamplingInterval.FixedStart.width must be greater than 0."
          )
        }
        if (fixedStart.start + fixedStart.width > 1) {
          throw IllegalArgumentException(
            "VidSamplingInterval.FixedStart start + width cannot be greater than 1."
          )
        }
      } else {
        throw IllegalArgumentException("VidSamplingInterval.FixedStart.start is missing.")
      }
    } else {
      throw IllegalArgumentException("VidSamplingInterval.FixedStart.width is missing.")
    }
  } else if (hasRandomStart()) {
    if (randomStart.hasWidth()) {
      if (randomStart.width <= 0 || randomStart.width > 1) {
        throw IllegalArgumentException(
          "VidSamplingInterval.RandomStart.width must be greater than 0 and less than or equal to 1."
        )
      }
    } else {
      throw IllegalArgumentException("VidSamplingInterval.RandomStart.width is missing.")
    }
  } else {
    throw IllegalArgumentException("VidSamplingInterval.start is missing")
  }
}

/**
 * Specifies default values using [MetricSpecConfig] when optional fields in the [MetricSpec] are
 * not set.
 */
fun MetricSpec.withDefaults(metricSpecConfig: MetricSpecConfig, secureRandom: Random): MetricSpec {
  val deprecatedVidSamplingInterval: MetricSpec.VidSamplingInterval? =
    if (this.hasVidSamplingInterval()) {
      this.vidSamplingInterval
    } else {
      null
    }

  return copy {
    when (typeCase) {
      MetricSpec.TypeCase.REACH -> {
        reach = reach.withDefaults(deprecatedVidSamplingInterval, metricSpecConfig, secureRandom)
      }
      MetricSpec.TypeCase.REACH_AND_FREQUENCY -> {
        reachAndFrequency =
          reachAndFrequency.withDefaults(
            deprecatedVidSamplingInterval,
            metricSpecConfig,
            secureRandom,
          )
      }
      MetricSpec.TypeCase.IMPRESSION_COUNT -> {
        impressionCount =
          impressionCount.withDefaults(
            deprecatedVidSamplingInterval,
            metricSpecConfig,
            secureRandom,
          )
      }
      MetricSpec.TypeCase.WATCH_DURATION -> {
        watchDuration =
          watchDuration.withDefaults(deprecatedVidSamplingInterval, metricSpecConfig, secureRandom)
      }
      MetricSpec.TypeCase.POPULATION_COUNT -> {
        populationCount = MetricSpec.PopulationCountParams.getDefaultInstance()
      }
      MetricSpec.TypeCase.TYPE_NOT_SET ->
        throw MetricSpecDefaultsException(
          "Invalid metric spec type",
          IllegalArgumentException("The metric type in Metric is not specified."),
        )
    }
    clearVidSamplingInterval()
  }
}

/**
 * Specifies default values using [MetricSpecConfig] when optional fields in the
 * [MetricSpec.ReachParams] are not set.
 */
private fun MetricSpec.ReachParams.withDefaults(
  deprecatedVidSamplingInterval: MetricSpec.VidSamplingInterval?,
  metricSpecConfig: MetricSpecConfig,
  secureRandom: Random,
): MetricSpec.ReachParams {
  return copy {
    if (
      multipleDataProviderParams.hasPrivacyParams() &&
        multipleDataProviderParams.hasVidSamplingInterval() &&
        singleDataProviderParams.hasPrivacyParams() &&
        singleDataProviderParams.hasVidSamplingInterval()
    ) {
      clearPrivacyParams()
      multipleDataProviderParams =
        multipleDataProviderParams.copy {
          privacyParams =
            privacyParams.withDefaults(
              defaultEpsilon =
                metricSpecConfig.reachParams.multipleDataProviderParams.privacyParams.epsilon,
              defaultDelta =
                metricSpecConfig.reachParams.multipleDataProviderParams.privacyParams.delta,
            )
          vidSamplingInterval =
            vidSamplingInterval.withDefaults(
              metricSpecConfig.reachParams.multipleDataProviderParams.vidSamplingInterval
                .toVidSamplingInterval(secureRandom)
            )
          vidSamplingInterval.validate()
        }

      singleDataProviderParams =
        singleDataProviderParams.copy {
          privacyParams =
            privacyParams.withDefaults(
              defaultEpsilon =
                metricSpecConfig.reachParams.singleDataProviderParams.privacyParams.epsilon,
              defaultDelta =
                metricSpecConfig.reachParams.singleDataProviderParams.privacyParams.delta,
            )
          vidSamplingInterval =
            vidSamplingInterval.withDefaults(
              metricSpecConfig.reachParams.singleDataProviderParams.vidSamplingInterval
                .toVidSamplingInterval(secureRandom)
            )
          vidSamplingInterval.validate()
        }
    } else if (
      hasPrivacyParams() && !hasSingleDataProviderParams() && !hasMultipleDataProviderParams()
    ) {
      val source = this
      multipleDataProviderParams =
        multipleDataProviderParams.copy {
          privacyParams =
            source.privacyParams.withDefaults(
              defaultEpsilon =
                metricSpecConfig.reachParams.multipleDataProviderParams.privacyParams.epsilon,
              defaultDelta =
                metricSpecConfig.reachParams.multipleDataProviderParams.privacyParams.delta,
            )
          vidSamplingInterval =
            deprecatedVidSamplingInterval
              ?: metricSpecConfig.reachParams.multipleDataProviderParams.vidSamplingInterval
                .toVidSamplingInterval(secureRandom)
          vidSamplingInterval.validate()
        }
      clearPrivacyParams()
    } else {
      throw MetricSpecDefaultsException(
        "Invalid params",
        IllegalArgumentException(
          "privacy_params in reach is not set, or fields in single_data_provider_params or multiple_data_provider_params not set."
        ),
      )
    }
  }
}

/**
 * Specifies default values using [MetricSpecConfig] when optional fields in the
 * [MetricSpec.ReachAndFrequencyParams] are not set.
 */
private fun MetricSpec.ReachAndFrequencyParams.withDefaults(
  deprecatedVidSamplingInterval: MetricSpec.VidSamplingInterval?,
  metricSpecConfig: MetricSpecConfig,
  secureRandom: Random,
): MetricSpec.ReachAndFrequencyParams {
  return copy {
    if (maximumFrequency == 0) {
      maximumFrequency = metricSpecConfig.reachAndFrequencyParams.maximumFrequency
    }

    if (
      multipleDataProviderParams.hasReachPrivacyParams() &&
        multipleDataProviderParams.hasFrequencyPrivacyParams() &&
        multipleDataProviderParams.hasVidSamplingInterval() &&
        singleDataProviderParams.hasReachPrivacyParams() &&
        singleDataProviderParams.hasFrequencyPrivacyParams() &&
        singleDataProviderParams.hasVidSamplingInterval()
    ) {
      clearReachPrivacyParams()
      clearFrequencyPrivacyParams()
      multipleDataProviderParams =
        multipleDataProviderParams.copy {
          reachPrivacyParams =
            reachPrivacyParams.withDefaults(
              defaultEpsilon =
                metricSpecConfig.reachAndFrequencyParams.multipleDataProviderParams
                  .reachPrivacyParams
                  .epsilon,
              defaultDelta =
                metricSpecConfig.reachAndFrequencyParams.multipleDataProviderParams
                  .reachPrivacyParams
                  .delta,
            )
          frequencyPrivacyParams =
            frequencyPrivacyParams.withDefaults(
              defaultEpsilon =
                metricSpecConfig.reachAndFrequencyParams.multipleDataProviderParams
                  .frequencyPrivacyParams
                  .epsilon,
              defaultDelta =
                metricSpecConfig.reachAndFrequencyParams.multipleDataProviderParams
                  .frequencyPrivacyParams
                  .delta,
            )
          vidSamplingInterval =
            vidSamplingInterval.withDefaults(
              metricSpecConfig.reachAndFrequencyParams.multipleDataProviderParams
                .vidSamplingInterval
                .toVidSamplingInterval(secureRandom)
            )
          vidSamplingInterval.validate()
        }

      singleDataProviderParams =
        singleDataProviderParams.copy {
          reachPrivacyParams =
            reachPrivacyParams.withDefaults(
              defaultEpsilon =
                metricSpecConfig.reachAndFrequencyParams.singleDataProviderParams.reachPrivacyParams
                  .epsilon,
              defaultDelta =
                metricSpecConfig.reachAndFrequencyParams.singleDataProviderParams.reachPrivacyParams
                  .delta,
            )
          frequencyPrivacyParams =
            frequencyPrivacyParams.withDefaults(
              defaultEpsilon =
                metricSpecConfig.reachAndFrequencyParams.singleDataProviderParams
                  .frequencyPrivacyParams
                  .epsilon,
              defaultDelta =
                metricSpecConfig.reachAndFrequencyParams.singleDataProviderParams
                  .frequencyPrivacyParams
                  .delta,
            )
          vidSamplingInterval =
            vidSamplingInterval.withDefaults(
              metricSpecConfig.reachAndFrequencyParams.singleDataProviderParams.vidSamplingInterval
                .toVidSamplingInterval(secureRandom)
            )
          vidSamplingInterval.validate()
        }
    } else if (
      hasReachPrivacyParams() &&
        hasFrequencyPrivacyParams() &&
        !hasSingleDataProviderParams() &&
        !hasMultipleDataProviderParams()
    ) {
      val source = this
      multipleDataProviderParams =
        multipleDataProviderParams.copy {
          reachPrivacyParams =
            source.reachPrivacyParams.withDefaults(
              defaultEpsilon =
                metricSpecConfig.reachAndFrequencyParams.multipleDataProviderParams
                  .reachPrivacyParams
                  .epsilon,
              defaultDelta =
                metricSpecConfig.reachAndFrequencyParams.multipleDataProviderParams
                  .reachPrivacyParams
                  .delta,
            )
          frequencyPrivacyParams =
            source.frequencyPrivacyParams.withDefaults(
              defaultEpsilon =
                metricSpecConfig.reachAndFrequencyParams.multipleDataProviderParams
                  .frequencyPrivacyParams
                  .epsilon,
              defaultDelta =
                metricSpecConfig.reachAndFrequencyParams.multipleDataProviderParams
                  .frequencyPrivacyParams
                  .delta,
            )
          vidSamplingInterval =
            deprecatedVidSamplingInterval
              ?: metricSpecConfig.reachAndFrequencyParams.multipleDataProviderParams
                .vidSamplingInterval
                .toVidSamplingInterval(secureRandom)
          vidSamplingInterval.validate()
        }
      clearReachPrivacyParams()
      clearFrequencyPrivacyParams()
    } else {
      throw MetricSpecDefaultsException(
        "Invalid params",
        IllegalArgumentException(
          "reach_privacy_params or frequency_privacy_params in reach and frequency is not set, or fields in single_data_provider_params and multiple_data_provider_params not set."
        ),
      )
    }
  }
}

/**
 * Specifies default values using [MetricSpecConfig] when optional fields in the
 * [MetricSpec.WatchDurationParams] are not set.
 */
private fun MetricSpec.WatchDurationParams.withDefaults(
  deprecatedVidSamplingInterval: MetricSpec.VidSamplingInterval?,
  metricSpecConfig: MetricSpecConfig,
  secureRandom: Random,
): MetricSpec.WatchDurationParams {
  return copy {
    maximumWatchDurationPerUser =
      if (hasMaximumWatchDurationPerUser()) {
        maximumWatchDurationPerUser
      } else {
        metricSpecConfig.watchDurationParams.maximumWatchDurationPerUser
      }

    if (params.hasPrivacyParams() && params.hasVidSamplingInterval()) {
      clearPrivacyParams()
      params =
        params.copy {
          privacyParams =
            privacyParams.withDefaults(
              defaultEpsilon = metricSpecConfig.watchDurationParams.params.privacyParams.epsilon,
              defaultDelta = metricSpecConfig.watchDurationParams.params.privacyParams.delta,
            )
          vidSamplingInterval =
            vidSamplingInterval.withDefaults(
              metricSpecConfig.watchDurationParams.params.vidSamplingInterval.toVidSamplingInterval(
                secureRandom
              )
            )
          vidSamplingInterval.validate()
        }
    } else if (hasPrivacyParams() && !hasParams()) {
      val source = this
      params =
        params.copy {
          privacyParams =
            source.privacyParams.withDefaults(
              defaultEpsilon = metricSpecConfig.watchDurationParams.params.privacyParams.epsilon,
              defaultDelta = metricSpecConfig.watchDurationParams.params.privacyParams.delta,
            )
          vidSamplingInterval =
            deprecatedVidSamplingInterval
              ?: metricSpecConfig.watchDurationParams.params.vidSamplingInterval
                .toVidSamplingInterval(secureRandom)
          vidSamplingInterval.validate()
        }
      clearPrivacyParams()
    } else {
      throw MetricSpecDefaultsException(
        "Invalid params",
        IllegalArgumentException(
          "privacy_params in watch duration is not set, or fields in params not set."
        ),
      )
    }
  }
}

/**
 * Specifies default values using [MetricSpecConfig] when optional fields in the
 * [MetricSpec.ImpressionCountParams] are not set.
 */
private fun MetricSpec.ImpressionCountParams.withDefaults(
  deprecatedVidSamplingInterval: MetricSpec.VidSamplingInterval?,
  metricSpecConfig: MetricSpecConfig,
  secureRandom: Random,
): MetricSpec.ImpressionCountParams {
  return copy {
    maximumFrequencyPerUser =
      if (hasMaximumFrequencyPerUser()) {
        maximumFrequencyPerUser
      } else {
        metricSpecConfig.impressionCountParams.maximumFrequencyPerUser
      }

    if (params.hasPrivacyParams() && params.hasVidSamplingInterval()) {
      clearPrivacyParams()
      params =
        params.copy {
          privacyParams =
            privacyParams.withDefaults(
              defaultEpsilon = metricSpecConfig.impressionCountParams.params.privacyParams.epsilon,
              defaultDelta = metricSpecConfig.impressionCountParams.params.privacyParams.delta,
            )
          vidSamplingInterval =
            vidSamplingInterval.withDefaults(
              metricSpecConfig.impressionCountParams.params.vidSamplingInterval
                .toVidSamplingInterval(secureRandom)
            )
          vidSamplingInterval.validate()
        }
    } else if (hasPrivacyParams() && !hasParams()) {
      val source = this
      params =
        params.copy {
          privacyParams =
            source.privacyParams.withDefaults(
              defaultEpsilon = metricSpecConfig.impressionCountParams.params.privacyParams.epsilon,
              defaultDelta = metricSpecConfig.impressionCountParams.params.privacyParams.delta,
            )
          vidSamplingInterval =
            deprecatedVidSamplingInterval
              ?: metricSpecConfig.impressionCountParams.params.vidSamplingInterval
                .toVidSamplingInterval(secureRandom)
          vidSamplingInterval.validate()
        }
      clearPrivacyParams()
    } else {
      throw MetricSpecDefaultsException(
        "Invalid params",
        IllegalArgumentException(
          "privacy_params in impression count is not set, or fields in params not set."
        ),
      )
    }
  }
}

/**
 * Specifies the values in the optional fields of [MetricSpec.DifferentialPrivacyParams] when they
 * are not set.
 */
private fun MetricSpec.DifferentialPrivacyParams.withDefaults(
  defaultEpsilon: Double,
  defaultDelta: Double,
): MetricSpec.DifferentialPrivacyParams {
  return copy {
    epsilon = if (hasEpsilon()) epsilon else defaultEpsilon
    delta = if (hasDelta()) delta else defaultDelta
  }
}

/**
 * Specifies the values in the optional fields of [MetricSpec.VidSamplingInterval] when they are not
 * set.
 */
private fun MetricSpec.VidSamplingInterval.withDefaults(
  defaultVidSamplingInterval: MetricSpec.VidSamplingInterval
): MetricSpec.VidSamplingInterval {
  return copy {
    start = if (hasStart()) start else defaultVidSamplingInterval.start
    width = if (hasWidth()) width else defaultVidSamplingInterval.width
  }
}

/** Converts an [MetricSpecConfig.VidSamplingInterval] to a [MetricSpec.VidSamplingInterval]. */
private fun MetricSpecConfig.VidSamplingInterval.toVidSamplingInterval(
  secureRandom: Random
): MetricSpec.VidSamplingInterval {
  val source = this
  if (source.hasFixedStart()) {
    return MetricSpecKt.vidSamplingInterval {
      start = source.fixedStart.start
      width = source.fixedStart.width
    }
  } else {
    val maxStart = NUM_BUCKETS - (source.randomStart.width * NUM_BUCKETS).toInt()
    // The `- 1` is in case the rounding from source.randomStart.width * NUM_BUCKETS rounds down.
    // This
    // prevents the random start from being too big if rounding down does occur.
    val randomStart = secureRandom.nextInt(maxStart - 1)
    return MetricSpecKt.vidSamplingInterval {
      start = randomStart.toFloat() / NUM_BUCKETS
      width = source.randomStart.width
    }
  }
}

/**
 * Validates a [MetricSpec.VidSamplingInterval].
 *
 * @throws [IllegalArgumentException] if validation fails.
 */
private fun MetricSpec.VidSamplingInterval.validate() {
  if (this.start < 0) {
    throw MetricSpecDefaultsException(
      "Invalid vidSamplingInterval",
      IllegalArgumentException("vidSamplingInterval.start cannot be negative."),
    )
  }
  if (this.start >= 1) {
    throw MetricSpecDefaultsException(
      "Invalid vidSamplingInterval",
      IllegalArgumentException("vidSamplingInterval.start must be smaller than 1."),
    )
  }
  if (this.width <= 0) {
    throw MetricSpecDefaultsException(
      "Invalid vidSamplingInterval",
      IllegalArgumentException("vidSamplingInterval.width must be greater than 0."),
    )
  }
  if (this.start + this.width > 1) {
    throw MetricSpecDefaultsException(
      "Invalid vidSamplingInterval",
      IllegalArgumentException("vidSamplingInterval start + width cannot be greater than 1."),
    )
  }
}
