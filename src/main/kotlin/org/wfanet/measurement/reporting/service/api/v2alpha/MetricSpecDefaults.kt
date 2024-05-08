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
 * Specifies default values using [MetricSpecConfig] when optional fields in the [MetricSpec] are
 * not set.
 */
fun MetricSpec.withDefaults(metricSpecConfig: MetricSpecConfig, secureRandom: Random): MetricSpec {
  return copy {
    when (typeCase) {
      MetricSpec.TypeCase.REACH -> {
        reach = reach.withDefaults(this@withDefaults, metricSpecConfig, secureRandom)
      }
      MetricSpec.TypeCase.REACH_AND_FREQUENCY -> {
        reachAndFrequency =
          reachAndFrequency.withDefaults(this@withDefaults, metricSpecConfig, secureRandom)
      }
      MetricSpec.TypeCase.IMPRESSION_COUNT -> {
        impressionCount =
          impressionCount.withDefaults(this@withDefaults, metricSpecConfig, secureRandom)
      }
      MetricSpec.TypeCase.WATCH_DURATION -> {
        watchDuration =
          watchDuration.withDefaults(this@withDefaults, metricSpecConfig, secureRandom)
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
  metricSpec: MetricSpec,
  metricSpecConfig: MetricSpecConfig,
  secureRandom: Random,
): MetricSpec.ReachParams {
  return copy {
    if (hasMultipleDataProviderParams() && hasSingleDataProviderParams()) {
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
            if (hasVidSamplingInterval()) {
              vidSamplingInterval
            } else {
              metricSpecConfig.reachParams.multipleDataProviderParams.vidSamplingInterval
                .toVidSamplingInterval(secureRandom)
            }
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
            if (hasVidSamplingInterval()) {
              vidSamplingInterval
            } else {
              metricSpecConfig.reachParams.singleDataProviderParams.vidSamplingInterval
                .toVidSamplingInterval(secureRandom)
            }
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
            if (metricSpec.hasVidSamplingInterval()) {
              metricSpec.vidSamplingInterval
            } else {
              metricSpecConfig.reachParams.multipleDataProviderParams.vidSamplingInterval
                .toVidSamplingInterval(secureRandom)
            }
          vidSamplingInterval.validate()
        }
      clearPrivacyParams()
    } else {
      throw MetricSpecDefaultsException(
        "Invalid privacy_params",
        IllegalArgumentException(
          "privacy_params in reach is not set, or only one of single_data_provider_params and multiple_data_provider_params set."
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
  metricSpec: MetricSpec,
  metricSpecConfig: MetricSpecConfig,
  secureRandom: Random,
): MetricSpec.ReachAndFrequencyParams {
  return copy {
    if (maximumFrequency == 0) {
      maximumFrequency = metricSpecConfig.reachAndFrequencyParams.maximumFrequency
    }

    if (hasMultipleDataProviderParams() && hasSingleDataProviderParams()) {
      clearReachPrivacyParams()
      clearFrequencyPrivacyParams()
      multipleDataProviderParams =
        multipleDataProviderParams.copy {
          privacyParams =
            privacyParams.withDefaults(
              defaultEpsilon =
                metricSpecConfig.reachAndFrequencyParams.multipleDataProviderParams.privacyParams
                  .epsilon,
              defaultDelta =
                metricSpecConfig.reachAndFrequencyParams.multipleDataProviderParams.privacyParams
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
            if (hasVidSamplingInterval()) {
              vidSamplingInterval
            } else {
              metricSpecConfig.reachAndFrequencyParams.multipleDataProviderParams
                .vidSamplingInterval
                .toVidSamplingInterval(secureRandom)
            }
          vidSamplingInterval.validate()
        }

      singleDataProviderParams =
        singleDataProviderParams.copy {
          privacyParams =
            privacyParams.withDefaults(
              defaultEpsilon =
                metricSpecConfig.reachAndFrequencyParams.singleDataProviderParams.privacyParams
                  .epsilon,
              defaultDelta =
                metricSpecConfig.reachAndFrequencyParams.singleDataProviderParams.privacyParams
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
            if (hasVidSamplingInterval()) {
              vidSamplingInterval
            } else {
              metricSpecConfig.reachAndFrequencyParams.singleDataProviderParams.vidSamplingInterval
                .toVidSamplingInterval(secureRandom)
            }
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
          privacyParams =
            source.reachPrivacyParams.withDefaults(
              defaultEpsilon =
                metricSpecConfig.reachAndFrequencyParams.multipleDataProviderParams.privacyParams
                  .epsilon,
              defaultDelta =
                metricSpecConfig.reachAndFrequencyParams.multipleDataProviderParams.privacyParams
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
            if (metricSpec.hasVidSamplingInterval()) {
              metricSpec.vidSamplingInterval
            } else {
              metricSpecConfig.reachAndFrequencyParams.multipleDataProviderParams
                .vidSamplingInterval
                .toVidSamplingInterval(secureRandom)
            }
          vidSamplingInterval.validate()
        }
      clearReachPrivacyParams()
      clearFrequencyPrivacyParams()
    } else {
      throw MetricSpecDefaultsException(
        "Invalid privacy_params",
        IllegalArgumentException(
          "reach_privacy_params or frequency_privacy_params in reach and frequency is not set, or only one of single_data_provider_params and multiple_data_provider_params set."
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
  metricSpec: MetricSpec,
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

    if (hasParams()) {
      clearPrivacyParams()
      params =
        params.copy {
          privacyParams =
            privacyParams.withDefaults(
              defaultEpsilon = metricSpecConfig.watchDurationParams.params.privacyParams.epsilon,
              defaultDelta = metricSpecConfig.watchDurationParams.params.privacyParams.delta,
            )
          vidSamplingInterval =
            if (hasVidSamplingInterval()) {
              vidSamplingInterval
            } else {
              metricSpecConfig.watchDurationParams.params.vidSamplingInterval.toVidSamplingInterval(
                secureRandom
              )
            }
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
            if (metricSpec.hasVidSamplingInterval()) {
              metricSpec.vidSamplingInterval
            } else {
              metricSpecConfig.watchDurationParams.params.vidSamplingInterval.toVidSamplingInterval(
                secureRandom
              )
            }
          vidSamplingInterval.validate()
        }
      clearPrivacyParams()
    } else {
      throw MetricSpecDefaultsException(
        "Invalid privacy_params",
        IllegalArgumentException("privacy_params in watch duration is not set."),
      )
    }
  }
}

/**
 * Specifies default values using [MetricSpecConfig] when optional fields in the
 * [MetricSpec.ImpressionCountParams] are not set.
 */
private fun MetricSpec.ImpressionCountParams.withDefaults(
  metricSpec: MetricSpec,
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

    if (hasParams()) {
      clearPrivacyParams()
      params =
        params.copy {
          privacyParams =
            privacyParams.withDefaults(
              defaultEpsilon = metricSpecConfig.impressionCountParams.params.privacyParams.epsilon,
              defaultDelta = metricSpecConfig.impressionCountParams.params.privacyParams.delta,
            )
          vidSamplingInterval =
            if (hasVidSamplingInterval()) {
              vidSamplingInterval
            } else {
              metricSpecConfig.impressionCountParams.params.vidSamplingInterval
                .toVidSamplingInterval(secureRandom)
            }
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
            if (metricSpec.hasVidSamplingInterval()) {
              metricSpec.vidSamplingInterval
            } else {
              metricSpecConfig.impressionCountParams.params.vidSamplingInterval
                .toVidSamplingInterval(secureRandom)
            }
          vidSamplingInterval.validate()
        }
      clearPrivacyParams()
    } else {
      throw MetricSpecDefaultsException(
        "Invalid privacy_params",
        IllegalArgumentException("privacy_params in impression count is not set."),
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

/** Converts an [MetricSpecConfig.VidSamplingInterval] to an [MetricSpec.VidSamplingInterval]. */
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
