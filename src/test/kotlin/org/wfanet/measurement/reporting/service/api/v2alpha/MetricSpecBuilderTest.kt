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

import com.google.common.truth.Truth.assertThat
import org.junit.Assert.assertThrows
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.config.reporting.MetricSpecConfigKt
import org.wfanet.measurement.config.reporting.metricSpecConfig
import org.wfanet.measurement.reporting.v2alpha.MetricSpec
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt.frequencyHistogramParams
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt.impressionCountParams
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt.reachParams
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt.watchDurationParams
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.metricSpec

private const val NUMBER_VID_BUCKETS = 300
private const val REACH_ONLY_VID_SAMPLING_WIDTH = 3.0f / NUMBER_VID_BUCKETS
private const val REACH_ONLY_VID_SAMPLING_START = 0.0f
private const val REACH_ONLY_REACH_EPSILON = 0.0041

private const val REACH_FREQUENCY_VID_SAMPLING_WIDTH = 5.0f / NUMBER_VID_BUCKETS
private const val REACH_FREQUENCY_VID_SAMPLING_START = 48.0f / NUMBER_VID_BUCKETS
private const val REACH_FREQUENCY_REACH_EPSILON = 0.0033
private const val REACH_FREQUENCY_FREQUENCY_EPSILON = 0.115
private const val REACH_FREQUENCY_MAXIMUM_FREQUENCY_PER_USER = 10

private const val IMPRESSION_VID_SAMPLING_WIDTH = 62.0f / NUMBER_VID_BUCKETS
private const val IMPRESSION_VID_SAMPLING_START = 143.0f / NUMBER_VID_BUCKETS
private const val IMPRESSION_EPSILON = 0.0011
private const val IMPRESSION_MAXIMUM_FREQUENCY_PER_USER = 60

private const val WATCH_DURATION_VID_SAMPLING_WIDTH = 95.0f / NUMBER_VID_BUCKETS
private const val WATCH_DURATION_VID_SAMPLING_START = 205.0f / NUMBER_VID_BUCKETS
private const val WATCH_DURATION_EPSILON = 0.001
private const val MAXIMUM_WATCH_DURATION_PER_USER = 4000

private const val DIFFERENTIAL_PRIVACY_DELTA = 1e-12

@RunWith(JUnit4::class)
class MetricSpecBuilderTest {
  private lateinit var metricSpecBuilder: MetricSpecBuilder

  @Before
  fun initService() {
    metricSpecBuilder = MetricSpecBuilder(METRIC_SPEC_CONFIG)
  }

  @Test
  fun `buildMetricSpec builds a reach metric spec when no field is filled`() {
    val result = metricSpecBuilder.buildMetricSpec(REACH_METRIC_SPEC)
    val expect = metricSpec {
      reach = reachParams {
        privacyParams =
          MetricSpecKt.differentialPrivacyParams {
            epsilon = METRIC_SPEC_CONFIG.reachParams.privacyParams.epsilon
            delta = METRIC_SPEC_CONFIG.reachParams.privacyParams.delta
          }
      }
      vidSamplingInterval =
        MetricSpecKt.vidSamplingInterval {
          start = METRIC_SPEC_CONFIG.reachVidSamplingInterval.start
          width = METRIC_SPEC_CONFIG.reachVidSamplingInterval.width
        }
    }
    assertThat(result).isEqualTo(expect)
  }

  @Test
  fun `buildMetricSpec builds a reach metric spec when all fields are filled`() {
    val expect = metricSpec {
      reach = reachParams {
        privacyParams =
          MetricSpecKt.differentialPrivacyParams {
            epsilon = METRIC_SPEC_CONFIG.reachParams.privacyParams.epsilon
            delta = METRIC_SPEC_CONFIG.reachParams.privacyParams.delta
          }
      }
      vidSamplingInterval =
        MetricSpecKt.vidSamplingInterval {
          start = METRIC_SPEC_CONFIG.reachVidSamplingInterval.start
          width = METRIC_SPEC_CONFIG.reachVidSamplingInterval.width
        }
    }

    val result = metricSpecBuilder.buildMetricSpec(expect)

    assertThat(result).isEqualTo(expect)
  }

  @Test
  fun `buildMetricSpec builds a frequency histogram metric spec when no field is filled`() {
    val result = metricSpecBuilder.buildMetricSpec(FREQUENCY_HISTOGRAM_METRIC_SPEC)
    val expect = metricSpec {
      frequencyHistogram = frequencyHistogramParams {
        reachPrivacyParams =
          MetricSpecKt.differentialPrivacyParams {
            epsilon = METRIC_SPEC_CONFIG.frequencyHistogramParams.reachPrivacyParams.epsilon
            delta = METRIC_SPEC_CONFIG.frequencyHistogramParams.reachPrivacyParams.delta
          }
        frequencyPrivacyParams =
          MetricSpecKt.differentialPrivacyParams {
            epsilon = METRIC_SPEC_CONFIG.frequencyHistogramParams.frequencyPrivacyParams.epsilon
            delta = METRIC_SPEC_CONFIG.frequencyHistogramParams.frequencyPrivacyParams.delta
          }
        maximumFrequencyPerUser =
          METRIC_SPEC_CONFIG.frequencyHistogramParams.maximumFrequencyPerUser
      }
      vidSamplingInterval =
        MetricSpecKt.vidSamplingInterval {
          start = METRIC_SPEC_CONFIG.frequencyHistogramVidSamplingInterval.start
          width = METRIC_SPEC_CONFIG.frequencyHistogramVidSamplingInterval.width
        }
    }
    assertThat(result).isEqualTo(expect)
  }

  @Test
  fun `buildMetricSpec builds a frequency histogram metric spec when all fields are filled`() {
    val expect = metricSpec {
      frequencyHistogram = frequencyHistogramParams {
        reachPrivacyParams =
          MetricSpecKt.differentialPrivacyParams {
            epsilon = METRIC_SPEC_CONFIG.frequencyHistogramParams.reachPrivacyParams.epsilon
            delta = METRIC_SPEC_CONFIG.frequencyHistogramParams.reachPrivacyParams.delta
          }
        frequencyPrivacyParams =
          MetricSpecKt.differentialPrivacyParams {
            epsilon = METRIC_SPEC_CONFIG.frequencyHistogramParams.frequencyPrivacyParams.epsilon
            delta = METRIC_SPEC_CONFIG.frequencyHistogramParams.frequencyPrivacyParams.delta
          }
        maximumFrequencyPerUser =
          METRIC_SPEC_CONFIG.frequencyHistogramParams.maximumFrequencyPerUser
      }
      vidSamplingInterval =
        MetricSpecKt.vidSamplingInterval {
          start = METRIC_SPEC_CONFIG.frequencyHistogramVidSamplingInterval.start
          width = METRIC_SPEC_CONFIG.frequencyHistogramVidSamplingInterval.width
        }
    }
    val result = metricSpecBuilder.buildMetricSpec(expect)
    assertThat(result).isEqualTo(expect)
  }

  @Test
  fun `buildMetricSpec builds an impression count metric spec when no field is filled`() {
    val result = metricSpecBuilder.buildMetricSpec(IMPRESSION_COUNT_METRIC_SPEC)
    val expect = metricSpec {
      impressionCount = impressionCountParams {
        privacyParams =
          MetricSpecKt.differentialPrivacyParams {
            epsilon = METRIC_SPEC_CONFIG.impressionCountParams.privacyParams.epsilon
            delta = METRIC_SPEC_CONFIG.impressionCountParams.privacyParams.delta
          }
        maximumFrequencyPerUser = METRIC_SPEC_CONFIG.impressionCountParams.maximumFrequencyPerUser
      }
      vidSamplingInterval =
        MetricSpecKt.vidSamplingInterval {
          start = METRIC_SPEC_CONFIG.impressionCountVidSamplingInterval.start
          width = METRIC_SPEC_CONFIG.impressionCountVidSamplingInterval.width
        }
    }
    assertThat(result).isEqualTo(expect)
  }

  @Test
  fun `buildMetricSpec builds an impression count metric spec when all fields are filled`() {
    val expect = metricSpec {
      impressionCount = impressionCountParams {
        privacyParams =
          MetricSpecKt.differentialPrivacyParams {
            epsilon = METRIC_SPEC_CONFIG.impressionCountParams.privacyParams.epsilon
            delta = METRIC_SPEC_CONFIG.impressionCountParams.privacyParams.delta
          }
        maximumFrequencyPerUser = METRIC_SPEC_CONFIG.impressionCountParams.maximumFrequencyPerUser
      }
      vidSamplingInterval =
        MetricSpecKt.vidSamplingInterval {
          start = METRIC_SPEC_CONFIG.impressionCountVidSamplingInterval.start
          width = METRIC_SPEC_CONFIG.impressionCountVidSamplingInterval.width
        }
    }
    val result = metricSpecBuilder.buildMetricSpec(expect)
    assertThat(result).isEqualTo(expect)
  }

  @Test
  fun `buildMetricSpec builds a watch duration metric spec when no field is filled`() {
    val result = metricSpecBuilder.buildMetricSpec(WATCH_DURATION_METRIC_SPEC)
    val expect = metricSpec {
      watchDuration = watchDurationParams {
        privacyParams =
          MetricSpecKt.differentialPrivacyParams {
            epsilon = METRIC_SPEC_CONFIG.watchDurationParams.privacyParams.epsilon
            delta = METRIC_SPEC_CONFIG.watchDurationParams.privacyParams.delta
          }
        maximumWatchDurationPerUser =
          METRIC_SPEC_CONFIG.watchDurationParams.maximumWatchDurationPerUser
      }
      vidSamplingInterval =
        MetricSpecKt.vidSamplingInterval {
          start = METRIC_SPEC_CONFIG.watchDurationVidSamplingInterval.start
          width = METRIC_SPEC_CONFIG.watchDurationVidSamplingInterval.width
        }
    }
    assertThat(result).isEqualTo(expect)
  }

  @Test
  fun `buildMetricSpec builds a watch duration metric spec when all fields are filled`() {
    val expect = metricSpec {
      watchDuration = watchDurationParams {
        privacyParams =
          MetricSpecKt.differentialPrivacyParams {
            epsilon = METRIC_SPEC_CONFIG.watchDurationParams.privacyParams.epsilon
            delta = METRIC_SPEC_CONFIG.watchDurationParams.privacyParams.delta
          }
        maximumWatchDurationPerUser =
          METRIC_SPEC_CONFIG.watchDurationParams.maximumWatchDurationPerUser
      }
      vidSamplingInterval =
        MetricSpecKt.vidSamplingInterval {
          start = METRIC_SPEC_CONFIG.watchDurationVidSamplingInterval.start
          width = METRIC_SPEC_CONFIG.watchDurationVidSamplingInterval.width
        }
    }
    val result = metricSpecBuilder.buildMetricSpec(expect)
    assertThat(result).isEqualTo(expect)
  }

  @Test
  fun `buildMetricSpec throw MetricSpecBuildingException when no metric type specified`() {
    val metricSpecWithoutType = metricSpec {}

    val exception =
      assertThrows(MetricSpecBuildingException::class.java) {
        metricSpecBuilder.buildMetricSpec(metricSpecWithoutType)
      }
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception).hasMessageThat().contains("metric spec type")
  }

  @Test
  fun `buildMetricSpec throw MetricSpecBuildingException when reach privacy params is not set`() {
    val metricSpecWithoutPrivacyParams = metricSpec { reach = reachParams {} }

    val exception =
      assertThrows(MetricSpecBuildingException::class.java) {
        metricSpecBuilder.buildMetricSpec(metricSpecWithoutPrivacyParams)
      }
    assertThat(exception).hasMessageThat().contains("privacy params")
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception.cause).hasMessageThat().contains("reach")
  }

  @Test
  fun `buildMetricSpec throw MetricSpecBuildingException when frequency histogram privacy params is not set`() {
    val metricSpecWithoutPrivacyParams =
      FREQUENCY_HISTOGRAM_METRIC_SPEC.copy {
        frequencyHistogram = frequencyHistogram.copy { clearReachPrivacyParams() }
      }

    val exception =
      assertThrows(MetricSpecBuildingException::class.java) {
        metricSpecBuilder.buildMetricSpec(metricSpecWithoutPrivacyParams)
      }
    assertThat(exception).hasMessageThat().contains("privacy params")
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception.cause).hasMessageThat().contains("reachPrivacyParams")
  }

  @Test
  fun `buildMetricSpec throw MetricSpecBuildingException when reach privacy params in frequency histogram is not set`() {
    val metricSpecWithoutPrivacyParams =
      FREQUENCY_HISTOGRAM_METRIC_SPEC.copy {
        frequencyHistogram = frequencyHistogram.copy { clearFrequencyPrivacyParams() }
      }

    val exception =
      assertThrows(MetricSpecBuildingException::class.java) {
        metricSpecBuilder.buildMetricSpec(metricSpecWithoutPrivacyParams)
      }
    assertThat(exception).hasMessageThat().contains("privacy params")
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception.cause).hasMessageThat().contains("frequencyPrivacyParams")
  }

  @Test
  fun `buildMetricSpec throw MetricSpecBuildingException when impression privacy params is not set`() {
    val metricSpecWithoutPrivacyParams = metricSpec { impressionCount = impressionCountParams {} }

    val exception =
      assertThrows(MetricSpecBuildingException::class.java) {
        metricSpecBuilder.buildMetricSpec(metricSpecWithoutPrivacyParams)
      }
    assertThat(exception).hasMessageThat().contains("privacy params")
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception.cause).hasMessageThat().contains("impression count")
  }

  @Test
  fun `buildMetricSpec throw MetricSpecBuildingException when watch duration privacy params is not set`() {
    val metricSpecWithoutPrivacyParams = metricSpec { watchDuration = watchDurationParams {} }

    val exception =
      assertThrows(MetricSpecBuildingException::class.java) {
        metricSpecBuilder.buildMetricSpec(metricSpecWithoutPrivacyParams)
      }
    assertThat(exception).hasMessageThat().contains("privacy params")
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception.cause).hasMessageThat().contains("watch duration")
  }

  @Test
  fun `buildMetricSpec throw MetricSpecBuildingException when vidSamplingInterval start is less than 0`() {
    val metricSpec =
      REACH_METRIC_SPEC.copy {
        vidSamplingInterval = MetricSpecKt.vidSamplingInterval { start = -1.0f }
      }

    val exception =
      assertThrows(MetricSpecBuildingException::class.java) {
        metricSpecBuilder.buildMetricSpec(metricSpec)
      }
    assertThat(exception).hasMessageThat().contains("vidSamplingInterval")
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception.cause).hasMessageThat().contains("start")
  }

  @Test
  fun `buildMetricSpec throw MetricSpecBuildingException when vidSamplingInterval start is not less than 1`() {
    val metricSpec =
      REACH_METRIC_SPEC.copy {
        vidSamplingInterval = MetricSpecKt.vidSamplingInterval { start = 1.0f }
      }

    val exception =
      assertThrows(MetricSpecBuildingException::class.java) {
        metricSpecBuilder.buildMetricSpec(metricSpec)
      }
    assertThat(exception).hasMessageThat().contains("vidSamplingInterval")
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception.cause).hasMessageThat().contains("start")
  }

  @Test
  fun `buildMetricSpec throw MetricSpecBuildingException when vidSamplingInterval width is not larger than 0`() {
    val metricSpec =
      REACH_METRIC_SPEC.copy {
        vidSamplingInterval = MetricSpecKt.vidSamplingInterval { width = 0f }
      }

    val exception =
      assertThrows(MetricSpecBuildingException::class.java) {
        metricSpecBuilder.buildMetricSpec(metricSpec)
      }
    assertThat(exception).hasMessageThat().contains("vidSamplingInterval")
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception.cause).hasMessageThat().contains("width")
  }

  @Test
  fun `buildMetricSpec throw MetricSpecBuildingException when vidSamplingInterval is too long`() {
    val metricSpec =
      REACH_METRIC_SPEC.copy {
        vidSamplingInterval =
          MetricSpecKt.vidSamplingInterval {
            start = 0.5f
            width = 1.0f
          }
      }

    val exception =
      assertThrows(MetricSpecBuildingException::class.java) {
        metricSpecBuilder.buildMetricSpec(metricSpec)
      }
    assertThat(exception).hasMessageThat().contains("vidSamplingInterval")
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception.cause).hasMessageThat().contains("start + width")
  }

  companion object {
    private val METRIC_SPEC_CONFIG = metricSpecConfig {
      reachParams =
        MetricSpecConfigKt.reachParams {
          privacyParams =
            MetricSpecConfigKt.differentialPrivacyParams {
              epsilon = REACH_ONLY_REACH_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
        }
      reachVidSamplingInterval =
        MetricSpecConfigKt.vidSamplingInterval {
          start = REACH_ONLY_VID_SAMPLING_START
          width = REACH_ONLY_VID_SAMPLING_WIDTH
        }

      frequencyHistogramParams =
        MetricSpecConfigKt.frequencyHistogramParams {
          reachPrivacyParams =
            MetricSpecConfigKt.differentialPrivacyParams {
              epsilon = REACH_FREQUENCY_REACH_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
          frequencyPrivacyParams =
            MetricSpecConfigKt.differentialPrivacyParams {
              epsilon = REACH_FREQUENCY_FREQUENCY_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
          maximumFrequencyPerUser = REACH_FREQUENCY_MAXIMUM_FREQUENCY_PER_USER
        }
      frequencyHistogramVidSamplingInterval =
        MetricSpecConfigKt.vidSamplingInterval {
          start = REACH_FREQUENCY_VID_SAMPLING_START
          width = REACH_FREQUENCY_VID_SAMPLING_WIDTH
        }

      impressionCountParams =
        MetricSpecConfigKt.impressionCountParams {
          privacyParams =
            MetricSpecConfigKt.differentialPrivacyParams {
              epsilon = IMPRESSION_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
          maximumFrequencyPerUser = IMPRESSION_MAXIMUM_FREQUENCY_PER_USER
        }
      impressionCountVidSamplingInterval =
        MetricSpecConfigKt.vidSamplingInterval {
          start = IMPRESSION_VID_SAMPLING_START
          width = IMPRESSION_VID_SAMPLING_WIDTH
        }

      watchDurationParams =
        MetricSpecConfigKt.watchDurationParams {
          privacyParams =
            MetricSpecConfigKt.differentialPrivacyParams {
              epsilon = WATCH_DURATION_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
          maximumWatchDurationPerUser = MAXIMUM_WATCH_DURATION_PER_USER
        }
      watchDurationVidSamplingInterval =
        MetricSpecConfigKt.vidSamplingInterval {
          start = WATCH_DURATION_VID_SAMPLING_START
          width = WATCH_DURATION_VID_SAMPLING_WIDTH
        }
    }

    // Metric Specs

    private val REACH_METRIC_SPEC: MetricSpec = metricSpec {
      reach = reachParams {
        privacyParams = MetricSpec.DifferentialPrivacyParams.getDefaultInstance()
      }
    }
    private val FREQUENCY_HISTOGRAM_METRIC_SPEC: MetricSpec = metricSpec {
      frequencyHistogram = frequencyHistogramParams {
        reachPrivacyParams = MetricSpec.DifferentialPrivacyParams.getDefaultInstance()
        frequencyPrivacyParams = MetricSpec.DifferentialPrivacyParams.getDefaultInstance()
      }
    }
    private val IMPRESSION_COUNT_METRIC_SPEC: MetricSpec = metricSpec {
      impressionCount = impressionCountParams {
        privacyParams = MetricSpec.DifferentialPrivacyParams.getDefaultInstance()
      }
    }
    private val WATCH_DURATION_METRIC_SPEC: MetricSpec = metricSpec {
      watchDuration = watchDurationParams {
        privacyParams = MetricSpec.DifferentialPrivacyParams.getDefaultInstance()
      }
    }
  }
}
