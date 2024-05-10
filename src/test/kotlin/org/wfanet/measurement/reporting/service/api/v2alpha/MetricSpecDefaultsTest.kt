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
import com.google.protobuf.util.Durations
import kotlin.random.Random
import org.junit.Assert.assertThrows
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.stub
import org.wfanet.measurement.config.reporting.MetricSpecConfigKt
import org.wfanet.measurement.config.reporting.copy
import org.wfanet.measurement.config.reporting.metricSpecConfig
import org.wfanet.measurement.reporting.v2alpha.MetricSpec
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt.impressionCountParams
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt.reachAndFrequencyParams
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt.reachParams
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt.watchDurationParams
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.metricSpec

private const val NUMBER_VID_BUCKETS = 300
private const val REACH_ONLY_VID_SAMPLING_WIDTH = 3.0f / NUMBER_VID_BUCKETS
private const val REACH_ONLY_VID_SAMPLING_START = 0.0f
private const val REACH_ONLY_REACH_EPSILON = 0.0041
private const val SINGLE_DATA_PROVIDER_REACH_ONLY_VID_SAMPLING_WIDTH = 3.1f / NUMBER_VID_BUCKETS
private const val SINGLE_DATA_PROVIDER_REACH_ONLY_VID_SAMPLING_START = 0.01f
private const val SINGLE_DATA_PROVIDER_REACH_ONLY_REACH_EPSILON = 0.0042

private const val REACH_FREQUENCY_VID_SAMPLING_WIDTH = 5.0f / NUMBER_VID_BUCKETS
private const val REACH_FREQUENCY_VID_SAMPLING_START = 48.0f / NUMBER_VID_BUCKETS
private const val REACH_FREQUENCY_REACH_EPSILON = 0.0033
private const val REACH_FREQUENCY_FREQUENCY_EPSILON = 0.115
private const val REACH_FREQUENCY_MAXIMUM_FREQUENCY = 10
private const val SINGLE_DATA_PROVIDER_REACH_FREQUENCY_VID_SAMPLING_WIDTH =
  5.1f / NUMBER_VID_BUCKETS
private const val SINGLE_DATA_PROVIDER_REACH_FREQUENCY_VID_SAMPLING_START =
  48.1f / NUMBER_VID_BUCKETS
private const val SINGLE_DATA_PROVIDER_REACH_FREQUENCY_REACH_EPSILON = 0.0034
private const val SINGLE_DATA_PROVIDER_REACH_FREQUENCY_FREQUENCY_EPSILON = 0.116

private const val IMPRESSION_VID_SAMPLING_WIDTH = 62.0f / NUMBER_VID_BUCKETS
private const val IMPRESSION_VID_SAMPLING_START = 143.0f / NUMBER_VID_BUCKETS
private const val IMPRESSION_EPSILON = 0.0011
private const val IMPRESSION_MAXIMUM_FREQUENCY_PER_USER = 60

private const val WATCH_DURATION_VID_SAMPLING_WIDTH = 95.0f / NUMBER_VID_BUCKETS
private const val WATCH_DURATION_VID_SAMPLING_START = 205.0f / NUMBER_VID_BUCKETS
private const val WATCH_DURATION_EPSILON = 0.001
private val MAXIMUM_WATCH_DURATION_PER_USER = Durations.fromMinutes(5)

private const val DIFFERENTIAL_PRIVACY_DELTA = 1e-12

@RunWith(JUnit4::class)
class MetricSpecDefaultsTest {
  private val randomMock: Random = mock()

  @Test
  fun `buildMetricSpec builds a reach metric spec when no field is filled in privacy_params`() {
    val result = OLD_EMPTY_REACH_METRIC_SPEC.withDefaults(METRIC_SPEC_CONFIG, randomMock)
    val expected = metricSpec {
      reach = reachParams {
        multipleDataProviderParams =
          MetricSpecKt.samplingAndPrivacyParams {
            privacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon =
                  METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.privacyParams.epsilon
                delta =
                  METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.privacyParams.delta
              }
            vidSamplingInterval =
              MetricSpecKt.vidSamplingInterval {
                start =
                  METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.vidSamplingInterval
                    .fixedStart
                    .start
                width =
                  METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.vidSamplingInterval
                    .fixedStart
                    .width
              }
          }
      }
    }
    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `buildMetricSpec builds a reach metric spec when all fields are filled in privacy_params`() {
    val initial = metricSpec {
      reach = reachParams {
        privacyParams =
          MetricSpecKt.differentialPrivacyParams {
            epsilon =
              METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.privacyParams.epsilon * 2
            delta =
              METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.privacyParams.delta * 2
          }
      }
      vidSamplingInterval =
        MetricSpecKt.vidSamplingInterval {
          start =
            METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.vidSamplingInterval.fixedStart
              .start + 0.001f
          width =
            METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.vidSamplingInterval.fixedStart
              .width / 2
        }
    }
    val result = initial.withDefaults(METRIC_SPEC_CONFIG, randomMock)

    val expected = metricSpec {
      reach = reachParams {
        multipleDataProviderParams =
          MetricSpecKt.samplingAndPrivacyParams {
            privacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon =
                  METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.privacyParams.epsilon *
                    2
                delta =
                  METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.privacyParams.delta * 2
              }
            vidSamplingInterval =
              MetricSpecKt.vidSamplingInterval {
                start =
                  METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.vidSamplingInterval
                    .fixedStart
                    .start + 0.001f
                width =
                  METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.vidSamplingInterval
                    .fixedStart
                    .width / 2
              }
          }
      }
    }

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `buildMetricSpec builds reach spec when no field filled in multiple and single edp fields`() {
    val result = NEW_EMPTY_REACH_METRIC_SPEC.withDefaults(METRIC_SPEC_CONFIG, randomMock)
    val expected = metricSpec {
      reach = reachParams {
        multipleDataProviderParams =
          MetricSpecKt.samplingAndPrivacyParams {
            privacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon =
                  METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.privacyParams.epsilon
                delta =
                  METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.privacyParams.delta
              }
            vidSamplingInterval =
              MetricSpecKt.vidSamplingInterval {
                start =
                  METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.vidSamplingInterval
                    .fixedStart
                    .start
                width =
                  METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.vidSamplingInterval
                    .fixedStart
                    .width
              }
          }
        singleDataProviderParams =
          MetricSpecKt.samplingAndPrivacyParams {
            privacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon =
                  METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams.privacyParams.epsilon
                delta = METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams.privacyParams.delta
              }
            vidSamplingInterval =
              MetricSpecKt.vidSamplingInterval {
                start =
                  METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams.vidSamplingInterval
                    .fixedStart
                    .start
                width =
                  METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams.vidSamplingInterval
                    .fixedStart
                    .width
              }
          }
      }
    }
    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `buildMetricSpec builds reach spec when fields filled in multiple and single edp fields`() {
    val expected = metricSpec {
      reach = reachParams {
        multipleDataProviderParams =
          MetricSpecKt.samplingAndPrivacyParams {
            privacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon =
                  METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.privacyParams.epsilon *
                    2
                delta =
                  METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.privacyParams.delta * 2
              }
            vidSamplingInterval =
              MetricSpecKt.vidSamplingInterval {
                start =
                  METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.vidSamplingInterval
                    .fixedStart
                    .start + 0.001f
                width =
                  METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.vidSamplingInterval
                    .fixedStart
                    .width / 2
              }
          }
        singleDataProviderParams =
          MetricSpecKt.samplingAndPrivacyParams {
            privacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon =
                  METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams.privacyParams.epsilon * 2
                delta =
                  METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams.privacyParams.delta * 2
              }
            vidSamplingInterval =
              MetricSpecKt.vidSamplingInterval {
                start =
                  METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams.vidSamplingInterval
                    .fixedStart
                    .start + 0.001f
                width =
                  METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams.vidSamplingInterval
                    .fixedStart
                    .width / 2
              }
          }
      }
    }

    val result = expected.withDefaults(METRIC_SPEC_CONFIG, randomMock)

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `buildMetricSpec builds reach spec when edp fields set and privacy_params also set`() {
    val expected = metricSpec {
      reach = reachParams {
        privacyParams = MetricSpec.DifferentialPrivacyParams.getDefaultInstance()
        multipleDataProviderParams =
          MetricSpecKt.samplingAndPrivacyParams {
            privacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon =
                  METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.privacyParams.epsilon *
                    2
                delta =
                  METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.privacyParams.delta * 2
              }
            vidSamplingInterval =
              MetricSpecKt.vidSamplingInterval {
                start =
                  METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.vidSamplingInterval
                    .fixedStart
                    .start + 0.001f
                width =
                  METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.vidSamplingInterval
                    .fixedStart
                    .width / 2
              }
          }
        singleDataProviderParams =
          MetricSpecKt.samplingAndPrivacyParams {
            privacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon =
                  METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams.privacyParams.epsilon * 2
                delta =
                  METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams.privacyParams.delta * 2
              }
            vidSamplingInterval =
              MetricSpecKt.vidSamplingInterval {
                start =
                  METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams.vidSamplingInterval
                    .fixedStart
                    .start + 0.001f
                width =
                  METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams.vidSamplingInterval
                    .fixedStart
                    .width / 2
              }
          }
      }
    }

    val result = expected.withDefaults(METRIC_SPEC_CONFIG, randomMock)

    assertThat(result).isEqualTo(expected.copy { reach = reach.copy { clearPrivacyParams() } })
  }

  @Test
  fun `buildMetricSpec builds reach spec when fields not set and config has random start`() {
    val chosenStart = 5000
    randomMock.stub { on { nextInt(any()) } doReturn chosenStart }

    val configWithRandomStart =
      METRIC_SPEC_CONFIG.copy {
        reachParams =
          reachParams.copy {
            singleDataProviderParams =
              singleDataProviderParams.copy {
                vidSamplingInterval =
                  MetricSpecConfigKt.vidSamplingInterval {
                    randomStart =
                      MetricSpecConfigKt.VidSamplingIntervalKt.randomStart {
                        width =
                          METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams
                            .vidSamplingInterval
                            .fixedStart
                            .width
                      }
                  }
              }
          }
      }

    val result = NEW_EMPTY_REACH_METRIC_SPEC.withDefaults(configWithRandomStart, randomMock)
    val expected = metricSpec {
      reach = reachParams {
        multipleDataProviderParams =
          MetricSpecKt.samplingAndPrivacyParams {
            privacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon =
                  METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.privacyParams.epsilon
                delta =
                  METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.privacyParams.delta
              }
            vidSamplingInterval =
              MetricSpecKt.vidSamplingInterval {
                start =
                  METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.vidSamplingInterval
                    .fixedStart
                    .start
                width =
                  METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams.vidSamplingInterval
                    .fixedStart
                    .width
              }
          }
        singleDataProviderParams =
          MetricSpecKt.samplingAndPrivacyParams {
            privacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon =
                  METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams.privacyParams.epsilon
                delta = METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams.privacyParams.delta
              }
            vidSamplingInterval =
              MetricSpecKt.vidSamplingInterval {
                start = chosenStart.toFloat() / 10000
                width =
                  METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams.vidSamplingInterval
                    .fixedStart
                    .width
              }
          }
      }
    }
    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `buildMetricSpec builds rf spec when no field filled in reach and freq privacy_params`() {
    val result =
      OLD_EMPTY_REACH_AND_FREQUENCY_METRIC_SPEC.withDefaults(METRIC_SPEC_CONFIG, randomMock)
    val expected = metricSpec {
      reachAndFrequency = reachAndFrequencyParams {
        multipleDataProviderParams =
          MetricSpecKt.samplingAndPrivacyParamsForReachAndFrequency {
            reachPrivacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                    .reachPrivacyParams
                    .epsilon
                delta =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                    .reachPrivacyParams
                    .delta
              }
            frequencyPrivacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                    .frequencyPrivacyParams
                    .epsilon
                delta =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                    .frequencyPrivacyParams
                    .delta
              }
            vidSamplingInterval =
              MetricSpecKt.vidSamplingInterval {
                start =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                    .vidSamplingInterval
                    .fixedStart
                    .start
                width =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                    .vidSamplingInterval
                    .fixedStart
                    .width
              }
          }
        maximumFrequency = METRIC_SPEC_CONFIG.reachAndFrequencyParams.maximumFrequency
      }
    }
    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `buildMetricSpec builds rf spec when fields filled in reach and freq privacy_params`() {
    val initial = metricSpec {
      reachAndFrequency = reachAndFrequencyParams {
        reachPrivacyParams =
          MetricSpecKt.differentialPrivacyParams {
            epsilon =
              METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                .reachPrivacyParams
                .epsilon * 2
            delta =
              METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                .reachPrivacyParams
                .delta * 2
          }
        frequencyPrivacyParams =
          MetricSpecKt.differentialPrivacyParams {
            epsilon =
              METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                .frequencyPrivacyParams
                .epsilon * 2
            delta =
              METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                .frequencyPrivacyParams
                .delta * 2
          }
        maximumFrequency = METRIC_SPEC_CONFIG.reachAndFrequencyParams.maximumFrequency * 2
      }
      vidSamplingInterval =
        MetricSpecKt.vidSamplingInterval {
          start =
            METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
              .vidSamplingInterval
              .fixedStart
              .start + 0.001f
          width =
            METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
              .vidSamplingInterval
              .fixedStart
              .width / 2
        }
    }
    val result = initial.withDefaults(METRIC_SPEC_CONFIG, randomMock)
    val expected = metricSpec {
      reachAndFrequency = reachAndFrequencyParams {
        multipleDataProviderParams =
          MetricSpecKt.samplingAndPrivacyParamsForReachAndFrequency {
            reachPrivacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                    .reachPrivacyParams
                    .epsilon * 2
                delta =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                    .reachPrivacyParams
                    .delta * 2
              }
            frequencyPrivacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                    .frequencyPrivacyParams
                    .epsilon * 2
                delta =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                    .frequencyPrivacyParams
                    .delta * 2
              }
            vidSamplingInterval =
              MetricSpecKt.vidSamplingInterval {
                start =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                    .vidSamplingInterval
                    .fixedStart
                    .start + 0.001f
                width =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                    .vidSamplingInterval
                    .fixedStart
                    .width / 2
              }
          }
        maximumFrequency = METRIC_SPEC_CONFIG.reachAndFrequencyParams.maximumFrequency * 2
      }
    }
    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `buildMetricSpec builds rf spec when no fields filled in multiple and single edp fields`() {
    val result =
      NEW_EMPTY_REACH_AND_FREQUENCY_METRIC_SPEC.withDefaults(METRIC_SPEC_CONFIG, randomMock)
    val expected = metricSpec {
      reachAndFrequency = reachAndFrequencyParams {
        multipleDataProviderParams =
          MetricSpecKt.samplingAndPrivacyParamsForReachAndFrequency {
            reachPrivacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                    .reachPrivacyParams
                    .epsilon
                delta =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                    .reachPrivacyParams
                    .delta
              }
            frequencyPrivacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                    .frequencyPrivacyParams
                    .epsilon
                delta =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                    .frequencyPrivacyParams
                    .delta
              }
            vidSamplingInterval =
              MetricSpecKt.vidSamplingInterval {
                start =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                    .vidSamplingInterval
                    .fixedStart
                    .start
                width =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                    .vidSamplingInterval
                    .fixedStart
                    .width
              }
          }
        singleDataProviderParams =
          MetricSpecKt.samplingAndPrivacyParamsForReachAndFrequency {
            reachPrivacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.singleDataProviderParams
                    .reachPrivacyParams
                    .epsilon
                delta =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.singleDataProviderParams
                    .reachPrivacyParams
                    .delta
              }
            frequencyPrivacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.singleDataProviderParams
                    .frequencyPrivacyParams
                    .epsilon
                delta =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.singleDataProviderParams
                    .frequencyPrivacyParams
                    .delta
              }
            vidSamplingInterval =
              MetricSpecKt.vidSamplingInterval {
                start =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.singleDataProviderParams
                    .vidSamplingInterval
                    .fixedStart
                    .start
                width =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.singleDataProviderParams
                    .vidSamplingInterval
                    .fixedStart
                    .width
              }
          }
        maximumFrequency = METRIC_SPEC_CONFIG.reachAndFrequencyParams.maximumFrequency
      }
    }
    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `buildMetricSpec builds rf spec when fields filled in multiple and single edp fields`() {
    val expected = metricSpec {
      reachAndFrequency = reachAndFrequencyParams {
        multipleDataProviderParams =
          MetricSpecKt.samplingAndPrivacyParamsForReachAndFrequency {
            reachPrivacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                    .reachPrivacyParams
                    .epsilon * 2
                delta =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                    .reachPrivacyParams
                    .delta * 2
              }
            frequencyPrivacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                    .frequencyPrivacyParams
                    .epsilon * 2
                delta =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                    .frequencyPrivacyParams
                    .delta * 2
              }
            vidSamplingInterval =
              MetricSpecKt.vidSamplingInterval {
                start =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                    .vidSamplingInterval
                    .fixedStart
                    .start + 0.001f
                width =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                    .vidSamplingInterval
                    .fixedStart
                    .width / 2
              }
          }
        singleDataProviderParams =
          MetricSpecKt.samplingAndPrivacyParamsForReachAndFrequency {
            reachPrivacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.singleDataProviderParams
                    .reachPrivacyParams
                    .epsilon * 2
                delta =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.singleDataProviderParams
                    .reachPrivacyParams
                    .delta * 2
              }
            frequencyPrivacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.singleDataProviderParams
                    .frequencyPrivacyParams
                    .epsilon * 2
                delta =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.singleDataProviderParams
                    .frequencyPrivacyParams
                    .delta * 2
              }
            vidSamplingInterval =
              MetricSpecKt.vidSamplingInterval {
                start =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.singleDataProviderParams
                    .vidSamplingInterval
                    .fixedStart
                    .start + 0.001f
                width =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.singleDataProviderParams
                    .vidSamplingInterval
                    .fixedStart
                    .width / 2
              }
          }
        maximumFrequency = METRIC_SPEC_CONFIG.reachAndFrequencyParams.maximumFrequency
      }
    }
    val result = expected.withDefaults(METRIC_SPEC_CONFIG, randomMock)
    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `buildMetricSpec builds rf spec when edp fields set and reach_privacy_params also set`() {
    val expected = metricSpec {
      reachAndFrequency = reachAndFrequencyParams {
        reachPrivacyParams = MetricSpec.DifferentialPrivacyParams.getDefaultInstance()
        frequencyPrivacyParams = MetricSpec.DifferentialPrivacyParams.getDefaultInstance()
        multipleDataProviderParams =
          MetricSpecKt.samplingAndPrivacyParamsForReachAndFrequency {
            reachPrivacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                    .reachPrivacyParams
                    .epsilon * 2
                delta =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                    .reachPrivacyParams
                    .delta * 2
              }
            frequencyPrivacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                    .frequencyPrivacyParams
                    .epsilon * 2
                delta =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                    .frequencyPrivacyParams
                    .delta * 2
              }
            vidSamplingInterval =
              MetricSpecKt.vidSamplingInterval {
                start =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                    .vidSamplingInterval
                    .fixedStart
                    .start + 0.001f
                width =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.multipleDataProviderParams
                    .vidSamplingInterval
                    .fixedStart
                    .width / 2
              }
          }
        singleDataProviderParams =
          MetricSpecKt.samplingAndPrivacyParamsForReachAndFrequency {
            reachPrivacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.singleDataProviderParams
                    .reachPrivacyParams
                    .epsilon * 2
                delta =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.singleDataProviderParams
                    .reachPrivacyParams
                    .delta * 2
              }
            frequencyPrivacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.singleDataProviderParams
                    .frequencyPrivacyParams
                    .epsilon * 2
                delta =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.singleDataProviderParams
                    .frequencyPrivacyParams
                    .delta * 2
              }
            vidSamplingInterval =
              MetricSpecKt.vidSamplingInterval {
                start =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.singleDataProviderParams
                    .vidSamplingInterval
                    .fixedStart
                    .start + 0.001f
                width =
                  METRIC_SPEC_CONFIG.reachAndFrequencyParams.singleDataProviderParams
                    .vidSamplingInterval
                    .fixedStart
                    .width / 2
              }
          }
        maximumFrequency = METRIC_SPEC_CONFIG.reachAndFrequencyParams.maximumFrequency
      }
    }
    val result = expected.withDefaults(METRIC_SPEC_CONFIG, randomMock)
    assertThat(result)
      .isEqualTo(
        expected.copy {
          reachAndFrequency =
            reachAndFrequency.copy {
              clearReachPrivacyParams()
              clearFrequencyPrivacyParams()
            }
        }
      )
  }

  @Test
  fun `buildMetricSpec builds impression count spec when no field filled in privacy_params`() {
    val result = OLD_EMPTY_IMPRESSION_COUNT_METRIC_SPEC.withDefaults(METRIC_SPEC_CONFIG, randomMock)
    val expected = metricSpec {
      impressionCount = impressionCountParams {
        params =
          MetricSpecKt.samplingAndPrivacyParams {
            privacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon = METRIC_SPEC_CONFIG.impressionCountParams.params.privacyParams.epsilon
                delta = METRIC_SPEC_CONFIG.impressionCountParams.params.privacyParams.delta
              }
            vidSamplingInterval =
              MetricSpecKt.vidSamplingInterval {
                start =
                  METRIC_SPEC_CONFIG.impressionCountParams.params.vidSamplingInterval.fixedStart
                    .start
                width =
                  METRIC_SPEC_CONFIG.impressionCountParams.params.vidSamplingInterval.fixedStart
                    .width
              }
          }
        maximumFrequencyPerUser = METRIC_SPEC_CONFIG.impressionCountParams.maximumFrequencyPerUser
      }
    }
    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `buildMetricSpec builds impression count spec when all fields filled in privacy_params`() {
    val initial = metricSpec {
      impressionCount = impressionCountParams {
        privacyParams =
          MetricSpecKt.differentialPrivacyParams {
            epsilon = METRIC_SPEC_CONFIG.impressionCountParams.params.privacyParams.epsilon * 2
            delta = METRIC_SPEC_CONFIG.impressionCountParams.params.privacyParams.delta * 2
          }
        maximumFrequencyPerUser =
          METRIC_SPEC_CONFIG.impressionCountParams.maximumFrequencyPerUser * 2
      }
      vidSamplingInterval =
        MetricSpecKt.vidSamplingInterval {
          start =
            METRIC_SPEC_CONFIG.impressionCountParams.params.vidSamplingInterval.fixedStart.start +
              0.001f
          width =
            METRIC_SPEC_CONFIG.impressionCountParams.params.vidSamplingInterval.fixedStart.width / 2
        }
    }
    val result = initial.withDefaults(METRIC_SPEC_CONFIG, randomMock)
    val expected = metricSpec {
      impressionCount = impressionCountParams {
        params =
          MetricSpecKt.samplingAndPrivacyParams {
            privacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon = METRIC_SPEC_CONFIG.impressionCountParams.params.privacyParams.epsilon * 2
                delta = METRIC_SPEC_CONFIG.impressionCountParams.params.privacyParams.delta * 2
              }
            vidSamplingInterval =
              MetricSpecKt.vidSamplingInterval {
                start =
                  METRIC_SPEC_CONFIG.impressionCountParams.params.vidSamplingInterval.fixedStart
                    .start + 0.001f
                width =
                  METRIC_SPEC_CONFIG.impressionCountParams.params.vidSamplingInterval.fixedStart
                    .width / 2
              }
          }
        maximumFrequencyPerUser =
          METRIC_SPEC_CONFIG.impressionCountParams.maximumFrequencyPerUser * 2
      }
    }
    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `buildMetricSpec builds impression count spec when no field filled in params`() {
    val result = NEW_EMPTY_IMPRESSION_COUNT_METRIC_SPEC.withDefaults(METRIC_SPEC_CONFIG, randomMock)
    val expected = metricSpec {
      impressionCount = impressionCountParams {
        params =
          MetricSpecKt.samplingAndPrivacyParams {
            privacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon = METRIC_SPEC_CONFIG.impressionCountParams.params.privacyParams.epsilon
                delta = METRIC_SPEC_CONFIG.impressionCountParams.params.privacyParams.delta
              }
            vidSamplingInterval =
              MetricSpecKt.vidSamplingInterval {
                start =
                  METRIC_SPEC_CONFIG.impressionCountParams.params.vidSamplingInterval.fixedStart
                    .start
                width =
                  METRIC_SPEC_CONFIG.impressionCountParams.params.vidSamplingInterval.fixedStart
                    .width
              }
          }
        maximumFrequencyPerUser = METRIC_SPEC_CONFIG.impressionCountParams.maximumFrequencyPerUser
      }
    }
    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `buildMetricSpec builds impression count spec when all fields filled in params`() {
    val expected = metricSpec {
      impressionCount = impressionCountParams {
        params =
          MetricSpecKt.samplingAndPrivacyParams {
            privacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon = METRIC_SPEC_CONFIG.impressionCountParams.params.privacyParams.epsilon * 2
                delta = METRIC_SPEC_CONFIG.impressionCountParams.params.privacyParams.delta * 2
              }
            vidSamplingInterval =
              MetricSpecKt.vidSamplingInterval {
                start =
                  METRIC_SPEC_CONFIG.impressionCountParams.params.vidSamplingInterval.fixedStart
                    .start + 0.001f
                width =
                  METRIC_SPEC_CONFIG.impressionCountParams.params.vidSamplingInterval.fixedStart
                    .width / 2
              }
          }
        maximumFrequencyPerUser =
          METRIC_SPEC_CONFIG.impressionCountParams.maximumFrequencyPerUser * 2
      }
    }
    val result = expected.withDefaults(METRIC_SPEC_CONFIG, randomMock)
    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `buildMetricSpec builds impression count spec when params and privacy_params set`() {
    val expected = metricSpec {
      impressionCount = impressionCountParams {
        privacyParams = MetricSpec.DifferentialPrivacyParams.getDefaultInstance()
        params =
          MetricSpecKt.samplingAndPrivacyParams {
            privacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon = METRIC_SPEC_CONFIG.impressionCountParams.params.privacyParams.epsilon * 2
                delta = METRIC_SPEC_CONFIG.impressionCountParams.params.privacyParams.delta * 2
              }
            vidSamplingInterval =
              MetricSpecKt.vidSamplingInterval {
                start =
                  METRIC_SPEC_CONFIG.impressionCountParams.params.vidSamplingInterval.fixedStart
                    .start + 0.001f
                width =
                  METRIC_SPEC_CONFIG.impressionCountParams.params.vidSamplingInterval.fixedStart
                    .width / 2
              }
          }
        maximumFrequencyPerUser =
          METRIC_SPEC_CONFIG.impressionCountParams.maximumFrequencyPerUser * 2
      }
    }
    val result = expected.withDefaults(METRIC_SPEC_CONFIG, randomMock)
    assertThat(result)
      .isEqualTo(expected.copy { impressionCount = impressionCount.copy { clearPrivacyParams() } })
  }

  @Test
  fun `buildMetricSpec builds watch duration metric spec when no field filled in privacy_params`() {
    val result = OLD_EMPTY_WATCH_DURATION_METRIC_SPEC.withDefaults(METRIC_SPEC_CONFIG, randomMock)
    val expected = metricSpec {
      watchDuration = watchDurationParams {
        params =
          MetricSpecKt.samplingAndPrivacyParams {
            privacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon = METRIC_SPEC_CONFIG.watchDurationParams.params.privacyParams.epsilon
                delta = METRIC_SPEC_CONFIG.watchDurationParams.params.privacyParams.delta
              }
            vidSamplingInterval =
              MetricSpecKt.vidSamplingInterval {
                start =
                  METRIC_SPEC_CONFIG.watchDurationParams.params.vidSamplingInterval.fixedStart.start
                width =
                  METRIC_SPEC_CONFIG.watchDurationParams.params.vidSamplingInterval.fixedStart.width
              }
          }
        maximumWatchDurationPerUser =
          METRIC_SPEC_CONFIG.watchDurationParams.maximumWatchDurationPerUser
      }
    }
    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `buildMetricSpec builds watch duration metric spec when fields filled in privacy_params`() {
    val initial = metricSpec {
      watchDuration = watchDurationParams {
        privacyParams =
          MetricSpecKt.differentialPrivacyParams {
            epsilon = METRIC_SPEC_CONFIG.watchDurationParams.params.privacyParams.epsilon * 2
            delta = METRIC_SPEC_CONFIG.watchDurationParams.params.privacyParams.delta * 2
          }
        maximumWatchDurationPerUser =
          Durations.add(
            METRIC_SPEC_CONFIG.watchDurationParams.maximumWatchDurationPerUser,
            METRIC_SPEC_CONFIG.watchDurationParams.maximumWatchDurationPerUser,
          )
      }
      vidSamplingInterval =
        MetricSpecKt.vidSamplingInterval {
          start =
            METRIC_SPEC_CONFIG.watchDurationParams.params.vidSamplingInterval.fixedStart.start +
              0.001f
          width =
            METRIC_SPEC_CONFIG.watchDurationParams.params.vidSamplingInterval.fixedStart.width / 2
        }
    }
    val result = initial.withDefaults(METRIC_SPEC_CONFIG, randomMock)
    val expected = metricSpec {
      watchDuration = watchDurationParams {
        params =
          MetricSpecKt.samplingAndPrivacyParams {
            privacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon = METRIC_SPEC_CONFIG.watchDurationParams.params.privacyParams.epsilon * 2
                delta = METRIC_SPEC_CONFIG.watchDurationParams.params.privacyParams.delta * 2
              }
            vidSamplingInterval =
              MetricSpecKt.vidSamplingInterval {
                start =
                  METRIC_SPEC_CONFIG.watchDurationParams.params.vidSamplingInterval.fixedStart
                    .start + 0.001f
                width =
                  METRIC_SPEC_CONFIG.watchDurationParams.params.vidSamplingInterval.fixedStart
                    .width / 2
              }
          }
        maximumWatchDurationPerUser =
          Durations.add(
            METRIC_SPEC_CONFIG.watchDurationParams.maximumWatchDurationPerUser,
            METRIC_SPEC_CONFIG.watchDurationParams.maximumWatchDurationPerUser,
          )
      }
    }
    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `buildMetricSpec builds watch duration metric spec when no field filled in params`() {
    val result = NEW_EMPTY_WATCH_DURATION_METRIC_SPEC.withDefaults(METRIC_SPEC_CONFIG, randomMock)
    val expected = metricSpec {
      watchDuration = watchDurationParams {
        params =
          MetricSpecKt.samplingAndPrivacyParams {
            privacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon = METRIC_SPEC_CONFIG.watchDurationParams.params.privacyParams.epsilon
                delta = METRIC_SPEC_CONFIG.watchDurationParams.params.privacyParams.delta
              }
            vidSamplingInterval =
              MetricSpecKt.vidSamplingInterval {
                start =
                  METRIC_SPEC_CONFIG.watchDurationParams.params.vidSamplingInterval.fixedStart.start
                width =
                  METRIC_SPEC_CONFIG.watchDurationParams.params.vidSamplingInterval.fixedStart.width
              }
          }
        maximumWatchDurationPerUser =
          METRIC_SPEC_CONFIG.watchDurationParams.maximumWatchDurationPerUser
      }
    }
    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `buildMetricSpec builds watch duration metric spec when fields filled in params`() {
    val expected = metricSpec {
      watchDuration = watchDurationParams {
        params =
          MetricSpecKt.samplingAndPrivacyParams {
            privacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon = METRIC_SPEC_CONFIG.watchDurationParams.params.privacyParams.epsilon * 2
                delta = METRIC_SPEC_CONFIG.watchDurationParams.params.privacyParams.delta * 2
              }
            vidSamplingInterval =
              MetricSpecKt.vidSamplingInterval {
                start =
                  METRIC_SPEC_CONFIG.watchDurationParams.params.vidSamplingInterval.fixedStart
                    .start + 0.001f
                width =
                  METRIC_SPEC_CONFIG.watchDurationParams.params.vidSamplingInterval.fixedStart
                    .width / 2
              }
          }
        maximumWatchDurationPerUser =
          Durations.add(
            METRIC_SPEC_CONFIG.watchDurationParams.maximumWatchDurationPerUser,
            METRIC_SPEC_CONFIG.watchDurationParams.maximumWatchDurationPerUser,
          )
      }
    }
    val result = expected.withDefaults(METRIC_SPEC_CONFIG, randomMock)
    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `buildMetricSpec builds watch duration metric spec when params and privacy_params set`() {
    val expected = metricSpec {
      watchDuration = watchDurationParams {
        privacyParams = MetricSpec.DifferentialPrivacyParams.getDefaultInstance()
        params =
          MetricSpecKt.samplingAndPrivacyParams {
            privacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon = METRIC_SPEC_CONFIG.watchDurationParams.params.privacyParams.epsilon * 2
                delta = METRIC_SPEC_CONFIG.watchDurationParams.params.privacyParams.delta * 2
              }
            vidSamplingInterval =
              MetricSpecKt.vidSamplingInterval {
                start =
                  METRIC_SPEC_CONFIG.watchDurationParams.params.vidSamplingInterval.fixedStart
                    .start + 0.001f
                width =
                  METRIC_SPEC_CONFIG.watchDurationParams.params.vidSamplingInterval.fixedStart
                    .width / 2
              }
          }
        maximumWatchDurationPerUser =
          Durations.add(
            METRIC_SPEC_CONFIG.watchDurationParams.maximumWatchDurationPerUser,
            METRIC_SPEC_CONFIG.watchDurationParams.maximumWatchDurationPerUser,
          )
      }
    }
    val result = expected.withDefaults(METRIC_SPEC_CONFIG, randomMock)
    assertThat(result)
      .isEqualTo(expected.copy { watchDuration = watchDuration.copy { clearPrivacyParams() } })
  }

  @Test
  fun `buildMetricSpec throw MetricSpecBuildingException when no metric type specified`() {
    val metricSpecWithoutType = metricSpec {}

    val exception =
      assertThrows(MetricSpecDefaultsException::class.java) {
        metricSpecWithoutType.withDefaults(METRIC_SPEC_CONFIG, randomMock)
      }
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception).hasMessageThat().contains("metric spec type")
  }

  @Test
  fun `buildMetricSpec throw MetricSpecBuildingException when no reach params set set`() {
    val metricSpecWithoutPrivacyParams = metricSpec { reach = reachParams {} }

    val exception =
      assertThrows(MetricSpecDefaultsException::class.java) {
        metricSpecWithoutPrivacyParams.withDefaults(METRIC_SPEC_CONFIG, randomMock)
      }
    assertThat(exception).hasMessageThat().contains("privacy_params")
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception.cause).hasMessageThat().contains("reach")
  }

  @Test
  fun `buildMetricSpec throw MetricSpecBuildingException when single reach params not set`() {
    val metricSpec = metricSpec {
      reach = reachParams {
        multipleDataProviderParams = MetricSpec.SamplingAndPrivacyParams.getDefaultInstance()
      }
    }

    val exception =
      assertThrows(MetricSpecDefaultsException::class.java) {
        metricSpec.withDefaults(METRIC_SPEC_CONFIG, randomMock)
      }
    assertThat(exception).hasMessageThat().contains("privacy_params")
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception.cause).hasMessageThat().contains("reach")
  }

  @Test
  fun `buildMetricSpec throw MetricSpecBuildingException when multiple reach params not set`() {
    val metricSpec = metricSpec {
      reach = reachParams {
        singleDataProviderParams = MetricSpec.SamplingAndPrivacyParams.getDefaultInstance()
      }
    }

    val exception =
      assertThrows(MetricSpecDefaultsException::class.java) {
        metricSpec.withDefaults(METRIC_SPEC_CONFIG, randomMock)
      }
    assertThat(exception).hasMessageThat().contains("privacy_params")
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception.cause).hasMessageThat().contains("reach")
  }

  @Test
  fun `buildMetricSpec throw MetricSpecBuildingException when reach privacy params in reach and frequency is not set`() {
    val metricSpecWithoutPrivacyParams =
      OLD_EMPTY_REACH_AND_FREQUENCY_METRIC_SPEC.copy {
        reachAndFrequency = reachAndFrequency.copy { clearReachPrivacyParams() }
      }

    val exception =
      assertThrows(MetricSpecDefaultsException::class.java) {
        metricSpecWithoutPrivacyParams.withDefaults(METRIC_SPEC_CONFIG, randomMock)
      }
    assertThat(exception).hasMessageThat().contains("privacy_params")
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception.cause).hasMessageThat().contains("frequency")
  }

  @Test
  fun `buildMetricSpec throw MetricSpecBuildingException when frequency privacy params in reach and frequency is not set`() {
    val metricSpecWithoutPrivacyParams =
      OLD_EMPTY_REACH_AND_FREQUENCY_METRIC_SPEC.copy {
        reachAndFrequency = reachAndFrequency.copy { clearFrequencyPrivacyParams() }
      }

    val exception =
      assertThrows(MetricSpecDefaultsException::class.java) {
        metricSpecWithoutPrivacyParams.withDefaults(METRIC_SPEC_CONFIG, randomMock)
      }
    assertThat(exception).hasMessageThat().contains("privacy_params")
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception.cause).hasMessageThat().contains("frequency")
  }

  @Test
  fun `buildMetricSpec throw MetricSpecBuildingException when single rf params not set`() {
    val metricSpec = metricSpec {
      reachAndFrequency = reachAndFrequencyParams {
        multipleDataProviderParams =
          MetricSpec.SamplingAndPrivacyParamsForReachAndFrequency.getDefaultInstance()
      }
    }

    val exception =
      assertThrows(MetricSpecDefaultsException::class.java) {
        metricSpec.withDefaults(METRIC_SPEC_CONFIG, randomMock)
      }
    assertThat(exception).hasMessageThat().contains("privacy_params")
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception.cause).hasMessageThat().contains("frequency")
  }

  @Test
  fun `buildMetricSpec throw MetricSpecBuildingException when multiple rf params not set`() {
    val metricSpec = metricSpec {
      reachAndFrequency = reachAndFrequencyParams {
        singleDataProviderParams =
          MetricSpec.SamplingAndPrivacyParamsForReachAndFrequency.getDefaultInstance()
      }
    }

    val exception =
      assertThrows(MetricSpecDefaultsException::class.java) {
        metricSpec.withDefaults(METRIC_SPEC_CONFIG, randomMock)
      }
    assertThat(exception).hasMessageThat().contains("privacy_params")
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception.cause).hasMessageThat().contains("frequency")
  }

  @Test
  fun `buildMetricSpec throw MetricSpecBuildingException when impression params not set`() {
    val metricSpecWithoutPrivacyParams = metricSpec { impressionCount = impressionCountParams {} }

    val exception =
      assertThrows(MetricSpecDefaultsException::class.java) {
        metricSpecWithoutPrivacyParams.withDefaults(METRIC_SPEC_CONFIG, randomMock)
      }
    assertThat(exception).hasMessageThat().contains("privacy_params")
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception.cause).hasMessageThat().contains("impression")
  }

  @Test
  fun `buildMetricSpec throw MetricSpecBuildingException when watch duration params not set`() {
    val metricSpecWithoutPrivacyParams = metricSpec { watchDuration = watchDurationParams {} }

    val exception =
      assertThrows(MetricSpecDefaultsException::class.java) {
        metricSpecWithoutPrivacyParams.withDefaults(METRIC_SPEC_CONFIG, randomMock)
      }
    assertThat(exception).hasMessageThat().contains("privacy_params")
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception.cause).hasMessageThat().contains("duration")
  }

  @Test
  fun `buildMetricSpec throw MetricSpecBuildingException when vidSamplingInterval start is less than 0`() {
    val metricSpec =
      OLD_EMPTY_REACH_METRIC_SPEC.copy {
        vidSamplingInterval = MetricSpecKt.vidSamplingInterval { start = -1.0f }
      }

    val exception =
      assertThrows(MetricSpecDefaultsException::class.java) {
        metricSpec.withDefaults(METRIC_SPEC_CONFIG, randomMock)
      }
    assertThat(exception).hasMessageThat().contains("vidSamplingInterval")
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception.cause).hasMessageThat().contains("start")
  }

  @Test
  fun `buildMetricSpec throw MetricSpecBuildingException when vidSamplingInterval start is not less than 1`() {
    val metricSpec =
      OLD_EMPTY_REACH_METRIC_SPEC.copy {
        vidSamplingInterval = MetricSpecKt.vidSamplingInterval { start = 1.0f }
      }

    val exception =
      assertThrows(MetricSpecDefaultsException::class.java) {
        metricSpec.withDefaults(METRIC_SPEC_CONFIG, randomMock)
      }
    assertThat(exception).hasMessageThat().contains("vidSamplingInterval")
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception.cause).hasMessageThat().contains("start")
  }

  @Test
  fun `buildMetricSpec throw MetricSpecBuildingException when vidSamplingInterval width is not larger than 0`() {
    val metricSpec =
      OLD_EMPTY_REACH_METRIC_SPEC.copy {
        vidSamplingInterval = MetricSpecKt.vidSamplingInterval { width = 0f }
      }

    val exception =
      assertThrows(MetricSpecDefaultsException::class.java) {
        metricSpec.withDefaults(METRIC_SPEC_CONFIG, randomMock)
      }
    assertThat(exception).hasMessageThat().contains("vidSamplingInterval")
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception.cause).hasMessageThat().contains("width")
  }

  @Test
  fun `buildMetricSpec throw MetricSpecBuildingException when vidSamplingInterval is too long`() {
    val metricSpec =
      OLD_EMPTY_REACH_METRIC_SPEC.copy {
        vidSamplingInterval =
          MetricSpecKt.vidSamplingInterval {
            start = 0.5f
            width = 1.0f
          }
      }

    val exception =
      assertThrows(MetricSpecDefaultsException::class.java) {
        metricSpec.withDefaults(METRIC_SPEC_CONFIG, randomMock)
      }
    assertThat(exception).hasMessageThat().contains("vidSamplingInterval")
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception.cause).hasMessageThat().contains("start + width")
  }

  companion object {
    private val METRIC_SPEC_CONFIG = metricSpecConfig {
      reachParams =
        MetricSpecConfigKt.reachParams {
          multipleDataProviderParams =
            MetricSpecConfigKt.samplingAndPrivacyParams {
              privacyParams =
                MetricSpecConfigKt.differentialPrivacyParams {
                  epsilon = REACH_ONLY_REACH_EPSILON
                  delta = DIFFERENTIAL_PRIVACY_DELTA
                }
              vidSamplingInterval =
                MetricSpecConfigKt.vidSamplingInterval {
                  fixedStart =
                    MetricSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                      start = REACH_ONLY_VID_SAMPLING_START
                      width = REACH_ONLY_VID_SAMPLING_WIDTH
                    }
                }
            }

          singleDataProviderParams =
            MetricSpecConfigKt.samplingAndPrivacyParams {
              privacyParams =
                MetricSpecConfigKt.differentialPrivacyParams {
                  epsilon = SINGLE_DATA_PROVIDER_REACH_ONLY_REACH_EPSILON
                  delta = DIFFERENTIAL_PRIVACY_DELTA
                }
              vidSamplingInterval =
                MetricSpecConfigKt.vidSamplingInterval {
                  fixedStart =
                    MetricSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                      start = SINGLE_DATA_PROVIDER_REACH_ONLY_VID_SAMPLING_START
                      width = SINGLE_DATA_PROVIDER_REACH_ONLY_VID_SAMPLING_WIDTH
                    }
                }
            }
        }

      reachAndFrequencyParams =
        MetricSpecConfigKt.reachAndFrequencyParams {
          multipleDataProviderParams =
            MetricSpecConfigKt.samplingAndPrivacyParamsForReachAndFrequency {
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
              vidSamplingInterval =
                MetricSpecConfigKt.vidSamplingInterval {
                  fixedStart =
                    MetricSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                      start = REACH_FREQUENCY_VID_SAMPLING_START
                      width = REACH_FREQUENCY_VID_SAMPLING_WIDTH
                    }
                }
            }

          singleDataProviderParams =
            MetricSpecConfigKt.samplingAndPrivacyParamsForReachAndFrequency {
              reachPrivacyParams =
                MetricSpecConfigKt.differentialPrivacyParams {
                  epsilon = SINGLE_DATA_PROVIDER_REACH_FREQUENCY_REACH_EPSILON
                  delta = DIFFERENTIAL_PRIVACY_DELTA
                }
              frequencyPrivacyParams =
                MetricSpecConfigKt.differentialPrivacyParams {
                  epsilon = SINGLE_DATA_PROVIDER_REACH_FREQUENCY_FREQUENCY_EPSILON
                  delta = DIFFERENTIAL_PRIVACY_DELTA
                }
              vidSamplingInterval =
                MetricSpecConfigKt.vidSamplingInterval {
                  fixedStart =
                    MetricSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                      start = SINGLE_DATA_PROVIDER_REACH_FREQUENCY_VID_SAMPLING_START
                      width = SINGLE_DATA_PROVIDER_REACH_FREQUENCY_VID_SAMPLING_WIDTH
                    }
                }
            }
          maximumFrequency = REACH_FREQUENCY_MAXIMUM_FREQUENCY
        }

      impressionCountParams =
        MetricSpecConfigKt.impressionCountParams {
          params =
            MetricSpecConfigKt.samplingAndPrivacyParams {
              privacyParams =
                MetricSpecConfigKt.differentialPrivacyParams {
                  epsilon = IMPRESSION_EPSILON
                  delta = DIFFERENTIAL_PRIVACY_DELTA
                }
              vidSamplingInterval =
                MetricSpecConfigKt.vidSamplingInterval {
                  fixedStart =
                    MetricSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                      start = IMPRESSION_VID_SAMPLING_START
                      width = IMPRESSION_VID_SAMPLING_WIDTH
                    }
                }
            }
          maximumFrequencyPerUser = IMPRESSION_MAXIMUM_FREQUENCY_PER_USER
        }

      watchDurationParams =
        MetricSpecConfigKt.watchDurationParams {
          params =
            MetricSpecConfigKt.samplingAndPrivacyParams {
              privacyParams =
                MetricSpecConfigKt.differentialPrivacyParams {
                  epsilon = WATCH_DURATION_EPSILON
                  delta = DIFFERENTIAL_PRIVACY_DELTA
                }
              vidSamplingInterval =
                MetricSpecConfigKt.vidSamplingInterval {
                  fixedStart =
                    MetricSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                      start = WATCH_DURATION_VID_SAMPLING_START
                      width = WATCH_DURATION_VID_SAMPLING_WIDTH
                    }
                }
            }
          maximumWatchDurationPerUser = MAXIMUM_WATCH_DURATION_PER_USER
        }
    }

    // Metric Specs

    private val OLD_EMPTY_REACH_METRIC_SPEC: MetricSpec = metricSpec {
      reach = reachParams {
        privacyParams = MetricSpec.DifferentialPrivacyParams.getDefaultInstance()
      }
    }
    private val NEW_EMPTY_REACH_METRIC_SPEC: MetricSpec = metricSpec {
      reach = reachParams {
        multipleDataProviderParams = MetricSpec.SamplingAndPrivacyParams.getDefaultInstance()
        singleDataProviderParams = MetricSpec.SamplingAndPrivacyParams.getDefaultInstance()
      }
    }
    private val OLD_EMPTY_REACH_AND_FREQUENCY_METRIC_SPEC: MetricSpec = metricSpec {
      reachAndFrequency = reachAndFrequencyParams {
        reachPrivacyParams = MetricSpec.DifferentialPrivacyParams.getDefaultInstance()
        frequencyPrivacyParams = MetricSpec.DifferentialPrivacyParams.getDefaultInstance()
      }
    }
    private val NEW_EMPTY_REACH_AND_FREQUENCY_METRIC_SPEC: MetricSpec = metricSpec {
      reachAndFrequency = reachAndFrequencyParams {
        multipleDataProviderParams =
          MetricSpec.SamplingAndPrivacyParamsForReachAndFrequency.getDefaultInstance()
        singleDataProviderParams =
          MetricSpec.SamplingAndPrivacyParamsForReachAndFrequency.getDefaultInstance()
      }
    }
    private val OLD_EMPTY_IMPRESSION_COUNT_METRIC_SPEC: MetricSpec = metricSpec {
      impressionCount = impressionCountParams {
        privacyParams = MetricSpec.DifferentialPrivacyParams.getDefaultInstance()
      }
    }
    private val NEW_EMPTY_IMPRESSION_COUNT_METRIC_SPEC: MetricSpec = metricSpec {
      impressionCount = impressionCountParams {
        params = MetricSpec.SamplingAndPrivacyParams.getDefaultInstance()
      }
    }
    private val OLD_EMPTY_WATCH_DURATION_METRIC_SPEC: MetricSpec = metricSpec {
      watchDuration = watchDurationParams {
        privacyParams = MetricSpec.DifferentialPrivacyParams.getDefaultInstance()
      }
    }
    private val NEW_EMPTY_WATCH_DURATION_METRIC_SPEC: MetricSpec = metricSpec {
      watchDuration = watchDurationParams {
        params = MetricSpec.SamplingAndPrivacyParams.getDefaultInstance()
      }
    }
  }
}
