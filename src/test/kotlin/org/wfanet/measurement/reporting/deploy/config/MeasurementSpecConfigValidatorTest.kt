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

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.config.reporting.MeasurementSpecConfigKt
import org.wfanet.measurement.config.reporting.copy
import org.wfanet.measurement.config.reporting.measurementSpecConfig
import org.wfanet.measurement.reporting.deploy.config.MeasurementSpecConfigValidator.validate

@RunWith(JUnit4::class)
class MeasurementSpecConfigValidatorTest {
  @Test
  fun `validate throws no exception when config is valid`() {
    MEASUREMENT_SPEC_CONFIG.validate()
  }

  @Test
  fun `validate throws ILLEGAL_ARGUMENT_EXCEPTION when epsilon is 0`() {
    val invalidMeasurementSpecConfig =
      MEASUREMENT_SPEC_CONFIG.copy {
        reachSingleDataProvider =
          MEASUREMENT_SPEC_CONFIG.reachSingleDataProvider.copy {
            privacyParams =
              MeasurementSpecConfigKt.differentialPrivacyParams {
                epsilon = 0.0
                delta = 0.0
              }
          }
      }
    val exception =
      assertFailsWith<IllegalArgumentException> { invalidMeasurementSpecConfig.validate() }
    assertThat(exception.message).contains("privacy_params")
  }

  @Test
  fun `validate throws ILLEGAL_ARGUMENT_EXCEPTION when delta is negaitve`() {
    val invalidMeasurementSpecConfig =
      MEASUREMENT_SPEC_CONFIG.copy {
        reachSingleDataProvider =
          MEASUREMENT_SPEC_CONFIG.reachSingleDataProvider.copy {
            privacyParams =
              MeasurementSpecConfigKt.differentialPrivacyParams {
                epsilon = 1.0
                delta = -1.0
              }
          }
      }
    val exception =
      assertFailsWith<IllegalArgumentException> { invalidMeasurementSpecConfig.validate() }
    assertThat(exception.message).contains("privacy_params")
  }

  @Test
  fun `validate throws ILLEGAL_ARGUMENT_EXCEPTION when fixed_start width is 0`() {
    val invalidMeasurementSpecConfig =
      MEASUREMENT_SPEC_CONFIG.copy {
        reachSingleDataProvider =
          MEASUREMENT_SPEC_CONFIG.reachSingleDataProvider.copy {
            vidSamplingInterval =
              MeasurementSpecConfigKt.vidSamplingInterval {
                fixedStart =
                  MeasurementSpecConfigKt.VidSamplingIntervalKt.fixedStart { width = 0.0f }
              }
          }
      }
    val exception =
      assertFailsWith<IllegalArgumentException> { invalidMeasurementSpecConfig.validate() }
    assertThat(exception.message).contains("vid_sampling_interval")
  }

  @Test
  fun `validate throws ILLEGAL_ARGUMENT_EXCEPTION when fixed_start start is negative`() {
    val invalidMeasurementSpecConfig =
      MEASUREMENT_SPEC_CONFIG.copy {
        reachSingleDataProvider =
          MEASUREMENT_SPEC_CONFIG.reachSingleDataProvider.copy {
            vidSamplingInterval =
              MeasurementSpecConfigKt.vidSamplingInterval {
                fixedStart =
                  MeasurementSpecConfigKt.VidSamplingIntervalKt.fixedStart { start = -1.0f }
              }
          }
      }
    val exception =
      assertFailsWith<IllegalArgumentException> { invalidMeasurementSpecConfig.validate() }
    assertThat(exception.message).contains("vid_sampling_interval")
  }

  @Test
  fun `validate throws ILLEGAL_ARGUMENT_EXCEPTION when fixed_start interval is more than 1`() {
    val invalidMeasurementSpecConfig =
      MEASUREMENT_SPEC_CONFIG.copy {
        reachSingleDataProvider =
          MEASUREMENT_SPEC_CONFIG.reachSingleDataProvider.copy {
            vidSamplingInterval =
              MeasurementSpecConfigKt.vidSamplingInterval {
                fixedStart =
                  MeasurementSpecConfigKt.VidSamplingIntervalKt.fixedStart { width = 1.1f }
              }
          }
      }
    val exception =
      assertFailsWith<IllegalArgumentException> { invalidMeasurementSpecConfig.validate() }
    assertThat(exception.message).contains("vid_sampling_interval")
  }

  @Test
  fun `validate throws ILLEGAL_ARGUMENT_EXCEPTION when random_start width is negative`() {
    val invalidMeasurementSpecConfig =
      MEASUREMENT_SPEC_CONFIG.copy {
        reachSingleDataProvider =
          MEASUREMENT_SPEC_CONFIG.reachSingleDataProvider.copy {
            vidSamplingInterval =
              MeasurementSpecConfigKt.vidSamplingInterval {
                randomStart =
                  MeasurementSpecConfigKt.VidSamplingIntervalKt.randomStart {
                    width = -5
                    numVidBuckets = 5
                  }
              }
          }
      }
    val exception =
      assertFailsWith<IllegalArgumentException> { invalidMeasurementSpecConfig.validate() }
    assertThat(exception.message).contains("vid_sampling_interval")
  }

  @Test
  fun `validate throws ILLEGAL_ARGUMENT_EXCEPTION when random_start num_vid_buckets is 0`() {
    val invalidMeasurementSpecConfig =
      MEASUREMENT_SPEC_CONFIG.copy {
        reachSingleDataProvider =
          MEASUREMENT_SPEC_CONFIG.reachSingleDataProvider.copy {
            vidSamplingInterval =
              MeasurementSpecConfigKt.vidSamplingInterval {
                randomStart =
                  MeasurementSpecConfigKt.VidSamplingIntervalKt.randomStart { numVidBuckets = 0 }
              }
          }
      }
    val exception =
      assertFailsWith<IllegalArgumentException> { invalidMeasurementSpecConfig.validate() }
    assertThat(exception.message).contains("vid_sampling_interval")
  }

  @Test
  fun `validate throws ILLEGAL_ARGUMENT_EXCEPTION when random_start width exceeds vid buckets`() {
    val invalidMeasurementSpecConfig =
      MEASUREMENT_SPEC_CONFIG.copy {
        reachSingleDataProvider =
          MEASUREMENT_SPEC_CONFIG.reachSingleDataProvider.copy {
            vidSamplingInterval =
              MeasurementSpecConfigKt.vidSamplingInterval {
                randomStart =
                  MeasurementSpecConfigKt.VidSamplingIntervalKt.randomStart {
                    width = 6
                    numVidBuckets = 5
                  }
              }
          }
      }
    val exception =
      assertFailsWith<IllegalArgumentException> { invalidMeasurementSpecConfig.validate() }
    assertThat(exception.message).contains("vid_sampling_interval")
  }

  companion object {
    private val MEASUREMENT_SPEC_CONFIG = measurementSpecConfig {
      reachSingleDataProvider =
        MeasurementSpecConfigKt.reachSingleDataProvider {
          privacyParams =
            MeasurementSpecConfigKt.differentialPrivacyParams {
              epsilon = 0.000207
              delta = 1e-15
            }
          vidSamplingInterval =
            MeasurementSpecConfigKt.vidSamplingInterval {
              fixedStart =
                MeasurementSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                  start = 0f
                  width = 1f
                }
            }
        }
      reach =
        MeasurementSpecConfigKt.reach {
          privacyParams =
            MeasurementSpecConfigKt.differentialPrivacyParams {
              epsilon = 0.0007444
              delta = 1e-15
            }
          vidSamplingInterval =
            MeasurementSpecConfigKt.vidSamplingInterval {
              randomStart =
                MeasurementSpecConfigKt.VidSamplingIntervalKt.randomStart {
                  width = 256
                  numVidBuckets = 300
                }
            }
        }
      reachAndFrequencySingleDataProvider =
        MeasurementSpecConfigKt.reachAndFrequencySingleDataProvider {
          reachPrivacyParams =
            MeasurementSpecConfigKt.differentialPrivacyParams {
              epsilon = 0.000207
              delta = 1e-15
            }
          frequencyPrivacyParams =
            MeasurementSpecConfigKt.differentialPrivacyParams {
              epsilon = 0.004728
              delta = 1e-15
            }
          vidSamplingInterval =
            MeasurementSpecConfigKt.vidSamplingInterval {
              fixedStart =
                MeasurementSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                  start = 0f
                  width = 1f
                }
            }
        }
      reachAndFrequency =
        MeasurementSpecConfigKt.reachAndFrequency {
          reachPrivacyParams =
            MeasurementSpecConfigKt.differentialPrivacyParams {
              epsilon = 0.0007444
              delta = 1e-15
            }
          frequencyPrivacyParams =
            MeasurementSpecConfigKt.differentialPrivacyParams {
              epsilon = 0.014638
              delta = 1e-15
            }
          vidSamplingInterval =
            MeasurementSpecConfigKt.vidSamplingInterval {
              randomStart =
                MeasurementSpecConfigKt.VidSamplingIntervalKt.randomStart {
                  width = 256
                  numVidBuckets = 300
                }
            }
        }
      impression =
        MeasurementSpecConfigKt.impression {
          privacyParams =
            MeasurementSpecConfigKt.differentialPrivacyParams {
              epsilon = 0.003592
              delta = 1e-15
            }
          vidSamplingInterval =
            MeasurementSpecConfigKt.vidSamplingInterval {
              fixedStart =
                MeasurementSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                  start = 0f
                  width = 1f
                }
            }
        }
      duration =
        MeasurementSpecConfigKt.duration {
          privacyParams =
            MeasurementSpecConfigKt.differentialPrivacyParams {
              epsilon = 0.007418
              delta = 1e-15
            }
          vidSamplingInterval =
            MeasurementSpecConfigKt.vidSamplingInterval {
              fixedStart =
                MeasurementSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                  start = 0f
                  width = 1f
                }
            }
        }
    }
  }
}
