/*
 * Copyright 2023 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement

import com.google.common.truth.Truth.assertThat
import kotlin.math.ln
import kotlin.test.assertFails
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams

@RunWith(JUnit4::class)
class AcdpParamsConverterTest {

  @Test
  fun getMpcAcdpChargeThrowsExceptionWithInvalidEpsilon() {
    assertFails {
      AcdpParamsConverter.getMpcAcdpCharge(
        differentialPrivacyParams {
          epsilon = -0.1
          delta = 0.1
        },
        CONTRIBUTOR_COUNT,
      )
    }
  }

  @Test
  fun getMpcAcdpChargeThrowsExceptionWithInvalidDelta() {
    assertFails {
      AcdpParamsConverter.getMpcAcdpCharge(
        differentialPrivacyParams {
          epsilon = 0.1
          delta = -0.1
        },
        CONTRIBUTOR_COUNT,
      )
    }
  }

  @Test
  fun getMpcAcdpChargeThrowsExceptionWithInvalidContributorCount() {
    assertFails {
      AcdpParamsConverter.getMpcAcdpCharge(
        DP_PARAMS,
        -1,
      )
    }
  }

  @Test
  fun getDirectAcdpChargeThrowsExceptionWithInvalidEpsilon() {
    assertFails {
      AcdpParamsConverter.getDirectAcdpCharge(
        differentialPrivacyParams {
          epsilon = -0.1
          delta = 0.1
        },
        SENSITIVITY,
      )
    }
  }

  @Test
  fun getDirectAcdpChargeThrowsExceptionWithInvalidDelta() {
    assertFails {
      AcdpParamsConverter.getDirectAcdpCharge(
        differentialPrivacyParams {
          epsilon = 0.1
          delta = -0.1
        },
        SENSITIVITY,
      )
    }
  }

  @Test
  fun mpcRhoAndThetaShouldBeCorrectWithGivenDpParamsAndOneContributor() {
    // mu and sigmaDistributed with given DP params: mu = 261.0, sigma = 48.23177914088707
    val acdpCharge =
      AcdpParamsConverter.getMpcAcdpCharge(
        DP_PARAMS,
        CONTRIBUTOR_COUNT,
      )
    val expectedAcdpCharge = AcdpCharge(2.149331679905983E-4, 4.378180881551259E-7)

    assertThat(acdpCharge).isEqualTo(expectedAcdpCharge)
  }

  @Test
  fun mpcRhoAndThetaShouldBeCorrectWithGivenDpParamsAndThreeContributors() {
    val acdpCharge =
      AcdpParamsConverter.getMpcAcdpCharge(
        DP_PARAMS,
        3,
      )
    val expectedAcdpCharge = AcdpCharge(5.000214933167991, 4.5945334251551807E-7)

    assertThat(acdpCharge).isEqualTo(expectedAcdpCharge)
  }

  @Test
  fun directRhoAndThetaShouldBeCorrectWithGivenDpParams() {
    val acdpCharge =
      AcdpParamsConverter.getDirectAcdpCharge(
        DP_PARAMS,
        SENSITIVITY,
      )
    val expectedAcdpCharge = AcdpCharge(4.946819611450154E-4, 0.0)

    assertThat(acdpCharge).isEqualTo(expectedAcdpCharge)
  }

  companion object {
    private val DP_PARAMS = differentialPrivacyParams {
      epsilon = ln(3.0) / 10 // 0.1098
      delta = 0.2 / 100000
    }
    private const val CONTRIBUTOR_COUNT = 1
    private const val SENSITIVITY = 1
  }
}
