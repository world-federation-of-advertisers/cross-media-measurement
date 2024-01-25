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
import org.wfanet.measurement.eventdataprovider.noiser.DpParams

@RunWith(JUnit4::class)
class AcdpParamsConverterTest {

  @Test
  fun `getLlv2AcdpCharge throws exception with invalid epsilon`() {
    assertFails { AcdpParamsConverter.getLlv2AcdpCharge(DpParams(-0.1, 0.1), CONTRIBUTOR_COUNT) }
  }

  @Test
  fun `getLlv2AcdpCharge throws exception with invalid delta`() {
    assertFails { AcdpParamsConverter.getLlv2AcdpCharge(DpParams(0.1, -0.1), CONTRIBUTOR_COUNT) }
  }

  @Test
  fun `getLlv2AcdpCharge throws exception with invalid contributorCount`() {
    assertFails { AcdpParamsConverter.getLlv2AcdpCharge(DP_PARAMS, -1) }
  }

  @Test
  fun `getDirectAcdpCharge throws exception with invalid epsilon`() {
    assertFails { AcdpParamsConverter.getDirectAcdpCharge(DpParams(-0.1, 0.1), SENSITIVITY) }
  }

  @Test
  fun `getDirectAcdpCharge throws exception with invalid delta`() {
    assertFails { AcdpParamsConverter.getDirectAcdpCharge(DpParams(0.1, -0.1), SENSITIVITY) }
  }

  @Test
  fun `llv2 rho and theta should be correct with given dpParams and one contributor`() {
    // mu and sigmaDistributed with given DP params: mu = 261.0, sigma = 48.23177914088707
    val acdpCharge = AcdpParamsConverter.getLlv2AcdpCharge(DP_PARAMS, CONTRIBUTOR_COUNT)
    val expectedAcdpCharge = AcdpCharge(2.149331679905983E-4, 4.378180881551259E-7)

    assertThat(acdpCharge.rho).isWithin(TOLERANCE).of(expectedAcdpCharge.rho)
    assertThat(acdpCharge.theta).isWithin(TOLERANCE).of(expectedAcdpCharge.theta)
  }

  @Test
  fun `llv2 rho and theta should be correct with given dpParams and three contributors`() {
    val acdpCharge = AcdpParamsConverter.getLlv2AcdpCharge(DP_PARAMS, 3)
    val expectedAcdpCharge = AcdpCharge(2.149331679905983E-4, 4.5945334251551807E-7)

    assertThat(acdpCharge.rho).isWithin(TOLERANCE).of(expectedAcdpCharge.rho)
    assertThat(acdpCharge.theta).isWithin(TOLERANCE).of(expectedAcdpCharge.theta)
  }

  @Test
  fun `llv2 rho and theta should be correct with large dpParams and three contributors`() {
    // sigmaDistributed and lambda with this set of params: sigmaDistributed = 1.1509301704045332,
    // lambda = 2.12667410579919E-6
    val acdpCharge = AcdpParamsConverter.getLlv2AcdpCharge(DpParams(0.9, 0.5), 3)
    val expectedAcdpCharge = AcdpCharge(0.12582564093358586, 0.007149139528009278)

    assertThat(acdpCharge.rho).isWithin(TOLERANCE).of(expectedAcdpCharge.rho)
    assertThat(acdpCharge.theta).isWithin(TOLERANCE).of(expectedAcdpCharge.theta)
  }

  @Test
  fun `direct rho and theta should be correct with given dpParams`() {
    val acdpCharge = AcdpParamsConverter.getDirectAcdpCharge(DP_PARAMS, SENSITIVITY)
    val expectedAcdpCharge = AcdpCharge(4.946819611450154E-4, 0.0)

    assertThat(acdpCharge.rho).isWithin(TOLERANCE).of(expectedAcdpCharge.rho)
    assertThat(acdpCharge.theta).isWithin(TOLERANCE).of(expectedAcdpCharge.theta)
  }

  companion object {
    // ln(3.0) / 10 = 0.1098
    private val DP_PARAMS = DpParams(ln(3.0) / 10, 0.2 / 100000)

    private const val CONTRIBUTOR_COUNT = 1
    private const val SENSITIVITY = 1.0
    private const val TOLERANCE = 1E-10
  }
}
