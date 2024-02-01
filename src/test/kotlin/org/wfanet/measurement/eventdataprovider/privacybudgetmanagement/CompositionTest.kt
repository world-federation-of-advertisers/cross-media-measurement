/*
 * Copyright 2022 The Cross-Media Measurement Authors
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
import org.junit.Test
import org.wfanet.measurement.eventdataprovider.noiser.DpParams

class CompositionTest {
  @Test
  fun `advanced composition computation works as expected`() {
    assertThat(
        Composition.totalPrivacyBudgetUsageUnderAdvancedComposition(DpCharge(1.0f, 0.0f), 30, 0.0f)
      )
      .isEqualTo(30.0f)
    assertThat(
        Composition.totalPrivacyBudgetUsageUnderAdvancedComposition(
          DpCharge(1.0f, 0.001f),
          30,
          0.06f,
        )
      )
      .isEqualTo(22.0f)
    assertThat(
        Composition.totalPrivacyBudgetUsageUnderAdvancedComposition(
          DpCharge(1.0f, 0.001f),
          30,
          0.1f,
        )
      )
      .isEqualTo(20.0f)
    assertThat(
        Composition.totalPrivacyBudgetUsageUnderAdvancedComposition(DpCharge(1.0f, 0.2f), 1, 0.1f)
      )
      .isEqualTo(Float.MAX_VALUE)
    assertThat(
        Composition.totalPrivacyBudgetUsageUnderAdvancedComposition(
          DpCharge(1.0f, 0.01f),
          30,
          0.26f,
        )
      )
      .isEqualTo(Float.MAX_VALUE)
    assertThat(
        Composition.totalPrivacyBudgetUsageUnderAdvancedComposition(
          DpCharge(0.01f, 1.0e-12.toFloat()),
          200,
          1.0e-9.toFloat(),
        )
      )
      .isWithin(0.0001f)
      .of(0.78f)
    assertThat(
        Composition.totalPrivacyBudgetUsageUnderAdvancedComposition(
          DpCharge(0.0042f, 0.0f),
          1880,
          1.0e-9.toFloat(),
        )
      )
      .isWithin(0.001f)
      .of(1.0f)
  }

  @Test
  fun `acdp composition works as expected for single acdpCharge`() {
    val acdpCharges = listOf(AcdpCharge(0.1, 1E-5))
    val targetEpsilon = 3.0
    val delta = Composition.totalPrivacyBudgetUsageUnderAcdpComposition(acdpCharges, targetEpsilon)
    val expectedDelta = 2.108553871763024E-4

    assertThat(delta).isEqualTo(expectedDelta)
  }

  @Test
  fun `acdp composition works as expected for another single acdpCharge`() {
    val acdpCharges = listOf(AcdpCharge(0.04, 5.0E-6))
    val targetEpsilon = 3.0
    val delta = Composition.totalPrivacyBudgetUsageUnderAcdpComposition(acdpCharges, targetEpsilon)
    val expectedDelta = 1.0542768461593835E-4

    assertThat(delta).isEqualTo(expectedDelta)
  }

  @Test
  fun `acdp composition works as expected for single acdpCharge and smaller epsilon and theta`() {
    val acdpCharges = listOf(AcdpCharge(0.3, 1E-12))
    val targetEpsilon = 0.5
    // (exp(epsilon) + 1) * theta  = 2.6487212181091307E-12
    val delta = Composition.totalPrivacyBudgetUsageUnderAcdpComposition(acdpCharges, targetEpsilon)

    val expectedDelta = 0.26509576368586407

    assertThat(delta).isEqualTo(expectedDelta)
  }

  @Test
  fun `acdp composition works as expected for multiple acdpCharges`() {
    val acdpCharges = listOf(AcdpCharge(0.04, 5.0E-6), AcdpCharge(0.06, 5.0E-6))
    val targetEpsilon = 3.0
    val delta = Composition.totalPrivacyBudgetUsageUnderAcdpComposition(acdpCharges, targetEpsilon)
    val expectedDelta = 2.108553871763024E-4

    assertThat(delta).isEqualTo(expectedDelta)
  }

  @Test
  fun `computed delta from totalPrivacyBudgetUsageUnderAcdpComposition is too large when targetEpsilon is large`() {
    val acdpCharges = listOf(AcdpCharge(0.04, 5.0E-6))
    val targetEpsilon = 100.0
    val delta = Composition.totalPrivacyBudgetUsageUnderAcdpComposition(acdpCharges, targetEpsilon)
    val expectedDelta = 1.344058570908068E38

    assertThat(delta).isEqualTo(expectedDelta)
  }

  @Test
  fun `acdp composition for direct measurement works as expected from sample dpParams and targetEpsilon`() {
    val acdpCharges = listOf(AcdpParamsConverter.getDirectAcdpCharge(DpParams(1.0, 1e-5), 1.0))
    val targetEpsilon = 10.0
    val delta = Composition.totalPrivacyBudgetUsageUnderAcdpComposition(acdpCharges, targetEpsilon)
    val expectedDelta = 2.3594607405220148E-303

    assertThat(delta).isEqualTo(expectedDelta)
  }

  @Test
  fun `acdp composition for llv2 measurement works as expected from sample dpParams and targetEpsilon`() {
    val acdpCharges = listOf(AcdpParamsConverter.getLlv2AcdpCharge(DpParams(1.0, 1e-5), 3))
    val targetEpsilon = 10.0
    val delta = Composition.totalPrivacyBudgetUsageUnderAcdpComposition(acdpCharges, targetEpsilon)
    val expectedDelta = 0.012922093064440253

    assertThat(delta).isEqualTo(expectedDelta)
  }

  @Test
  fun `computed delta for llv2 measurement from totalPrivacyBudgetUsageUnderAcdpComposition is large when delta is large in dpParams`() {
    val acdpCharges = listOf(AcdpParamsConverter.getLlv2AcdpCharge(DpParams(1.0, 0.1), 3))
    val targetEpsilon = 10.0
    val delta = Composition.totalPrivacyBudgetUsageUnderAcdpComposition(acdpCharges, targetEpsilon)
    val expectedDelta = 195.12544824856639

    assertThat(delta).isEqualTo(expectedDelta)
  }
}
