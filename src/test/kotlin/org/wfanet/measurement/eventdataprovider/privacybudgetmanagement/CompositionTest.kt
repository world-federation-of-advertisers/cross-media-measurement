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
          0.06f
        )
      )
      .isEqualTo(22.0f)
    assertThat(
        Composition.totalPrivacyBudgetUsageUnderAdvancedComposition(
          DpCharge(1.0f, 0.001f),
          30,
          0.1f
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
          0.26f
        )
      )
      .isEqualTo(Float.MAX_VALUE)
    assertThat(
        Composition.totalPrivacyBudgetUsageUnderAdvancedComposition(
          DpCharge(0.01f, 1.0e-12.toFloat()),
          200,
          1.0e-9.toFloat()
        )
      )
      .isWithin(0.0001f)
      .of(0.78f)
    assertThat(
        Composition.totalPrivacyBudgetUsageUnderAdvancedComposition(
          DpCharge(0.0042f, 0.0f),
          1880,
          1.0e-9.toFloat()
        )
      )
      .isWithin(0.001f)
      .of(1.0f)
  }

  @Test
  fun `acdp composition works as expected for single AcdpCharge`() {
    val acdpCharges = listOf(AcdpCharge(0.1, 1E-5))
    val targetEpsilon = 3.0f
    val delta = Composition.totalPrivacyBudgetUsageUnderAcdpComposition(acdpCharges, targetEpsilon)
    val expectedDelta = 2.1085539E-4f

    assertThat(delta).isEqualTo(expectedDelta)
  }

  @Test
  fun `acdp composition works as expected for single AcdpCharge and smaller epsilon and theta`() {
    val acdpCharges = listOf(AcdpCharge(0.3, 1E-12))
    val targetEpsilon = 0.5f
    // (exp(epsilon) + 1) * theta  = 2.6487212181091307E-12
    val delta = Composition.totalPrivacyBudgetUsageUnderAcdpComposition(acdpCharges, targetEpsilon)
    val expectedDelta = 0.26509577f

    assertThat(delta).isEqualTo(expectedDelta)
  }

  @Test
  fun `acdp composition works as expected for multiple AcdpCharges`() {
    val acdpCharges = listOf(AcdpCharge(0.04, 5.0E-6), AcdpCharge(0.06, 5.0E-6))
    val targetEpsilon = 3.0f
    val delta = Composition.totalPrivacyBudgetUsageUnderAcdpComposition(acdpCharges, targetEpsilon)
    val expectedDelta = 2.1085539E-4f

    assertThat(delta).isEqualTo(expectedDelta)
  }
}
