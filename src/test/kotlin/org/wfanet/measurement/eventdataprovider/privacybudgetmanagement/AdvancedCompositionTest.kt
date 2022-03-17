/**
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * ```
 *      http://www.apache.org/licenses/LICENSE-2.0
 * ```
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement

import kotlin.test.assertEquals
import org.junit.Test

class AdvancedCompositionTest {
  @Test
  fun `binomial coefficients compute as expected`() {
    val binom = Binom()
    assertEquals(binom.coeff(0, 0), 1.0f)
    assertEquals(binom.coeff(1, 0), 1.0f)
    assertEquals(binom.coeff(1, 1), 1.0f)
    assertEquals(binom.coeff(2, 0), 1.0f)
    assertEquals(binom.coeff(2, 1), 2.0f)
    assertEquals(binom.coeff(2, 2), 1.0f)
    assertEquals(binom.coeff(3, 0), 1.0f)
    assertEquals(binom.coeff(3, 1), 3.0f)
    assertEquals(binom.coeff(3, 2), 3.0f)
    assertEquals(binom.coeff(3, 3), 1.0f)
    assertEquals(binom.coeff(20, 10), 184756.0f)
  }

  @Test
  fun `advanced composition`() {
    assertEquals(
      totalPrivacyBudgetUsageUnderAdvancedComposition(PrivacyCharge(1.0f, 0.0f), 30, 0.0f),
      30.0f
    )
    assertEquals(
      totalPrivacyBudgetUsageUnderAdvancedComposition(PrivacyCharge(1.0f, 0.001f), 30, 0.06f),
      22.0f
    )
    assertEquals(
      totalPrivacyBudgetUsageUnderAdvancedComposition(PrivacyCharge(1.0f, 0.001f), 30, 0.1f),
      20.0f
    )
    assertEquals(
      totalPrivacyBudgetUsageUnderAdvancedComposition(PrivacyCharge(1.0f, 0.2f), 1, 0.1f),
      null
    )
    assertEquals(
      totalPrivacyBudgetUsageUnderAdvancedComposition(PrivacyCharge(1.0f, 0.01f), 30, 0.26f),
      null
    )
  }
}
