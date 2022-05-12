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

import java.time.LocalDate
import kotlin.test.assertFailsWith
import org.junit.Test

class PrivacyBudgetLedgerTest {

  private fun createPrivacyReference(id: Int, charge: Boolean = true) =
    PrivacyReference("RequisitioId${id}", charge)

  @Test
  fun `Charge works when privacy bucket groups are empty`() {
    val backingStore = InMemoryBackingStore()
    val ledger = PrivacyBudgetLedger(backingStore, 1.0f, 0.01f)
    val bucket =
      PrivacyBucketGroup(
        "ACME",
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.MALE,
        0.3f,
        0.1f
      )
    val charge = PrivacyCharge(1.0f, 0.01f)

    // The charges succeed and fills the Privacy Budget.
    ledger.chargePrivacyBucketGroups(
      createPrivacyReference(0),
      listOf<PrivacyBucketGroup>(),
      listOf(charge)
    )
    ledger.chargePrivacyBucketGroups(createPrivacyReference(1), listOf(bucket), listOf(charge))

    // Charge should exceed the budget.
    assertFailsWith<PrivacyBudgetManagerException> {
      ledger.chargePrivacyBucketGroups(
        createPrivacyReference(2),
        listOf(bucket),
        listOf(PrivacyCharge(0.0001f, 0.0001f))
      )
    }
  }

  @Test
  fun `Charge works when charge list is empty`() {
    val backingStore = InMemoryBackingStore()
    val ledger = PrivacyBudgetLedger(backingStore, 1.0f, 0.01f)
    val bucket =
      PrivacyBucketGroup(
        "ACME",
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.MALE,
        0.3f,
        0.1f
      )
    val charge = PrivacyCharge(1.0f, 0.01f)

    // The charges succeed and doesn't charge anything.
    ledger.chargePrivacyBucketGroups(createPrivacyReference(0), listOf(bucket), listOf())
    // The charges succeed and fills the Privacy Budget.
    ledger.chargePrivacyBucketGroups(createPrivacyReference(1), listOf(bucket), listOf(charge))

    // Charge should exceed the budget.
    assertFailsWith<PrivacyBudgetManagerException> {
      ledger.chargePrivacyBucketGroups(
        createPrivacyReference(2),
        listOf(bucket),
        listOf(PrivacyCharge(0.0001f, 0.0001f))
      )
    }
  }

  @Test
  fun `Charge same value repeatedly`() {
    // See the second test in AdvancedCompositionTest`advanced composition`
    val backingStore = InMemoryBackingStore()
    val ledger = PrivacyBudgetLedger(backingStore, 21.5f, 0.06f)
    val bucket =
      PrivacyBucketGroup(
        "ACME",
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.MALE,
        0.3f,
        0.1f
      )
    val charge = PrivacyCharge(1.0f, 0.001f)
    // The charges succeed and fills the Privacy Budget.
    for (i in 1..29) {
      ledger.chargePrivacyBucketGroups(createPrivacyReference(i), listOf(bucket), listOf(charge))
    }
    assertFailsWith<PrivacyBudgetManagerException> {
      ledger.chargePrivacyBucketGroups(createPrivacyReference(30), listOf(bucket), listOf(charge))
    }
  }

  @Test
  fun `Charges with the same reference key are no op`() {
    // See the second test in AdvancedCompositionTest`advanced composition`
    val backingStore = InMemoryBackingStore()
    val ledger = PrivacyBudgetLedger(backingStore, 21.5f, 0.06f)
    val bucket =
      PrivacyBucketGroup(
        "ACME",
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.MALE,
        0.3f,
        0.1f
      )
    val charge = PrivacyCharge(1.0f, 0.001f)
    // Only the first charge would fill the budget, rest are not processed by the ledger
    for (i in 1..100) {
      ledger.chargePrivacyBucketGroups(createPrivacyReference(0), listOf(bucket), listOf(charge))
    }
  }

  @Test
  fun `Refund of the charge works`() {
    // See the second test in AdvancedCompositionTest`advanced composition`
    val backingStore = InMemoryBackingStore()
    val ledger = PrivacyBudgetLedger(backingStore, 21.5f, 0.06f)
    val bucket =
      PrivacyBucketGroup(
        "ACME",
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.MALE,
        0.3f,
        0.1f
      )
    val charge = PrivacyCharge(1.0f, 0.001f)
    // The charges succeed and fills the Privacy Budget.
    for (i in 1..29) {
      ledger.chargePrivacyBucketGroups(createPrivacyReference(i), listOf(bucket), listOf(charge))
    }
    assertFailsWith<PrivacyBudgetManagerException> {
      ledger.chargePrivacyBucketGroups(createPrivacyReference(30), listOf(bucket), listOf(charge))
    }

    // The refund opens up Privacy Budget.
    ledger.chargePrivacyBucketGroups(
      createPrivacyReference(31, false),
      listOf(bucket),
      listOf(charge)
    )
    // Thus, this charge succeeds and fills the budget.
    ledger.chargePrivacyBucketGroups(createPrivacyReference(32), listOf(bucket), listOf(charge))

    // Then this charge fails.
    assertFailsWith<PrivacyBudgetManagerException> {
      ledger.chargePrivacyBucketGroups(createPrivacyReference(33), listOf(bucket), listOf(charge))
    }
  }

  @Test
  fun `Charge different values`() {
    val backingStore = InMemoryBackingStore()
    val ledger = PrivacyBudgetLedger(backingStore, 1.0001f, 0.01f)
    val bucket =
      PrivacyBucketGroup(
        "ACME",
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.MALE,
        0.3f,
        0.1f
      )

    // The charges succeed and fills the Privacy Budget.
    ledger.chargePrivacyBucketGroups(
      createPrivacyReference(0),
      listOf(bucket),
      listOf(PrivacyCharge(0.4f, 0.001f))
    )
    ledger.chargePrivacyBucketGroups(
      createPrivacyReference(1),
      listOf(bucket),
      listOf(PrivacyCharge(0.3f, 0.001f))
    )
    ledger.chargePrivacyBucketGroups(
      createPrivacyReference(2),
      listOf(bucket),
      listOf(PrivacyCharge(0.2f, 0.001f))
    )
    ledger.chargePrivacyBucketGroups(
      createPrivacyReference(3),
      listOf(bucket),
      listOf(PrivacyCharge(0.1f, 0.001f))
    )

    // Charge should exceed the budget.
    assertFailsWith<PrivacyBudgetManagerException> {
      ledger.chargePrivacyBucketGroups(
        createPrivacyReference(4),
        listOf(bucket),
        listOf(PrivacyCharge(0.1f, 0.001f))
      )
    }
  }

  @Test
  fun `Charge multiple buckets`() {
    val backingStore = InMemoryBackingStore()
    val ledger = PrivacyBudgetLedger(backingStore, 1.0001f, 0.01f)
    val bucket1 =
      PrivacyBucketGroup(
        "ACME",
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.MALE,
        0.3f,
        0.1f
      )
    val bucket2 =
      PrivacyBucketGroup(
        "ACME",
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.FEMALE,
        0.3f,
        0.1f
      )

    // The charges succeed and fills the Privacy Budget.
    ledger.chargePrivacyBucketGroups(
      createPrivacyReference(0),
      listOf(bucket1),
      listOf(PrivacyCharge(0.5f, 0.001f))
    )
    ledger.chargePrivacyBucketGroups(
      createPrivacyReference(1),
      listOf(bucket2),
      listOf(PrivacyCharge(0.5f, 0.001f))
    )
    ledger.chargePrivacyBucketGroups(
      createPrivacyReference(2),
      listOf(bucket1),
      listOf(PrivacyCharge(0.5f, 0.001f))
    )
    ledger.chargePrivacyBucketGroups(
      createPrivacyReference(3),
      listOf(bucket2),
      listOf(PrivacyCharge(0.5f, 0.001f))
    )

    // Charge should exceed the budget.
    assertFailsWith<PrivacyBudgetManagerException> {
      ledger.chargePrivacyBucketGroups(
        createPrivacyReference(4),
        listOf(bucket1),
        listOf(PrivacyCharge(0.5f, 0.001f))
      )
    }

    // Charge should exceed the budget.
    assertFailsWith<PrivacyBudgetManagerException> {
      ledger.chargePrivacyBucketGroups(
        createPrivacyReference(5),
        listOf(bucket2),
        listOf(PrivacyCharge(0.5f, 0.001f))
      )
    }
  }

  @Test
  fun `Charge list of values`() {
    val backingStore = InMemoryBackingStore()
    val ledger = PrivacyBudgetLedger(backingStore, 1.0001f, 0.01f)
    val bucket =
      PrivacyBucketGroup(
        "ACME",
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.MALE,
        0.3f,
        0.1f
      )

    val chargeList =
      listOf(PrivacyCharge(0.5f, 0.001f), PrivacyCharge(0.3f, 0.001f), PrivacyCharge(0.2f, 0.001f))

    // The charges succeed and fills the Privacy Budget.
    ledger.chargePrivacyBucketGroups(createPrivacyReference(0), listOf(bucket), chargeList)

    // Charge should exceed the budget.
    assertFailsWith<PrivacyBudgetManagerException> {
      ledger.chargePrivacyBucketGroups(
        createPrivacyReference(1),
        listOf(bucket),
        listOf(PrivacyCharge(0.1f, 0.001f))
      )
    }
  }
}
