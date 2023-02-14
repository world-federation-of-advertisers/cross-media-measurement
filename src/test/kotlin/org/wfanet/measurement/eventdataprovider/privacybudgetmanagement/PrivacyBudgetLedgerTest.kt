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

import java.time.LocalDate
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test

class PrivacyBudgetLedgerTest {

  private fun createReference(id: Int, isRefund: Boolean = false) =
    Reference("MC1", "RequisitioId$id", isRefund)

  @Test
  fun `Charge works when privacy bucket groups are empty`() =
    runBlocking<Unit> {
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
      val charge = Charge(1.0f, 0.01f)

      // The charges succeed and fills the Privacy Budget.
      ledger.charge(createReference(0), setOf<PrivacyBucketGroup>(), setOf(charge))
      ledger.charge(createReference(1), setOf(bucket), setOf(charge))

      // Charge should exceed the budget.
      assertFailsWith<PrivacyBudgetManagerException> {
        ledger.charge(createReference(2), setOf(bucket), setOf(Charge(0.0001f, 0.0001f)))
      }
    }

  @Test
  fun `Charge works when charge list is empty`() =
    runBlocking<Unit> {
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
      val charge = Charge(1.0f, 0.01f)

      // The charges succeed and doesn't charge anything.
      ledger.charge(createReference(0), setOf(bucket), setOf())
      // The charges succeed and fills the Privacy Budget.
      ledger.charge(createReference(1), setOf(bucket), setOf(charge))

      // Charge should exceed the budget.
      assertFailsWith<PrivacyBudgetManagerException> {
        ledger.charge(createReference(2), setOf(bucket), setOf(Charge(0.0001f, 0.0001f)))
      }
    }

  @Test
  fun `Charge same value repeatedly`() =
    runBlocking<Unit> {
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
      val charge = Charge(1.0f, 0.001f)
      // The charges succeed and fills the Privacy Budget.
      for (i in 1..29) {
        ledger.charge(createReference(i), setOf(bucket), setOf(charge))
      }
      assertFailsWith<PrivacyBudgetManagerException> {
        ledger.charge(createReference(30), setOf(bucket), setOf(charge))
      }
    }

  @Test
  fun `Charges with the same reference key are no op`() =
    runBlocking<Unit> {
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
      val charge = Charge(1.0f, 0.001f)
      // Only the first charge would fill the budget, rest are not processed by the ledger
      for (i in 1..100) {
        ledger.charge(createReference(0), setOf(bucket), setOf(charge))
      }
    }

  @Test
  fun `Refund of the charge works`() =
    runBlocking<Unit> {
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
      val charge = Charge(1.0f, 0.001f)
      // The charges succeed and fills the Privacy Budget.
      for (i in 1..29) {
        ledger.charge(createReference(i), setOf(bucket), setOf(charge))
      }
      assertFailsWith<PrivacyBudgetManagerException> {
        ledger.charge(createReference(30), setOf(bucket), setOf(charge))
      }

      // The refund opens up Privacy Budget.
      ledger.charge(createReference(31, true), setOf(bucket), setOf(charge))
      // Thus, this charge succeeds and fills the budget.
      ledger.charge(createReference(32), setOf(bucket), setOf(charge))

      // Then this charge fails.
      assertFailsWith<PrivacyBudgetManagerException> {
        ledger.charge(createReference(33), setOf(bucket), setOf(charge))
      }
    }

  @Test
  fun `Charge different values`() =
    runBlocking<Unit> {
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
      ledger.charge(createReference(0), setOf(bucket), setOf(Charge(0.4f, 0.001f)))
      ledger.charge(createReference(1), setOf(bucket), setOf(Charge(0.3f, 0.001f)))
      ledger.charge(createReference(2), setOf(bucket), setOf(Charge(0.2f, 0.001f)))
      ledger.charge(createReference(3), setOf(bucket), setOf(Charge(0.1f, 0.001f)))

      // Charge should exceed the budget.
      assertFailsWith<PrivacyBudgetManagerException> {
        ledger.charge(createReference(4), setOf(bucket), setOf(Charge(0.1f, 0.001f)))
      }
    }

  @Test
  fun `Charge multiple buckets`() =
    runBlocking<Unit> {
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
      ledger.charge(createReference(0), setOf(bucket1), setOf(Charge(0.5f, 0.001f)))
      ledger.charge(createReference(1), setOf(bucket2), setOf(Charge(0.5f, 0.001f)))
      ledger.charge(createReference(2), setOf(bucket1), setOf(Charge(0.5f, 0.001f)))
      ledger.charge(createReference(3), setOf(bucket2), setOf(Charge(0.5f, 0.001f)))

      // Charge should exceed the budget.
      assertFailsWith<PrivacyBudgetManagerException> {
        ledger.charge(createReference(4), setOf(bucket1), setOf(Charge(0.5f, 0.001f)))
      }

      // Charge should exceed the budget.
      assertFailsWith<PrivacyBudgetManagerException> {
        ledger.charge(createReference(5), setOf(bucket2), setOf(Charge(0.5f, 0.001f)))
      }
    }

  @Test
  fun `Charge list of values`() =
    runBlocking<Unit> {
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

      val chargeList = setOf(Charge(0.5f, 0.001f), Charge(0.3f, 0.001f), Charge(0.2f, 0.001f))

      // The charges succeed and fills the Privacy Budget.
      ledger.charge(createReference(0), setOf(bucket), chargeList)

      // Charge should exceed the budget.
      assertFailsWith<PrivacyBudgetManagerException> {
        ledger.charge(createReference(1), setOf(bucket), setOf(Charge(0.1f, 0.001f)))
      }
    }
}
