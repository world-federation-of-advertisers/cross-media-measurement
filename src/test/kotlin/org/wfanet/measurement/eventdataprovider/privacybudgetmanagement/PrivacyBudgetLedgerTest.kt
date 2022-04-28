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
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.testing.InMemoryBackingStore

class PrivacyBudgetLedgerTest {
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

    ledger.chargePrivacyBucketGroups(listOf<PrivacyBucketGroup>(), listOf(charge))
    ledger.chargePrivacyBucketGroups(listOf(bucket), listOf(charge))
    assertFailsWith<PrivacyBudgetManagerException> {
      ledger.chargePrivacyBucketGroups(listOf(bucket), listOf(PrivacyCharge(0.0001f, 0.0001f)))
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

    ledger.chargePrivacyBucketGroups(listOf(bucket), listOf())
    ledger.chargePrivacyBucketGroups(listOf(bucket), listOf(charge))
    assertFailsWith<PrivacyBudgetManagerException> {
      ledger.chargePrivacyBucketGroups(listOf(bucket), listOf(PrivacyCharge(0.0001f, 0.0001f)))
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

    for (i in 1..29) {
      ledger.chargePrivacyBucketGroups(listOf(bucket), listOf(charge))
    }
    assertFailsWith<PrivacyBudgetManagerException> {
      ledger.chargePrivacyBucketGroups(listOf(bucket), listOf(charge))
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

    ledger.chargePrivacyBucketGroups(listOf(bucket), listOf(PrivacyCharge(0.4f, 0.001f)))
    ledger.chargePrivacyBucketGroups(listOf(bucket), listOf(PrivacyCharge(0.3f, 0.001f)))
    ledger.chargePrivacyBucketGroups(listOf(bucket), listOf(PrivacyCharge(0.2f, 0.001f)))
    ledger.chargePrivacyBucketGroups(listOf(bucket), listOf(PrivacyCharge(0.1f, 0.001f)))

    assertFailsWith<PrivacyBudgetManagerException> {
      ledger.chargePrivacyBucketGroups(listOf(bucket), listOf(PrivacyCharge(0.1f, 0.001f)))
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

    ledger.chargePrivacyBucketGroups(listOf(bucket1), listOf(PrivacyCharge(0.5f, 0.001f)))

    ledger.chargePrivacyBucketGroups(listOf(bucket2), listOf(PrivacyCharge(0.5f, 0.001f)))

    ledger.chargePrivacyBucketGroups(listOf(bucket1), listOf(PrivacyCharge(0.5f, 0.001f)))

    ledger.chargePrivacyBucketGroups(listOf(bucket2), listOf(PrivacyCharge(0.5f, 0.001f)))

    assertFailsWith<PrivacyBudgetManagerException> {
      ledger.chargePrivacyBucketGroups(listOf(bucket1), listOf(PrivacyCharge(0.5f, 0.001f)))
    }

    assertFailsWith<PrivacyBudgetManagerException> {
      ledger.chargePrivacyBucketGroups(listOf(bucket2), listOf(PrivacyCharge(0.5f, 0.001f)))
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

    ledger.chargePrivacyBucketGroups(listOf(bucket), chargeList)

    assertFailsWith<PrivacyBudgetManagerException> {
      ledger.chargePrivacyBucketGroups(listOf(bucket), listOf(PrivacyCharge(0.1f, 0.001f)))
    }
  }
}
