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
package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.testing

import java.time.LocalDate
import kotlin.test.assertEquals
import org.junit.Before
import org.junit.Test
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.AgeGroup
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.Gender
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBucketGroup
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetLedgerBackingStore
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetLedgerEntry
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetLedgerTransactionContext
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyCharge

abstract class AbstractPrivacyBudgetLedgerStoreTest {
  protected abstract fun createBackingStore(): PrivacyBudgetLedgerBackingStore
  protected abstract fun recreateSchema()

  @Before
  fun prepareTest() {
    recreateSchema()
  }

  @Test(timeout = 15000)
  fun `findIntersectingEntries finds ledger entries`() {
    createBackingStore().use { backingStore: PrivacyBudgetLedgerBackingStore ->
      backingStore.startTransaction().use { txContext: PrivacyBudgetLedgerTransactionContext ->
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
            Gender.MALE,
            0.5f,
            0.1f
          )

        val bucket3 =
          PrivacyBucketGroup(
            "ACME",
            LocalDate.parse("2021-07-01"),
            LocalDate.parse("2021-07-01"),
            AgeGroup.RANGE_35_54,
            Gender.FEMALE,
            0.3f,
            0.1f
          )

        val bucket4 =
          PrivacyBucketGroup(
            "ACME",
            LocalDate.parse("2021-07-01"),
            LocalDate.parse("2021-07-01"),
            AgeGroup.RANGE_35_54,
            Gender.FEMALE,
            0.5f,
            0.1f
          )

        val charge = PrivacyCharge(0.01f, 0.0001f)

        txContext.addLedgerEntry(bucket1, charge)
        txContext.addLedgerEntry(bucket1, charge)
        txContext.addLedgerEntry(bucket2, charge)
        txContext.addLedgerEntry(bucket3, charge)

        assertEquals(2, txContext.findIntersectingLedgerEntries(bucket1).size)
        assertEquals(1, txContext.findIntersectingLedgerEntries(bucket2).size)
        assertEquals(1, txContext.findIntersectingLedgerEntries(bucket3).size)
        assertEquals(0, txContext.findIntersectingLedgerEntries(bucket4).size)
      }
    }
  }

  @Test(timeout = 15000)
  fun `updateLedgerEntry can modify an entry's repetitionCount`() {
    createBackingStore().use { backingStore: PrivacyBudgetLedgerBackingStore ->
      backingStore.startTransaction().use { txContext: PrivacyBudgetLedgerTransactionContext ->
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

        val charge = PrivacyCharge(0.01f, 0.0001f)
        txContext.addLedgerEntry(bucket1, charge)

        val matchingLedgerEntries = txContext.findIntersectingLedgerEntries(bucket1)
        assertEquals(1, matchingLedgerEntries.size)

        val ledgerEntry = matchingLedgerEntries[0]
        assertEquals(1, ledgerEntry.repetitionCount)

        val updatedLedgerEntry =
          PrivacyBudgetLedgerEntry(ledgerEntry.rowId, ledgerEntry.transactionId, bucket1, charge, 2)

        txContext.updateLedgerEntry(updatedLedgerEntry)

        val newMatchingLedgerEntries = txContext.findIntersectingLedgerEntries(bucket1)
        assertEquals(1, newMatchingLedgerEntries.size)

        val newLedgerEntry = newMatchingLedgerEntries[0]
        assertEquals(2, newLedgerEntry.repetitionCount)
      }
    }
  }

  @Test(timeout = 15000)
  fun `commit() persists a transaction after it closes`() {
    val backingStore = createBackingStore()
    val txContext = backingStore.startTransaction()

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

    val charge = PrivacyCharge(0.01f, 0.0001f)

    txContext.addLedgerEntry(bucket1, charge)

    assertEquals(1, txContext.findIntersectingLedgerEntries(bucket1).size)

    val newBackingStore = createBackingStore()
    newBackingStore.startTransaction().use { newTxContext ->
      assertEquals(0, newTxContext.findIntersectingLedgerEntries(bucket1).size)
    }

    txContext.commit()
    txContext.close()
    backingStore.close()

    newBackingStore.startTransaction().use { newTxContext ->
      assertEquals(1, newTxContext.findIntersectingLedgerEntries(bucket1).size)
    }
    newBackingStore.close()
  }
}
