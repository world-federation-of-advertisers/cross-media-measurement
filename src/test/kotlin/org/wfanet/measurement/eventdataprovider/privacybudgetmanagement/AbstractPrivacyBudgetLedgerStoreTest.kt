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
import kotlin.test.assertEquals
import org.junit.Test

abstract class AbstractPrivacyBudgetLedgerStoreTest {
  protected abstract fun createBackingStore(
    createSchema: Boolean = false
  ): PrivacyBudgetLedgerBackingStore

  @Test(timeout = 15000)
  fun `findIntersectingEntries works as expected`() {
    createBackingStore(true).use { backingStore: PrivacyBudgetLedgerBackingStore ->
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

        assertEquals(txContext.findIntersectingLedgerEntries(bucket1).size, 2)
        assertEquals(txContext.findIntersectingLedgerEntries(bucket2).size, 1)
        assertEquals(txContext.findIntersectingLedgerEntries(bucket3).size, 1)
        assertEquals(txContext.findIntersectingLedgerEntries(bucket4).size, 0)
      }
    }
  }

  @Test(timeout = 15000)
  fun `updateLedgerEntry works as expected`() {
    createBackingStore(true).use { backingStore: PrivacyBudgetLedgerBackingStore ->
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
        assertEquals(matchingLedgerEntries.size, 1)

        val ledgerEntry = matchingLedgerEntries[0]
        assertEquals(ledgerEntry.repetitionCount, 1)

        val updatedLedgerEntry =
          PrivacyBudgetLedgerEntry(ledgerEntry.rowId, ledgerEntry.transactionId, bucket1, charge, 2)

        txContext.updateLedgerEntry(updatedLedgerEntry)

        val newMatchingLedgerEntries = txContext.findIntersectingLedgerEntries(bucket1)
        assertEquals(newMatchingLedgerEntries.size, 1)

        val newLedgerEntry = newMatchingLedgerEntries[0]
        assertEquals(newLedgerEntry.repetitionCount, 2)
      }
    }
  }

  @Test(timeout = 15000)
  fun `mergePreviousTransaction works as expected`() {
    createBackingStore(true).use { backingStore: PrivacyBudgetLedgerBackingStore ->
      var txId: Long
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
      backingStore.startTransaction().use { txContext: PrivacyBudgetLedgerTransactionContext ->
        txId = txContext.transactionId
        val charge = PrivacyCharge(0.01f, 0.0001f)
        txContext.addLedgerEntry(bucket1, charge)
        txContext.commit()
      }

      backingStore.startTransaction().use { newTxContext: PrivacyBudgetLedgerTransactionContext ->
        val matchingLedgerEntries = newTxContext.findIntersectingLedgerEntries(bucket1)
        assertEquals(matchingLedgerEntries.size, 1)
        assertEquals(matchingLedgerEntries[0].transactionId, txId)
        newTxContext.mergePreviousTransaction(txId)
        val newMatchingLedgerEntries = newTxContext.findIntersectingLedgerEntries(bucket1)
        assertEquals(newMatchingLedgerEntries.size, 1)
        assertEquals(newMatchingLedgerEntries[0].transactionId, 0)
      }
    }
  }

  @Test(timeout = 15000)
  fun `undoPreviousTransaction works as expected`() {
    createBackingStore(true).use { backingStore: PrivacyBudgetLedgerBackingStore ->
      var txId: Long
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
      backingStore.startTransaction().use { txContext: PrivacyBudgetLedgerTransactionContext ->
        txId = txContext.transactionId
        val charge = PrivacyCharge(0.01f, 0.0001f)
        txContext.addLedgerEntry(bucket1, charge)
        txContext.commit()
      }

      backingStore.startTransaction().use { newTxContext: PrivacyBudgetLedgerTransactionContext ->
        val matchingLedgerEntries = newTxContext.findIntersectingLedgerEntries(bucket1)
        assertEquals(matchingLedgerEntries.size, 1)
        assertEquals(matchingLedgerEntries[0].transactionId, txId)
        newTxContext.undoPreviousTransaction(txId)
        val newMatchingLedgerEntries = newTxContext.findIntersectingLedgerEntries(bucket1)
        assertEquals(newMatchingLedgerEntries.size, 0)
      }
    }
  }

  @Test(timeout = 15000)
  fun `commit works as expected`() {
    val backingStore = createBackingStore(true)
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

    assertEquals(txContext.findIntersectingLedgerEntries(bucket1).size, 1)

    val newBackingStore = createBackingStore()
    newBackingStore.startTransaction().use { newTxContext ->
      assertEquals(newTxContext.findIntersectingLedgerEntries(bucket1).size, 0)
    }

    txContext.commit()
    txContext.close()
    backingStore.close()

    newBackingStore.startTransaction().use { newTxContext ->
      assertEquals(newTxContext.findIntersectingLedgerEntries(bucket1).size, 1)
    }
    newBackingStore.close()
  }
}
