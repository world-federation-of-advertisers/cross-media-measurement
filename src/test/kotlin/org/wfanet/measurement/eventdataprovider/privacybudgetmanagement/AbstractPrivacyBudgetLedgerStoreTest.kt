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
import kotlin.test.assertNotNull
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

        assertEquals(2, txContext.findIntersectingLedgerEntries(bucket1).size)
        assertEquals(1, txContext.findIntersectingLedgerEntries(bucket2).size)
        assertEquals(1, txContext.findIntersectingLedgerEntries(bucket3).size)
        assertEquals(0, txContext.findIntersectingLedgerEntries(bucket4).size)
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
        assertEquals(1, matchingLedgerEntries.size)
        assertEquals(txId, matchingLedgerEntries[0].transactionId)
        newTxContext.mergePreviousTransaction(txId)
        val newMatchingLedgerEntries = newTxContext.findIntersectingLedgerEntries(bucket1)
        assertEquals(1, newMatchingLedgerEntries.size)
        assertEquals(0, newMatchingLedgerEntries[0].transactionId)
      }
    }
  }

  private val targetBucketGroup =
    PrivacyBucketGroup(
      "ACME",
      LocalDate.parse("2021-07-01"),
      LocalDate.parse("2021-07-01"),
      AgeGroup.RANGE_35_54,
      Gender.MALE,
      0.3f,
      0.1f
    )
  // Each sample bucket group are a modification of targetBucketGroup with a change in a single
  // dimension
  // to allow tests to check if the backing stores are updating only the target entry
  private val sampleBucketGroups =
    listOf(
      // Different consumer ID
      PrivacyBucketGroup(
        "COMPANY",
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.MALE,
        0.5f,
        0.1f
      ),
      // Different Date
      PrivacyBucketGroup(
        "ACME",
        LocalDate.parse("2021-07-02"),
        LocalDate.parse("2021-07-02"),
        AgeGroup.RANGE_35_54,
        Gender.MALE,
        0.3f,
        0.1f
      ),
      // Different AgeGroup
      PrivacyBucketGroup(
        "ACME",
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_18_34,
        Gender.MALE,
        0.3f,
        0.1f
      ),
      // Different Gender
      PrivacyBucketGroup(
        "ACME",
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.FEMALE,
        0.3f,
        0.1f
      ),
      // Different Vid start
      PrivacyBucketGroup(
        "ACME",
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.MALE,
        0.5f,
        0.1f
      ),
    )

  @Test(timeout = 15000)
  fun `mergePreviousTransaction updates repetition count of matching entry`() {
    val charge = PrivacyCharge(0.01f, 0.0001f)
    createBackingStore(true).use { backingStore: PrivacyBudgetLedgerBackingStore ->
      // Add target bucket entry and merge it.
      // Add sample bucket groups.
      backingStore.startTransaction().use { txContext: PrivacyBudgetLedgerTransactionContext ->
        txContext.addLedgerEntry(targetBucketGroup, charge)
        sampleBucketGroups.forEach { txContext.addLedgerEntry(it, charge) }
        txContext.mergePreviousTransaction(txContext.transactionId)
        txContext.commit()
      }
      // Add the same entry again as a new transaction
      var txId: Long
      backingStore.startTransaction().use { txContext: PrivacyBudgetLedgerTransactionContext ->
        txId = txContext.transactionId
        txContext.addLedgerEntry(targetBucketGroup, charge)
        txContext.commit()
      }

      backingStore.startTransaction().use { newTxContext: PrivacyBudgetLedgerTransactionContext ->
        val matchingLedgerEntries = newTxContext.findIntersectingLedgerEntries(targetBucketGroup)
        assertEquals(2, matchingLedgerEntries.size)
        val hasMergedEntry =
          matchingLedgerEntries[0].transactionId == 0L ||
            matchingLedgerEntries[1].transactionId == 0L
        val hasNonMergedEntry =
          matchingLedgerEntries[0].transactionId == txId ||
            matchingLedgerEntries[1].transactionId == txId
        assert(hasMergedEntry && hasNonMergedEntry)
        newTxContext.mergePreviousTransaction(txId)
        val newMatchingLedgerEntries = newTxContext.findIntersectingLedgerEntries(targetBucketGroup)
        assertEquals(1, newMatchingLedgerEntries.size)
        assertEquals(0, newMatchingLedgerEntries[0].transactionId)
        assertEquals(2, newMatchingLedgerEntries[0].repetitionCount)
        // Sanity check - check the unrelated sample bucket groups are unchanged
        sampleBucketGroups.forEach { bucket ->
          val unchangedLedgerEntries = newTxContext.findIntersectingLedgerEntries(bucket)
          assertEquals(1, unchangedLedgerEntries.size)
          assertEquals(1, unchangedLedgerEntries[0].repetitionCount)
        }
      }
    }
  }

  fun `mergePreviousTransaction only merges the right transaction`() {
    val charge = PrivacyCharge(0.01f, 0.0001f)
    createBackingStore(true).use { backingStore: PrivacyBudgetLedgerBackingStore ->
      backingStore.startTransaction().use { txContext: PrivacyBudgetLedgerTransactionContext ->
        txContext.addLedgerEntry(targetBucketGroup, charge)
        txContext.mergePreviousTransaction(txContext.transactionId)
        txContext.commit()
      }
      var notToMergeTransactionId: Long
      backingStore.startTransaction().use { txContext: PrivacyBudgetLedgerTransactionContext ->
        notToMergeTransactionId = txContext.transactionId
        txContext.addLedgerEntry(targetBucketGroup, charge)
        txContext.commit()
      }
      backingStore.startTransaction().use { txContext: PrivacyBudgetLedgerTransactionContext ->
        val txId = txContext.transactionId
        txContext.addLedgerEntry(targetBucketGroup, charge)
        txContext.mergePreviousTransaction(txId)
        txContext.commit()
      }
      backingStore.startTransaction().use { txContext: PrivacyBudgetLedgerTransactionContext ->
        val matchingEntries = txContext.findIntersectingLedgerEntries(targetBucketGroup)
        assertEquals(2, matchingEntries.size)
        val mergedEntry = assertNotNull(matchingEntries.find { it.transactionId == 0L })
        val notToMergeEntry =
          assertNotNull(matchingEntries.find { it.transactionId == notToMergeTransactionId })
        assertEquals(2, mergedEntry.repetitionCount)
        assertEquals(1, notToMergeEntry.repetitionCount)
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
        assertEquals(1, matchingLedgerEntries.size)
        assertEquals(txId, matchingLedgerEntries[0].transactionId)
        newTxContext.undoPreviousTransaction(txId)
        val newMatchingLedgerEntries = newTxContext.findIntersectingLedgerEntries(bucket1)
        assertEquals(0, newMatchingLedgerEntries.size)
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
