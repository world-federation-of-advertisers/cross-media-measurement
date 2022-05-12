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

import com.google.common.truth.Truth.assertThat
import java.time.LocalDate
import org.junit.Before
import org.junit.Test
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.AgeGroup
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.Gender
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBucketGroup
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetLedgerBackingStore
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetLedgerTransactionContext
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyCharge
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyReference

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

        txContext.addLedgerEntries(
          listOf(bucket1, bucket1, bucket2, bucket3),
          listOf(charge),
          PrivacyReference("RequisitioId1", true)
        )

        val intersectingEntry = txContext.findIntersectingLedgerEntries(bucket1)
        assertThat(intersectingEntry.size).isEqualTo(1)
        assertThat(intersectingEntry.get(0).repetitionCount).isEqualTo(2)
        assertThat(txContext.findIntersectingLedgerEntries(bucket2).size).isEqualTo(1)
        assertThat(txContext.findIntersectingLedgerEntries(bucket3).size).isEqualTo(1)
        assertThat(txContext.findIntersectingLedgerEntries(bucket4).size).isEqualTo(0)
      }
    }
  }

  @Test(timeout = 15000)
  fun `addLedgerEntries as a refund decresases repetitionCount`() {
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
        txContext.addLedgerEntries(
          listOf(bucket1),
          listOf(charge),
          PrivacyReference("RequisitioId1", true)
        )

        val matchingLedgerEntries = txContext.findIntersectingLedgerEntries(bucket1)
        assertThat(matchingLedgerEntries.size).isEqualTo(1)

        assertThat(matchingLedgerEntries[0].repetitionCount).isEqualTo(1)

        txContext.addLedgerEntries(
          listOf(bucket1),
          listOf(charge),
          PrivacyReference("RequisitioId1", false)
        )

        val newMatchingLedgerEntries = txContext.findIntersectingLedgerEntries(bucket1)
        assertThat(newMatchingLedgerEntries.size).isEqualTo(1)
        assertThat(newMatchingLedgerEntries[0].repetitionCount).isEqualTo(0)
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

    txContext.addLedgerEntries(
      listOf(bucket1),
      listOf(charge),
      PrivacyReference("RequisitioId1", true)
    )

    assertThat(txContext.findIntersectingLedgerEntries(bucket1).size).isEqualTo(1)

    val newBackingStore = createBackingStore()
    newBackingStore.startTransaction().use { newTxContext ->
      assertThat(newTxContext.findIntersectingLedgerEntries(bucket1).size).isEqualTo(0)
    }

    txContext.commit()
    txContext.close()
    backingStore.close()

    newBackingStore.startTransaction().use { newTxContext ->
      assertThat(newTxContext.findIntersectingLedgerEntries(bucket1).size).isEqualTo(1)
    }
    newBackingStore.close()
  }
}
