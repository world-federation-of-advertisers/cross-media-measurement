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
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.Charge
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.Gender
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBucketGroup
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetLedgerBackingStore
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetLedgerTransactionContext
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.Reference

private const val MEASUREMENT_CONSUMER_ID = "MC"

abstract class AbstractPrivacyBudgetLedgerStoreTest {
  protected abstract fun createBackingStore(): PrivacyBudgetLedgerBackingStore
  protected abstract fun recreateSchema()

  @Before
  fun prepareTest() {
    recreateSchema()
  }

  @Test(timeout = 15000)
  fun `findIntersectingBalanceEntries finds balance entries`() {
    createBackingStore().use { backingStore: PrivacyBudgetLedgerBackingStore ->
      backingStore.startTransaction().use { txContext: PrivacyBudgetLedgerTransactionContext ->
        val bucket1 =
          PrivacyBucketGroup(
            MEASUREMENT_CONSUMER_ID,
            LocalDate.parse("2021-07-01"),
            LocalDate.parse("2021-07-01"),
            AgeGroup.RANGE_35_54,
            Gender.MALE,
            0.3f,
            0.1f
          )

        val bucket2 =
          PrivacyBucketGroup(
            MEASUREMENT_CONSUMER_ID,
            LocalDate.parse("2021-07-01"),
            LocalDate.parse("2021-07-01"),
            AgeGroup.RANGE_35_54,
            Gender.MALE,
            0.5f,
            0.1f
          )

        val bucket3 =
          PrivacyBucketGroup(
            MEASUREMENT_CONSUMER_ID,
            LocalDate.parse("2021-07-01"),
            LocalDate.parse("2021-07-01"),
            AgeGroup.RANGE_35_54,
            Gender.FEMALE,
            0.3f,
            0.1f
          )

        val bucket4 =
          PrivacyBucketGroup(
            MEASUREMENT_CONSUMER_ID,
            LocalDate.parse("2021-07-01"),
            LocalDate.parse("2021-07-01"),
            AgeGroup.RANGE_35_54,
            Gender.FEMALE,
            0.5f,
            0.1f
          )

        val charge = Charge(0.01f, 0.0001f)

        txContext.addLedgerEntries(
          setOf(bucket1, bucket2, bucket3),
          setOf(charge),
          Reference(MEASUREMENT_CONSUMER_ID, "RequisitioId1", false)
        )

        txContext.addLedgerEntries(
          setOf(bucket1),
          setOf(charge),
          Reference(MEASUREMENT_CONSUMER_ID, "RequisitioId2", false)
        )

        val intersectingEntry = txContext.findIntersectingBalanceEntries(bucket1)
        assertThat(intersectingEntry.size).isEqualTo(1)
        assertThat(intersectingEntry.get(0).repetitionCount).isEqualTo(2)
        assertThat(txContext.findIntersectingBalanceEntries(bucket2).size).isEqualTo(1)
        assertThat(txContext.findIntersectingBalanceEntries(bucket3).size).isEqualTo(1)
        assertThat(txContext.findIntersectingBalanceEntries(bucket4).size).isEqualTo(0)
      }
    }
  }

  @Test(timeout = 15000)
  fun `addLedgerEntries as a refund decresases repetitionCount`() {
    createBackingStore().use { backingStore: PrivacyBudgetLedgerBackingStore ->
      backingStore.startTransaction().use { txContext: PrivacyBudgetLedgerTransactionContext ->
        val bucket1 =
          PrivacyBucketGroup(
            MEASUREMENT_CONSUMER_ID,
            LocalDate.parse("2021-07-01"),
            LocalDate.parse("2021-07-01"),
            AgeGroup.RANGE_35_54,
            Gender.MALE,
            0.3f,
            0.1f
          )

        val charge = Charge(0.01f, 0.0001f)
        txContext.addLedgerEntries(
          setOf(bucket1),
          setOf(charge),
          Reference(MEASUREMENT_CONSUMER_ID, "RequisitioId1", false)
        )
        val matchingBalanceEntries = txContext.findIntersectingBalanceEntries(bucket1)
        assertThat(matchingBalanceEntries.size).isEqualTo(1)

        assertThat(matchingBalanceEntries[0].repetitionCount).isEqualTo(1)
        txContext.addLedgerEntries(
          setOf(bucket1),
          setOf(charge),
          Reference(MEASUREMENT_CONSUMER_ID, "RequisitioId1", true)
        )
        val newmatchingBalanceEntries = txContext.findIntersectingBalanceEntries(bucket1)
        assertThat(newmatchingBalanceEntries.size).isEqualTo(1)
        assertThat(newmatchingBalanceEntries[0].repetitionCount).isEqualTo(0)
      }
    }
  }

  @Test(timeout = 15000)
  fun `addLedgerEntries for different MCs and same referenceId don't point to same balances`() {
    val requisitionId = "RequisitioId1"
    val otherMeasurementConsumerId = "otherMeasurementConsumerId"
    createBackingStore().use { backingStore: PrivacyBudgetLedgerBackingStore ->
      backingStore.startTransaction().use { txContext: PrivacyBudgetLedgerTransactionContext ->
        val bucket =
          PrivacyBucketGroup(
            MEASUREMENT_CONSUMER_ID,
            LocalDate.parse("2021-07-01"),
            LocalDate.parse("2021-07-01"),
            AgeGroup.RANGE_35_54,
            Gender.MALE,
            0.3f,
            0.1f
          )
        val bucketForOtherMC = bucket.copy(measurementConsumerId = otherMeasurementConsumerId)
        val charge = Charge(0.01f, 0.0001f)

        txContext.addLedgerEntries(
          setOf(bucket),
          setOf(charge),
          Reference(MEASUREMENT_CONSUMER_ID, requisitionId, false)
        )

        txContext.addLedgerEntries(
          setOf(bucketForOtherMC),
          setOf(charge),
          Reference(otherMeasurementConsumerId, requisitionId, false)
        )

        val matchingBalanceEntries = txContext.findIntersectingBalanceEntries(bucket)
        val otherMcMatchingBalanceEntries =
          txContext.findIntersectingBalanceEntries(bucketForOtherMC)

        assertThat(matchingBalanceEntries.size).isEqualTo(1)
        assertThat(matchingBalanceEntries[0].repetitionCount).isEqualTo(1)

        assertThat(otherMcMatchingBalanceEntries.size).isEqualTo(1)
        assertThat(otherMcMatchingBalanceEntries[0].repetitionCount).isEqualTo(1)
      }
    }
  }

  @Test(timeout = 15000)
  fun `commit() persists a transaction after it closes`() {
    val backingStore = createBackingStore()
    val txContext = backingStore.startTransaction()

    val bucket1 =
      PrivacyBucketGroup(
        MEASUREMENT_CONSUMER_ID,
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.MALE,
        0.3f,
        0.1f
      )

    val charge = Charge(0.01f, 0.0001f)
    txContext.addLedgerEntries(
      setOf(bucket1),
      setOf(charge),
      Reference(MEASUREMENT_CONSUMER_ID, "RequisitioId1", false)
    )
    assertThat(txContext.findIntersectingBalanceEntries(bucket1).size).isEqualTo(1)

    val newBackingStore = createBackingStore()
    newBackingStore.startTransaction().use { newTxContext ->
      assertThat(newTxContext.findIntersectingBalanceEntries(bucket1).size).isEqualTo(0)
    }

    txContext.commit()
    txContext.close()
    backingStore.close()

    newBackingStore.startTransaction().use { newTxContext ->
      assertThat(newTxContext.findIntersectingBalanceEntries(bucket1).size).isEqualTo(1)
    }
    newBackingStore.close()
  }

  @Test(timeout = 15000)
  fun `hasLedgerEntry returns true when ledger entry with same isRefund is found false o w`() {
    val backingStore = createBackingStore()
    val txContext1 = backingStore.startTransaction()
    val bucket1 =
      PrivacyBucketGroup(
        MEASUREMENT_CONSUMER_ID,
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.MALE,
        0.3f,
        0.1f
      )

    val charge = Charge(0.01f, 0.0001f)
    // charge works
    txContext1.addLedgerEntries(
      setOf(bucket1),
      setOf(charge),
      Reference(MEASUREMENT_CONSUMER_ID, "RequisitioId1", false)
    )

    txContext1.commit()
    val txContext2 = backingStore.startTransaction()

    // charge should not be proccessed if same
    assertThat(
        txContext2.hasLedgerEntry(Reference(MEASUREMENT_CONSUMER_ID, "RequisitioId1", false))
      )
      .isFalse()
    // but refund is allowed
    assertThat(txContext2.hasLedgerEntry(Reference(MEASUREMENT_CONSUMER_ID, "RequisitioId1", true)))
      .isTrue()
    // refund works

    txContext2.addLedgerEntries(
      setOf(bucket1),
      setOf(charge),
      Reference(MEASUREMENT_CONSUMER_ID, "RequisitioId1", true)
    )
    txContext2.commit()
    val txContext3 = backingStore.startTransaction()
    // now charge is allowed again
    assertThat(
        txContext3.hasLedgerEntry(Reference(MEASUREMENT_CONSUMER_ID, "RequisitioId1", false))
      )
      .isTrue()
    txContext3.commit()
  }

  @Test(timeout = 15000)
  fun `hasLedgerEntry returns true for ledger entry with same isRefund for Multiple MCs`() {
    val backingStore = createBackingStore()
    val txContext1 = backingStore.startTransaction()
    val requisitionId = "RequisitioId1"
    val bucket1 =
      PrivacyBucketGroup(
        MEASUREMENT_CONSUMER_ID,
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.MALE,
        0.3f,
        0.1f
      )

    val charge = Charge(0.01f, 0.0001f)
    // charge works
    txContext1.addLedgerEntries(
      setOf(bucket1),
      setOf(charge),
      Reference(MEASUREMENT_CONSUMER_ID, requisitionId, false)
    )

    txContext1.commit()
    val txContext2 = backingStore.startTransaction()

    // charge should not be proccessed if same
    assertThat(txContext2.hasLedgerEntry(Reference(MEASUREMENT_CONSUMER_ID, requisitionId, false)))
      .isFalse()
    // but other measurement consumer can charge with the same RequisitionId is allowed
    assertThat(
        txContext2.hasLedgerEntry(Reference("OtherMeasurementConsumer", requisitionId, false))
      )
      .isTrue()

    txContext2.commit()
  }
}
