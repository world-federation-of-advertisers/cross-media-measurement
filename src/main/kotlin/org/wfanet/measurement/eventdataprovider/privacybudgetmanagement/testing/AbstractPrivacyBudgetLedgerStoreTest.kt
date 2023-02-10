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
package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.testing

import com.google.common.truth.Truth.assertThat
import java.time.LocalDate
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.AgeGroup
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.Charge
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.Gender
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBucketGroup
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetBalanceEntry
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetLedgerBackingStore
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetLedgerTransactionContext
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyLandscape
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
  fun `findIntersectingBalanceEntries finds balance entries`() = runBlocking {
    val bucket1 =
      PrivacyBucketGroup(
        MEASUREMENT_CONSUMER_ID,
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.MALE,
        0.3f,
        PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
      )

    val bucket2 =
      PrivacyBucketGroup(
        MEASUREMENT_CONSUMER_ID,
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.MALE,
        0.5f,
        PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
      )

    val bucket3 =
      PrivacyBucketGroup(
        MEASUREMENT_CONSUMER_ID,
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.FEMALE,
        0.3f,
        PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
      )

    val bucket4 =
      PrivacyBucketGroup(
        MEASUREMENT_CONSUMER_ID,
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.FEMALE,
        0.5f,
        PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
      )

    val charge = Charge(0.01f, 0.0001f)

    val backingStore = createBackingStore()
    val txContext1 = backingStore.startTransaction()
    txContext1.addLedgerEntries(
      setOf(bucket1, bucket2, bucket3),
      setOf(charge),
      Reference(MEASUREMENT_CONSUMER_ID, "RequisitioId1", false)
    )

    txContext1.addLedgerEntries(
      setOf(bucket1),
      setOf(charge),
      Reference(MEASUREMENT_CONSUMER_ID, "RequisitioId2", false)
    )
    txContext1.commit()
    val txContext2 = backingStore.startTransaction()
    val intersectingEntries: List<PrivacyBudgetBalanceEntry> =
      txContext2.findIntersectingBalanceEntries(bucket1)
    assertThat(intersectingEntries).hasSize(1)
    assertThat(intersectingEntries.get(0).repetitionCount).isEqualTo(2)
    assertThat(txContext2.findIntersectingBalanceEntries(bucket2)).hasSize(1)
    assertThat(txContext2.findIntersectingBalanceEntries(bucket3)).hasSize(1)
    assertThat(txContext2.findIntersectingBalanceEntries(bucket4)).hasSize(0)
    backingStore.close()
  }

  @Test(timeout = 15000)
  fun `addLedgerEntries as a refund decresases repetitionCount`() = runBlocking {
    createBackingStore().use { backingStore: PrivacyBudgetLedgerBackingStore ->
      val bucket1 =
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          LocalDate.parse("2021-07-01"),
          LocalDate.parse("2021-07-01"),
          AgeGroup.RANGE_35_54,
          Gender.MALE,
          0.3f,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
        )
      val charge = Charge(0.01f, 0.0001f)
      backingStore.startTransaction().use { txContext: PrivacyBudgetLedgerTransactionContext ->
        txContext.addLedgerEntries(
          setOf(bucket1),
          setOf(charge),
          Reference(MEASUREMENT_CONSUMER_ID, "RequisitioId1", false)
        )
        txContext.commit()
      }

      backingStore.startTransaction().use { txContext: PrivacyBudgetLedgerTransactionContext ->
        val matchingBalanceEntries = txContext.findIntersectingBalanceEntries(bucket1)
        assertThat(matchingBalanceEntries.size).isEqualTo(1)

        assertThat(matchingBalanceEntries[0].repetitionCount).isEqualTo(1)
        txContext.addLedgerEntries(
          setOf(bucket1),
          setOf(charge),
          Reference(MEASUREMENT_CONSUMER_ID, "RequisitioId1", true)
        )
        txContext.commit()
      }

      backingStore.startTransaction().use { txContext: PrivacyBudgetLedgerTransactionContext ->
        val newmatchingBalanceEntries = txContext.findIntersectingBalanceEntries(bucket1)
        assertThat(newmatchingBalanceEntries.size).isEqualTo(1)
        assertThat(newmatchingBalanceEntries[0].repetitionCount).isEqualTo(0)
      }
    }
  }

  @Test(timeout = 15000)
  fun `addLedgerEntries for different MCs and same referenceId don't point to same balances`() =
    runBlocking {
      val requisitionId = "RequisitioId1"
      val otherMeasurementConsumerId = "otherMeasurementConsumerId"
      val bucket =
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          LocalDate.parse("2021-07-01"),
          LocalDate.parse("2021-07-01"),
          AgeGroup.RANGE_35_54,
          Gender.MALE,
          0.3f,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
        )
      val bucketForOtherMC = bucket.copy(measurementConsumerId = otherMeasurementConsumerId)
      val charge = Charge(0.01f, 0.0001f)
      createBackingStore().use { backingStore: PrivacyBudgetLedgerBackingStore ->
        backingStore.startTransaction().use { txContext: PrivacyBudgetLedgerTransactionContext ->
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
          txContext.commit()
        }

        backingStore.startTransaction().use { txContext: PrivacyBudgetLedgerTransactionContext ->
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
  fun `hasLedgerEntry returns false when ledger is empty`() = runBlocking {
    val backingStore = createBackingStore()
    val txContext1 = backingStore.startTransaction()
    assertThat(
        txContext1.hasLedgerEntry(Reference(MEASUREMENT_CONSUMER_ID, "RequisitioId1", false))
      )
      .isFalse()
    txContext1.commit()
    txContext1.close()
    backingStore.close()
  }

  @Test(timeout = 15000)
  fun `commit() persists a transaction after it closes`() = runBlocking {
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
        PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
      )

    val charge = Charge(0.01f, 0.0001f)
    txContext.addLedgerEntries(
      setOf(bucket1),
      setOf(charge),
      Reference(MEASUREMENT_CONSUMER_ID, "RequisitioId1", false)
    )

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
  fun `hasLedgerEntry returns true when ledger entry with same isRefund is found false o w`() =
    runBlocking {
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
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
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

      // backing store already has the ledger entry
      assertThat(
          txContext2.hasLedgerEntry(Reference(MEASUREMENT_CONSUMER_ID, "RequisitioId1", false))
        )
        .isTrue()

      // but refund is allowed
      assertThat(
          txContext2.hasLedgerEntry(Reference(MEASUREMENT_CONSUMER_ID, "RequisitioId1", true))
        )
        .isFalse()

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
        .isFalse()
      txContext3.commit()
      backingStore.close()
    }

  @Test(timeout = 15000)
  fun `hasLedgerEntry returns true for ledger entry with same isRefund for Multiple MCs`() =
    runBlocking {
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
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
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

      // backing store already has the ledger entry
      assertThat(
          txContext2.hasLedgerEntry(Reference(MEASUREMENT_CONSUMER_ID, requisitionId, false))
        )
        .isTrue()
      // but other measurement consumer can charge with the same RequisitionId is allowed
      assertThat(
          txContext2.hasLedgerEntry(Reference("OtherMeasurementConsumer", requisitionId, false))
        )
        .isFalse()

      txContext2.commit()
      backingStore.close()
    }
}
