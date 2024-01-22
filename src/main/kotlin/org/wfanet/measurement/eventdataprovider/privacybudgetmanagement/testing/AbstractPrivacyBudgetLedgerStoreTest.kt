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
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.AcdpCharge
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.AgeGroup
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.DpCharge
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.Gender
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBucketGroup
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetAcdpBalanceEntry
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetBalanceEntry
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetLedgerBackingStore
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetLedgerTransactionContext
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyLandscape
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.Reference

abstract class AbstractPrivacyBudgetLedgerStoreTest {
  protected abstract fun createBackingStore(): PrivacyBudgetLedgerBackingStore

  protected abstract fun recreateSchema()

  @Before
  fun prepareTest() {
    recreateSchema()
  }

  @Test(timeout = 15000)
  fun `findIntersectingBalanceEntries finds dp balance entries`() = runBlocking {
    val bucket1 =
      PrivacyBucketGroup(
        MEASUREMENT_CONSUMER_ID,
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.MALE,
        0.3f,
        PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
      )

    val bucket2 =
      PrivacyBucketGroup(
        MEASUREMENT_CONSUMER_ID,
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.MALE,
        0.5f,
        PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
      )

    val bucket3 =
      PrivacyBucketGroup(
        MEASUREMENT_CONSUMER_ID,
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.FEMALE,
        0.3f,
        PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
      )

    val bucket4 =
      PrivacyBucketGroup(
        MEASUREMENT_CONSUMER_ID,
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.FEMALE,
        0.5f,
        PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
      )

    val dpCharge = DpCharge(0.01f, 0.0001f)

    val backingStore = createBackingStore()
    val txContext1 = backingStore.startTransaction()
    txContext1.addLedgerEntries(
      setOf(bucket1, bucket2, bucket3),
      setOf(dpCharge),
      Reference(MEASUREMENT_CONSUMER_ID, "RequisitionId1", false),
    )

    txContext1.addLedgerEntries(
      setOf(bucket1),
      setOf(dpCharge),
      Reference(MEASUREMENT_CONSUMER_ID, "RequisitionId2", false),
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
  fun `addLedgerEntries as a refund decreases dp repetitionCount`() = runBlocking {
    createBackingStore().use { backingStore: PrivacyBudgetLedgerBackingStore ->
      val bucket1 =
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          LocalDate.parse("2021-07-01"),
          LocalDate.parse("2021-07-01"),
          AgeGroup.RANGE_35_54,
          Gender.MALE,
          0.3f,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
        )
      val dpCharge = DpCharge(0.01f, 0.0001f)
      backingStore.startTransaction().use { txContext: PrivacyBudgetLedgerTransactionContext ->
        txContext.addLedgerEntries(
          setOf(bucket1),
          setOf(dpCharge),
          Reference(MEASUREMENT_CONSUMER_ID, "RequisitionId1", false),
        )
        txContext.commit()
      }

      backingStore.startTransaction().use { txContext: PrivacyBudgetLedgerTransactionContext ->
        val matchingBalanceEntries = txContext.findIntersectingBalanceEntries(bucket1)
        assertThat(matchingBalanceEntries.size).isEqualTo(1)

        assertThat(matchingBalanceEntries[0].repetitionCount).isEqualTo(1)
        txContext.addLedgerEntries(
          setOf(bucket1),
          setOf(dpCharge),
          Reference(MEASUREMENT_CONSUMER_ID, "RequisitionId1", true),
        )
        txContext.commit()
      }

      backingStore.startTransaction().use { txContext: PrivacyBudgetLedgerTransactionContext ->
        val newMatchingBalanceEntries = txContext.findIntersectingBalanceEntries(bucket1)
        assertThat(newMatchingBalanceEntries.size).isEqualTo(1)
        assertThat(newMatchingBalanceEntries[0].repetitionCount).isEqualTo(0)
      }
    }
  }

  @Test(timeout = 15000)
  fun `addLedgerEntries for different MCs and same requisitionId don't point to same dp balances`() =
    runBlocking {
      val requisitionId = "RequisitionId1"
      val otherMeasurementConsumerId = "otherMeasurementConsumerId"
      val bucket =
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          LocalDate.parse("2021-07-01"),
          LocalDate.parse("2021-07-01"),
          AgeGroup.RANGE_35_54,
          Gender.MALE,
          0.3f,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
        )
      val bucketForOtherMC = bucket.copy(measurementConsumerId = otherMeasurementConsumerId)
      val dpCharge = DpCharge(0.01f, 0.0001f)
      createBackingStore().use { backingStore: PrivacyBudgetLedgerBackingStore ->
        backingStore.startTransaction().use { txContext: PrivacyBudgetLedgerTransactionContext ->
          txContext.addLedgerEntries(
            setOf(bucket),
            setOf(dpCharge),
            Reference(MEASUREMENT_CONSUMER_ID, requisitionId, false),
          )

          txContext.addLedgerEntries(
            setOf(bucketForOtherMC),
            setOf(dpCharge),
            Reference(otherMeasurementConsumerId, requisitionId, false),
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
  fun `hasLedgerEntry returns false when reference ledger is empty`() = runBlocking {
    val backingStore = createBackingStore()
    val txContext1 = backingStore.startTransaction()
    assertThat(
        txContext1.hasLedgerEntry(Reference(MEASUREMENT_CONSUMER_ID, "RequisitionId1", false))
      )
      .isFalse()
    txContext1.commit()
    txContext1.close()
    backingStore.close()
  }

  @Test(timeout = 15000)
  fun `commit() persists a transaction for dp ledger after it closes`() = runBlocking {
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
        PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
      )

    val dpCharge = DpCharge(0.01f, 0.0001f)
    txContext.addLedgerEntries(
      setOf(bucket1),
      setOf(dpCharge),
      Reference(MEASUREMENT_CONSUMER_ID, "RequisitionId1", false),
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
  fun `hasLedgerEntry returns true when dp ledger entry with same isRefund is found`() =
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
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
        )

      val dpCharge = DpCharge(0.01f, 0.0001f)
      // charge works
      txContext1.addLedgerEntries(
        setOf(bucket1),
        setOf(dpCharge),
        Reference(MEASUREMENT_CONSUMER_ID, "RequisitionId1", false),
      )

      txContext1.commit()
      val txContext2 = backingStore.startTransaction()

      // backing store already has the ledger entry
      assertThat(
          txContext2.hasLedgerEntry(Reference(MEASUREMENT_CONSUMER_ID, "RequisitionId1", false))
        )
        .isTrue()

      // but refund is allowed
      assertThat(
          txContext2.hasLedgerEntry(Reference(MEASUREMENT_CONSUMER_ID, "RequisitionId1", true))
        )
        .isFalse()

      // refund works
      txContext2.addLedgerEntries(
        setOf(bucket1),
        setOf(dpCharge),
        Reference(MEASUREMENT_CONSUMER_ID, "RequisitionId1", true),
      )
      txContext2.commit()
      val txContext3 = backingStore.startTransaction()
      // now charge is allowed again
      assertThat(
          txContext3.hasLedgerEntry(Reference(MEASUREMENT_CONSUMER_ID, "RequisitionId1", false))
        )
        .isFalse()
      txContext3.commit()
      backingStore.close()
    }

  @Test(timeout = 15000)
  fun `hasLedgerEntry returns true for dp ledger entry with same isRefund for multiple MCs`() =
    runBlocking {
      val backingStore = createBackingStore()
      val txContext1 = backingStore.startTransaction()
      val requisitionId = "RequisitionId1"
      val bucket1 =
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          LocalDate.parse("2021-07-01"),
          LocalDate.parse("2021-07-01"),
          AgeGroup.RANGE_35_54,
          Gender.MALE,
          0.3f,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
        )

      val dpCharge = DpCharge(0.01f, 0.0001f)
      // charge works
      txContext1.addLedgerEntries(
        setOf(bucket1),
        setOf(dpCharge),
        Reference(MEASUREMENT_CONSUMER_ID, requisitionId, false),
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

  @Test(timeout = 15000)
  fun `findAcdpBalanceEntry returns PrivacyBudgetAcdpBalanceEntry with zero AcdpCharge when acdpBalances table is empty`() =
    runBlocking {
      val bucket =
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          LocalDate.parse("2021-07-01"),
          LocalDate.parse("2021-07-01"),
          AgeGroup.RANGE_35_54,
          Gender.MALE,
          0.3f,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
        )

      val backingStore = createBackingStore()

      val txContext = backingStore.startTransaction()
      val bucketAcdpBalanceEntry: PrivacyBudgetAcdpBalanceEntry =
        txContext.findAcdpBalanceEntry(bucket)

      backingStore.close()

      assertThat(bucketAcdpBalanceEntry)
        .isEqualTo(PrivacyBudgetAcdpBalanceEntry(bucket, AcdpCharge(0.0, 0.0)))
    }

  @Test(timeout = 15000)
  fun `addAcdpLedgerEntries and findAcdpBalanceEntry finds the correct acdp balance entry`() =
    runBlocking {
      val bucket1 =
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          LocalDate.parse("2021-07-01"),
          LocalDate.parse("2021-07-01"),
          AgeGroup.RANGE_35_54,
          Gender.MALE,
          0.3f,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
        )

      val bucket2 =
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          LocalDate.parse("2021-07-01"),
          LocalDate.parse("2021-07-01"),
          AgeGroup.RANGE_35_54,
          Gender.MALE,
          0.5f,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
        )

      val bucket3 =
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          LocalDate.parse("2021-07-01"),
          LocalDate.parse("2021-07-01"),
          AgeGroup.RANGE_35_54,
          Gender.FEMALE,
          0.3f,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
        )

      val backingStore = createBackingStore()
      val acdpCharge = AcdpCharge(0.04, 5.0E-6)

      val txContext1 = backingStore.startTransaction()
      txContext1.addAcdpLedgerEntries(
        setOf(bucket1, bucket2, bucket3),
        setOf(acdpCharge),
        Reference(MEASUREMENT_CONSUMER_ID, "RequisitionId1", false),
      )
      txContext1.addAcdpLedgerEntries(
        setOf(bucket1),
        setOf(acdpCharge),
        Reference(MEASUREMENT_CONSUMER_ID, "RequisitionId2", false),
      )
      txContext1.commit()

      val txContext2 = backingStore.startTransaction()
      val bucket1AcdpBalanceEntry: PrivacyBudgetAcdpBalanceEntry =
        txContext2.findAcdpBalanceEntry(bucket1)
      val bucket2AcdpBalanceEntry: PrivacyBudgetAcdpBalanceEntry =
        txContext2.findAcdpBalanceEntry(bucket2)
      val bucket3AcdpBalanceEntry: PrivacyBudgetAcdpBalanceEntry =
        txContext2.findAcdpBalanceEntry(bucket3)

      backingStore.close()

      assertThat(bucket1AcdpBalanceEntry)
        .isEqualTo(PrivacyBudgetAcdpBalanceEntry(bucket1, AcdpCharge(0.04 * 2, 5.0E-6 * 2)))
      assertThat(bucket2AcdpBalanceEntry)
        .isEqualTo(PrivacyBudgetAcdpBalanceEntry(bucket2, AcdpCharge(0.04, 5E-6)))
      assertThat(bucket3AcdpBalanceEntry)
        .isEqualTo(PrivacyBudgetAcdpBalanceEntry(bucket3, AcdpCharge(0.04, 5E-6)))
    }

  @Test(timeout = 15000)
  fun `addLedgerEntries for different MCs and same requisitionId don't point to same acdp balances`() =
    runBlocking {
      val requisitionId = "RequisitionId1"
      val otherMeasurementConsumerId = "otherMeasurementConsumerId"
      val bucket =
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          LocalDate.parse("2021-07-01"),
          LocalDate.parse("2021-07-01"),
          AgeGroup.RANGE_35_54,
          Gender.MALE,
          0.3f,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
        )
      val bucketForOtherMC = bucket.copy(measurementConsumerId = otherMeasurementConsumerId)
      val acdpCharge = AcdpCharge(0.04, 5.0E-6)

      val backingStore = createBackingStore()
      val txContext1 = backingStore.startTransaction()
      txContext1.addAcdpLedgerEntries(
        setOf(bucket),
        setOf(acdpCharge),
        Reference(MEASUREMENT_CONSUMER_ID, requisitionId, false),
      )

      txContext1.addAcdpLedgerEntries(
        setOf(bucketForOtherMC),
        setOf(acdpCharge),
        Reference(otherMeasurementConsumerId, requisitionId, false),
      )

      txContext1.commit()

      val txContext2 = backingStore.startTransaction()
      val mcBalanceEntry = txContext2.findAcdpBalanceEntry(bucket)
      val otherMcBalanceEntry = txContext2.findAcdpBalanceEntry(bucketForOtherMC)

      backingStore.close()

      assertThat(mcBalanceEntry.privacyBucketGroup.measurementConsumerId)
        .isEqualTo(MEASUREMENT_CONSUMER_ID)
      assertThat(mcBalanceEntry.acdpCharge).isEqualTo(acdpCharge)
      assertThat(otherMcBalanceEntry.privacyBucketGroup.measurementConsumerId)
        .isEqualTo(otherMeasurementConsumerId)
      assertThat(otherMcBalanceEntry.acdpCharge).isEqualTo(acdpCharge)
    }

  @Test(timeout = 15000)
  fun `hasLedgerEntry returns true when acdp ledger entry with same isRefund is found`() =
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
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
        )

      val acdpCharge = AcdpCharge(0.04, 5.0E-6)
      // charge works
      txContext1.addAcdpLedgerEntries(
        setOf(bucket1),
        setOf(acdpCharge),
        Reference(MEASUREMENT_CONSUMER_ID, "RequisitionId1", false),
      )

      txContext1.commit()
      val txContext2 = backingStore.startTransaction()

      // backing store already has the ledger entry
      assertThat(
          txContext2.hasLedgerEntry(Reference(MEASUREMENT_CONSUMER_ID, "RequisitionId1", false))
        )
        .isTrue()

      // but refund is allowed
      assertThat(
          txContext2.hasLedgerEntry(Reference(MEASUREMENT_CONSUMER_ID, "RequisitionId1", true))
        )
        .isFalse()

      // refund works
      txContext2.addAcdpLedgerEntries(
        setOf(bucket1),
        setOf(acdpCharge),
        Reference(MEASUREMENT_CONSUMER_ID, "RequisitionId1", true),
      )
      txContext2.commit()
      val txContext3 = backingStore.startTransaction()
      // now charge is allowed again
      assertThat(
          txContext3.hasLedgerEntry(Reference(MEASUREMENT_CONSUMER_ID, "RequisitionId1", false))
        )
        .isFalse()
      txContext3.commit()
      backingStore.close()
    }

  @Test(timeout = 15000)
  fun `hasLedgerEntry returns true for acdp ledger entry with the same isRefund is found for multiple MCs`() =
    runBlocking {
      val requisitionId = "RequisitionId1"
      val bucket =
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          LocalDate.parse("2021-07-01"),
          LocalDate.parse("2021-07-01"),
          AgeGroup.RANGE_35_54,
          Gender.MALE,
          0.3f,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
        )
      val backingStore = createBackingStore()

      val txContext1 = backingStore.startTransaction()
      val acdpCharge = AcdpCharge(0.04, 5.0E-6)
      // acdpCharge works
      txContext1.addAcdpLedgerEntries(
        setOf(bucket),
        setOf(acdpCharge),
        Reference(MEASUREMENT_CONSUMER_ID, requisitionId, false),
      )
      txContext1.commit()

      val txContext2 = backingStore.startTransaction()
      val mcIsInLedger =
        txContext2.hasLedgerEntry(Reference(MEASUREMENT_CONSUMER_ID, requisitionId, false))
      val otherMcIsInLedger =
        txContext2.hasLedgerEntry(Reference("OtherMeasurementConsumer", requisitionId, false))
      txContext2.commit()

      backingStore.close()

      // backing store already has the ledger entry
      assertThat(mcIsInLedger).isTrue()
      // but other measurement consumer is allowed to charge with the same RequisitionId
      assertThat(otherMcIsInLedger).isFalse()
    }

  @Test(timeout = 15000)
  fun `addAcdpLedgerEntries as a refund decreases acdp balance`() = runBlocking {
    val bucket =
      PrivacyBucketGroup(
        MEASUREMENT_CONSUMER_ID,
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.MALE,
        0.3f,
        PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
      )
    val acdpCharge = AcdpCharge(0.04, 5E-6)

    val backingStore = createBackingStore()

    val txContext1 = backingStore.startTransaction()
    txContext1.addAcdpLedgerEntries(
      setOf(bucket),
      setOf(acdpCharge),
      Reference(MEASUREMENT_CONSUMER_ID, "RequisitionId1", false),
    )
    txContext1.commit()

    val txContext2 = backingStore.startTransaction()
    val bucketAcdpBalanceEntry = txContext2.findAcdpBalanceEntry(bucket)

    txContext2.addAcdpLedgerEntries(
      setOf(bucket),
      setOf(acdpCharge),
      Reference(MEASUREMENT_CONSUMER_ID, "RequisitionId1", true),
    )
    txContext2.commit()

    val txContext3 = backingStore.startTransaction()
    val bucketAcdpBalanceEntryAfterRefund = txContext3.findAcdpBalanceEntry(bucket)

    backingStore.close()

    assertThat(bucketAcdpBalanceEntry)
      .isEqualTo(PrivacyBudgetAcdpBalanceEntry(bucket, AcdpCharge(0.04, 5.0E-6)))
    assertThat(bucketAcdpBalanceEntryAfterRefund.privacyBucketGroup).isEqualTo(bucket)
    assertThat(bucketAcdpBalanceEntryAfterRefund.acdpCharge.rho).isWithin(TOLERANCE).of(0.0)
    assertThat(bucketAcdpBalanceEntryAfterRefund.acdpCharge.theta).isWithin(TOLERANCE).of(0.0)
  }

  @Test(timeout = 15000)
  fun `commit() persists a transaction for acdp ledger after it closes`() = runBlocking {
    val backingStore = createBackingStore()
    val txContext = backingStore.startTransaction()

    val bucket =
      PrivacyBucketGroup(
        MEASUREMENT_CONSUMER_ID,
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.MALE,
        0.3f,
        PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
      )

    val acdpCharge = AcdpCharge(0.04, 5E-6)
    txContext.addAcdpLedgerEntries(
      setOf(bucket),
      setOf(acdpCharge),
      Reference(MEASUREMENT_CONSUMER_ID, "RequisitionId1", false),
    )

    val newBackingStore = createBackingStore()
    newBackingStore.startTransaction().use { newTxContext ->
      assertThat(newTxContext.findAcdpBalanceEntry(bucket).acdpCharge)
        .isEqualTo(AcdpCharge(0.0, 0.0))
    }

    txContext.commit()
    txContext.close()
    backingStore.close()

    newBackingStore.startTransaction().use { newTxContext ->
      assertThat(newTxContext.findAcdpBalanceEntry(bucket).acdpCharge).isEqualTo(acdpCharge)
    }
    newBackingStore.close()
  }

  companion object {
    private const val MEASUREMENT_CONSUMER_ID = "MC"
    private const val TOLERANCE = 1E-8
  }
}
