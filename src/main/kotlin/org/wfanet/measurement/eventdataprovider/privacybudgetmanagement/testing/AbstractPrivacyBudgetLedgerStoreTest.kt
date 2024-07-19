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
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.Gender
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBucketGroup
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetAcdpBalanceEntry
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetLedgerBackingStore
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

  @Test
  fun `findBalanceEntries is the same as calling findBalanceEntry for every bucket`() {
    runBlocking {
      val bucket1 =
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          LocalDate.parse("2024-07-01"),
          LocalDate.parse("2024-07-01"),
          AgeGroup.RANGE_18_34,
          Gender.MALE,
          0.3f,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
        )

      val bucket2 =
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          LocalDate.parse("2024-07-01"),
          LocalDate.parse("2024-07-01"),
          AgeGroup.RANGE_18_34,
          Gender.MALE,
          0.5f,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
        )

      val buckets = setOf(bucket1, bucket2)

      val backingStore = createBackingStore()
      val acdpCharge = AcdpCharge(0.04, 5.0E-6)

      val txContext1 = backingStore.startTransaction()
      txContext1.addAcdpLedgerEntries(
        setOf(bucket1, bucket2),
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

      val bucketsAcdpBalanceEntry = txContext2.findAcdpBalanceEntries(buckets)

      backingStore.close()

      assertThat(bucketsAcdpBalanceEntry.elementAt(0)).isEqualTo(bucket1AcdpBalanceEntry)
      assertThat(bucketsAcdpBalanceEntry.elementAt(1)).isEqualTo(bucket2AcdpBalanceEntry)

      assertThat(bucket1AcdpBalanceEntry)
        .isEqualTo(PrivacyBudgetAcdpBalanceEntry(bucket1, AcdpCharge(0.04 * 2, 5.0E-6 * 2)))
      assertThat(bucketsAcdpBalanceEntry.elementAt(0))
        .isEqualTo(PrivacyBudgetAcdpBalanceEntry(bucket1, AcdpCharge(0.04 * 2, 5.0E-6 * 2)))
      assertThat(bucket2AcdpBalanceEntry)
        .isEqualTo(PrivacyBudgetAcdpBalanceEntry(bucket2, AcdpCharge(0.04, 5E-6)))
      assertThat(bucketsAcdpBalanceEntry.elementAt(1))
        .isEqualTo(PrivacyBudgetAcdpBalanceEntry(bucket2, AcdpCharge(0.04, 5E-6)))
    }
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
