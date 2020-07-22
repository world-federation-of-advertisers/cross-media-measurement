// Copyright 2020 The Measurement System Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.Timestamp
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.time.Instant
import kotlin.test.assertNull
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.InternalId
import org.wfanet.measurement.common.RandomIdGenerator
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.db.gcp.runReadWriteTransaction
import org.wfanet.measurement.db.kingdom.gcp.testing.KingdomDatabaseTestBase
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState

@RunWith(JUnit4::class)
class CreateRequisitionTransactionTest : KingdomDatabaseTestBase() {
  companion object {
    const val DATA_PROVIDER_ID = 1L
    const val EXTERNAL_DATA_PROVIDER_ID = 2L
    const val CAMPAIGN_ID = 3L
    const val EXTERNAL_CAMPAIGN_ID = 4L

    const val REQUISITION_ID = 5L
    const val EXTERNAL_REQUISITION_ID = 6L
    const val NEW_REQUISITION_ID = 7L
    const val NEW_EXTERNAL_REQUISITION_ID = 8L

    const val ADVERTISER_ID = 9L
    const val EXTERNAL_REPORT_CONFIG_ID = 10L

    val WINDOW_START_TIME: Instant = Instant.ofEpochSecond(123)
    val WINDOW_END_TIME: Instant = Instant.ofEpochSecond(456)

    val REQUISITION_DETAILS = buildRequisitionDetails(10101L)
    val NEW_REQUISITION_DETAILS = buildRequisitionDetails(20202L)

    val REQUISITION: Requisition = Requisition.newBuilder().apply {
      externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
      externalCampaignId = EXTERNAL_CAMPAIGN_ID
      externalRequisitionId = EXTERNAL_REQUISITION_ID
      windowStartTime = WINDOW_START_TIME.toProtoTime()
      windowEndTime = WINDOW_END_TIME.toProtoTime()
      state = RequisitionState.UNFULFILLED
      requisitionDetails = REQUISITION_DETAILS
      requisitionDetailsJson = REQUISITION_DETAILS.toJson()
    }.build()

    val INPUT_REQUISITION: Requisition =
      REQUISITION.toBuilder()
        .clearExternalRequisitionId()
        .build()

    val NEW_TIMESTAMP: Timestamp = Timestamp.ofTimeSecondsAndNanos(999, 0)
  }

  object FakeIdGenerator : RandomIdGenerator {
    override fun generateInternalId(): InternalId = InternalId(NEW_REQUISITION_ID)
    override fun generateExternalId(): ExternalId = ExternalId(NEW_EXTERNAL_REQUISITION_ID)
  }

  @Before
  fun populateDatabase() {
    insertDataProvider(DATA_PROVIDER_ID, EXTERNAL_DATA_PROVIDER_ID)
    insertCampaign(DATA_PROVIDER_ID, CAMPAIGN_ID, EXTERNAL_CAMPAIGN_ID, ADVERTISER_ID)
    insertRequisition(
      DATA_PROVIDER_ID,
      CAMPAIGN_ID,
      REQUISITION_ID,
      EXTERNAL_REQUISITION_ID,
      state = RequisitionState.UNFULFILLED,
      windowStartTime = WINDOW_START_TIME,
      windowEndTime = WINDOW_END_TIME,
      requisitionDetails = REQUISITION_DETAILS
    )
  }

  private val createRequisitionTransaction = CreateRequisitionTransaction(FakeIdGenerator)

  @Test
  fun `requisition already exists`() {
    val existing: Requisition? = databaseClient.runReadWriteTransaction {
      createRequisitionTransaction.execute(it, INPUT_REQUISITION)
    }
    assertThat(existing)
      .comparingExpectedFieldsOnly()
      .isEqualTo(REQUISITION)
    assertThat(readAllRequisitionsInSpanner())
      .comparingExpectedFieldsOnly()
      .containsExactly(REQUISITION)
  }

  @Test
  fun `start time used in idempotency`() {
    val newRequisition = INPUT_REQUISITION.toBuilder().apply {
      externalRequisitionId = NEW_EXTERNAL_REQUISITION_ID
      windowStartTime = NEW_TIMESTAMP.toProto()
    }.build()

    val existing: Requisition? = databaseClient.readWriteTransaction().run {
      createRequisitionTransaction.execute(it, newRequisition)
    }
    assertNull(existing)
    assertThat(readAllRequisitionsInSpanner())
      .comparingExpectedFieldsOnly()
      .containsExactly(REQUISITION, newRequisition)
  }

  @Test
  fun `end time used in idempotency`() {
    val newRequisition = INPUT_REQUISITION.toBuilder().apply {
      externalRequisitionId = NEW_EXTERNAL_REQUISITION_ID
      windowEndTime = NEW_TIMESTAMP.toProto()
    }.build()

    val existing: Requisition? = databaseClient.readWriteTransaction().run {
      createRequisitionTransaction.execute(it, newRequisition)
    }
    assertNull(existing)
    assertThat(readAllRequisitionsInSpanner())
      .comparingExpectedFieldsOnly()
      .containsExactly(REQUISITION, newRequisition)
  }

  @Test
  fun `details used in idempotency`() {
    val newRequisition = INPUT_REQUISITION.toBuilder().apply {
      externalRequisitionId = NEW_EXTERNAL_REQUISITION_ID
      requisitionDetails = NEW_REQUISITION_DETAILS
    }.build()

    val existing: Requisition? = databaseClient.readWriteTransaction().run {
      createRequisitionTransaction.execute(it, newRequisition)
    }
    assertNull(existing)
    assertThat(readAllRequisitionsInSpanner())
      .comparingExpectedFieldsOnly()
      .containsExactly(REQUISITION, newRequisition)
  }
}
