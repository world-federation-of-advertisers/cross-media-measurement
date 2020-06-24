package org.wfanet.measurement.db.kingdom.gcp

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlin.test.assertNull
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.InternalId
import org.wfanet.measurement.common.RandomIdGenerator
import org.wfanet.measurement.db.gcp.runReadWriteTransaction
import org.wfanet.measurement.db.gcp.toInstant
import org.wfanet.measurement.db.kingdom.gcp.testing.RequisitionTestBase
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState

@RunWith(JUnit4::class)
class CreateRequisitionTransactionTest : RequisitionTestBase() {
  companion object {
    val INPUT_REQUISITION: Requisition =
      REQUISITION.toBuilder()
        .clearExternalRequisitionId()
        .build()
  }

  object FakeIdGenerator : RandomIdGenerator {
    override fun generateInternalId(): InternalId = InternalId(NEW_REQUISITION_ID)
    override fun generateExternalId(): ExternalId = ExternalId(NEW_EXTERNAL_REQUISITION_ID)
  }

  @Before
  fun populateDatabase() {
    spanner.client.write(listOf(insertDataProviderMutation()))

    insertCampaign(DATA_PROVIDER_ID, CAMPAIGN_ID, EXTERNAL_CAMPAIGN_ID, IRRELEVANT_ADVERTISER_ID)

    insertRequisition(
      DATA_PROVIDER_ID,
      CAMPAIGN_ID,
      REQUISITION_ID,
      EXTERNAL_REQUISITION_ID,
      state = RequisitionState.UNFULFILLED,
      windowStartTime = START_TIME.toInstant(),
      windowEndTime = END_TIME.toInstant(),
      requisitionDetails = DETAILS
    )
  }

  private val createRequisitionTransaction = CreateRequisitionTransaction(FakeIdGenerator)

  @Test
  fun `requisition already exists`() {
    val existing: Requisition? = spanner.client.runReadWriteTransaction {
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

    val existing: Requisition? = spanner.client.readWriteTransaction().run {
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

    val existing: Requisition? = spanner.client.readWriteTransaction().run {
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
      requisitionDetails = NEW_DETAILS
    }.build()

    val existing: Requisition? = spanner.client.readWriteTransaction().run {
      createRequisitionTransaction.execute(it, newRequisition)
    }
    assertNull(existing)
    assertThat(readAllRequisitionsInSpanner())
      .comparingExpectedFieldsOnly()
      .containsExactly(REQUISITION, newRequisition)
  }
}
