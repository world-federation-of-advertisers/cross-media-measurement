package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Mutation
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.gcp.toInstant
import org.wfanet.measurement.db.gcp.toProtoEnum
import org.wfanet.measurement.db.kingdom.gcp.testing.RequisitionTestBase
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState

@RunWith(JUnit4::class)
class FulfillRequisitionTransactionTest : RequisitionTestBase() {
  private fun updateExistingRequisitionState(state: RequisitionState) {
    spanner.client.write(
      listOf(
        Mutation
          .newUpdateBuilder("Requisitions")
          .set("DataProviderId").to(DATA_PROVIDER_ID)
          .set("CampaignId").to(CAMPAIGN_ID)
          .set("RequisitionId").to(REQUISITION_ID)
          .set("State").toProtoEnum(state)
          .build()
      )
    )
  }

  @Before
  fun populateDatabase() {
    insertDataProvider(DATA_PROVIDER_ID, EXTERNAL_DATA_PROVIDER_ID)
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

  @Test
  fun success() {
    val expectedRequisition: Requisition =
      REQUISITION
        .toBuilder()
        .setState(RequisitionState.FULFILLED)
        .build()

    for (state in listOf(RequisitionState.FULFILLED, RequisitionState.UNFULFILLED)) {
      updateExistingRequisitionState(state)
      val requisition: Requisition? =
        spanner.client.readWriteTransaction().run { transactionContext ->
          FulfillRequisitionTransaction()
            .execute(transactionContext, ExternalId(EXTERNAL_REQUISITION_ID))
        }
      assertThat(requisition).comparingExpectedFieldsOnly().isEqualTo(expectedRequisition)
      assertThat(readAllRequisitionsInSpanner())
        .comparingExpectedFieldsOnly()
        .containsExactly(expectedRequisition)
    }
  }

  @Test
  fun `missing requisition`() {
    spanner.client.readWriteTransaction().run { transactionContext ->
      assertFailsWith<NoSuchElementException> {
        FulfillRequisitionTransaction()
          .execute(transactionContext, ExternalId(EXTERNAL_REQUISITION_ID + 1))
      }
    }
    assertThat(readAllRequisitionsInSpanner())
      .comparingExpectedFieldsOnly()
      .containsExactly(REQUISITION)
  }
}
