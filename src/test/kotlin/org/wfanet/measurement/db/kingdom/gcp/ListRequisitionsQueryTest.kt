package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.Timestamp
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlin.test.assertFails
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.Pagination
import org.wfanet.measurement.db.gcp.runReadWriteTransaction
import org.wfanet.measurement.db.kingdom.gcp.testing.RequisitionTestBase
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionState

@RunWith(JUnit4::class)
class ListRequisitionsQueryTest : RequisitionTestBase() {
  companion object {
    const val CAMPAIGN_ID1 = 1000L
    const val CAMPAIGN_ID2 = 1001L

    const val EXTERNAL_CAMPAIGN_ID1 = 1002L
    const val EXTERNAL_CAMPAIGN_ID2 = 1003L

    const val REQUISITION_ID1 = 2000L
    const val REQUISITION_ID2 = 2001L
    const val REQUISITION_ID3 = 2002L

    val REQUISITION1: Requisition = REQUISITION.toBuilder().apply {
      externalCampaignId = EXTERNAL_CAMPAIGN_ID1
      externalRequisitionId = 2003L
      createTime = Timestamp.ofTimeSecondsAndNanos(100, 0).toProto()
      state = RequisitionState.FULFILLED
    }.build()

    val REQUISITION2: Requisition = REQUISITION.toBuilder().apply {
      externalCampaignId = EXTERNAL_CAMPAIGN_ID1
      externalRequisitionId = 2004L
      createTime = Timestamp.ofTimeSecondsAndNanos(200, 0).toProto()
      state = RequisitionState.UNFULFILLED
    }.build()

    val REQUISITION3: Requisition = REQUISITION.toBuilder().apply {
      externalCampaignId = EXTERNAL_CAMPAIGN_ID2
      externalRequisitionId = 2005L
      createTime = Timestamp.ofTimeSecondsAndNanos(300, 0).toProto()
      state = RequisitionState.FULFILLED
    }.build()
  }

  @Before
  /**
   * Set-up: there are 4 requisitions in the database: two belonging to two campaigns under the same
   * data provider.
   */
  fun populateDatabase() {
    spanner.client.runReadWriteTransaction { transactionContext ->
      transactionContext.buffer(
        listOf(
          insertDataProviderMutation(),
          insertCampaignMutation(
            campaignId = CAMPAIGN_ID1,
            externalCampaignId = EXTERNAL_CAMPAIGN_ID1
          ),
          insertCampaignMutation(
            campaignId = CAMPAIGN_ID2,
            externalCampaignId = EXTERNAL_CAMPAIGN_ID2
          ),
          insertRequisitionMutation(CAMPAIGN_ID1, REQUISITION_ID1, REQUISITION1),
          insertRequisitionMutation(CAMPAIGN_ID1, REQUISITION_ID2, REQUISITION2),
          insertRequisitionMutation(CAMPAIGN_ID2, REQUISITION_ID3, REQUISITION3)
        )
      )
    }
  }

  @Test
  fun `database sanity check`() {
    assertThat(readAllRequisitions())
      .comparingExpectedFieldsOnly()
      .containsExactly(REQUISITION1, REQUISITION2, REQUISITION3)
  }

  @Test
  fun paging() {
    fun executeQueryWithPagination(pagination: Pagination) =
      ListRequisitionsQuery().execute(
        spanner.client.singleUse(),
        ExternalId(EXTERNAL_CAMPAIGN_ID1),
        setOf(RequisitionState.UNFULFILLED, RequisitionState.FULFILLED),
        pagination
      )

    val page1 = executeQueryWithPagination(Pagination(1, ""))
    assertThat(page1).isNotNull()
    assertThat(page1.nextPageToken).isNotNull()
    assertThat(page1.requisitions).comparingExpectedFieldsOnly().containsExactly(REQUISITION1)

    val page2 = executeQueryWithPagination(Pagination(1, page1.nextPageToken.orEmpty()))
    assertThat(page2).isNotNull()
    assertThat(page2.nextPageToken).isNotNull()
    assertThat(page2.requisitions).comparingExpectedFieldsOnly().containsExactly(REQUISITION2)

    val page3 = executeQueryWithPagination(Pagination(1, page2.nextPageToken.orEmpty()))
    assertThat(page3).isNotNull()
    assertThat(page3.nextPageToken).isNull()
    assertThat(page3.requisitions).isEmpty()
  }

  @Test
  fun `invalid page token`() {
    assertFails {
      ListRequisitionsQuery().execute(
        spanner.client.singleUse(),
        ExternalId(EXTERNAL_CAMPAIGN_ID1),
        setOf(RequisitionState.UNFULFILLED, RequisitionState.FULFILLED),
        Pagination(1, "nonsense")
      )
    }
  }

  @Test
  fun `invalid page size`() {
    assertFails {
      ListRequisitionsQuery().execute(
        spanner.client.singleUse(),
        ExternalId(EXTERNAL_CAMPAIGN_ID1),
        setOf(RequisitionState.UNFULFILLED, RequisitionState.FULFILLED),
        Pagination(1001, "")
      )
    }
  }

  @Test
  fun `no states`() {
    assertFails {
      ListRequisitionsQuery().execute(
        spanner.client.singleUse(),
        ExternalId(EXTERNAL_CAMPAIGN_ID1),
        setOf(),
        Pagination(1, "")
      )
    }
  }
}
