package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.Timestamp
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.util.concurrent.TimeUnit
import kotlin.test.assertFails
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.Pagination
import org.wfanet.measurement.db.gcp.runReadWriteTransaction
import org.wfanet.measurement.db.gcp.toGcpTimestamp
import org.wfanet.measurement.db.kingdom.gcp.testing.RequisitionTestBase
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionState

@RunWith(JUnit4::class)
class ListRequisitionsQueryTest : RequisitionTestBase() {
  companion object {
    val CAMPAIGN_IDS = listOf(1000L, 1001L)
    val EXTERNAL_CAMPAIGN_IDS = listOf(2000L, 2001L)
    private val REQUISITION_IDS = listOf(3000L, 3001L, 3002L, 30003L)
    private val EXTERNAL_REQUISITION_IDS = listOf(4000L, 4001L, 4002L, 4003L)
    private val STATES = listOf(RequisitionState.FULFILLED, RequisitionState.UNFULFILLED)

    val REQUISITIONS: List<Requisition> = (0..3).map { i ->
      REQUISITION.toBuilder().apply {
        campaignId = CAMPAIGN_IDS[i / 2]
        externalCampaignId = EXTERNAL_CAMPAIGN_IDS[i / 2]

        requisitionId = REQUISITION_IDS[i]
        externalRequisitionId = EXTERNAL_REQUISITION_IDS[i]

        createTime = Timestamp.ofTimeMicroseconds(TimeUnit.SECONDS.toMicros(10) * (i + 1)).toProto()
        state = STATES[i % 2]
      }.build()
    }.toList()
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
            campaignId = CAMPAIGN_IDS[0],
            externalCampaignId = EXTERNAL_CAMPAIGN_IDS[0]
          ),
          insertCampaignMutation(
            campaignId = CAMPAIGN_IDS[1],
            externalCampaignId = EXTERNAL_CAMPAIGN_IDS[1]
          )
        )
      )
      transactionContext.buffer(
        REQUISITIONS.map {
          insertRequisitionMutation(
            campaignId = it.campaignId,
            requisitionId = it.requisitionId,
            externalRequisitionId = it.externalRequisitionId,
            createTime = it.createTime.toGcpTimestamp(),
            state = it.state
          )
        }
      )
    }
  }

  @Test
  fun `database sanity check`() {
    assertThat(readAllRequisitions())
      .comparingExpectedFieldsOnly()
      .containsExactlyElementsIn(REQUISITIONS)
  }

  @Test
  fun paging() {
    fun executeQueryWithPagination(pagination: Pagination) =
      ListRequisitionsQuery().execute(
        spanner.client.singleUse(),
        ExternalId(EXTERNAL_DATA_PROVIDER_ID),
        ExternalId(EXTERNAL_CAMPAIGN_IDS[0]),
        setOf(RequisitionState.UNFULFILLED, RequisitionState.FULFILLED),
        pagination
      )

    val page1 = executeQueryWithPagination(Pagination(1, ""))
    assertThat(page1).isNotNull()
    assertThat(page1.nextPageToken).isNotNull()
    assertThat(page1.requisitions).comparingExpectedFieldsOnly().containsExactly(REQUISITIONS[0])

    val page2 = executeQueryWithPagination(Pagination(1, page1.nextPageToken.orEmpty()))
    assertThat(page2).isNotNull()
    assertThat(page2.nextPageToken).isNotNull()
    assertThat(page2.requisitions).comparingExpectedFieldsOnly().containsExactly(REQUISITIONS[1])

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
        ExternalId(EXTERNAL_DATA_PROVIDER_ID),
        ExternalId(EXTERNAL_CAMPAIGN_IDS[0]),
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
        ExternalId(EXTERNAL_DATA_PROVIDER_ID),
        ExternalId(CAMPAIGN_IDS[0]),
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
        ExternalId(EXTERNAL_DATA_PROVIDER_ID),
        ExternalId(CAMPAIGN_IDS[0]),
        setOf(),
        Pagination(1, "")
      )
    }
  }
}
