package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.Timestamp
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.time.Instant
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.db.gcp.runReadWriteTransaction
import org.wfanet.measurement.db.kingdom.StreamRequisitionsFilter
import org.wfanet.measurement.db.kingdom.gcp.testing.RequisitionTestBase
import org.wfanet.measurement.db.kingdom.streamRequisitionsFilter
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState

@RunWith(JUnit4::class)
class StreamRequisitionsQueryTest : RequisitionTestBase() {
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

  private fun executeToList(filter: StreamRequisitionsFilter, limit: Long): List<Requisition> =
    runBlocking {
      StreamRequisitionsQuery().execute(
        spanner.client.singleUse(),
        filter,
        limit
      ).toList()
    }

  @Test
  fun `create time filter`() {
    fun filter(time: Instant) = streamRequisitionsFilter(createdAfter = time)

    assertThat(executeToList(filter(Instant.EPOCH), 10))
      .comparingExpectedFieldsOnly()
      .containsExactly(REQUISITION1, REQUISITION2, REQUISITION3)

    assertThat(executeToList(filter(REQUISITION1.createTime.toInstant()), 10))
      .comparingExpectedFieldsOnly()
      .containsExactly(REQUISITION2, REQUISITION3)

    assertThat(executeToList(filter(REQUISITION2.createTime.toInstant()), 10))
      .comparingExpectedFieldsOnly()
      .containsExactly(REQUISITION3)

    assertThat(executeToList(filter(REQUISITION3.createTime.toInstant()), 10))
      .isEmpty()
  }

  @Test
  fun `externalDataProviderId filter`() {
    fun filter(vararg ids: Long) =
      streamRequisitionsFilter(externalDataProviderIds = ids.map(::ExternalId))

    assertThat(executeToList(filter(EXTERNAL_DATA_PROVIDER_ID), 10))
      .comparingExpectedFieldsOnly()
      .containsExactly(REQUISITION1, REQUISITION2, REQUISITION3)

    assertThat(executeToList(filter(EXTERNAL_DATA_PROVIDER_ID, EXTERNAL_DATA_PROVIDER_ID + 1), 10))
      .comparingExpectedFieldsOnly()
      .containsExactly(REQUISITION1, REQUISITION2, REQUISITION3)

    assertThat(executeToList(filter(EXTERNAL_DATA_PROVIDER_ID + 1), 10))
      .isEmpty()
  }

  @Test
  fun `state filter`() {
    fun filter(vararg ids: Long) =
      streamRequisitionsFilter(externalCampaignIds = ids.map(::ExternalId))

    assertThat(executeToList(filter(EXTERNAL_CAMPAIGN_ID1), 10))
      .comparingExpectedFieldsOnly()
      .containsExactly(REQUISITION1, REQUISITION2)

    assertThat(executeToList(filter(EXTERNAL_CAMPAIGN_ID2), 10))
      .comparingExpectedFieldsOnly()
      .containsExactly(REQUISITION3)

    assertThat(executeToList(filter(EXTERNAL_CAMPAIGN_ID1, EXTERNAL_CAMPAIGN_ID2), 10))
      .comparingExpectedFieldsOnly()
      .containsExactly(REQUISITION1, REQUISITION2, REQUISITION3)
  }

  @Test
  fun `externalCampaignId filter`() {
    fun filter(vararg states: RequisitionState) =
      streamRequisitionsFilter(
        externalCampaignIds = listOf(ExternalId(EXTERNAL_CAMPAIGN_ID1)),
        states = states.toList()
      )

    assertThat(executeToList(filter(RequisitionState.UNFULFILLED), 10))
      .comparingExpectedFieldsOnly()
      .containsExactly(REQUISITION2)

    assertThat(executeToList(filter(RequisitionState.FULFILLED), 10))
      .comparingExpectedFieldsOnly()
      .containsExactly(REQUISITION1)

    assertThat(executeToList(filter(RequisitionState.FULFILLED, RequisitionState.UNFULFILLED), 10))
      .comparingExpectedFieldsOnly()
      .containsExactly(REQUISITION1, REQUISITION2)
  }

  @Test
  fun `limit filter`() {
    assertThat(executeToList(streamRequisitionsFilter(), 10))
      .comparingExpectedFieldsOnly()
      .containsExactly(REQUISITION1, REQUISITION2, REQUISITION3)

    assertThat(executeToList(streamRequisitionsFilter(), 2))
      .comparingExpectedFieldsOnly()
      .containsExactly(REQUISITION1, REQUISITION2)

    assertThat(executeToList(streamRequisitionsFilter(), 1))
      .comparingExpectedFieldsOnly()
      .containsExactly(REQUISITION1)

    assertThat(executeToList(streamRequisitionsFilter(), 0))
      .isEmpty()
  }
}
