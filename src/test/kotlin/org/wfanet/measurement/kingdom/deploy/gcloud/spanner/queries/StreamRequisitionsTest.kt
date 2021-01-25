// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.time.Instant
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState
import org.wfanet.measurement.kingdom.db.StreamRequisitionsFilter
import org.wfanet.measurement.kingdom.db.streamRequisitionsFilter
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.KingdomDatabaseTestBase
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.buildRequisitionDetails

private const val DATA_PROVIDER_ID = 1L
private const val EXTERNAL_DATA_PROVIDER_ID = 2L
private const val ADVERTISER_ID = 7L
private val WINDOW_START_TIME: Instant = Instant.ofEpochSecond(123)
private val WINDOW_END_TIME: Instant = Instant.ofEpochSecond(456)
private val REQUISITION_DETAILS = buildRequisitionDetails(10101)
private const val CAMPAIGN_ID1 = 1000L
private const val CAMPAIGN_ID2 = 1001L
private const val EXTERNAL_CAMPAIGN_ID1 = 1002L
private const val EXTERNAL_CAMPAIGN_ID2 = 1003L
private const val REQUISITION_ID1 = 2000L
private const val EXTERNAL_REQUISITION_ID1 = 2001L
private const val REQUISITION_ID2 = 2002L
private const val EXTERNAL_REQUISITION_ID2 = 2003L
private const val REQUISITION_ID3 = 2004L
private const val EXTERNAL_REQUISITION_ID3 = 2005L

private val REQUISITION1 = buildRequisition(
  EXTERNAL_CAMPAIGN_ID1,
  EXTERNAL_REQUISITION_ID1,
  Instant.ofEpochSecond(100),
  RequisitionState.FULFILLED
)

private val REQUISITION2 = buildRequisition(
  EXTERNAL_CAMPAIGN_ID1,
  EXTERNAL_REQUISITION_ID2,
  Instant.ofEpochSecond(200),
  RequisitionState.UNFULFILLED
)

private val REQUISITION3 = buildRequisition(
  EXTERNAL_CAMPAIGN_ID2,
  EXTERNAL_REQUISITION_ID3,
  Instant.ofEpochSecond(300),
  RequisitionState.FULFILLED
)

@RunWith(JUnit4::class)
class StreamRequisitionsTest : KingdomDatabaseTestBase() {
  /**
   * Set-up: there are 4 requisitions in the database: two belonging to two campaigns under the same
   * data provider.
   */
  @Before
  fun populateDatabase() = runBlocking {
    insertDataProvider(DATA_PROVIDER_ID, EXTERNAL_DATA_PROVIDER_ID)

    insertCampaign(DATA_PROVIDER_ID, CAMPAIGN_ID1, EXTERNAL_CAMPAIGN_ID1, ADVERTISER_ID)
    insertCampaign(DATA_PROVIDER_ID, CAMPAIGN_ID2, EXTERNAL_CAMPAIGN_ID2, ADVERTISER_ID)

    insertRequisition(CAMPAIGN_ID1, REQUISITION_ID1, REQUISITION1)
    insertRequisition(CAMPAIGN_ID1, REQUISITION_ID2, REQUISITION2)
    insertRequisition(CAMPAIGN_ID2, REQUISITION_ID3, REQUISITION3)
  }

  private suspend fun insertRequisition(
    campaignId: Long,
    requisitionId: Long,
    requisition: Requisition
  ) {
    insertRequisition(
      DATA_PROVIDER_ID,
      campaignId,
      requisitionId,
      requisition.externalRequisitionId,
      state = requisition.state,
      createTime = requisition.createTime.toInstant(),
      windowStartTime = requisition.windowStartTime.toInstant(),
      windowEndTime = requisition.windowEndTime.toInstant(),
      requisitionDetails = requisition.requisitionDetails
    )
  }

  @Test
  fun `database sanity check`() {
    assertThat(readAllRequisitionsInSpanner())
      .comparingExpectedFieldsOnly()
      .containsExactly(REQUISITION1, REQUISITION2, REQUISITION3)
  }

  private fun executeToList(filter: StreamRequisitionsFilter, limit: Long): List<Requisition> =
    runBlocking {
      StreamRequisitions(filter, limit).execute(databaseClient.singleUse()).toList()
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

private fun buildRequisition(
  externalCampaignId: Long,
  externalRequisitionId: Long,
  createTime: Instant,
  state: RequisitionState
): Requisition {
  return Requisition.newBuilder().apply {
    externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
    this.externalCampaignId = externalCampaignId
    this.externalRequisitionId = externalRequisitionId
    this.createTime = createTime.toProtoTime()
    this.state = state
    windowStartTime = WINDOW_START_TIME.toProtoTime()
    windowEndTime = WINDOW_END_TIME.toProtoTime()
    requisitionDetails = REQUISITION_DETAILS
    requisitionDetailsJson = REQUISITION_DETAILS.toJson()
  }.build()
}
