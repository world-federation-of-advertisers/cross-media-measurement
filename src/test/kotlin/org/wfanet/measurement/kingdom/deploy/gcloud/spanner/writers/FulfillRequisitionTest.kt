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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Mutation
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.time.Instant
import kotlin.test.assertFails
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.gcloud.spanner.toProtoEnum
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportDetails
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState
import org.wfanet.measurement.kingdom.db.RequisitionUpdate
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.KingdomDatabaseTestBase
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.buildRequisitionDetails

private const val DATA_PROVIDER_ID = 1L
private const val EXTERNAL_DATA_PROVIDER_ID = 2L
private const val CAMPAIGN_ID = 3L
private const val EXTERNAL_CAMPAIGN_ID = 4L
private const val REQUISITION_ID = 5L
private const val EXTERNAL_REQUISITION_ID = 6L
private const val ADVERTISER_ID = 7L
private const val EXTERNAL_ADVERTISER_ID = 8L
private const val REPORT_CONFIG_ID = 9L
private const val EXTERNAL_REPORT_CONFIG_ID = 10L
private const val SCHEDULE_ID = 11L
private const val EXTERNAL_SCHEDULE_ID = 12L
private const val REPORT_ID = 13L
private const val EXTERNAL_REPORT_ID = 14L

private val WINDOW_START_TIME: Instant = Instant.ofEpochSecond(123)
private val WINDOW_END_TIME: Instant = Instant.ofEpochSecond(456)

private val REQUISITION_DETAILS = buildRequisitionDetails(10101)

private val REQUISITION: Requisition = Requisition.newBuilder().apply {
  externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
  externalCampaignId = EXTERNAL_CAMPAIGN_ID
  externalRequisitionId = EXTERNAL_REQUISITION_ID
  windowStartTime = WINDOW_START_TIME.toProtoTime()
  windowEndTime = WINDOW_END_TIME.toProtoTime()
  state = RequisitionState.UNFULFILLED
  requisitionDetails = REQUISITION_DETAILS
  requisitionDetailsJson = REQUISITION_DETAILS.toJson()
}.build()

private const val DUCHY_ID = "some-duchy-id"

@RunWith(JUnit4::class)
class FulfillRequisitionTest : KingdomDatabaseTestBase() {
  private suspend fun updateExistingRequisitionState(state: RequisitionState) {
    databaseClient.write(
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

  private suspend fun fulfillRequisition(
    externalRequisitionId: Long
  ): RequisitionUpdate {
    return FulfillRequisition(ExternalId(externalRequisitionId), DUCHY_ID).execute(databaseClient)
  }

  @Before
  fun populateDatabase() = runBlocking {
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

  @Test
  fun success() = runBlocking<Unit> {
    updateExistingRequisitionState(RequisitionState.UNFULFILLED)
    val requisition = fulfillRequisition(EXTERNAL_REQUISITION_ID).current

    val expectedRequisition: Requisition =
      REQUISITION
        .toBuilder()
        .setState(RequisitionState.FULFILLED)
        .setDuchyId(DUCHY_ID)
        .build()

    assertThat(requisition).comparingExpectedFieldsOnly().isEqualTo(expectedRequisition)
    assertThat(readAllRequisitionsInSpanner())
      .comparingExpectedFieldsOnly()
      .containsExactly(expectedRequisition)
  }

  @Test
  fun `denormalizes duchy id to associated reports`() = runBlocking<Unit> {
    updateExistingRequisitionState(RequisitionState.UNFULFILLED)

    val reportDetails = ReportDetails.newBuilder().apply {
      addRequisitionsBuilder().apply {
        externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
        externalCampaignId = EXTERNAL_CAMPAIGN_ID
        externalRequisitionId = EXTERNAL_REQUISITION_ID
      }
    }.build()

    insertReportWithParents(
      ADVERTISER_ID, EXTERNAL_ADVERTISER_ID, REPORT_CONFIG_ID, EXTERNAL_REPORT_CONFIG_ID,
      SCHEDULE_ID, EXTERNAL_SCHEDULE_ID, REPORT_ID, EXTERNAL_REPORT_ID,
      state = ReportState.AWAITING_REQUISITION_CREATION,
      reportDetails = reportDetails
    )
    insertReportRequisition(
      ADVERTISER_ID,
      REPORT_CONFIG_ID,
      SCHEDULE_ID,
      REPORT_ID,
      DATA_PROVIDER_ID,
      CAMPAIGN_ID,
      REQUISITION_ID
    )

    fulfillRequisition(EXTERNAL_REQUISITION_ID)

    assertThat(readAllReportsInSpanner())
      .comparingExpectedFieldsOnly()
      .containsExactly(
        Report.newBuilder().apply {
          reportDetailsBuilder.addRequisitionsBuilder().apply {
            externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
            externalCampaignId = EXTERNAL_CAMPAIGN_ID
            externalRequisitionId = EXTERNAL_REQUISITION_ID
            duchyId = DUCHY_ID
          }
        }.build()
      )
  }

  @Test
  fun `already fulfilled`() = runBlocking<Unit> {
    updateExistingRequisitionState(RequisitionState.FULFILLED)
    val existingRequisitions = readAllRequisitionsInSpanner()

    val transition = fulfillRequisition(EXTERNAL_REQUISITION_ID)

    assertThat(transition.original).isEqualTo(transition.current)
    assertThat(readAllRequisitionsInSpanner())
      .comparingExpectedFieldsOnly()
      .containsExactlyElementsIn(existingRequisitions)
  }

  @Test
  fun `missing requisition`() = runBlocking<Unit> {
    val existingRequisitions = readAllRequisitionsInSpanner()
    assertFails {
      fulfillRequisition(EXTERNAL_REQUISITION_ID + 1)
    }
    assertThat(readAllRequisitionsInSpanner())
      .comparingExpectedFieldsOnly()
      .containsExactlyElementsIn(existingRequisitions)
  }
}
