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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Statement
import com.google.common.truth.Truth.assertThat
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportDetails
import org.wfanet.measurement.internal.kingdom.ReportDetails.ExternalRequisitionKey
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ReportReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.KingdomDatabaseTestBase

private const val ADVERTISER_ID = 1L
private const val REPORT_CONFIG_ID = 2L
private const val SCHEDULE_ID = 3L
private const val EXTERNAL_ADVERTISER_ID = 4L
private const val EXTERNAL_REPORT_CONFIG_ID = 5L
private const val EXTERNAL_SCHEDULE_ID = 6L
private const val REPORT_ID = 7L
private const val EXTERNAL_REPORT_ID = 8L
private const val DATA_PROVIDER_ID = 9L
private const val EXTERNAL_DATA_PROVIDER_ID = 10L
private const val CAMPAIGN_ID = 11L
private const val EXTERNAL_CAMPAIGN_ID = 12L
private const val REQUISITION_ID = 13L
private const val EXTERNAL_REQUISITION_ID = 14L
private const val DUCHY_ID = "some-duchy-id"

@RunWith(JUnit4::class)
class AssociateRequisitionAndReportTest : KingdomDatabaseTestBase() {
  private suspend fun associateRequisitionAndReport(
    externalRequisitionId: ExternalId,
    externalReportId: ExternalId
  ) {
    AssociateRequisitionAndReport(externalRequisitionId, externalReportId)
      .execute(databaseClient)
  }

  @Before
  fun populateDatabase() = runBlocking {
    insertAdvertiser(ADVERTISER_ID, EXTERNAL_ADVERTISER_ID)
    insertReportConfig(
      ADVERTISER_ID,
      REPORT_CONFIG_ID,
      EXTERNAL_REPORT_CONFIG_ID
    )
    insertReportConfigSchedule(
      advertiserId = ADVERTISER_ID,
      reportConfigId = REPORT_CONFIG_ID,
      scheduleId = SCHEDULE_ID,
      externalScheduleId = EXTERNAL_SCHEDULE_ID
    )

    insertDataProvider(DATA_PROVIDER_ID, EXTERNAL_DATA_PROVIDER_ID)
    insertCampaign(DATA_PROVIDER_ID, CAMPAIGN_ID, EXTERNAL_CAMPAIGN_ID, ADVERTISER_ID)
  }

  private suspend fun insertTheReport(
    reportDetails: ReportDetails = ReportDetails.getDefaultInstance()
  ) {
    insertReport(
      ADVERTISER_ID,
      REPORT_CONFIG_ID,
      SCHEDULE_ID,
      REPORT_ID,
      EXTERNAL_REPORT_ID,
      ReportState.AWAITING_REQUISITION_CREATION,
      reportDetails = reportDetails
    )
  }

  private suspend fun insertTheRequisition() {
    insertRequisition(
      DATA_PROVIDER_ID,
      CAMPAIGN_ID,
      REQUISITION_ID,
      EXTERNAL_REQUISITION_ID,
      duchyId = DUCHY_ID
    )
  }

  @Test
  fun success() = runBlocking<Unit> {
    insertTheReport()
    insertTheRequisition()

    associateRequisitionAndReport(
      ExternalId(EXTERNAL_REQUISITION_ID),
      ExternalId(EXTERNAL_REPORT_ID)
    )

    val reportRequisitions = databaseClient
      .singleUse()
      .executeQuery(Statement.of("SELECT * FROM ReportRequisitions"))
      .toList()

    assertThat(reportRequisitions).hasSize(1)
    val reportRequisition = reportRequisitions[0]

    val expectedColumns: Map<String, Long> = mapOf(
      "AdvertiserId" to ADVERTISER_ID,
      "ReportConfigId" to REPORT_CONFIG_ID,
      "ScheduleId" to SCHEDULE_ID,
      "ReportId" to REPORT_ID,
      "DataProviderId" to DATA_PROVIDER_ID,
      "CampaignId" to CAMPAIGN_ID,
      "RequisitionId" to REQUISITION_ID
    )

    for ((column, expectedValue) in expectedColumns) {
      assertEquals(expectedValue, reportRequisition.getLong(column))
    }

    val reportReadResult =
      ReportReader().readExternalId(databaseClient.singleUse(), ExternalId(EXTERNAL_REPORT_ID))

    assertThat(reportReadResult.report.reportDetails.requisitionsList)
      .containsExactly(
        ExternalRequisitionKey.newBuilder().apply {
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          externalCampaignId = EXTERNAL_CAMPAIGN_ID
          externalRequisitionId = EXTERNAL_REQUISITION_ID
          duchyId = DUCHY_ID
        }.build()
      )
  }

  @Test
  fun `missing requisition`() = runBlocking<Unit> {
    insertTheReport()
    assertFails {
      associateRequisitionAndReport(
        ExternalId(EXTERNAL_REQUISITION_ID),
        ExternalId(EXTERNAL_REPORT_ID)
      )
    }
  }

  @Test
  fun `missing report`() = runBlocking<Unit> {
    insertTheRequisition()
    assertFails {
      associateRequisitionAndReport(
        ExternalId(EXTERNAL_REQUISITION_ID),
        ExternalId(EXTERNAL_REPORT_ID)
      )
    }
  }

  @Test
  fun `already exists`() = runBlocking {
    insertTheReport(
      ReportDetails.newBuilder().apply {
        addRequisitionsBuilder().apply {
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          externalCampaignId = EXTERNAL_CAMPAIGN_ID
          externalRequisitionId = EXTERNAL_REQUISITION_ID
        }
      }.build()
    )
    insertTheRequisition()

    val expectedColumns: Map<String, Long> = mapOf(
      "AdvertiserId" to ADVERTISER_ID,
      "ReportConfigId" to REPORT_CONFIG_ID,
      "ScheduleId" to SCHEDULE_ID,
      "ReportId" to REPORT_ID,
      "DataProviderId" to DATA_PROVIDER_ID,
      "CampaignId" to CAMPAIGN_ID,
      "RequisitionId" to REQUISITION_ID
    )

    databaseClient.write(
      listOf(
        Mutation.newInsertBuilder("ReportRequisitions").also { builder ->
          for ((column, value) in expectedColumns) {
            builder.set(column).to(value)
          }
        }.build()
      )
    )

    associateRequisitionAndReport(
      ExternalId(EXTERNAL_REQUISITION_ID),
      ExternalId(EXTERNAL_REPORT_ID)
    )

    val reportRequisitions = databaseClient
      .singleUse()
      .executeQuery(Statement.of("SELECT * FROM ReportRequisitions"))
      .toList()

    assertThat(reportRequisitions).hasSize(1)
    val reportRequisition = reportRequisitions[0]

    for ((column, expectedValue) in expectedColumns) {
      assertEquals(expectedValue, reportRequisition.getLong(column))
    }
  }
}
