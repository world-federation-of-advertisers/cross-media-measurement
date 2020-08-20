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

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.TransactionContext
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlin.test.assertFails
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.identity.testing.DuchyIdSetter
import org.wfanet.measurement.db.gcp.runReadWriteTransaction
import org.wfanet.measurement.db.gcp.toProtoEnum
import org.wfanet.measurement.db.kingdom.gcp.testing.KingdomDatabaseTestBase
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.Requisition

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
private const val REQUISITION_ID1 = 13L
private const val EXTERNAL_REQUISITION_ID1 = 14L
private const val REQUISITION_ID2 = 15L
private const val EXTERNAL_REQUISITION_ID2 = 16L

private const val DUCHY_ID = "some-duchy-id"
private const val OTHER_DUCHY_ID = "other-duchy-id"

@RunWith(JUnit4::class)
class ConfirmDuchyReadinessTransactionTest : KingdomDatabaseTestBase() {
  @get:Rule val duchyIdSetter = DuchyIdSetter(DUCHY_ID, OTHER_DUCHY_ID)

  private lateinit var originalReport: Report

  private val originalReportBuilder: Report.Builder
    get() = originalReport.toBuilder()

  @Before
  fun populateDatabase() {
    insertAdvertiser(ADVERTISER_ID, EXTERNAL_ADVERTISER_ID)
    insertReportConfig(ADVERTISER_ID, REPORT_CONFIG_ID, EXTERNAL_REPORT_CONFIG_ID)
    insertReportConfigSchedule(ADVERTISER_ID, REPORT_CONFIG_ID, SCHEDULE_ID, EXTERNAL_SCHEDULE_ID)
    insertReport(
      ADVERTISER_ID,
      REPORT_CONFIG_ID,
      SCHEDULE_ID,
      REPORT_ID,
      EXTERNAL_REPORT_ID,
      ReportState.AWAITING_DUCHY_CONFIRMATION
    )
    insertDataProvider(DATA_PROVIDER_ID, EXTERNAL_DATA_PROVIDER_ID)
    insertCampaign(DATA_PROVIDER_ID, CAMPAIGN_ID, EXTERNAL_CAMPAIGN_ID, ADVERTISER_ID)

    originalReport = readAllReportsInSpanner().single()
  }

  private fun Report.Builder.withConfirmedDuchies(vararg confirmedDuchies: String): Report.Builder {
    reportDetailsBuilder.clearConfirmedDuchies()
    reportDetailsBuilder.addAllConfirmedDuchies(confirmedDuchies.asList())
    return this
  }

  private fun assertReportInDatabaseIs(report: Report.Builder) {
    assertThat(readAllReportsInSpanner())
      .ignoringFields(Report.REPORT_DETAILS_JSON_FIELD_NUMBER)
      .containsExactly(report.build())
  }

  private fun insertFulfilledRequisition(
    requisitionId: Long,
    externalRequisitionId: Long,
    duchyId: String
  ) {
    insertRequisition(
      DATA_PROVIDER_ID,
      CAMPAIGN_ID,
      requisitionId,
      externalRequisitionId,
      state = Requisition.RequisitionState.FULFILLED,
      duchyId = duchyId
    )
  }

  private fun linkRequisitionToReport(requisitionId: Long) {
    insertReportRequisition(
      ADVERTISER_ID, REPORT_CONFIG_ID, SCHEDULE_ID, REPORT_ID, DATA_PROVIDER_ID, CAMPAIGN_ID,
      requisitionId
    )
  }

  private fun runConfirmDuchyReadinessTransaction(
    duchyId: String,
    vararg requisitions: Long
  ) {
    databaseClient.runReadWriteTransaction { transactionContext: TransactionContext ->
      ConfirmDuchyReadinessTransaction()
        .execute(
          transactionContext,
          ExternalId(EXTERNAL_REPORT_ID),
          duchyId,
          requisitions.map(::ExternalId).toSet()
        )
    }
  }

  @Test
  fun success() {
    insertFulfilledRequisition(REQUISITION_ID1, EXTERNAL_REQUISITION_ID1, DUCHY_ID)
    linkRequisitionToReport(REQUISITION_ID1)

    insertFulfilledRequisition(REQUISITION_ID2, EXTERNAL_REQUISITION_ID2, DUCHY_ID)
    linkRequisitionToReport(REQUISITION_ID2)

    runConfirmDuchyReadinessTransaction(
      DUCHY_ID, EXTERNAL_REQUISITION_ID1, EXTERNAL_REQUISITION_ID2
    )

    assertReportInDatabaseIs(originalReportBuilder.withConfirmedDuchies(DUCHY_ID))

    runConfirmDuchyReadinessTransaction(OTHER_DUCHY_ID)

    assertReportInDatabaseIs(
      originalReportBuilder
        .setState(ReportState.IN_PROGRESS)
        .withConfirmedDuchies(DUCHY_ID, OTHER_DUCHY_ID)
    )
  }

  @Test
  fun `no requisitions`() {
    runConfirmDuchyReadinessTransaction(DUCHY_ID)
    assertReportInDatabaseIs(originalReportBuilder.withConfirmedDuchies(DUCHY_ID))
  }

  @Test
  fun `call is missing Requisitions`() {
    insertFulfilledRequisition(REQUISITION_ID1, EXTERNAL_REQUISITION_ID1, DUCHY_ID)
    linkRequisitionToReport(REQUISITION_ID1)

    assertFails {
      runConfirmDuchyReadinessTransaction(DUCHY_ID)
    }
    assertReportInDatabaseIs(originalReportBuilder)
  }

  @Test
  fun `call has extra Requisitions that it owns but not linked`() {
    insertFulfilledRequisition(REQUISITION_ID1, EXTERNAL_REQUISITION_ID1, DUCHY_ID)
    assertFails {
      runConfirmDuchyReadinessTransaction(DUCHY_ID, EXTERNAL_REQUISITION_ID1)
    }
    assertReportInDatabaseIs(originalReportBuilder)
  }

  @Test
  fun `call has extra Requisitions that another Duchy owns`() {
    insertFulfilledRequisition(REQUISITION_ID1, EXTERNAL_REQUISITION_ID1, OTHER_DUCHY_ID)
    linkRequisitionToReport(REQUISITION_ID1)
    assertFails {
      runConfirmDuchyReadinessTransaction(DUCHY_ID, EXTERNAL_REQUISITION_ID1)
    }
    assertReportInDatabaseIs(originalReportBuilder)
  }

  @Test
  fun `wrong report state`() {
    databaseClient.write(
      listOf(
        Mutation.newUpdateBuilder("Reports")
          .set("AdvertiserId").to(ADVERTISER_ID)
          .set("ReportConfigId").to(REPORT_CONFIG_ID)
          .set("ScheduleId").to(SCHEDULE_ID)
          .set("ReportId").to(REPORT_ID)
          .set("State").toProtoEnum(ReportState.AWAITING_REQUISITION_FULFILLMENT)
          .build()
      )
    )
    originalReport = readAllReportsInSpanner().single()

    assertFails {
      runConfirmDuchyReadinessTransaction(DUCHY_ID)
    }

    assertReportInDatabaseIs(originalReportBuilder)
  }
}
