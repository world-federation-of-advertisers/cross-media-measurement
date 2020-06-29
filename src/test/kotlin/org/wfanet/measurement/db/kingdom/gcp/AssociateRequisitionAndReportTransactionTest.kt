package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.TransactionContext
import com.google.common.truth.Truth.assertThat
import kotlin.test.assertEquals
import kotlin.test.assertFails
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.gcp.asSequence
import org.wfanet.measurement.db.gcp.runReadWriteTransaction
import org.wfanet.measurement.db.kingdom.gcp.testing.KingdomDatabaseTestBase
import org.wfanet.measurement.internal.kingdom.Report.ReportState

@RunWith(JUnit4::class)
class AssociateRequisitionAndReportTransactionTest : KingdomDatabaseTestBase() {
  companion object {
    const val ADVERTISER_ID = 1L
    const val REPORT_CONFIG_ID = 2L
    const val SCHEDULE_ID = 3L
    const val EXTERNAL_ADVERTISER_ID = 4L
    const val EXTERNAL_REPORT_CONFIG_ID = 5L
    const val EXTERNAL_SCHEDULE_ID = 6L
    const val REPORT_ID = 7L
    const val EXTERNAL_REPORT_ID = 8L
    const val DATA_PROVIDER_ID = 9L
    const val EXTERNAL_DATA_PROVIDER_ID = 10L
    const val CAMPAIGN_ID = 11L
    const val EXTERNAL_CAMPAIGN_ID = 12L
    const val REQUISITION_ID = 13L
    const val EXTERNAL_REQUISITION_ID = 14L
  }

  private fun runAssociateRequisitionAndReportTransaction(
    externalRequisitionId: ExternalId,
    externalReportId: ExternalId
  ) {
    databaseClient.runReadWriteTransaction { transactionContext: TransactionContext ->
      AssociateRequisitionAndReportTransaction()
        .execute(transactionContext, externalRequisitionId, externalReportId)
    }
  }

  @Before
  fun populateDatabase() {
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

  private fun insertTheReport() {
    insertReport(
      ADVERTISER_ID, REPORT_CONFIG_ID, SCHEDULE_ID, REPORT_ID, EXTERNAL_REPORT_ID,
      ReportState.AWAITING_REQUISITION_CREATION
    )
  }

  private fun insertTheRequisition() {
    insertRequisition(DATA_PROVIDER_ID, CAMPAIGN_ID, REQUISITION_ID, EXTERNAL_REQUISITION_ID)
  }

  @Test
  fun success() {
    insertTheReport()
    insertTheRequisition()

    runAssociateRequisitionAndReportTransaction(
      ExternalId(EXTERNAL_REQUISITION_ID),
      ExternalId(EXTERNAL_REPORT_ID)
    )

    val reportRequisitions = databaseClient
      .singleUse()
      .executeQuery(Statement.of("SELECT * FROM ReportRequisitions"))
      .asSequence()
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
  }

  @Test
  fun `missing requisition`() {
    insertTheReport()
    assertFails {
      runAssociateRequisitionAndReportTransaction(
        ExternalId(EXTERNAL_REQUISITION_ID),
        ExternalId(EXTERNAL_REPORT_ID)
      )
    }
  }

  @Test
  fun `missing report`() {
    insertTheRequisition()
    assertFails {
      runAssociateRequisitionAndReportTransaction(
        ExternalId(EXTERNAL_REQUISITION_ID),
        ExternalId(EXTERNAL_REPORT_ID)
      )
    }
  }

  @Test
  fun `already exists`() {
    insertTheReport()
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

    runAssociateRequisitionAndReportTransaction(
      ExternalId(EXTERNAL_REQUISITION_ID),
      ExternalId(EXTERNAL_REPORT_ID)
    )

    val reportRequisitions = databaseClient
      .singleUse()
      .executeQuery(Statement.of("SELECT * FROM ReportRequisitions"))
      .asSequence()
      .toList()

    assertThat(reportRequisitions).hasSize(1)
    val reportRequisition = reportRequisitions[0]

    for ((column, expectedValue) in expectedColumns) {
      assertEquals(expectedValue, reportRequisition.getLong(column))
    }
  }
}
