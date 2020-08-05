package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.Timestamp
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.TransactionContext
import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFails
import org.junit.Before
import org.junit.Test
import org.wfanet.measurement.db.gcp.asSequence
import org.wfanet.measurement.db.gcp.toProtoBytes
import org.wfanet.measurement.db.gcp.toProtoJson
import org.wfanet.measurement.db.kingdom.gcp.testing.KingdomDatabaseTestBase
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportLogEntry

private const val ADVERTISER_ID = 1L
private const val REPORT_CONFIG_ID = 2L
private const val SCHEDULE_ID = 3L
private const val EXTERNAL_ADVERTISER_ID = 4L
private const val EXTERNAL_REPORT_CONFIG_ID = 5L
private const val EXTERNAL_SCHEDULE_ID = 6L
private const val REPORT_ID = 7L
private const val EXTERNAL_REPORT_ID = 8L

private val REPORT_LOG_ENTRY: ReportLogEntry = ReportLogEntry.newBuilder().apply {
  externalReportId = EXTERNAL_REPORT_ID
  sourceBuilder.advertiserBuilder.externalAdvertiserId = 99999
  reportLogDetailsBuilder.apply {
    advertiserLogDetailsBuilder.apiMethod = "/Foo.Bar"
    reportMessage = "some-report-message"
  }
}.build()

class CreateReportLogEntryTransactionTest : KingdomDatabaseTestBase() {
  @Before
  fun populateDatabase() {
    insertAdvertiser(ADVERTISER_ID, EXTERNAL_ADVERTISER_ID)
    insertReportConfig(ADVERTISER_ID, REPORT_CONFIG_ID, EXTERNAL_REPORT_CONFIG_ID)
    insertReportConfigSchedule(ADVERTISER_ID, REPORT_CONFIG_ID, SCHEDULE_ID, EXTERNAL_SCHEDULE_ID)
    insertReport(
      ADVERTISER_ID, REPORT_CONFIG_ID, SCHEDULE_ID, REPORT_ID, EXTERNAL_REPORT_ID,
      state = ReportState.IN_PROGRESS
    )
  }

  private fun execute(reportLogEntry: ReportLogEntry): Timestamp {
    val runner = databaseClient.readWriteTransaction()
    runner.run { transactionContext: TransactionContext ->
      CreateReportLogEntryTransaction().execute(transactionContext, reportLogEntry)
    }
    return runner.commitTimestamp
  }

  private fun readReportLogEntries(): List<Struct> {
    return databaseClient
      .singleUse()
      .executeQuery(Statement.of("SELECT * FROM ReportLogEntries"))
      .asSequence()
      .toList()
  }

  private fun reportLogEntryStructWithCreateTime(createTime: Timestamp): Struct =
    Struct.newBuilder()
      .set("AdvertiserId").to(ADVERTISER_ID)
      .set("ReportConfigId").to(REPORT_CONFIG_ID)
      .set("ScheduleId").to(SCHEDULE_ID)
      .set("ReportId").to(REPORT_ID)
      .set("CreateTime").to(createTime)
      .set("ReportLogDetails").toProtoBytes(REPORT_LOG_ENTRY.reportLogDetails)
      .set("ReportLogDetailsJson").toProtoJson(REPORT_LOG_ENTRY.reportLogDetails)
      .build()

  @Test
  fun success() {
    val createTime = execute(REPORT_LOG_ENTRY)
    assertThat(readReportLogEntries())
      .containsExactly(reportLogEntryStructWithCreateTime(createTime))
  }

  @Test
  fun `multiple ReportLogEntries`() {
    val createTime1 = execute(REPORT_LOG_ENTRY)
    val createTime2 = execute(REPORT_LOG_ENTRY)
    val createTime3 = execute(REPORT_LOG_ENTRY)
    assertThat(readReportLogEntries())
      .containsExactly(
        reportLogEntryStructWithCreateTime(createTime1),
        reportLogEntryStructWithCreateTime(createTime2),
        reportLogEntryStructWithCreateTime(createTime3)
      )
  }

  @Test
  fun `missing Report`() {
    assertFails {
      val missingExternalReportId = EXTERNAL_REPORT_ID + 1
      val reportLogEntry =
        REPORT_LOG_ENTRY.toBuilder()
          .setExternalReportId(missingExternalReportId)
          .build()
      execute(reportLogEntry)
    }
  }
}
