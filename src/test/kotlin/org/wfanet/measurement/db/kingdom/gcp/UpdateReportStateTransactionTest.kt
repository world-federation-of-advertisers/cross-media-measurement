package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.TransactionContext
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlin.test.assertFails
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.gcp.runReadWriteTransaction
import org.wfanet.measurement.db.kingdom.gcp.testing.KingdomDatabaseTestBase
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState

@RunWith(JUnit4::class)
class UpdateReportStateTransactionTest : KingdomDatabaseTestBase() {
  companion object {
    const val ADVERTISER_ID = 1L
    const val REPORT_CONFIG_ID = 2L
    const val SCHEDULE_ID = 3L
    const val EXTERNAL_ADVERTISER_ID = 4L
    const val EXTERNAL_REPORT_CONFIG_ID = 5L
    const val EXTERNAL_SCHEDULE_ID = 6L
    const val REPORT_ID = 7L
    const val EXTERNAL_REPORT_ID = 8L
  }

  private fun runUpdateReportStateTransaction(
    externalReportId: ExternalId,
    state: ReportState
  ) {
    spanner.client.runReadWriteTransaction { transactionContext: TransactionContext ->
      UpdateReportStateTransaction()
        .execute(transactionContext, externalReportId, state)
    }
  }

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
      ReportState.AWAITING_REQUISITION_FULFILLMENT
    )
  }

  @Test
  fun success() {
    for (state in ReportState.values()) {
      if (state == ReportState.UNRECOGNIZED) {
        assertFails {
          runUpdateReportStateTransaction(ExternalId(EXTERNAL_REPORT_ID), state)
        }
      } else {
        runUpdateReportStateTransaction(ExternalId(EXTERNAL_REPORT_ID), state)

        assertThat(readAllReportsInSpanner())
          .comparingExpectedFieldsOnly()
          .containsExactly(Report.newBuilder().setState(state).build())
      }
    }
  }

  @Test
  fun `noop update`() {
    runUpdateReportStateTransaction(ExternalId(EXTERNAL_REPORT_ID), ReportState.SUCCEEDED)

    // Does not fail:
    runUpdateReportStateTransaction(ExternalId(EXTERNAL_REPORT_ID), ReportState.SUCCEEDED)

    assertThat(readAllReportsInSpanner())
      .comparingExpectedFieldsOnly()
      .containsExactly(Report.newBuilder().setState(ReportState.SUCCEEDED).build())
  }
}
