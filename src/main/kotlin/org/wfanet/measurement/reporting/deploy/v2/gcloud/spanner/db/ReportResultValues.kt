package org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db

import com.google.cloud.spanner.Value
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResult

/** Buffers an insert mutation to the ReportResultValues table. */
fun AsyncDatabaseClient.TransactionContext.insertReportResultValues(
  measurementConsumerId: Long,
  reportResultId: Long,
  reportingSetResultId: Long,
  reportingWindowResultId: Long,
  values: ReportingSetResult.ReportingWindowResult.ReportResultValues,
) {
  bufferInsertMutation("ReportResultValues") {
    set("MeasurementConsumerId").to(measurementConsumerId)
    set("ReportResultId").to(reportResultId)
    set("ReportingSetResultId").to(reportingSetResultId)
    set("ReportingWindowResultId").to(reportingWindowResultId)
    if (values.hasCumulativeResults()) {
      set("CumulativeResults").to(values.cumulativeResults)
    }
    if (values.hasNonCumulativeResults()) {
      set("NonCumulativeResults").to(values.nonCumulativeResults)
    }
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
  }
}
