/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db

import com.google.cloud.spanner.Value
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertOrUpdateMutation
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResult

/** Buffers a retryable upsert mutation to the ReportResultValues table. */
fun AsyncDatabaseClient.TransactionContext.upsertReportResultValues(
  measurementConsumerId: Long,
  reportResultId: Long,
  reportingSetResultId: Long,
  reportingWindowResultId: Long,
  values: ReportingSetResult.ReportingWindowResult.ReportResultValues,
) {
  bufferInsertOrUpdateMutation("ReportResultValues") {
    set("MeasurementConsumerId").to(measurementConsumerId)
    set("ReportResultId").to(reportResultId)
    set("ReportingSetResultId").to(reportingSetResultId)
    set("ReportingWindowResultId").to(reportingWindowResultId)
    set("CumulativeResults")
      .to(
        if (values.hasCumulativeResults()) Value.protoMessage(values.cumulativeResults)
        else Value.protoMessage(null, values.cumulativeResults.descriptorForType)
      )
    set("NonCumulativeResults")
      .to(
        if (values.hasNonCumulativeResults()) Value.protoMessage(values.nonCumulativeResults)
        else Value.protoMessage(null, values.nonCumulativeResults.descriptorForType)
      )
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
  }
}
