/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.internal.edpaggregator.ReportRoutingDecision
import org.wfanet.measurement.internal.edpaggregator.reportRoutingDecision

/** Reads a report-routing decision by its provider/report identity. */
suspend fun AsyncDatabaseClient.ReadContext.findReportRoutingDecision(
  dataProviderResourceId: String,
  report: String,
): ReportRoutingDecision? =
  readRow(
      "ReportRoutingDecision",
      Key.of(dataProviderResourceId, report),
      REPORT_ROUTING_DECISION_COLUMNS,
    )
    ?.let(::buildReportRoutingDecision)

/** Inserts an immutable report-routing decision. */
fun AsyncDatabaseClient.TransactionContext.insertReportRoutingDecision(
  decision: ReportRoutingDecision
) {
  bufferInsertMutation("ReportRoutingDecision") {
    set("DataProviderResourceId").to(decision.dataProviderResourceId)
    set("Report").to(decision.report)
    set("ReportIntervalStartTime").to(decision.reportInterval.startTime.toGcloudTimestamp())
    set("ReportIntervalEndTime").to(decision.reportInterval.endTime.toGcloudTimestamp())
    set("ExternalModelLine").to(decision.externalModelLine)
    set("SelectedModelLine").to(decision.selectedModelLine)
    if (decision.hasCutoverTime()) {
      set("CutoverTime").to(decision.cutoverTime.toGcloudTimestamp())
    } else {
      set("CutoverTime").to(Value.timestamp(null))
    }
    set("CreateTime").to(decision.createTime.toGcloudTimestamp())
  }
}

private fun buildReportRoutingDecision(row: Struct): ReportRoutingDecision = reportRoutingDecision {
  dataProviderResourceId = row.getString("DataProviderResourceId")
  report = row.getString("Report")
  reportInterval =
    com.google.type.interval {
      startTime = row.getTimestamp("ReportIntervalStartTime").toProto()
      endTime = row.getTimestamp("ReportIntervalEndTime").toProto()
    }
  externalModelLine = row.getString("ExternalModelLine")
  selectedModelLine = row.getString("SelectedModelLine")
  if (!row.isNull("CutoverTime")) {
    cutoverTime = row.getTimestamp("CutoverTime").toProto()
  }
  createTime = row.getTimestamp("CreateTime").toProto()
}

private val REPORT_ROUTING_DECISION_COLUMNS =
  listOf(
    "DataProviderResourceId",
    "Report",
    "ReportIntervalStartTime",
    "ReportIntervalEndTime",
    "ExternalModelLine",
    "SelectedModelLine",
    "CutoverTime",
    "CreateTime",
  )
