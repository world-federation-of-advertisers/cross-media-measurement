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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Value
import com.google.protobuf.ProtocolMessageEnum
import java.time.Instant
import org.wfanet.measurement.common.AllOfClause
import org.wfanet.measurement.common.TerminalClause
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.numberAsLong
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.kingdom.db.StreamReportsClause
import org.wfanet.measurement.kingdom.db.StreamRequisitionsClause
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.SqlData.ClauseType.ANY_OF
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.SqlData.ClauseType.GREATER_THAN

/** Represents the information necessary to build a standard WHERE-clause predicate. */
data class SqlData(
  val column: String,
  val clauseType: ClauseType,
  val spannerValue: Value,
  val bindName: String = column.replace(".", "_")
) {
  enum class ClauseType {
    ANY_OF,
    GREATER_THAN
  }
}

infix fun String.containedIn(spannerValue: Value): SqlData {
  return SqlData(this, ANY_OF, spannerValue)
}

infix fun String.greaterThan(spannerValue: Value): SqlData {
  return SqlData(this, GREATER_THAN, spannerValue)
}

/** Converts [V] into SQL. */
interface SqlConverter<V> {
  fun sqlData(v: V): SqlData
}

/** Converts an [AllOfClause] to Spanner-compliant SQL. */
fun <V : TerminalClause> AllOfClause<V>.toSql(
  query: Statement.Builder,
  sqlConverter: SqlConverter<V>
) {
  for ((i, clause) in clauses.withIndex()) {
    if (i > 0) query.appendClause("  AND")
    val sqlData = sqlConverter.sqlData(clause)
    val fieldName = sqlData.column
    val bindName = sqlData.bindName
    when (sqlData.clauseType) {
      ANY_OF -> query.append("($fieldName IN UNNEST(@$bindName))")
      GREATER_THAN -> query.append("($fieldName > @$bindName)")
    }
    query.bind(bindName).to(sqlData.spannerValue)
  }
}

object StreamRequisitionsFilterSqlConverter : SqlConverter<StreamRequisitionsClause> {
  override fun sqlData(v: StreamRequisitionsClause): SqlData =
    when (v) {
      is StreamRequisitionsClause.ExternalDataProviderId ->
        "DataProviders.ExternalDataProviderId" containedIn externalIdValueArray(v.values)
      is StreamRequisitionsClause.ExternalCampaignId ->
        "Campaigns.ExternalCampaignId" containedIn externalIdValueArray(v.values)
      is StreamRequisitionsClause.CreatedAfter ->
        "Requisitions.CreateTime" greaterThan timestampValue(v.value)
      is StreamRequisitionsClause.State -> "Requisitions.State" containedIn enumValueArray(v.values)
    }
}

object StreamReportsFilterSqlConverter : SqlConverter<StreamReportsClause> {
  override fun sqlData(v: StreamReportsClause): SqlData =
    when (v) {
      is StreamReportsClause.ExternalAdvertiserId ->
        "Advertisers.ExternalAdvertiserId" containedIn externalIdValueArray(v.values)
      is StreamReportsClause.ExternalReportConfigId ->
        "ReportConfigs.ExternalReportConfigId" containedIn externalIdValueArray(v.values)
      is StreamReportsClause.ExternalScheduleId ->
        "ReportConfigSchedules.ExternalScheduleId" containedIn externalIdValueArray(v.values)
      is StreamReportsClause.State -> "Reports.State" containedIn enumValueArray(v.values)
      is StreamReportsClause.UpdatedAfter ->
        "Reports.UpdateTime" greaterThan timestampValue(v.value)
    }
}

private fun externalIdValueArray(ids: Iterable<ExternalId>): Value {
  return Value.int64Array(ids.map { it.value })
}

private fun enumValueArray(enums: Iterable<ProtocolMessageEnum>): Value {
  return Value.int64Array(enums.map { it.numberAsLong })
}

private fun timestampValue(time: Instant): Value {
  return Value.timestamp(time.toGcloudTimestamp())
}
