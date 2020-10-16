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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Value
import com.google.protobuf.ProtocolMessageEnum
import java.time.Instant
import org.wfanet.measurement.common.AllOfClause
import org.wfanet.measurement.common.AnyOfClause
import org.wfanet.measurement.common.GreaterThanClause
import org.wfanet.measurement.common.TerminalClause
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.numberAsLong
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.kingdom.db.StreamReportsClause
import org.wfanet.measurement.kingdom.db.StreamRequisitionsClause
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.SqlConverter.SqlData

interface SqlConverter<V> {
  data class SqlData(val fieldName: String, val bindingName: String, val spannerValue: Value)
  fun sqlData(v: V): SqlData
}

fun <V : TerminalClause> AllOfClause<V>.toSql(
  query: Statement.Builder,
  sqlConverter: SqlConverter<V>
) {
  for ((i, clause) in clauses.withIndex()) {
    if (i > 0) query.appendClause("  AND")
    val sqlData = sqlConverter.sqlData(clause)
    val fieldName = sqlData.fieldName
    val bindName = sqlData.bindingName
    when (clause) {
      is AnyOfClause -> query.append("($fieldName IN UNNEST(@$bindName))")
      is GreaterThanClause -> query.append("($fieldName > @$bindName)")
    }
    query.bind(bindName).to(sqlData.spannerValue)
  }
}

object StreamRequisitionsFilterSqlConverter : SqlConverter<StreamRequisitionsClause> {
  override fun sqlData(v: StreamRequisitionsClause): SqlData = when (v) {
    is StreamRequisitionsClause.ExternalDataProviderId -> SqlData(
      "DataProviders.ExternalDataProviderId",
      "external_data_provider_id",
      externalIdValueArray(v.values)
    )

    is StreamRequisitionsClause.ExternalCampaignId -> SqlData(
      "Campaigns.ExternalCampaignId",
      "external_campaignId",
      externalIdValueArray(v.values)
    )

    is StreamRequisitionsClause.CreatedAfter ->
      SqlData("Requisitions.CreateTime", "create_time", timestampValue(v.value))

    is StreamRequisitionsClause.State ->
      SqlData("Requisitions.State", "state", enumValueArray(v.values))
  }
}

object StreamReportsFilterSqlConverter : SqlConverter<StreamReportsClause> {
  override fun sqlData(v: StreamReportsClause): SqlData = when (v) {
    is StreamReportsClause.ExternalAdvertiserId -> SqlData(
      "Advertisers.ExternalAdvertiserId",
      "external_advertiser_id",
      externalIdValueArray(v.values)
    )

    is StreamReportsClause.ExternalReportConfigId -> SqlData(
      "ReportConfigs.ExternalReportConfigId",
      "external_report_config_id",
      externalIdValueArray(v.values)
    )

    is StreamReportsClause.ExternalScheduleId -> SqlData(
      "ReportConfigSchedules.ExternalScheduleId",
      "external_schedule_id",
      externalIdValueArray(v.values)
    )

    is StreamReportsClause.State ->
      SqlData("Reports.State", "state", enumValueArray(v.values))

    is StreamReportsClause.UpdatedAfter ->
      SqlData("Reports.UpdateTime", "update_time", timestampValue(v.value))
  }
}

private fun externalIdValueArray(ids: Iterable<ExternalId>): Value =
  Value.int64Array(ids.map(ExternalId::value))

private fun enumValueArray(enums: Iterable<ProtocolMessageEnum>): Value =
  Value.int64Array(enums.map { it.numberAsLong })

private fun timestampValue(time: Instant): Value = Value.timestamp(time.toGcloudTimestamp())
