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
import com.google.type.Date
import java.time.Instant
import org.wfanet.measurement.common.AllOfClause
import org.wfanet.measurement.common.AnyOfClause
import org.wfanet.measurement.common.GreaterThanClause
import org.wfanet.measurement.common.LessThanClause
import org.wfanet.measurement.common.TerminalClause
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.numberAsLong
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.kingdom.db.EqualClause
import org.wfanet.measurement.kingdom.db.GetExchangeStepClause
import org.wfanet.measurement.kingdom.db.StreamRecurringExchangesClause
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
      is LessThanClause -> query.append("($fieldName < @$bindName)")
      is EqualClause -> query.append("($fieldName = @$bindName)")
    }
    query.bind(bindName).to(sqlData.spannerValue)
  }
}

object StreamRecurringExchangesFilterSqlConverter : SqlConverter<StreamRecurringExchangesClause> {
  override fun sqlData(v: StreamRecurringExchangesClause): SqlData =
    when (v) {
      is StreamRecurringExchangesClause.ExternalModelProviderId ->
        SqlData(
          "ModelProviders.ExternalModelProviderId",
          "external_model_provider_id",
          externalIdValueArray(v.values)
        )
      is StreamRecurringExchangesClause.ExternalDataProviderId ->
        SqlData(
          "DataProviders.ExternalDataProviderId",
          "external_data_provider_id",
          externalIdValueArray(v.values)
        )
      is StreamRecurringExchangesClause.State ->
        SqlData("RecurringExchanges.State", "state", enumValueArray(v.values))
      is StreamRecurringExchangesClause.NextExchangeDateBefore ->
        SqlData("RecurringExchanges.NextExchangeDate", "next_exchange_date", dateValue(v.value))
    }
}

object GetExchangeStepFilterSqlConverter : SqlConverter<GetExchangeStepClause> {
  override fun sqlData(v: GetExchangeStepClause): SqlData =
    when (v) {
      is GetExchangeStepClause.ExternalModelProviderId ->
        SqlData(
          "ModelProviders.ExternalModelProviderId",
          "external_model_provider_id",
          externalIdValueArray(v.values)
        )
      is GetExchangeStepClause.ExternalDataProviderId ->
        SqlData(
          "DataProviders.ExternalDataProviderId",
          "external_data_provider_id",
          externalIdValueArray(v.values)
        )
      is GetExchangeStepClause.RecurringExchangeId ->
        SqlData("ExchangeSteps.RecurringExchangeId", "recurring_exchange_id", Value.int64(v.value))
      is GetExchangeStepClause.Date -> SqlData("ExchangeSteps.Date", "date", dateValue(v.value))
      is GetExchangeStepClause.StepIndex ->
        SqlData("ExchangeSteps.StepIndex", "step_index", Value.int64(v.value))
      is GetExchangeStepClause.State ->
        SqlData("ExchangeSteps.State", "state", enumValueArray(v.values))
    }
}

private fun externalIdValueArray(ids: Iterable<ExternalId>): Value =
  Value.int64Array(ids.map(ExternalId::value))

private fun enumValueArray(enums: Iterable<ProtocolMessageEnum>): Value =
  Value.int64Array(enums.map { it.numberAsLong })

private fun timestampValue(time: Instant): Value = Value.timestamp(time.toGcloudTimestamp())

private fun dateValue(date: Date): Value = Value.date(date.toCloudDate())
