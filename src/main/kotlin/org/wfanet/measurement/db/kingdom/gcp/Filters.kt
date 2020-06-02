package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Value
import org.wfanet.measurement.common.AllOfClause
import org.wfanet.measurement.common.AnyOfClause
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.GreaterThanClause
import org.wfanet.measurement.common.TerminalClause
import org.wfanet.measurement.db.gcp.appendClause
import org.wfanet.measurement.db.gcp.toGcpTimestamp
import org.wfanet.measurement.db.kingdom.StreamRequisitionsClause
import org.wfanet.measurement.db.kingdom.gcp.SqlConverter.SqlData

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
    when (clause) {
      is AnyOfClause -> clause.toSql(query, sqlData)
      is GreaterThanClause -> clause.toSql(query, sqlData)
    }
  }
}

fun <V : AnyOfClause> V.toSql(
  query: Statement.Builder,
  sqlData: SqlData
) {
  val fieldName = sqlData.fieldName
  val bindName = sqlData.bindingName
  query
    .append("($fieldName IN UNNEST(@$bindName))")
    .bind(bindName).to(sqlData.spannerValue)
}

fun <V : GreaterThanClause> V.toSql(
  query: Statement.Builder,
  sqlData: SqlData
) {
  val fieldName = sqlData.fieldName
  val bindName = sqlData.bindingName
  query
    .append("($fieldName > @$bindName)")
    .bind(bindName).to(sqlData.spannerValue)
}

object StreamRequisitionsFilterSqlConverter : SqlConverter<StreamRequisitionsClause> {
  override fun sqlData(v: StreamRequisitionsClause): SqlData = when (v) {
    is StreamRequisitionsClause.CreatedAfter -> SqlData(
      "Requisitions.CreateTime",
      "create_time",
      Value.timestamp(v.value.toGcpTimestamp())
    )

    is StreamRequisitionsClause.ExternalCampaignId -> SqlData(
      "Campaigns.ExternalCampaignId",
      "external_campaign_id",
      Value.int64Array(v.values.map(ExternalId::value))
    )

    is StreamRequisitionsClause.ExternalDataProviderId -> SqlData(
      "DataProviders.ExternalDataProviderId",
      "external_data_provider_id",
      Value.int64Array(v.values.map(ExternalId::value))
    )

    is StreamRequisitionsClause.State -> SqlData(
      "Requisitions.State",
      "state",
      Value.int64Array(v.values.map { it.ordinal.toLong() })
    )
  }
}
