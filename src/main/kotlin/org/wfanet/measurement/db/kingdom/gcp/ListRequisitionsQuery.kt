package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.ReadContext
import com.google.protobuf.Timestamp as ProtoTimestamp
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.Pagination
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.db.gcp.appendClause
import org.wfanet.measurement.db.gcp.executeSqlQuery
import org.wfanet.measurement.db.gcp.spannerDispatcher
import org.wfanet.measurement.db.gcp.toGcpTimestamp
import org.wfanet.measurement.db.kingdom.MeasurementProviderStorage
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionState

/**
 * Reads pages of Requisitions.
 */
class ListRequisitionsQuery {
  /**
   * Runs a read query for a specific campaign.
   *
   * @param[readContext] Spanner read context
   * @param[externalDataProviderId] the Data Provider
   * @param[externalCampaignId] the Campaign
   * @param[states] which states to include
   * @param[pagination] what page of results (and how many)
   * @return a page of [Requisition]s
   */
  fun execute(
    readContext: ReadContext,
    externalDataProviderId: ExternalId,
    externalCampaignId: ExternalId,
    states: Set<RequisitionState>,
    pagination: Pagination
  ): MeasurementProviderStorage.ListResult {
    require(pagination.pageSize <= 1000) { "Page size of $pagination is too large" }
    require(states.isNotEmpty())

    val whereClause =
      """
      WHERE DataProviders.ExternalDataProviderId = @external_data_provider_id
        AND Campaigns.ExternalCampaignId = @external_campaign_id
        AND Requisitions.State IN UNNEST(@states)
        AND CreateTime > @last_create_time
      """.trimIndent()

    val paginationClause =
      """
      ORDER BY CreateTime ASC
      LIMIT @page_size
      """.trimIndent()

    val pageTokenBytes = pagination.pageToken.base64UrlDecode()
    val lastCreateTime = ProtoTimestamp.parseFrom(pageTokenBytes).toGcpTimestamp()

    val query = REQUISITION_READ_QUERY
      .toBuilder()
      .appendClause(whereClause)
      .appendClause(paginationClause)
      .bind("external_data_provider_id").to(externalDataProviderId.value)
      .bind("external_campaign_id").to(externalCampaignId.value)
      .bind("states").toInt64Array(states.map { it.ordinal.toLong() })
      .bind("page_size").to(pagination.pageSize.toLong())
      .bind("last_create_time").to(lastCreateTime)
      .build()

    val requisitions: List<Requisition> = runBlocking(spannerDispatcher()) {
      readContext
        .executeSqlQuery(query)
        .map { it.toRequisition() }
        .toList()
    }

    val nextPageToken =
      requisitions
        .lastOrNull()
        ?.createTime
        ?.toByteArray()
        ?.base64UrlEncode()

    return MeasurementProviderStorage.ListResult(requisitions, nextPageToken)
  }
}
