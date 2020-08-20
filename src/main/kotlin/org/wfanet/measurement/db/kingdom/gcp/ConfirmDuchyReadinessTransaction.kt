package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.ReadContext
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.TransactionContext
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.identity.DuchyIds
import org.wfanet.measurement.db.gcp.getProtoMessage
import org.wfanet.measurement.db.gcp.single
import org.wfanet.measurement.db.gcp.toProtoBytes
import org.wfanet.measurement.db.gcp.toProtoEnum
import org.wfanet.measurement.db.gcp.toProtoJson
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportDetails
import org.wfanet.measurement.internal.kingdom.Requisition

/**
 * Confirms a Duchy's readiness for a [Report].
 *
 * If all Duchies are ready, the [Report] is put into state [ReportState.IN_PROGRESS].
 */
class ConfirmDuchyReadinessTransaction {
  /**
   * Runs the transaction.
   *
   * @param[transactionContext] the transaction to use
   * @param[externalReportId] the [Report]
   * @param[duchyId] the Duchy
   * @param[externalRequisitionIds] all [Requisition]s the Duchy is providing for the computation
   * @throws[IllegalArgumentException] if [externalRequisitionIds] is not what is expected
   */
  fun execute(
    transactionContext: TransactionContext,
    externalReportId: ExternalId,
    duchyId: String,
    externalRequisitionIds: Set<ExternalId>
  ) {
    val readResult = readReportAndRequisitionIds(transactionContext, externalReportId, duchyId)

    val expectedIds = readResult.getLongList("ExternalRequisitionIds").map(::ExternalId).toSet()
    validateRequisitions(externalRequisitionIds, expectedIds)

    transactionContext.buffer(updateReportDetailsMutation(readResult, duchyId))
  }

  private fun validateRequisitions(providedIds: Set<ExternalId>, expectedIds: Set<ExternalId>) {
    require(providedIds == expectedIds) {
      """
      Provided Requisitions do not match what's expected:
        - Matching external ids: ${providedIds intersect expectedIds}
        - Missing external ids: ${expectedIds subtract providedIds}
        - Extra external ids: ${providedIds subtract expectedIds}
      """.trimIndent()
    }
  }

  /**
   * Reads the PK of the [Report] with [externalReportId] and all of the external [Requisition] ids
   * that belong to Duchy [duchyId] for the [Report].
   *
   * The requisition ids are represented in the return [Struct] as an array of Longs in column
   * "ExternalRequisitionIds".
   */
  private fun readReportAndRequisitionIds(
    readContext: ReadContext,
    externalReportId: ExternalId,
    duchyId: String
  ): Struct {
    val sql =
      """
      SELECT
        Reports.AdvertiserId,
        Reports.ReportConfigId,
        Reports.ScheduleId,
        Reports.ReportId,
        Reports.ReportDetails,
        IFNULL(
          ARRAY_CONCAT_AGG(DuchyRequisitions.ExternalRequisitionIds),
          ARRAY<INT64>[]
        ) AS ExternalRequisitionIds
      FROM Reports
      LEFT JOIN ReportRequisitions USING (AdvertiserId, ReportConfigId, ScheduleId, ReportId)
      LEFT JOIN (
        SELECT Requisitions.DataProviderId,
               Requisitions.CampaignId,
               Requisitions.RequisitionId,
               ARRAY_AGG(Requisitions.ExternalRequisitionId) AS ExternalRequisitionIds
        FROM Requisitions
        WHERE Requisitions.DuchyId = @duchy_id
        GROUP BY 1, 2, 3
      ) AS DuchyRequisitions USING (DataProviderId, CampaignId, RequisitionId)
      WHERE Reports.ExternalReportId = @external_report_id
        AND Reports.State = @report_state
      GROUP BY 1, 2, 3, 4, 5
      LIMIT 1
      """.trimIndent()

    val query =
      Statement.newBuilder(sql)
        .bind("external_report_id").to(externalReportId.value)
        .bind("report_state").toProtoEnum(ReportState.AWAITING_DUCHY_CONFIRMATION)
        .bind("duchy_id").to(duchyId)
        .build()

    return readContext.executeQuery(query).single()
  }

  private fun updateReportDetailsMutation(readResult: Struct, duchyId: String): Mutation {
    require(duchyId in DuchyIds.ALL) {
      "Duchy id '$duchyId' not in list of valid duchies: ${DuchyIds.ALL}"
    }

    val reportDetails =
      readResult.getProtoMessage("ReportDetails", ReportDetails.parser())
        .toBuilder()
        .addConfirmedDuchies(duchyId)
        .build()

    return Mutation.newUpdateBuilder("Reports").apply {
      set("AdvertiserId").to(readResult.getLong("AdvertiserId"))
      set("ReportConfigId").to(readResult.getLong("ReportConfigId"))
      set("ScheduleId").to(readResult.getLong("ScheduleId"))
      set("ReportId").to(readResult.getLong("ReportId"))
      set("ReportDetails").toProtoBytes(reportDetails)
      set("ReportDetailsJson").toProtoJson(reportDetails)
      if (reportDetails.confirmedDuchiesCount == DuchyIds.size) {
        set("State").toProtoEnum(ReportState.IN_PROGRESS)
      }
    }.build()
  }
}
