package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.Timestamp
import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.TimestampBound
import java.time.Clock
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.RandomIdGenerator
import org.wfanet.measurement.db.gcp.runReadWriteTransaction
import org.wfanet.measurement.db.kingdom.KingdomRelationalDatabase
import org.wfanet.measurement.db.kingdom.StreamReportsFilter
import org.wfanet.measurement.db.kingdom.StreamRequisitionsFilter
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionTemplate

class GcpKingdomRelationalDatabase(
  clock: Clock,
  randomIdGenerator: RandomIdGenerator,
  private val client: DatabaseClient
) : KingdomRelationalDatabase {

  private val createRequisitionTransaction = CreateRequisitionTransaction(randomIdGenerator)
  private val createNextReportTransaction = CreateNextReportTransaction(clock, randomIdGenerator)

  override suspend fun writeNewRequisition(requisition: Requisition): Requisition =
    client.runReadWriteTransaction { transactionContext ->
      createRequisitionTransaction.execute(transactionContext, requisition)
    } ?: requisition

  override suspend fun fulfillRequisition(externalRequisitionId: ExternalId): Requisition =
    client.runReadWriteTransaction { transactionContext ->
      FulfillRequisitionTransaction().execute(transactionContext, externalRequisitionId)
    }

  override fun streamRequisitions(
    filter: StreamRequisitionsFilter,
    limit: Long
  ): Flow<Requisition> =
    StreamRequisitionsQuery().execute(
      client.singleUse(),
      filter,
      limit
    )

  override fun createNextReport(externalScheduleId: ExternalId): Report {
    val runner = client.readWriteTransaction()
    runner.run { transactionContext ->
      createNextReportTransaction.execute(transactionContext, externalScheduleId)
    }
    val commitTimestamp: Timestamp = runner.commitTimestamp

    return ReadReportQuery().execute(
      client.singleUse(TimestampBound.ofMinReadTimestamp(commitTimestamp)),
      externalScheduleId
    )
  }

  override fun updateReportState(externalReportId: ExternalId, state: ReportState) =
    client.runReadWriteTransaction { transactionContext ->
      UpdateReportStateTransaction().execute(transactionContext, externalReportId, state)
    }

  override fun streamReports(filter: StreamReportsFilter, limit: Long): Flow<Report> =
    StreamReportsQuery().execute(client.singleUse(), filter, limit)

  override fun streamReadyReports(limit: Long): Flow<Report> =
    StreamReadyReportsQuery().execute(client.singleUse(), limit)

  override fun associateRequisitionToReport(
    externalRequisitionId: ExternalId,
    externalReportId: ExternalId
  ) {
    client.runReadWriteTransaction { transactionContext ->
      AssociateRequisitionAndReportTransaction()
        .execute(transactionContext, externalRequisitionId, externalReportId)
    }
  }

  override fun listRequisitionTemplates(reportConfigId: ExternalId): Iterable<RequisitionTemplate> =
    ReadRequisitionTemplatesQuery().execute(client.singleUse(), reportConfigId)

  override fun streamReadySchedules(limit: Long): Flow<ReportConfigSchedule> {
    TODO("Not yet implemented")
  }
}
