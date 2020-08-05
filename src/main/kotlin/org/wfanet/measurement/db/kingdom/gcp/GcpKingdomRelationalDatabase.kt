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
import org.wfanet.measurement.internal.kingdom.ReportLogEntry
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionTemplate

class GcpKingdomRelationalDatabase(
  clock: Clock,
  randomIdGenerator: RandomIdGenerator,
  lazyClient: () -> DatabaseClient
) : KingdomRelationalDatabase {
  private val client: DatabaseClient by lazy { lazyClient() }
  private val createRequisitionTransaction = CreateRequisitionTransaction(randomIdGenerator)
  private val createNextReportTransaction = CreateNextReportTransaction(clock, randomIdGenerator)

  constructor(
    clock: Clock,
    randomIdGenerator: RandomIdGenerator,
    client: DatabaseClient
  ) : this(clock, randomIdGenerator, { client })

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

  override fun getReport(externalId: ExternalId): Report =
    GetReportQuery().execute(client.singleUse(), externalId)

  override fun createNextReport(externalScheduleId: ExternalId): Report {
    val runner = client.readWriteTransaction()
    runner.run { transactionContext ->
      createNextReportTransaction.execute(transactionContext, externalScheduleId)
    }
    val commitTimestamp: Timestamp = runner.commitTimestamp

    return ReadLatestReportByScheduleQuery().execute(
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

  override fun streamReadySchedules(limit: Long): Flow<ReportConfigSchedule> =
    StreamReadySchedulesQuery().execute(client.singleUse(), limit)

  override fun addReportLogEntry(reportLogEntry: ReportLogEntry): ReportLogEntry {
    val runner = client.readWriteTransaction()
    runner.run { transactionContext ->
      CreateReportLogEntryTransaction().execute(transactionContext, reportLogEntry)
    }
    val commitTimestamp: Timestamp = runner.commitTimestamp
    return reportLogEntry.toBuilder().apply {
      createTime = commitTimestamp.toProto()
    }.build()
  }
}
