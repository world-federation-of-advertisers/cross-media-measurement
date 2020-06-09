package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.DatabaseClient
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.RandomIdGenerator
import org.wfanet.measurement.db.gcp.runReadWriteTransaction
import org.wfanet.measurement.db.kingdom.KingdomRelationalDatabase
import org.wfanet.measurement.db.kingdom.StreamReportsFilter
import org.wfanet.measurement.db.kingdom.StreamRequisitionsFilter
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Requisition

class GcpKingdomRelationalDatabase(
  randomIdGenerator: RandomIdGenerator,
  private val client: DatabaseClient
) : KingdomRelationalDatabase {

  private val createRequisitionTransaction = CreateRequisitionTransaction(randomIdGenerator)

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

  override fun streamReports(filter: StreamReportsFilter, limit: Long): Flow<Report> =
    StreamReportsQuery().execute(
      client.singleUse(),
      filter,
      limit
    )
}
