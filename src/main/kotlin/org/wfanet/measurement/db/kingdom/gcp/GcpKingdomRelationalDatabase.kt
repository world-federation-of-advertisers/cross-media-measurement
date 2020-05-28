package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.DatabaseClient
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.Pagination
import org.wfanet.measurement.db.gcp.runReadWriteTransaction
import org.wfanet.measurement.db.kingdom.KingdomRelationalDatabase
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionState

class GcpKingdomRelationalDatabase(private val client: DatabaseClient) : KingdomRelationalDatabase {

  override suspend fun writeNewRequisition(requisition: Requisition): Requisition =
    client.runReadWriteTransaction { transactionContext ->
      CreateRequisitionTransaction().execute(transactionContext, requisition)
    } ?: requisition

  override suspend fun fulfillRequisition(externalRequisitionId: ExternalId): Requisition =
    client.runReadWriteTransaction { transactionContext ->
      FulfillRequisitionTransaction().execute(transactionContext, externalRequisitionId)
    }

  override suspend fun listRequisitions(
    externalCampaignId: ExternalId,
    states: Set<RequisitionState>,
    pagination: Pagination
  ): KingdomRelationalDatabase.ListResult = ListRequisitionsQuery().execute(
    client.singleUse(),
    externalCampaignId,
    states,
    pagination
  )
}
