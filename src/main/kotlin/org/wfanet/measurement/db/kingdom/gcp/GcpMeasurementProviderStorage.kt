package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.DatabaseClient
import java.time.Clock
import org.wfanet.measurement.common.Pagination
import org.wfanet.measurement.db.gcp.runReadWriteTransaction
import org.wfanet.measurement.db.kingdom.CampaignExternalKey
import org.wfanet.measurement.db.kingdom.MeasurementProviderStorage
import org.wfanet.measurement.db.kingdom.RequisitionExternalKey
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionState

class GcpMeasurementProviderStorage(
  private val client: DatabaseClient,
  clock: Clock
) : MeasurementProviderStorage(clock) {

  override suspend fun writeNewRequisition(requisition: Requisition): Requisition =
    client.runReadWriteTransaction { transactionContext ->
      CreateRequisitionTransaction().execute(transactionContext, requisition)
    } ?: requisition

  override suspend fun fulfillRequisition(
    requisitionExternalKey: RequisitionExternalKey
  ): Requisition =
    client.runReadWriteTransaction { transactionContext ->
      FulfillRequisitionTransaction().execute(transactionContext, requisitionExternalKey.externalId)
    }

  override suspend fun listRequisitions(
    campaignExternalKey: CampaignExternalKey,
    states: Set<RequisitionState>,
    pagination: Pagination
  ): List<Requisition> {
    TODO("Not yet implemented")
  }
}
