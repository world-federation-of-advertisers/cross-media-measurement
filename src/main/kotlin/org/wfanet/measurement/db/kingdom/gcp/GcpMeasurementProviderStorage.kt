package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.Struct
import java.time.Clock
import org.wfanet.measurement.common.Pagination
import org.wfanet.measurement.db.gcp.runReadWriteTransaction
import org.wfanet.measurement.db.kingdom.CampaignExternalKey
import org.wfanet.measurement.db.kingdom.MeasurementProviderStorage
import org.wfanet.measurement.db.kingdom.RequisitionExternalKey
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionDetails
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
  ): Requisition {
    TODO("Not yet implemented")
  }

  override suspend fun listRequisitions(
    campaignExternalKey: CampaignExternalKey,
    states: Set<RequisitionState>,
    pagination: Pagination
  ): List<Requisition> {
    TODO("Not yet implemented")
  }
}

fun Struct.getByteArray(column: String): ByteArray = getBytes(column).toByteArray()

fun Struct.toRequisition(): Requisition {
  return Requisition.newBuilder().apply {
    dataProviderId = getLong("DataProviderId")
    campaignId = getLong("CampaignId")
    requisitionId = getLong("RequisitionId")

    externalDataProviderId = getLong("ExternalDataProviderId")
    externalCampaignId = getLong("ExternalCampaignId")
    externalRequisitionId = getLong("ExternalRequisitionId")

    windowStartTime = getTimestamp("WindowStartTime").toProto()
    windowEndTime = getTimestamp("WindowEndTime").toProto()

    state = RequisitionState.forNumber(getLong("State").toInt())
    requisitionDetails = RequisitionDetails.parseFrom(getByteArray("RequisitionDetails"))
    requisitionDetailsJson = getString("RequisitionDetailsJson")
  }.build()
}
