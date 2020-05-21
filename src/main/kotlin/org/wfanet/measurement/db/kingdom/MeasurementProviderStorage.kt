package org.wfanet.measurement.db.kingdom

import java.time.Clock
import java.util.Random
import kotlin.math.abs
import org.wfanet.measurement.common.Pagination
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionState

abstract class MeasurementProviderStorage(private val clock: Clock) {

  /**
   * Persists a [Requisition] in the database.
   *
   * The input [Requisition] must not have a requisition_id or external_requisition_id.
   * Furthermore, the state must be UNFULFILLED.
   *
   * @param[requisition] the Requisition to save
   * @return the new [Requisition] with ids, or an equivalent already existing one.
   */
  suspend fun createRequisition(requisition: Requisition): Requisition {
    require(requisition.requisitionId == 0L) {
      "Cannot create a Requisition with a set requisitionId: $requisition"
    }
    require(requisition.externalRequisitionId == 0L) {
      "Cannot create a Requisition with a set externalRequisitionId: $requisition"
    }
    require(requisition.state == RequisitionState.UNFULFILLED) {
      "Initial requisitions must be unfulfilled: $requisition"
    }

    val requisitionWithId: Requisition = requisition
      .toBuilder()
      .setRequisitionId(generateId())
      .setExternalRequisitionId(generateId())
      .build()

    return writeNewRequisition(requisitionWithId)
  }

  /**
   * Persists a [Requisition] in the database.
   *
   * @param[requisition] the Requisition to save
   * @return the [Requisition] in the database -- old or new
   */
  protected abstract suspend fun writeNewRequisition(requisition: Requisition): Requisition

  abstract suspend fun fulfillRequisition(
    requisitionExternalKey: RequisitionExternalKey
  ): Requisition

  abstract suspend fun listRequisitions(
    campaignExternalKey: CampaignExternalKey,
    states: Set<RequisitionState>,
    pagination: Pagination
  ): List<Requisition>

  private fun generateId(): Long =
    abs((Random().nextLong() shl 32) or (clock.millis() and 0xFFFFFFFF))
}
