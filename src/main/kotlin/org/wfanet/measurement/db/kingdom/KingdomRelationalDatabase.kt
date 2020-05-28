package org.wfanet.measurement.db.kingdom

import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.Pagination
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionState

/**
 * Wrapper interface for the Kingdom's relational database.
 */
interface KingdomRelationalDatabase {
  /**
   * Persists a [Requisition] in the database.
   *
   * If an equivalent [Requisition] already exists, this will return that instead.
   *
   * @param[requisition] the Requisition to save
   * @return the [Requisition] in the database -- old or new
   */
  suspend fun writeNewRequisition(requisition: Requisition): Requisition

  /**
   * Updates the state of a [Requisition] to [RequisitionState.FULFILLED].
   */
  suspend fun fulfillRequisition(externalRequisitionId: ExternalId): Requisition

  /**
   * Result type of [listRequisitions].
   */
  data class ListResult(val requisitions: List<Requisition>, val nextPageToken: String?)

  /**
   * Loads pages of [Requisition]s for a Campaign.
   */
  suspend fun listRequisitions(
    externalCampaignId: ExternalId,
    states: Set<RequisitionState>,
    pagination: Pagination
  ): ListResult
}
