package org.wfanet.measurement.kingdom

import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.kingdom.StreamRequisitionsFilter
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionState

/**
 * Interface for high-level Kingdom operations on Requisitions.
 */
interface RequisitionManager {
  /**
   * Persists a new [Requisition].
   *
   * The input [Requisition] must not have a requisition_id or external_requisition_id.
   * Furthermore, the state must be UNFULFILLED.
   *
   * @param[requisition] the Requisition to save
   * @return the new [Requisition] with ids, or an equivalent already existing one.
   */
  suspend fun createRequisition(requisition: Requisition): Requisition

  /**
   * Updates the state of a [Requisition] to [RequisitionState.FULFILLED].
   *
   * @param[externalRequisitionId]: the identifiers of the Requisition
   * @return the resulting [Requisition]
   */
  suspend fun fulfillRequisition(externalRequisitionId: ExternalId): Requisition

  /**
   * Reads [Requisition]s.
   *
   * @param[filter] a filter for which requisitions to read
   * @param[limit] maximum number of requisitions to read
   * @return the Requisitions
   */
  fun streamRequisitions(
    filter: StreamRequisitionsFilter,
    limit: Long
  ): Flow<Requisition>
}
