package org.wfanet.measurement.kingdom

import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.Pagination
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
   * Output type of [listRequisitions].
   *
   * @property[requisitions] the results -- possibly empty
   * @property[nextPageToken] the token to get the next page, or null if there are no more pages
   */
  data class ListResult(val requisitions: List<Requisition>, val nextPageToken: String?)

  /**
   * Reads pages of [Requisition]s.
   *
   * @param[campaignExternalKey] the [Campaign] to which the [Requisition]s belong
   * @param[states] a filter: only results with a state in this set will be returned
   * @param[pagination] which page of results to return
   * @return a page of results (possibly empty)
   */
  suspend fun listRequisitions(
    campaignExternalKey: CampaignExternalKey,
    states: Set<RequisitionState>,
    pagination: Pagination
  ): ListResult
}
