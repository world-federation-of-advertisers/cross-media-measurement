// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.db

import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState
import org.wfanet.measurement.internal.kingdom.RequisitionDetails

/** Abstraction around the Kingdom's relational database for storing [Requisition] metadata. */
interface RequisitionDatabase {
  /**
   * Persists a [Requisition] in the database.
   *
   * If an equivalent [Requisition] already exists, this will return that instead.
   *
   * @param requisition the Requisition to save
   * @return the [Requisition] in the database -- old or new
   */
  suspend fun createRequisition(requisition: Requisition): Requisition

  /**
   * Returns the [Requisition] with the given ID from the database, or null if none can be found
   * with that ID.
   */
  suspend fun getRequisition(externalRequisitionId: ExternalId): Requisition?

  /**
   * Transitions the state of a [Requisition] to [RequisitionState.FULFILLED] if its current state
   * is [RequisitionState.UNFULFILLED].
   */
  suspend fun fulfillRequisition(
    externalRequisitionId: ExternalId,
    duchyId: String
  ): RequisitionUpdate

  /**
   * Transitions the state of a [Requisition] to [RequisitionState.PERMANENTLY_UNAVAILABLE] if its
   * current state is [RequisitionState.UNFULFILLED], setting [requisition_details.refusal]
   * [RequisitionDetails.getRefusal].
   */
  suspend fun refuseRequisition(
    externalRequisitionId: ExternalId,
    refusal: RequisitionDetails.Refusal
  ): RequisitionUpdate

  /** Streams [Requisition]s. */
  fun streamRequisitions(filter: StreamRequisitionsFilter, limit: Long): Flow<Requisition>
}
