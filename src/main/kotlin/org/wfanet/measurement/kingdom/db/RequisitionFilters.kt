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

import java.time.Instant
import org.wfanet.measurement.common.AllOfClause
import org.wfanet.measurement.common.TerminalClause
import org.wfanet.measurement.common.allOf
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState

/** Filter type for Requisitions. */
typealias StreamRequisitionsFilter = AllOfClause<StreamRequisitionsClause>

/**
 * Creates a filter for Requisitions.
 *
 * The list inputs are treated as disjunctions, and all non-null inputs are conjoined.
 *
 * For example,
 *
 * streamRequisitionsFilter(externalDataProviderIds = listOf(ID1, ID2), createdAfter = SOME_TIME)
 *
 * would match each Requisition that matches both these criteria:
 * - it is associated with either ID1 or ID2, and
 * - it was created after SOME_TIME.
 *
 * @param externalDataProviderIds a list of Data Providers
 * @param externalCampaignIds a list of Campaigns
 * @param states a list of [RequisitionState]s
 * @param createdAfter a time after which Requisitions must be created
 */
fun streamRequisitionsFilter(
  externalDataProviderIds: List<ExternalId>? = null,
  externalCampaignIds: List<ExternalId>? = null,
  states: List<RequisitionState>? = null,
  createdAfter: Instant? = null
): StreamRequisitionsFilter {
  return allOf(
    listOfNotNull(
      externalDataProviderIds.ifNotNullOrEmpty(StreamRequisitionsClause::ExternalDataProviderId),
      externalCampaignIds.ifNotNullOrEmpty(StreamRequisitionsClause::ExternalCampaignId),
      states.ifNotNullOrEmpty(StreamRequisitionsClause::State),
      createdAfter?.let(StreamRequisitionsClause::CreatedAfter)
    )
  )
}

/** Base class for Requisition filters. Never directly instantiated. */
sealed class StreamRequisitionsClause : TerminalClause {
  /** Matching Requisitions must belong to a Data Provider with an external id in [values]. */
  data class ExternalDataProviderId internal constructor(val values: List<ExternalId>) :
    StreamRequisitionsClause()

  /** Matching Requisitions must belong to a Campaign with an external id in [values]. */
  data class ExternalCampaignId internal constructor(val values: List<ExternalId>) :
    StreamRequisitionsClause()

  /** Matching Requisitions must have a state among those in [values]. */
  data class State internal constructor(val values: List<RequisitionState>) :
    StreamRequisitionsClause()

  /** Matching Requisitions must have been created after [value]. */
  data class CreatedAfter internal constructor(val value: Instant) : StreamRequisitionsClause()
}
