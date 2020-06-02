package org.wfanet.measurement.db.kingdom

import java.time.Instant
import org.wfanet.measurement.common.AllOfClause
import org.wfanet.measurement.common.AnyOfClause
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.GreaterThanClause
import org.wfanet.measurement.common.TerminalClause
import org.wfanet.measurement.common.allOf
import org.wfanet.measurement.internal.kingdom.RequisitionState

typealias StreamRequisitionsFilter = AllOfClause<StreamRequisitionsClause>

sealed class StreamRequisitionsClause : TerminalClause {
  data class ExternalDataProviderId(val values: List<ExternalId>) :
    StreamRequisitionsClause(), AnyOfClause

  data class ExternalCampaignId(val values: List<ExternalId>) :
    StreamRequisitionsClause(), AnyOfClause

  class State(val values: List<RequisitionState>) :
    StreamRequisitionsClause(), AnyOfClause

  class CreatedAfter(val value: Instant) :
    StreamRequisitionsClause(), GreaterThanClause
}

fun streamRequisitionsFilter(vararg clauses: StreamRequisitionsClause): StreamRequisitionsFilter =
  allOf(clauses.asIterable())
