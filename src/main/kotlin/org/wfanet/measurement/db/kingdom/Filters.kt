package org.wfanet.measurement.db.kingdom

import java.time.Instant
import org.wfanet.measurement.common.AllOfClause
import org.wfanet.measurement.common.AnyOfClause
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.GreaterThanClause
import org.wfanet.measurement.common.TerminalClause
import org.wfanet.measurement.common.allOf
import org.wfanet.measurement.db.kingdom.StreamRequisitionsClause.CreatedAfter
import org.wfanet.measurement.db.kingdom.StreamRequisitionsClause.ExternalCampaignId
import org.wfanet.measurement.db.kingdom.StreamRequisitionsClause.ExternalDataProviderId
import org.wfanet.measurement.db.kingdom.StreamRequisitionsClause.State
import org.wfanet.measurement.internal.kingdom.RequisitionState

typealias StreamRequisitionsFilter = AllOfClause<StreamRequisitionsClause>

sealed class StreamRequisitionsClause : TerminalClause {
  data class ExternalDataProviderId internal constructor (val values: List<ExternalId>) :
    StreamRequisitionsClause(), AnyOfClause

  data class ExternalCampaignId internal constructor(val values: List<ExternalId>) :
    StreamRequisitionsClause(), AnyOfClause

  data class State internal constructor(val values: List<RequisitionState>) :
    StreamRequisitionsClause(), AnyOfClause

  data class CreatedAfter internal constructor(val value: Instant) :
    StreamRequisitionsClause(), GreaterThanClause
}

fun streamRequisitionsFilter(
  externalDataProviderIds: List<ExternalId>? = null,
  externalCampaignIds: List<ExternalId>? = null,
  states: List<RequisitionState>? = null,
  createdAfter: Instant? = null
): StreamRequisitionsFilter =
  allOf(
    listOfNotNull(
      externalDataProviderIds.ifNotNullOrEmpty(::ExternalDataProviderId),
      externalCampaignIds.ifNotNullOrEmpty(::ExternalCampaignId),
      states.ifNotNullOrEmpty(::State),
      createdAfter?.let(::CreatedAfter)
    )
  )

internal fun <T, V> List<T>?.ifNotNullOrEmpty(block: (List<T>) -> V): V? =
  this?.ifEmpty { null }?.let(block)
