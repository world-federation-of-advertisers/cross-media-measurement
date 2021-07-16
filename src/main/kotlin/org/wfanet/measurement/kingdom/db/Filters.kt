// Copyright 2020 The Cross-Media Measurement Authors
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

import com.google.type.Date
import java.time.Instant
import org.wfanet.measurement.common.AllOfClause
import org.wfanet.measurement.common.AnyOfClause
import org.wfanet.measurement.common.GreaterThanClause
import org.wfanet.measurement.common.LessThanClause
import org.wfanet.measurement.common.TerminalClause
import org.wfanet.measurement.common.allOf
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState

typealias StreamRequisitionsFilter = AllOfClause<StreamRequisitionsClause>

typealias StreamReportsFilter = AllOfClause<StreamReportsClause>

typealias StreamExchangesFilter = AllOfClause<StreamExchangesClause>

typealias GetExchangeStepFilter = AllOfClause<GetExchangeStepClause>

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
): StreamRequisitionsFilter =
  allOf(
    listOfNotNull(
      externalDataProviderIds.ifNotNullOrEmpty(StreamRequisitionsClause::ExternalDataProviderId),
      externalCampaignIds.ifNotNullOrEmpty(StreamRequisitionsClause::ExternalCampaignId),
      states.ifNotNullOrEmpty(StreamRequisitionsClause::State),
      createdAfter?.let(StreamRequisitionsClause::CreatedAfter)
    )
  )

/**
 * Creates a filter for Reports.
 *
 * The list inputs are treated as disjunctions, and all non-null inputs are conjoined.
 *
 * For example,
 *
 * streamReportsFilter(externalScheduleIds = listOf(ID1, ID2), createdAfter = SOME_TIME)
 *
 * would match each Report that matches both asdasthese criteria:
 * - it is associated with a schedule with external id either ID1 or ID2, and
 * - it was created after SOME_TIME.
 *
 * @param externalAdvertiserIds a list of Advertisers
 * @param externalReportConfigIds a list of Report Configs
 * @param externalScheduleIds a list of ReportConfigSchedules
 * @param states a list of [ReportState]s
 * @param updatedAfter a time after which results must be created
 */
fun streamReportsFilter(
  externalAdvertiserIds: List<ExternalId>? = null,
  externalReportConfigIds: List<ExternalId>? = null,
  externalScheduleIds: List<ExternalId>? = null,
  states: List<ReportState>? = null,
  updatedAfter: Instant? = null
): StreamReportsFilter =
  allOf(
    listOfNotNull(
      externalAdvertiserIds.ifNotNullOrEmpty(StreamReportsClause::ExternalAdvertiserId),
      externalReportConfigIds.ifNotNullOrEmpty(StreamReportsClause::ExternalReportConfigId),
      externalScheduleIds.ifNotNullOrEmpty(StreamReportsClause::ExternalScheduleId),
      states.ifNotNullOrEmpty(StreamReportsClause::State),
      updatedAfter.ifNotNullOrEpoch(StreamReportsClause::UpdatedAfter)
    )
  )

/**
 * Creates a filter for Exchanges.
 *
 * The list inputs are treated as disjunctions, and all non-null inputs are conjoined.
 *
 * For example,
 *
 * streamExchangesFilter(externalModelProviderIds = listOf(ID1, ID2), nextExchangeDate = SOME_DATE)
 *
 * would match each Exchange that matches both these criteria:
 * - it is associated with a ModelProvider with external id either ID1 or ID2, and
 * - it was associated with exchanges created before SOME_DATE.
 *
 * @param externalModelProviderIds a list of Model Providers
 * @param externalDataProviderIds a list of Data Providers
 * @param states a list of [RecurringExchange.State]s
 * @param nextExchangeDate a time before next exchange date scheduled
 */
fun streamExchangesFilter(
  externalModelProviderIds: List<ExternalId>? = null,
  externalDataProviderIds: List<ExternalId>? = null,
  states: List<RecurringExchange.State>? = null,
  nextExchangeDate: Date? = null
): StreamExchangesFilter =
  allOf(
    listOfNotNull(
      externalModelProviderIds.ifNotNullOrEmpty(StreamExchangesClause::ExternalModelProviderId),
      externalDataProviderIds.ifNotNullOrEmpty(StreamExchangesClause::ExternalDataProviderId),
      states.ifNotNullOrEmpty(StreamExchangesClause::State),
      nextExchangeDate.ifNotNull(StreamExchangesClause::NextExchangeDate)
    )
  )

/**
 * Creates a filter for an Exchange Step.
 *
 * The inputs are treated as disjunctions, and all non-null inputs are conjoined. All the parameters
 * other than [states] are singular.
 *
 * For example,
 *
 * getExchangeStepFilter(externalModelProviderId = ID1, date = SOME_DATE)
 *
 * would match an Exchange Step that matches both these criteria:
 * - it is associated with a ModelProvider with external id equal to ID1, and
 * - it is associated with a date equal to SOME_DATE.
 *
 * @param externalModelProviderId a Model Provider id
 * @param externalDataProviderId a Data Provider id
 * @param recurringExchangeId a [RecurringExchange] id
 * @param date a Date
 * @param stepIndex a StepIndex of the ExchangeStep
 * @param states list of [ExchangeStep.State]s
 */
fun getExchangeStepFilter(
  externalModelProviderId: ExternalId? = null,
  externalDataProviderId: ExternalId? = null,
  recurringExchangeId: Long? = null,
  date: Date? = null,
  stepIndex: Long? = null,
  states: List<ExchangeStep.State>? = null
): GetExchangeStepFilter =
  allOf(
    listOfNotNull(
      externalModelProviderId.ifNotNull(GetExchangeStepClause::ExternalModelProviderId),
      externalDataProviderId.ifNotNull(GetExchangeStepClause::ExternalDataProviderId),
      recurringExchangeId.ifNotNull(GetExchangeStepClause::RecurringExchangeId),
      date.ifNotNull(GetExchangeStepClause::Date),
      stepIndex.ifNotNull(GetExchangeStepClause::StepIndex),
      states.ifNotNullOrEmpty(GetExchangeStepClause::State)
    )
  )

/** Base class for Requisition filters. Never directly instantiated. */
sealed class StreamRequisitionsClause : TerminalClause {

  /** Matching Requisitions must belong to a Data Provider with an external id in [values]. */
  data class ExternalDataProviderId internal constructor(val values: List<ExternalId>) :
    StreamRequisitionsClause(), AnyOfClause

  /** Matching Requisitions must belong to a Campaign with an external id in [values]. */
  data class ExternalCampaignId internal constructor(val values: List<ExternalId>) :
    StreamRequisitionsClause(), AnyOfClause

  /** Matching Requisitions must have a state among those in [values]. */
  data class State internal constructor(val values: List<RequisitionState>) :
    StreamRequisitionsClause(), AnyOfClause

  /** Matching Requisitions must have been created after [value]. */
  data class CreatedAfter internal constructor(val value: Instant) :
    StreamRequisitionsClause(), GreaterThanClause
}

/** Base class for filtering Report streams. Never directly instantiated. */
sealed class StreamReportsClause : TerminalClause {

  /** Matching Reports must belong to an Advertiser with an external id in [values]. */
  data class ExternalAdvertiserId internal constructor(val values: List<ExternalId>) :
    StreamReportsClause(), AnyOfClause

  /** Matching Reports must belong to a ReportConfig with an external id in [values]. */
  data class ExternalReportConfigId internal constructor(val values: List<ExternalId>) :
    StreamReportsClause(), AnyOfClause

  /** Matching Reports must belong to ReportConfigSchedule with an external id in [values]. */
  data class ExternalScheduleId internal constructor(val values: List<ExternalId>) :
    StreamReportsClause(), AnyOfClause

  /** Matching Reports must have a state among those in [values]. */
  data class State internal constructor(val values: List<ReportState>) :
    StreamReportsClause(), AnyOfClause

  /** Matching Reports must have been updated after [value]. */
  data class UpdatedAfter internal constructor(val value: Instant) :
    StreamReportsClause(), GreaterThanClause
}

/** Returns whether the filter acts on a Report's state. This is useful for forcing indexes. */
fun StreamReportsFilter.hasStateFilter(): Boolean {
  return clauses.any { it is StreamReportsClause.State }
}

/** Base class for filtering Exchange streams. Never directly instantiated. */
sealed class StreamExchangesClause : TerminalClause {

  /**
   * Matching RecurringExchanges must belong to an ModelProvider with an external id in [values].
   */
  data class ExternalModelProviderId internal constructor(val values: List<ExternalId>) :
    StreamExchangesClause(), AnyOfClause

  /** Matching RecurringExchanges must belong to an DataProvider with an external id in [values]. */
  data class ExternalDataProviderId internal constructor(val values: List<ExternalId>) :
    StreamExchangesClause(), AnyOfClause

  /** Matching RecurringExchanges must have a state among those in [values]. */
  data class State internal constructor(val values: List<RecurringExchange.State>) :
    StreamExchangesClause(), AnyOfClause

  /** Matching RecurringExchanges must have [NextExchangeDate] before [value]. */
  data class NextExchangeDate internal constructor(val value: Date) :
    StreamExchangesClause(), LessThanClause
}

/**
 * Returns whether the filter acts on a DataProviderId of the Exchange. This is useful for forcing
 * indexes.
 */
fun StreamExchangesFilter.hasDataProviderFilter(): Boolean {
  return clauses.any { it is StreamExchangesClause.ExternalDataProviderId }
}

/**
 * Returns whether the filter acts on a ModelProviderId of the Exchange. This is useful for forcing
 * indexes.
 */
fun StreamExchangesFilter.hasModelProviderFilter(): Boolean {
  return clauses.any { it is StreamExchangesClause.ExternalModelProviderId }
}

// TODO(@yunyeng): Move EqualCase to common-jvm.
interface EqualClause : TerminalClause

/** Base class for filtering to get an Exchange Step. Never directly instantiated. */
sealed class GetExchangeStepClause : TerminalClause {
  /** Matching ExchangeStep must belong to a ModelProvider with an external id of [value]. */
  data class ExternalModelProviderId internal constructor(val value: ExternalId) :
    GetExchangeStepClause(), EqualClause

  /** Matching ExchangeStep must belong to a DataProvider with an external id of [value]. */
  data class ExternalDataProviderId internal constructor(val value: ExternalId) :
    GetExchangeStepClause(), EqualClause

  /** Matching ExchangeStep must belong to a RecurringExchange with an external id of [value]. */
  data class RecurringExchangeId internal constructor(val value: Long) :
    GetExchangeStepClause(), EqualClause

  /** Matching ExchangeStep must have a [Date] equal to [value]. */
  data class Date internal constructor(val value: com.google.type.Date) :
    GetExchangeStepClause(), EqualClause

  /** Matching ExchangeStep must have a StepIndex equal to [value]. */
  data class StepIndex internal constructor(val value: Long) : GetExchangeStepClause(), EqualClause

  /** Matching ExchangeStep must have [ExchangeStep.State] among any of those in [values]. */
  data class State internal constructor(val values: List<ExchangeStep.State>) :
    GetExchangeStepClause(), AnyOfClause
}

private fun <T, V> List<T>?.ifNotNullOrEmpty(block: (List<T>) -> V): V? =
  this?.ifEmpty { null }?.let(block)

private fun <V> Instant?.ifNotNullOrEpoch(block: (Instant) -> V): V? =
  this?.let { if (it == Instant.EPOCH) null else block(it) }

private fun <V> ExternalId?.ifNotNull(block: (ExternalId) -> V): V? = this?.let { block(it) }

private fun <V> Long?.ifNotNull(block: (Long) -> V): V? = this?.let { block(it) }

private fun <V> Date?.ifNotNull(block: (Date) -> V): V? =
  this?.let { if (!it.isInitialized) null else block(it) }
