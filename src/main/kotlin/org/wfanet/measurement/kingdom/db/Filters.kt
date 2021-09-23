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
import org.wfanet.measurement.common.LessThanClause
import org.wfanet.measurement.common.TerminalClause
import org.wfanet.measurement.common.allOf
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.internal.kingdom.RecurringExchange

typealias StreamRecurringExchangesFilter = AllOfClause<StreamRecurringExchangesClause>

typealias GetExchangeStepFilter = AllOfClause<GetExchangeStepClause>

/**
 * Creates a filter for RecurringExchanges.
 *
 * The list inputs are treated as disjunctions, and all non-null inputs are conjoined.
 *
 * For example,
 *
 * streamRecurringExchangesFilter(externalModelProviderIds = listOf(ID1, ID2),
 * nextExchangeDateBefore = SOME_DATE)
 *
 * would match each RecurringExchange that matches both these criteria:
 * - it is associated with a ModelProvider with external id either ID1 or ID2, and
 * - it was associated with exchanges created before SOME_DATE.
 *
 * @param externalModelProviderIds a list of Model Provider IDs
 * @param externalDataProviderIds a list of Data Provider IDs
 * @param states a list of [RecurringExchange.State]s
 * @param nextExchangeDateBefore a time before next exchange date scheduled
 */
fun streamRecurringExchangesFilter(
  externalModelProviderIds: List<ExternalId>? = null,
  externalDataProviderIds: List<ExternalId>? = null,
  states: List<RecurringExchange.State>? = null,
  nextExchangeDateBefore: Date? = null
): StreamRecurringExchangesFilter =
  allOf(
    listOfNotNull(
      externalModelProviderIds.ifNotNullOrEmpty(
        StreamRecurringExchangesClause::ExternalModelProviderId
      ),
      externalDataProviderIds.ifNotNullOrEmpty(
        StreamRecurringExchangesClause::ExternalDataProviderId
      ),
      states.ifNotNullOrEmpty(StreamRecurringExchangesClause::State),
      nextExchangeDateBefore.ifNotNull(StreamRecurringExchangesClause::NextExchangeDateBefore)
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
 * @param externalModelProviderIds disjunction of Model Provider IDs
 * @param externalDataProviderIds disjunction of Data Provider IDs
 * @param recurringExchangeId a [RecurringExchange] id
 * @param date a Date
 * @param stepIndex a StepIndex of the ExchangeStep
 * @param states list of [ExchangeStep.State]s
 */
fun getExchangeStepFilter(
  externalModelProviderIds: List<ExternalId>? = null,
  externalDataProviderIds: List<ExternalId>? = null,
  recurringExchangeId: Long? = null,
  date: Date? = null,
  stepIndex: Long? = null,
  states: List<ExchangeStep.State>? = null
): GetExchangeStepFilter =
  allOf(
    listOfNotNull(
      externalModelProviderIds.ifNotNullOrEmpty(GetExchangeStepClause::ExternalModelProviderId),
      externalDataProviderIds.ifNotNullOrEmpty(GetExchangeStepClause::ExternalDataProviderId),
      recurringExchangeId.ifNotNull(GetExchangeStepClause::RecurringExchangeId),
      date.ifNotNull(GetExchangeStepClause::Date),
      stepIndex.ifNotNull(GetExchangeStepClause::StepIndex),
      states.ifNotNullOrEmpty(GetExchangeStepClause::State)
    )
  )

/** Base class for filtering Exchange streams. Never directly instantiated. */
sealed class StreamRecurringExchangesClause : TerminalClause {

  /**
   * Matching RecurringExchanges must belong to an ModelProvider with an external id in [values].
   */
  data class ExternalModelProviderId internal constructor(val values: List<ExternalId>) :
    StreamRecurringExchangesClause(), AnyOfClause

  /** Matching RecurringExchanges must belong to an DataProvider with an external id in [values]. */
  data class ExternalDataProviderId internal constructor(val values: List<ExternalId>) :
    StreamRecurringExchangesClause(), AnyOfClause

  /** Matching RecurringExchanges must have a state among those in [values]. */
  data class State internal constructor(val values: List<RecurringExchange.State>) :
    StreamRecurringExchangesClause(), AnyOfClause

  /** Matching RecurringExchanges must have [NextExchangeDateBefore] before [value]. */
  data class NextExchangeDateBefore internal constructor(val value: Date) :
    StreamRecurringExchangesClause(), LessThanClause
}

/**
 * Returns whether the filter acts on a DataProviderId of the RecurringExchange. This is useful for
 * forcing indexes.
 */
fun StreamRecurringExchangesFilter.hasDataProviderFilter(): Boolean {
  return clauses.any { it is StreamRecurringExchangesClause.ExternalDataProviderId }
}

/**
 * Returns whether the filter acts on a ModelProviderId of the RecurringExchange. This is useful for
 * forcing indexes.
 */
fun StreamRecurringExchangesFilter.hasModelProviderFilter(): Boolean {
  return clauses.any { it is StreamRecurringExchangesClause.ExternalModelProviderId }
}

// TODO(@yunyeng): Move EqualCase to common-jvm.
interface EqualClause : TerminalClause

/** Base class for filtering to get an Exchange Step. Never directly instantiated. */
sealed class GetExchangeStepClause : TerminalClause {
  /** Matching ExchangeStep must belong to a ModelProvider with an external id of [values]. */
  data class ExternalModelProviderId internal constructor(val values: List<ExternalId>) :
    GetExchangeStepClause(), AnyOfClause

  /** Matching ExchangeStep must belong to a DataProvider with an external id of [values]. */
  data class ExternalDataProviderId internal constructor(val values: List<ExternalId>) :
    GetExchangeStepClause(), AnyOfClause

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
