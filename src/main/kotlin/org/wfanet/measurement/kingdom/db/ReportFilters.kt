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
import org.wfanet.measurement.internal.kingdom.Report.ReportState

/** Filter type for Reports. */
typealias StreamReportsFilter = AllOfClause<StreamReportsClause>

/**
 * Creates a filter for Reports.
 *
 * The list inputs are treated as disjunctions, and all non-null inputs are conjoined.
 *
 * For example,
 *
 * streamReportsFilter(externalScheduleIds = listOf(ID1, ID2), createdAfter = SOME_TIME)
 *
 * would match each Report that matches both these criteria:
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

/** Base class for filtering Report streams. Never directly instantiated. */
sealed class StreamReportsClause : TerminalClause {

  /** Matching Reports must belong to an Advertiser with an external id in [values]. */
  data class ExternalAdvertiserId internal constructor(val values: List<ExternalId>) :
    StreamReportsClause()

  /** Matching Reports must belong to a ReportConfig with an external id in [values]. */
  data class ExternalReportConfigId internal constructor(val values: List<ExternalId>) :
    StreamReportsClause()

  /** Matching Reports must belong to ReportConfigSchedule with an external id in [values]. */
  data class ExternalScheduleId internal constructor(val values: List<ExternalId>) :
    StreamReportsClause()

  /** Matching Reports must have a state among those in [values]. */
  data class State internal constructor(val values: List<ReportState>) : StreamReportsClause()

  /** Matching Reports must have been updated after [value]. */
  data class UpdatedAfter internal constructor(val value: Instant) : StreamReportsClause()
}

/** Returns whether the filter acts on a Report's state. This is useful for forcing indexes. */
fun StreamReportsFilter.hasStateFilter(): Boolean {
  return clauses.any { it is StreamReportsClause.State }
}
