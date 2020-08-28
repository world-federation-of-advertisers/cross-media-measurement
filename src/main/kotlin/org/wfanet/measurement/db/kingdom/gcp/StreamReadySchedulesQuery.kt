// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.ReadContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.db.gcp.appendClause
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule

/**
 * Query for finding [ReportConfigSchedule]s with nextReportStartTimes in the past.
 */
class StreamReadySchedulesQuery {
  /**
   * Streams [ReportConfigSchedule]s with nextReportStartTimes in the past.
   *
   * @param readContext the context in which to perform Spanner reads
   * @param limit how many results to return -- if zero, there is no limit
   * @return a [Flow] of [ReportConfigSchedule]s in an arbitrary order
   */
  fun execute(
    readContext: ReadContext,
    limit: Long
  ): Flow<ReportConfigSchedule> {
    return ScheduleReader()
      .withBuilder {
        appendClause("WHERE ReportConfigSchedules.NextReportStartTime < CURRENT_TIMESTAMP()")

        if (limit > 0) {
          appendClause("LIMIT @limit")
          bind("limit").to(limit)
        }
      }
      .execute(readContext)
      .map { it.schedule }
  }
}
